use std::{
    collections::{HashMap, VecDeque},
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use anyhow::anyhow;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, RwLock, mpsc},
    time,
};

use crate::{
    conf::Conf,
    db::{aof::AofEntry, db::DB},
    server::proto::{HA_HEARTBEAT, HA_REGISTER, HA_SYNC, STATUS_OK},
};

pub struct HaConf {
    followers: Vec<String>,
}
impl HaConf {
    pub fn new() -> Self {
        HaConf {
            followers: Vec::new(),
        }
    }
}
pub struct HaManager {
    conf: Arc<Conf>,
    ha_conf: Arc<RwLock<HaConf>>,
    db: Arc<RwLock<DB>>,
    follower_timeout: Arc<Mutex<HashMap<String, u8>>>,
    rx: mpsc::UnboundedReceiver<AofEntry>,
    uncommitted_events: Vec<AofEntry>,
}
impl HaManager {
    pub fn new(
        conf: Arc<Conf>,
        ha_conf: Arc<RwLock<HaConf>>,
        db: Arc<RwLock<DB>>,
        rx: mpsc::UnboundedReceiver<AofEntry>,
    ) -> Self {
        return HaManager {
            conf,
            ha_conf,
            db,
            follower_timeout: Arc::new(Mutex::new(HashMap::new())),
            rx,
            uncommitted_events: Vec::new(),
        };
    }
    pub async fn start(&mut self) -> anyhow::Result<()> {
        let entry = match self.rx.try_recv() {
            Ok(entry) => entry,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => self
                .rx
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("aof event closed"))?,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                anyhow::bail!("aof event channel disconnected");
            }
        };
        self.uncommitted_events.push(entry);
        let replica_conf = self.conf.replica_conf.as_ref().unwrap();

        if self.uncommitted_events.len() >= replica_conf.event_buffer_size.unwrap_or(100) {
            let events_cp = self.uncommitted_events.clone();
            self.uncommitted_events = Vec::new();
            for follower in self.ha_conf.read().await.followers.clone() {
                sync_follower(&follower, &events_cp).await?;
            }
        }
        Ok(())
    }

    pub async fn heartbeat_followers(&mut self) {
        let ha_conf = Arc::clone(&self.ha_conf);
        let follower_timeout = Arc::clone(&self.follower_timeout);

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let followers = ha_conf.read().await.followers.clone();
                if followers.is_empty() {
                    continue;
                }
                for addr in followers {
                    // heartbeat each follower
                    match heartbeat_follower(addr.as_str()).await {
                        Ok(_) => return,

                        Err(_) => {
                            let mut map = follower_timeout.lock().await;
                            let count = map.entry(addr.to_string()).or_insert(0);
                            if *count >= 3 {
                                ha_conf
                                    .write()
                                    .await
                                    .followers
                                    .retain(|f| f.deref() != addr);
                                map.remove(&addr);
                            } else {
                                *count += 1;
                            }
                        }
                    }
                }
            }
        });
    }

    pub async fn register_master(&mut self, master: &str, addr: &str) -> anyhow::Result<()> {
        let mut stream = TcpStream::connect(master).await?;
        stream.set_nodelay(true)?;
        let mut frame = Vec::new();
        frame.push(HA_REGISTER);
        frame.extend_from_slice(&(addr.len() as u16).to_le_bytes());
        frame.extend_from_slice(addr.as_bytes());
        let resp = send_recv(&mut stream, &frame).await?;
        if resp[0] == STATUS_OK {
            Ok(())
        } else {
            Err(anyhow!(
                "failed to register to master with addr: {}",
                master
            ))
        }
    }
    pub async fn register_follower(&mut self, addr: &str) -> anyhow::Result<()> {
        // send a heartbeat message to address and if the request returned ack we add it to followers
        match heartbeat_follower(addr).await {
            Ok(_) => {
                self.ha_conf.write().await.followers.push(addr.to_string());
                // send a version of aof entries to new registered follower
                let aof_copy = self.db.read().await.get_aof_copy().await?;
                if !aof_copy.is_empty() {
                    // send aof copy to follower
                    match sync_follower(addr, &aof_copy).await {
                        Ok(_) => {
                            println!(
                                "{} synced successfully with {} entries",
                                addr,
                                aof_copy.len()
                            );
                        }
                        Err(err) => {
                            eprint!("failed to send aof copy to {}: {}", addr, err.to_string())
                        }
                    };
                }

                Ok(())
            }
            Err(err) => Err(anyhow!("failed to register follower: {}", err.to_string())),
        }
    }
    pub async fn ack_heartbeat(&mut self) -> anyhow::Result<()> {
        // this just return ok when called for heartbeat
        Ok(())
    }
}
async fn heartbeat_follower(addr: &str) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let mut frame = Vec::new();
    frame.push(HA_HEARTBEAT);
    let resp = send_recv(&mut stream, &frame).await?;
    if resp[0] == STATUS_OK {
        Ok(())
    } else {
        Err(anyhow!("heartbeat did not respond successfully"))
    }
}

pub async fn sync_follower(addr: &str, entries: &[AofEntry]) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;

    let encoded: Vec<String> = entries.iter().map(|e| e.to_base64()).collect();

    let mut frame = Vec::new();
    frame.push(HA_SYNC);
    frame.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    for s in &encoded {
        let b = s.as_bytes();
        frame.extend_from_slice(&(b.len() as u32).to_le_bytes());
        frame.extend_from_slice(b);
    }

    let resp = send_recv(&mut stream, &frame).await?;
    if resp[0] == STATUS_OK {
        Ok(())
    } else {
        Err(anyhow!("sync failed"))
    }
}

async fn send_recv(stream: &mut TcpStream, frame: &[u8]) -> anyhow::Result<Vec<u8>> {
    let body_len = frame.len() as u32;
    stream.write_all(&body_len.to_le_bytes()).await?;
    stream.write_all(frame).await?;
    stream.flush().await?;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let resp_len = u32::from_le_bytes(len_buf) as usize;
    let mut resp = vec![0u8; resp_len];
    stream.read_exact(&mut resp).await?;

    Ok(resp)
}
