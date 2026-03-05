use crate::codec::LengthPrefixCodec;
use crate::groups::{GroupError, GroupManager, GroupStats};
use crate::kv::KvStore;
use crate::proto::{self, Command, ParseError, ResponseBuilder};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::{net::TcpListener, sync::RwLock};
use tokio_util::codec::Framed;
pub struct Server {
    group_manager: Arc<RwLock<GroupManager>>,
    kv_store: Arc<RwLock<KvStore>>,
}

impl Server {
    pub async fn new() -> Self {
        let manager = Arc::new(RwLock::new(GroupManager::new().await));
        let kv_store = Arc::new(RwLock::new(KvStore::new().await));
        Server {
            group_manager: manager,
            kv_store: kv_store,
        }
    }
    pub async fn start(&self, addr: &str) -> anyhow::Result<()> {
        let ln = TcpListener::bind(addr).await?;
        loop {
            let (socket, peer) = ln.accept().await?;
            println!("New connection from {}", peer);
            socket.set_nodelay(true)?;
            // TODO: make handle command a implementation of server so we dont pass the group wrapper and kv store around for each command
            let manager = Arc::clone(&self.group_manager);
            let kv_store = Arc::clone(&self.kv_store);
            tokio::spawn(async move {
                let mut framed = Framed::new(socket, LengthPrefixCodec);
                let mut resp_builder = ResponseBuilder::new();
                while let Some(result) = framed.next().await {
                    let frame = match result {
                        Result::Ok(frame) => frame,
                        Result::Err(e) => {
                            eprintln!("Error reading frame: {}", e);
                            break;
                        }
                    };
                    let resp = match proto::parse_command(&frame) {
                        Result::<_, ParseError>::Ok(cmd) => {
                            handle_command(&manager, &kv_store, cmd, &mut resp_builder).await
                        }
                        Result::<_, ParseError>::Err(e) => {
                            let msg = format!("parse error: {}", e.0);
                            resp_builder.err(&msg).to_vec()
                        }
                    };
                    if let Err(e) = framed.send(resp).await {
                        eprintln!("[server] write error to {}: {}", peer, e);
                        break;
                    }
                }
                eprintln!("[server] connection closed: {}", peer);
            });
        }
    }
}

async fn handle_command(
    manager: &Arc<RwLock<GroupManager>>,
    kv_store: &Arc<RwLock<KvStore>>,
    cmd: Command<'_>,
    rb: &mut ResponseBuilder,
) -> Vec<u8> {
    match cmd {
        Command::CreateGroup { name } => {
            let mut mgr = manager.write().await;
            match mgr.create_group(name).await {
                Result::<(), GroupError>::Ok(()) => rb.ok_empty().to_vec(),
                Result::<(), GroupError>::Err(e) => rb.err(&e.to_string()).to_vec(),
            }
        }
        Command::DropGroup { name } => {
            let mut mgr = manager.write().await;
            match mgr.drop_group(name).await {
                Result::<(), GroupError>::Ok(()) => rb.ok_empty().to_vec(),
                Result::<(), GroupError>::Err(e) => rb.err(&e.to_string()).to_vec(),
            }
        }
        Command::Add {
            group,
            timestamp,
            payload,
        } => {
            let mut mgr = manager.write().await;
            match mgr.add(group, timestamp, payload).await {
                Result::<u64, GroupError>::Ok(id) => rb.ok_u64(id).to_vec(),
                Result::<u64, GroupError>::Err(e) => rb.err(&e.to_string()).to_vec(),
            }
        }
        Command::AddRange { group, entries } => {
            let mut mgr = manager.write().await;
            match mgr.add_range(group, &entries).await {
                Result::<(u64, u64), GroupError>::Ok((first, last)) => {
                    rb.ok_u64_pair(first, last).to_vec()
                }
                Result::<(u64, u64), GroupError>::Err(e) => rb.err(&e.to_string()).to_vec(),
            }
        }
        Command::Read { group, id } => {
            let mut mgr = manager.write().await;
            match mgr.read(group, id) {
                Result::<(u64, u64, Vec<u8>), GroupError>::Ok((entry_id, ts, payload)) => {
                    rb.ok_entry(entry_id, ts, &payload).to_vec()
                }
                Result::<(u64, u64, Vec<u8>), GroupError>::Err(e) => {
                    rb.err(&e.to_string()).to_vec()
                }
            }
        }
        Command::ReadRange { group, start, end } => {
            let mut mgr = manager.write().await;
            match mgr.read_range(group, start, end) {
                Result::<Vec<(u64, u64, Vec<u8>)>, GroupError>::Ok(entries) => {
                    rb.ok_entries(&entries).to_vec()
                }
                Result::<Vec<(u64, u64, Vec<u8>)>, GroupError>::Err(e) => {
                    rb.err(&e.to_string()).to_vec()
                }
            }
        }
        Command::Remove { group, up_to_id } => {
            let mut mgr = manager.write().await;
            match mgr.remove(group, up_to_id) {
                Result::<(), GroupError>::Ok(()) => rb.ok_empty().to_vec(),
                Result::<(), GroupError>::Err(e) => rb.err(&e.to_string()).to_vec(),
            }
        }
        Command::ListGroups => {
            let mgr = manager.read().await;
            let groups = mgr.list_groups();
            rb.ok_group_list(&groups).to_vec()
        }
        Command::GroupStats { group } => {
            let mgr = manager.read().await;
            match mgr.group_stats(group) {
                Result::<GroupStats, GroupError>::Ok(stats) => rb
                    .ok_stats(stats.total_entries, stats.total_segments, stats.next_id)
                    .to_vec(),
                Result::<GroupStats, GroupError>::Err(e) => rb.err(&e.to_string()).to_vec(),
            }
        }
        Command::SetKey {
            db,
            key,
            val,
            ttl_secs,
        } => {
            let mut kv = kv_store.write().await;
            match kv
                .set(
                    db.to_string(),
                    key.to_string(),
                    val.to_vec(),
                    (ttl_secs * 1000) as i64,
                )
                .await
            {
                Ok(()) => rb.ok_empty().to_vec(),
                Err(e) => rb.err(&e.to_string()).to_vec(),
            }
        }
        Command::GetKey { db, key } => {
            let mut kv = kv_store.write().await;
            match kv.get(db.to_string(), key.to_string()).await {
                Ok(Some(entry)) => rb.ok_bytes(&entry.val).to_vec(),
                Ok(None) => rb.ok_empty().to_vec(),
                Err(e) => rb.err(&e.to_string()).to_vec(),
            }
        }
        Command::DelKey { db, key } => {
            let mut kv = kv_store.write().await;
            match kv.del(db.to_string(), key.to_string()).await {
                Ok(()) => rb.ok_empty().to_vec(),
                Err(e) => rb.err(&e.to_string()).to_vec(),
            }
        }
        Command::Keys { db } => {
            let mut kv = kv_store.write().await;
            match kv.keys(db.to_string()).await {
                Ok(keys) => rb.ok_string_list(&keys).to_vec(),
                Err(e) => rb.err(&e.to_string()).to_vec(),
            }
        }
        Command::Flush { db } => {
            let mut kv = kv_store.write().await;
            match kv.flush(db.to_string()).await {
                Ok(()) => rb.ok_empty().to_vec(),
                Err(e) => rb.err(&e.to_string()).to_vec(),
            }
        }
    }
}
