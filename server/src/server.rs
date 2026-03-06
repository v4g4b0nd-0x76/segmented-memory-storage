use crate::codec::LengthPrefixCodec;
use crate::groups::GroupManager;
use crate::kv::KvStore;
use crate::proto::{self, Command, ResponseBuilder};
use crate::seg_list::SegList;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::{net::TcpListener, sync::RwLock};
use tokio_util::codec::Framed;

pub struct Server {
    group_manager: Arc<RwLock<GroupManager>>,
    kv_store: Arc<RwLock<KvStore>>,
    list: Arc<RwLock<SegList>>,
}

impl Server {
    pub async fn new() -> Self {
        Server {
            group_manager: Arc::new(RwLock::new(GroupManager::new().await)),
            kv_store: Arc::new(RwLock::new(KvStore::new(None).await)),
            list: Arc::new(RwLock::new(SegList::new().await)),
        }
    }

    pub async fn start(&self, addr: &str) -> anyhow::Result<()> {
        let ln = TcpListener::bind(addr).await?;
        loop {
            let (socket, peer) = ln.accept().await?;
            println!("New connection from {}", peer);
            socket.set_nodelay(true)?;
            let server = Arc::new(Server {
                group_manager: Arc::clone(&self.group_manager),
                kv_store: Arc::clone(&self.kv_store),
                list: Arc::clone(&self.list),
            });
            tokio::spawn(async move {
                let mut framed = Framed::new(socket, LengthPrefixCodec);
                let mut rb = ResponseBuilder::new();
                while let Some(result) = framed.next().await {
                    let frame = match result {
                        Ok(f) => f,
                        Err(e) => {
                            eprintln!("Error reading frame: {}", e);
                            break;
                        }
                    };
                    let resp = match proto::parse_command(&frame) {
                        Ok(cmd) => server.handle_command(cmd, &mut rb).await,
                        Err(e) => rb.err(&format!("parse error: {}", e.0)).to_vec(),
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

    async fn handle_command(&self, cmd: Command<'_>, rb: &mut ResponseBuilder) -> Vec<u8> {
        match cmd {
            Command::CreateGroup { name } => {
                let mut mgr = self.group_manager.write().await;
                match mgr.create_group(name).await {
                    Ok(()) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::DropGroup { name } => {
                let mut mgr = self.group_manager.write().await;
                match mgr.drop_group(name).await {
                    Ok(()) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::Add {
                group,
                timestamp,
                payload,
            } => {
                let mut mgr = self.group_manager.write().await;
                match mgr.add(group, timestamp, payload).await {
                    Ok(id) => rb.ok_u64(id).to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::AddRange { group, entries } => {
                let mut mgr = self.group_manager.write().await;
                match mgr.add_range(group, &entries).await {
                    Ok((first, last)) => rb.ok_u64_pair(first, last).to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::Read { group, id } => {
                let mut mgr = self.group_manager.write().await;
                match mgr.read(group, id) {
                    Ok((entry_id, ts, payload)) => rb.ok_entry(entry_id, ts, &payload).to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::ReadRange { group, start, end } => {
                let mut mgr = self.group_manager.write().await;
                match mgr.read_range(group, start, end) {
                    Ok(entries) => rb.ok_entries(&entries).to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::Remove { group, up_to_id } => {
                let mut mgr = self.group_manager.write().await;
                match mgr.remove(group, up_to_id) {
                    Ok(()) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::ListGroups => {
                let mgr = self.group_manager.read().await;
                rb.ok_group_list(&mgr.list_groups()).to_vec()
            }
            Command::GroupStats { group } => {
                let mgr = self.group_manager.read().await;
                match mgr.group_stats(group) {
                    Ok(stats) => rb
                        .ok_stats(stats.total_entries, stats.total_segments, stats.next_id)
                        .to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::SetKey {
                db,
                key,
                val,
                ttl_secs,
            } => {
                let mut kv = self.kv_store.write().await;
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
                let mut kv = self.kv_store.write().await;
                match kv.get(db.to_string(), key.to_string()).await {
                    Ok(Some(entry)) => rb.ok_bytes(&entry.val).to_vec(),
                    Ok(None) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::DelKey { db, key } => {
                let mut kv = self.kv_store.write().await;
                match kv.del(db.to_string(), key.to_string()).await {
                    Ok(()) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::Keys { db } => {
                let mut kv = self.kv_store.write().await;
                match kv.keys(db.to_string()).await {
                    Ok(keys) => rb.ok_string_list(&keys).to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::Flush { db } => {
                let mut kv = self.kv_store.write().await;
                match kv.flush(db.to_string()).await {
                    Ok(()) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::LPush { key, val } => {
                let mut list = self.list.write().await;
                match list.push(key.to_string(), val.to_vec()).await {
                    Ok(_) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::LPushRange { key, vals } => {
                let mut list = self.list.write().await;
                for val in &vals {
                    if let Err(e) = list.push(key.to_string(), val.to_vec()).await {
                        return rb.err(&e.to_string()).to_vec();
                    }
                }
                rb.ok_empty().to_vec()
            }
            Command::LPop { key } => {
                let mut list = self.list.write().await;
                match list.pop(key.to_string()).await {
                    Ok(val) => rb.ok_bytes(&val).to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::LPopRange { key, start, end } => {
                let mut list = self.list.write().await;
                match list
                    .pop_range(key.to_string(), start as usize, end as usize)
                    .await
                {
                    Ok(vals) => rb.ok_bytes_list(&vals).to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::LPopCount { key, count } => {
                let mut list = self.list.write().await;
                match list.pop_count(key.to_string(), count as usize).await {
                    Ok(vals) => rb.ok_bytes_list(&vals).to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::LLen { key } => {
                let mut list = self.list.write().await;
                match list.len(key.to_string()).await {
                    Ok(n) => rb.ok_u64(n as u64).to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::LFlush { key } => {
                let mut list = self.list.write().await;
                match list.flush(key.to_string()).await {
                    Ok(_) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
        }
    }
}
