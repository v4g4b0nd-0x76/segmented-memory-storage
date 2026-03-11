use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::{net::TcpListener, sync::RwLock};
use tokio_util::codec::Framed;

use crate::{
    db::{db::DB, pipeline::PipelineManager},
    server::{codec::*, proto::*},
};

pub struct Server {
    db: Arc<RwLock<DB>>,
    pipeline_manager: Arc<RwLock<PipelineManager>>,
}

impl Server {
    pub async fn new() -> Self {
        let db = Arc::new(RwLock::new(DB::new().await));
        let pipeline_manager = Arc::new(RwLock::new(PipelineManager::new(Arc::clone(&db))));
        Server {
            db,
            pipeline_manager,
        }
    }

    pub async fn start_pipeline(&self) -> u64 {
        self.pipeline_manager.write().await.start()
    }

    pub async fn start(&self, addr: &str) -> anyhow::Result<()> {
        let ln = TcpListener::bind(addr).await?;
        loop {
            let (socket, peer) = ln.accept().await?;
            println!("New connection from {}", peer);
            socket.set_nodelay(true)?;
            let server = Arc::new(Server {
                db: Arc::clone(&self.db),
                pipeline_manager: Arc::clone(&self.pipeline_manager),
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
                    let resp = match parse_command(&frame) {
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
                match self.db.write().await.log_create_group(name).await {
                    Ok(()) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::DropGroup { name } => match self.db.write().await.log_drop_group(name).await {
                Ok(()) => rb.ok_empty().to_vec(),
                Err(e) => rb.err(&e.to_string()).to_vec(),
            },
            Command::Add {
                group,
                timestamp,
                payload,
            } => {
                match self
                    .db
                    .write()
                    .await
                    .log_add(group, timestamp, payload)
                    .await
                {
                    Ok(id) => rb.ok_u64(id).to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::AddRange { group, entries } => {
                match self.db.write().await.log_add_range(group, &entries).await {
                    Ok((first, last)) => rb.ok_u64_pair(first, last).to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::Read { group, id } => match self.db.write().await.log_read(group, id).await {
                Ok((id, ts, data)) => rb.ok_entry(id, ts, &data).to_vec(),
                Err(e) => rb.err(&e.to_string()).to_vec(),
            },
            Command::ReadRange { group, start, end } => {
                match self
                    .db
                    .write()
                    .await
                    .log_read_range(group, start, end)
                    .await
                {
                    Ok(entries) => rb.ok_entries(&entries).to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::Remove { group, up_to_id } => {
                match self.db.write().await.log_remove(group, up_to_id).await {
                    Ok(()) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::ListGroups => match self.db.write().await.log_list_groups().await {
                Ok(list) => rb.ok_group_list(list).to_vec(),
                Err(e) => rb.err(&e.to_string()).to_vec(),
            },
            Command::GroupStats { group } => {
                match self.db.write().await.log_group_stats(group).await {
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
                match self
                    .db
                    .write()
                    .await
                    .kv_set(
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
                match self
                    .db
                    .write()
                    .await
                    .kv_get(db.to_string(), key.to_string())
                    .await
                {
                    Ok(Some(entry)) => rb.ok_bytes(&entry.val).to_vec(),
                    Ok(None) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::DelKey { db, key } => {
                match self
                    .db
                    .write()
                    .await
                    .kv_del(db.to_string(), key.to_string())
                    .await
                {
                    Ok(()) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::Keys { db } => match self.db.write().await.kv_keys(db.to_string()).await {
                Ok(keys) => rb.ok_string_list(&keys).to_vec(),
                Err(e) => rb.err(&e.to_string()).to_vec(),
            },
            Command::Flush { db } => match self.db.write().await.kv_flush(db.to_string()).await {
                Ok(()) => rb.ok_empty().to_vec(),
                Err(e) => rb.err(&e.to_string()).to_vec(),
            },
            Command::LPush { key, val } => {
                match self
                    .db
                    .write()
                    .await
                    .list_push(key.to_string(), val.to_vec())
                    .await
                {
                    Ok(_) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::LPushRange { key, vals } => {
                match self
                    .db
                    .write()
                    .await
                    .list_push_range(key.to_string(), vals.iter().map(|v| v.to_vec()).collect())
                    .await
                {
                    Ok(_) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::LPop { key } => match self.db.write().await.list_pop(key.to_string()).await {
                Ok(val) => rb.ok_bytes(&val).to_vec(),
                Err(e) => rb.err(&e.to_string()).to_vec(),
            },
            Command::LPopRange { key, start, end } => {
                match self
                    .db
                    .write()
                    .await
                    .list_pop_range(key.to_string(), start as usize, end as usize)
                    .await
                {
                    Ok(vals) => rb.ok_bytes_list(&vals).to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::LPopCount { key, count } => {
                match self
                    .db
                    .write()
                    .await
                    .list_pop_count(key.to_string(), count as usize)
                    .await
                {
                    Ok(vals) => rb.ok_bytes_list(&vals).to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            Command::LLen { key } => match self.db.write().await.list_len(key.to_string()).await {
                Ok(n) => rb.ok_u64(n as u64).to_vec(),
                Err(e) => rb.err(&e.to_string()).to_vec(),
            },
            Command::LFlush { key } => {
                match self.db.write().await.list_flush(key.to_string()).await {
                    Ok(_) => rb.ok_empty().to_vec(),
                    Err(e) => rb.err(&e.to_string()).to_vec(),
                }
            }
            // TODO: implement pipeline api
            // user starts a pipeline and in modification request can send pipeline as option if pipeline id is provided we add the given command to pipeline and when the pipeline is ended we execute it
            // TODO: each pipeline shall have a deadline of for example 10 second from previous command and if not given the pipeline would be removed
            Command::StartPipeline {} => rb.ok_u64(self.start_pipeline().await).to_vec(),
            Command::EndPipeline { id } => rb.ok_empty().to_vec(),
        }
    }
}
