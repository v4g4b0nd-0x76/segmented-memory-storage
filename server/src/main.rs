use std::sync::Arc;

use tokio::sync::{RwLock, mpsc};

use crate::{
    conf::{Conf, ReplicaRole},
    db::db::DB,
    ha::{HaConf, HaManager},
    server::server::Server,
};

mod conf;
mod db;
mod ha;
mod server;
#[tokio::main]
async fn main() {
    let conf = Arc::new(Conf::load().await.unwrap());
    let (tx, rx) = mpsc::unbounded_channel();
    let db = Arc::new(RwLock::new(DB::new(Arc::clone(&conf), Some(tx)).await));
    let replica_conf = conf.replica_conf.as_ref().unwrap();
    let addr = std::env::var("SERVER_ADDR").unwrap_or_else(|_| format!("localhost:{}", conf.port));
    let ha_conf = Arc::new(RwLock::new(HaConf::new()));
    let ha_manager = Arc::new(RwLock::new(HaManager::new(
        Arc::clone(&conf),
        Arc::clone(&ha_conf),
        Arc::clone(&db),
        rx,
    )));
    if replica_conf.is_replica {
        let master = replica_conf.master.as_deref().unwrap();
        let ha_manager_clone = Arc::clone(&ha_manager);
        tokio::spawn(async move {
            let _ = ha_manager_clone.write().await.start().await;
        });
        match &replica_conf.role {
            Some(ReplicaRole::Leader) => ha_manager.write().await.heartbeat_followers().await,
            Some(ReplicaRole::Follower) => ha_manager
                .write()
                .await
                .register_master(master, &addr)
                .await
                .expect("failed to register to master"),
            None => {}
        }
    }
    let srv = Server::new(Arc::clone(&conf), Arc::clone(&db), Arc::clone(&ha_manager)).await;
    println!("Starting server on {addr}...");
    if let Err(e) = srv.start(&addr).await {
        eprintln!("Server error: {}", e);
    }
}
