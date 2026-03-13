use std::sync::Arc;

use tokio::sync::RwLock;

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
    let db = Arc::new(RwLock::new(DB::new(Arc::clone(&conf)).await));
    let replica_conf = conf.replica_conf.as_ref().unwrap();
    let addr = std::env::var("SERVER_ADDR").unwrap_or_else(|_| format!("localhost:{}", conf.port));
    let ha_conf = Arc::new(RwLock::new(HaConf::new()));
    let mut ha_manager = HaManager::new(Arc::clone(&conf), Arc::clone(&ha_conf), Arc::clone(&db));
    if replica_conf.is_replica {
        let master = replica_conf.master.as_deref().unwrap();
        match &replica_conf.role {
            Some(ReplicaRole::Leader) => ha_manager.heartbeat_followers().await,
            Some(ReplicaRole::Follower) => ha_manager
                .register_master(master, &addr)
                .await
                .expect("failed to register to master"),
            None => {}
        }
    }
    let srv = Server::new(
        Arc::clone(&conf),
        Arc::clone(&db),
        Arc::new(RwLock::new(ha_manager)),
    )
    .await;
    println!("Starting server on {addr}...");
    if let Err(e) = srv.start(&addr).await {
        eprintln!("Server error: {}", e);
    }
}
