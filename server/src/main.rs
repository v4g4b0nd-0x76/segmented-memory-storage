use std::sync::Arc;

use crate::{conf::Conf, server::server::Server};

mod conf;
mod db;
mod server;
#[tokio::main]
async fn main() {
    let conf = Arc::new(Conf::load().await.unwrap());
    let addr = std::env::var("SERVER_ADDR").unwrap_or_else(|_| format!("localhost:{}", conf.port));
    let srv = Server::new(Arc::clone(&conf)).await;
    println!("Starting server on {addr}...");
    if let Err(e) = srv.start(&addr).await {
        eprintln!("Server error: {}", e);
    }
}
