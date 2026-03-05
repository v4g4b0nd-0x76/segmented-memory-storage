use crate::server::Server;

mod codec;
mod groups;
mod kv;
mod lru;
mod proto;
mod seg_log;
mod server;
#[tokio::main]
async fn main() {
    let srv: Server = Server::new().await;
    let addr = std::env::var("SERVER_ADDR").unwrap_or_else(|_| "localhost:9090".to_string());
    println!("Starting server on {addr}...");
    if let Err(e) = srv.start(&addr).await {
        eprintln!("Server error: {}", e);
    }
}
