//! CI Server binary.
//!
//! Your task: wire up `rust_ci_crafters::server::run()` here.

use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    rust_ci_crafters::server::run(addr).await;
}
