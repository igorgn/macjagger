//! CI Agent binary.
//!
//! Your task: wire up `rust_ci_crafters::agent::run()` here.

#[tokio::main]
async fn main() {
    let server_url =
        std::env::var("CI_SERVER_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());
    rust_ci_crafters::agent::run(server_url).await;
}
