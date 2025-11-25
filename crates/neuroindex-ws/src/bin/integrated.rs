/// Binary per server integrato HTTP + WebSocket
/// Usa neuroindex_ws::integrated_server::run_integrated_server()

#[tokio::main]
async fn main() {
    neuroindex_ws::integrated_server::run_integrated_server().await;
}
