use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    animus_queue_default::plugin::run().await
}
