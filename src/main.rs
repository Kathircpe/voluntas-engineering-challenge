use std::{sync::Arc, time::Duration};

use anyhow::Context;
use engineering_challenge::collector;
use engineering_challenge::config::Config;
use engineering_challenge::exchange::Exchange;
use engineering_challenge::exchange::polymarket::Polymarket;
use engineering_challenge::output;
use tracing_subscriber::EnvFilter;

const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;
const OUTPUT_BUFFER: usize = 1_024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let config = Config::from_env()?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("building the HTTP client")?;
    let exchange: Arc<dyn Exchange> = Arc::new(Polymarket::new(client));

    let (output_tx, output_rx) = tokio::sync::mpsc::channel(OUTPUT_BUFFER);
    let writer = output::spawn_writer(&config.output, CHUNK_SIZE_64_MEBIBYTES, output_rx)?;

    let result = collector::run(exchange, &config, &output_tx, shutdown()).await;
    drop(output_tx);

    let flushed = writer
        .join()
        .map_err(|_| anyhow::anyhow!("output writer thread panicked"))?;
    flushed?;
    result
}

async fn shutdown() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to listen for ctrl-c");
    }
}
