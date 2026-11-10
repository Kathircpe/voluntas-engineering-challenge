use std::{collections::BTreeSet, env};

use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

const SCALE: f64 = 1_000_000.0;
const DEFAULT_PREVIEW: usize = 20;

fn main() -> anyhow::Result<()> {
    let mut args = env::args().skip(1);
    let path = args
        .next()
        .expect("usage: cargo run --example decode -- <file> [--all]");
    let all = args.any(|arg| arg == "--all");

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut snapshots = 0usize;
    let mut deltas = 0usize;
    let mut levels = 0usize;
    let mut symbols = BTreeSet::new();
    let mut shown = 0usize;

    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoded) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoded)?;
                snapshots += 1;
                levels += snapshot.bids.len() + snapshot.asks.len();
                symbols.insert(snapshot.symbol.clone());
                if all || shown < DEFAULT_PREVIEW {
                    let best_bid = snapshot
                        .bids
                        .first()
                        .map(|level| level.price_level as f64 / SCALE);
                    let best_ask = snapshot
                        .asks
                        .first()
                        .map(|level| level.price_level as f64 / SCALE);
                    println!(
                        "SNAPSHOT {} received={} exchange={} bids={} asks={} best_bid={best_bid:?} best_ask={best_ask:?}",
                        snapshot.symbol,
                        snapshot.received_at.to_rfc3339(),
                        snapshot.exchange_timestamp.to_rfc3339(),
                        snapshot.bids.len(),
                        snapshot.asks.len(),
                    );
                    shown += 1;
                }
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let delta = PriceLevelUpdateDomain::try_from_decoder(decoded)?;
                deltas += 1;
                symbols.insert(delta.symbol.clone());
                if all || shown < DEFAULT_PREVIEW {
                    println!(
                        "DELTA {} side={:?} price={:.6} qty={:.6} received={} exchange={}",
                        delta.symbol,
                        delta.side,
                        delta.price_level as f64 / SCALE,
                        delta.quantity as f64 / SCALE,
                        delta.backtest_timestamp.to_rfc3339(),
                        delta.exchange_timestamp.to_rfc3339(),
                    );
                    shown += 1;
                }
            }
            _ => {}
        }
    }

    println!(
        "snapshots={snapshots} deltas={deltas} symbols={} levels={levels}",
        symbols.len()
    );
    Ok(())
}
