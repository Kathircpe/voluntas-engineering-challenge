use std::{path::Path, thread};

use anyhow::Context;
use tokio::sync::mpsc::Receiver;

use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

#[derive(Debug, Clone, PartialEq)]
pub enum Output {
    Snapshot(SnapshotDomain),
    Delta(PriceLevelUpdateDomain),
}

pub fn spawn_writer(
    path: impl AsRef<Path>,
    chunk: usize,
    mut receiver: Receiver<Output>,
) -> anyhow::Result<thread::JoinHandle<anyhow::Result<()>>> {
    let mut writer =
        AlignedSbeWriter::create(path, chunk).context("opening the SBE output stream")?;

    thread::Builder::new()
        .name("sbe-writer".into())
        .spawn(move || {
            while let Some(output) = receiver.blocking_recv() {
                match output {
                    Output::Snapshot(message) => writer.write_msg(&message)?,
                    Output::Delta(message) => writer.write_msg(&message)?,
                }
            }
            writer.flush()
        })
        .context("spawning the SBE writer thread")
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::*;
    use crate::Side;
    use crate::util::dtos::challenge_orderbook::{
        SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
    };
    use crate::util::{AlignedSbeReader, OrderbookSchema};

    const CHUNK_SIZE: usize = 64 << 20;

    fn timestamp() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
    }

    #[test]
    fn writes_and_reads_messages() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        let handle = spawn_writer(file.path(), CHUNK_SIZE, receiver).unwrap();

        let snapshot = SnapshotDomain {
            received_at: timestamp(),
            exchange_timestamp: timestamp(),
            bids: vec![SnapshotBidsEntryDomain {
                price_level: 400_000,
                quantity: 2_000_000,
            }],
            asks: vec![SnapshotAsksEntryDomain {
                price_level: 600_000,
                quantity: 3_500_000,
            }],
            symbol: "EXAMPLE".into(),
        };
        let delta = PriceLevelUpdateDomain {
            backtest_timestamp: timestamp(),
            exchange_timestamp: timestamp(),
            price_level: 400_000,
            quantity: 0,
            side: Side::Buy,
            symbol: "EXAMPLE".into(),
        };

        sender
            .blocking_send(Output::Snapshot(snapshot.clone()))
            .unwrap();
        sender.blocking_send(Output::Delta(delta.clone())).unwrap();
        drop(sender);
        handle.join().unwrap().unwrap();

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
        let Some(message) = reader.read_msg().unwrap() else {
            panic!("expected snapshot");
        };
        let crate::util::dtos::challenge_orderbook::AnyMessage::Snapshot(decoded) = message else {
            panic!("expected snapshot");
        };
        assert_eq!(SnapshotDomain::try_from_decoder(decoded).unwrap(), snapshot);

        let Some(message) = reader.read_msg().unwrap() else {
            panic!("expected delta");
        };
        let crate::util::dtos::challenge_orderbook::AnyMessage::PriceLevelUpdate(decoded) = message
        else {
            panic!("expected delta");
        };
        assert_eq!(
            PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap(),
            delta
        );
        assert!(reader.read_msg().unwrap().is_none());
    }

    #[test]
    fn flushes_buffered_messages_when_the_channel_closes() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let handle = spawn_writer(file.path(), CHUNK_SIZE, receiver).unwrap();
        drop(sender);
        handle.join().unwrap().unwrap();
        assert_eq!(std::fs::metadata(file.path()).unwrap().len(), 0);
    }

    #[test]
    fn reports_unopenable_output_paths() {
        let (_sender, receiver) = tokio::sync::mpsc::channel(1);
        assert!(spawn_writer("/nonexistent/directory/out.sbe", 1024, receiver).is_err());
    }
}
