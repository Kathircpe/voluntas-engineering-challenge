pub mod polymarket;

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;

use crate::Side;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Level {
    pub price: i64,
    pub quantity: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Update {
    Snapshot {
        symbol: String,
        exchange_timestamp: DateTime<Utc>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    Delta {
        symbol: String,
        exchange_timestamp: DateTime<Utc>,
        side: Side,
        price: i64,
        quantity: i64,
    },
}

impl Update {
    pub fn symbol(&self) -> &str {
        match self {
            Self::Snapshot { symbol, .. } | Self::Delta { symbol, .. } => symbol,
        }
    }
}

pub trait Exchange: Send + Sync {
    fn websocket_url(&self) -> &'static str;
    fn discover(&self, max: Option<usize>) -> BoxFuture<'_, anyhow::Result<Vec<String>>>;
    fn subscribe(&self, symbols: &[String]) -> Option<String>;
    fn add(&self, symbols: &[String]) -> Option<String>;
    fn remove(&self, symbols: &[String]) -> Option<String>;
    fn heartbeat(&self) -> Option<&'static str>;
    fn decode(&self, payload: &str) -> anyhow::Result<Vec<Update>>;
}
