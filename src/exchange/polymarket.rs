use std::collections::HashSet;

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, warn};

use crate::Side;
use crate::exchange::{Exchange, Level, Update};
use crate::fixed;

const GAMMA_URL: &str = "https://gamma-api.polymarket.com";
const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const MAX_PAGES: usize = 10_000;

pub struct Polymarket {
    client: reqwest::Client,
}

impl Polymarket {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    async fn fetch_symbols(&self, max: Option<usize>) -> anyhow::Result<Vec<String>> {
        let mut symbols = Vec::new();
        let mut cursor: Option<String> = None;
        let mut seen = HashSet::new();
        let mut pages = 0usize;

        for _ in 0..MAX_PAGES {
            pages += 1;
            let mut request = self
                .client
                .get(format!("{GAMMA_URL}/markets/keyset"))
                .query(&[("closed", "false"), ("active", "true"), ("limit", "100")]);
            if max.is_some() {
                request = request.query(&[("order", "volume24hr"), ("ascending", "false")]);
            }
            if let Some(cursor) = &cursor {
                request = request.query(&[("after_cursor", cursor)]);
            }

            let page: KeysetPage = request
                .send()
                .await
                .context("fetching markets")?
                .error_for_status()
                .context("fetching markets")?
                .json()
                .await
                .context("decoding markets")?;

            for market in page.markets {
                if !market.is_live() {
                    continue;
                }
                symbols.extend(market.asset_ids());
                if max.is_some_and(|max| symbols.len() >= max) {
                    break;
                }
            }

            if max.is_some_and(|max| symbols.len() >= max) {
                break;
            }

            if pages.is_multiple_of(25) {
                debug!(pages, symbols = symbols.len(), "discovering markets");
            }

            match page.next_cursor {
                Some(next) if !next.is_empty() && seen.insert(next.clone()) => cursor = Some(next),
                _ => break,
            }
        }

        symbols.sort_unstable();
        symbols.dedup();
        if let Some(max) = max {
            symbols.truncate(max);
        }
        Ok(symbols)
    }
}

impl Exchange for Polymarket {
    fn websocket_url(&self) -> &'static str {
        WS_URL
    }

    fn discover(&self, max: Option<usize>) -> BoxFuture<'_, anyhow::Result<Vec<String>>> {
        Box::pin(self.fetch_symbols(max))
    }

    fn subscribe(&self, symbols: &[String]) -> Option<String> {
        (!symbols.is_empty())
            .then(|| json!({ "assets_ids": symbols, "type": "market" }).to_string())
    }

    fn add(&self, symbols: &[String]) -> Option<String> {
        (!symbols.is_empty())
            .then(|| json!({ "assets_ids": symbols, "operation": "subscribe" }).to_string())
    }

    fn remove(&self, symbols: &[String]) -> Option<String> {
        (!symbols.is_empty())
            .then(|| json!({ "assets_ids": symbols, "operation": "unsubscribe" }).to_string())
    }

    fn heartbeat(&self) -> Option<&'static str> {
        Some("PING")
    }

    fn decode(&self, payload: &str) -> anyhow::Result<Vec<Update>> {
        decode(payload)
    }
}

pub fn decode(payload: &str) -> anyhow::Result<Vec<Update>> {
    let value: serde_json::Value =
        serde_json::from_str(payload).context("decoding websocket message")?;

    let mut updates = Vec::new();
    match value {
        serde_json::Value::Array(events) => {
            for event in events {
                if let Err(error) = decode_event(event, &mut updates) {
                    warn!(%error, "discarding malformed websocket event");
                }
            }
        }
        event @ serde_json::Value::Object(_) => decode_event(event, &mut updates)?,
        _ => bail!("unexpected websocket payload"),
    }
    Ok(updates)
}

fn decode_event(event: serde_json::Value, updates: &mut Vec<Update>) -> anyhow::Result<()> {
    let event_type = event
        .get("event_type")
        .or_else(|| event.get("type"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();

    match event_type {
        "book" => {
            let book: BookEvent =
                serde_json::from_value(event).context("decoding order book snapshot")?;
            let exchange_timestamp = book
                .timestamp
                .as_ref()
                .and_then(parse_timestamp)
                .unwrap_or_else(Utc::now);
            let bids = book
                .bids
                .iter()
                .map(parse_level)
                .collect::<anyhow::Result<Vec<_>>>()?;
            let asks = book
                .asks
                .iter()
                .map(parse_level)
                .collect::<anyhow::Result<Vec<_>>>()?;
            updates.push(Update::Snapshot {
                symbol: book.asset_id,
                exchange_timestamp,
                bids,
                asks,
            });
        }
        "price_change" => {
            let change: PriceChangeEvent =
                serde_json::from_value(event).context("decoding price change")?;
            let exchange_timestamp = change
                .timestamp
                .as_ref()
                .and_then(parse_timestamp)
                .unwrap_or_else(Utc::now);
            for level in change.price_changes {
                let level: RawChange = match serde_json::from_value(level) {
                    Ok(level) => level,
                    Err(error) => {
                        warn!(%error, "discarding malformed price change");
                        continue;
                    }
                };
                let side = match level.side.as_str() {
                    "BUY" => Side::Buy,
                    "SELL" => Side::Sell,
                    other => {
                        warn!(symbol = %level.asset_id, side = other, "unknown side");
                        continue;
                    }
                };
                let price = match fixed::parse(&level.price.as_str()) {
                    Ok(price) => price,
                    Err(error) => {
                        warn!(symbol = %level.asset_id, %error, "invalid price");
                        continue;
                    }
                };
                let quantity = match fixed::parse(&level.size.as_str()) {
                    Ok(quantity) => quantity,
                    Err(error) => {
                        warn!(symbol = %level.asset_id, %error, "invalid size");
                        continue;
                    }
                };
                updates.push(Update::Delta {
                    symbol: level.asset_id,
                    exchange_timestamp,
                    side,
                    price,
                    quantity,
                });
            }
        }
        _ => {}
    }
    Ok(())
}

fn parse_level(level: &RawLevel) -> anyhow::Result<Level> {
    Ok(Level {
        price: fixed::parse(&level.price.as_str())
            .with_context(|| format!("invalid price: {}", level.price.as_str()))?,
        quantity: fixed::parse(&level.size.as_str())
            .with_context(|| format!("invalid size: {}", level.size.as_str()))?,
    })
}

fn parse_timestamp(value: &RawTimestamp) -> Option<DateTime<Utc>> {
    let text = value.as_str();
    if let Ok(millis) = text.parse::<i64>() {
        return DateTime::from_timestamp_millis(millis);
    }
    DateTime::parse_from_rfc3339(&text)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

#[derive(Debug, Deserialize)]
struct KeysetPage {
    #[serde(default)]
    markets: Vec<Market>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Market {
    active: Option<bool>,
    closed: Option<bool>,
    archived: Option<bool>,
    enable_order_book: Option<bool>,
    accepting_orders: Option<bool>,
    clob_token_ids: Option<String>,
    position_ids: Option<Vec<String>>,
    version: Option<String>,
}

impl Market {
    fn is_live(&self) -> bool {
        self.active.unwrap_or(false)
            && !self.closed.unwrap_or(true)
            && !self.archived.unwrap_or(false)
            && self.enable_order_book.unwrap_or(false)
            && self.accepting_orders.unwrap_or(false)
    }

    fn asset_ids(&self) -> Vec<String> {
        if self.version.as_deref() == Some("v2") {
            return self.position_ids.clone().unwrap_or_default();
        }
        self.clob_token_ids
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
            .unwrap_or_default()
    }
}

#[derive(Debug, Deserialize)]
struct BookEvent {
    #[serde(alias = "assetId")]
    asset_id: String,
    #[serde(default)]
    timestamp: Option<RawTimestamp>,
    #[serde(default)]
    bids: Vec<RawLevel>,
    #[serde(default)]
    asks: Vec<RawLevel>,
}

#[derive(Debug, Deserialize)]
struct PriceChangeEvent {
    #[serde(default)]
    timestamp: Option<RawTimestamp>,
    #[serde(default, alias = "priceChanges")]
    price_changes: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct RawChange {
    #[serde(alias = "assetId")]
    asset_id: String,
    price: RawDecimal,
    size: RawDecimal,
    side: String,
}

#[derive(Debug, Deserialize)]
struct RawLevel {
    price: RawDecimal,
    size: RawDecimal,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawDecimal {
    Text(String),
    Number(serde_json::Number),
}

impl RawDecimal {
    fn as_str(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Number(number) => number.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawTimestamp {
    Text(String),
    Number(serde_json::Number),
}

impl RawTimestamp {
    fn as_str(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Number(number) => number.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exchange() -> Polymarket {
        Polymarket::new(reqwest::Client::new())
    }

    #[test]
    fn decodes_book_snapshot() {
        let payload = r#"{
            "event_type": "book",
            "asset_id": "123",
            "timestamp": "1782753357257",
            "bids": [{"price": "0.08", "size": "33343.4"}],
            "asks": [{"price": "0.99", "size": "10"}]
        }"#;

        let updates = decode(payload).unwrap();
        let Update::Snapshot {
            symbol,
            exchange_timestamp,
            bids,
            asks,
        } = &updates[0]
        else {
            panic!("expected snapshot");
        };
        assert_eq!(symbol, "123");
        assert_eq!(exchange_timestamp.timestamp_millis(), 1_782_753_357_257);
        assert_eq!(
            bids,
            &vec![Level {
                price: 80_000,
                quantity: 33_343_400_000
            }]
        );
        assert_eq!(
            asks,
            &vec![Level {
                price: 990_000,
                quantity: 10_000_000
            }]
        );
    }

    #[test]
    fn decodes_initial_dump_array() {
        let payload = r#"[
            {"event_type": "book", "asset_id": "1", "bids": [], "asks": []},
            {"event_type": "book", "asset_id": "2", "bids": [], "asks": []}
        ]"#;
        let updates = decode(payload).unwrap();
        assert_eq!(updates.len(), 2);
    }

    #[test]
    fn decodes_price_changes() {
        let payload = r#"{
            "event_type": "price_change",
            "timestamp": "1782753357257",
            "price_changes": [
                {"asset_id": "1", "price": "0.08", "size": "5", "side": "BUY"},
                {"asset_id": "1", "price": "0.09", "size": "0", "side": "SELL"}
            ]
        }"#;
        let updates = decode(payload).unwrap();
        assert_eq!(
            updates,
            vec![
                Update::Delta {
                    symbol: "1".into(),
                    exchange_timestamp: DateTime::from_timestamp_millis(1_782_753_357_257).unwrap(),
                    side: Side::Buy,
                    price: 80_000,
                    quantity: 5_000_000,
                },
                Update::Delta {
                    symbol: "1".into(),
                    exchange_timestamp: DateTime::from_timestamp_millis(1_782_753_357_257).unwrap(),
                    side: Side::Sell,
                    price: 90_000,
                    quantity: 0,
                },
            ]
        );
    }

    #[test]
    fn skips_unknown_events_and_bad_levels() {
        let payload = r#"[
            {"event_type": "last_trade_price", "asset_id": "1"},
            {"event_type": "book"},
            {"event_type": "price_change", "price_changes": [
                {"asset_id": "1", "price": "0.08", "size": "5", "side": "MAYBE"},
                {"price": "0.09", "size": "5", "side": "BUY"},
                {"asset_id": "1", "price": "0.07", "size": "5", "side": "BUY"}
            ]}
        ]"#;
        let updates = decode(payload).unwrap();
        assert_eq!(updates.len(), 1);
    }

    #[test]
    fn keeps_valid_events_from_a_partially_malformed_batch() {
        let payload = r#"[
            {"event_type": "book"},
            {"event_type": "book", "asset_id": "1", "bids": [], "asks": []}
        ]"#;
        let updates = decode(payload).unwrap();
        assert_eq!(updates.len(), 1);
        assert!(matches!(&updates[0], Update::Snapshot { symbol, .. } if symbol == "1"));
    }

    #[test]
    fn rejects_malformed_payloads() {
        assert!(decode("not json").is_err());
        assert!(decode("42").is_err());
        assert!(decode(r#"{"event_type":"book"}"#).is_err());
        assert!(
            decode(r#"{"event_type":"book","asset_id":"1","bids":[{"price":"x","size":"1"}]}"#)
                .is_err()
        );
        assert!(
            decode(
                r#"{"event_type":"price_change","price_changes":[{"price":"0.1","size":"1","side":"BUY"}]}"#
            )
            .unwrap()
            .is_empty()
        );
        assert!(decode(r#"{"event_type":"price_change","price_changes":"nope"}"#).is_err());
        assert_eq!(decode("[]").unwrap(), Vec::new());
    }

    #[test]
    fn accepts_numeric_decimals_and_missing_timestamps() {
        let payload = r#"{
            "event_type": "book",
            "asset_id": "1",
            "bids": [{"price": 0.08, "size": 5}],
            "asks": []
        }"#;
        let updates = decode(payload).unwrap();
        let Update::Snapshot { bids, .. } = &updates[0] else {
            panic!("expected snapshot");
        };
        assert_eq!(bids[0].price, 80_000);
        assert_eq!(bids[0].quantity, 5_000_000);
    }

    #[test]
    fn builds_subscription_frames() {
        let exchange = exchange();
        assert_eq!(
            exchange.subscribe(&["1".into()]).unwrap(),
            r#"{"assets_ids":["1"],"type":"market"}"#
        );
        assert_eq!(
            exchange.add(&["1".into()]).unwrap(),
            r#"{"assets_ids":["1"],"operation":"subscribe"}"#
        );
        assert_eq!(
            exchange.remove(&["1".into()]).unwrap(),
            r#"{"assets_ids":["1"],"operation":"unsubscribe"}"#
        );
        assert!(exchange.subscribe(&[]).is_none());
    }

    #[test]
    fn reads_asset_ids_per_market_version() {
        let v1: Market = serde_json::from_value(json!({
            "version": "v1",
            "clobTokenIds": "[\"a\", \"b\"]",
            "positionIds": ["c", "d"]
        }))
        .unwrap();
        assert_eq!(v1.asset_ids(), vec!["a", "b"]);

        let v2: Market = serde_json::from_value(json!({
            "version": "v2",
            "clobTokenIds": "[\"a\", \"b\"]",
            "positionIds": ["c", "d"]
        }))
        .unwrap();
        assert_eq!(v2.asset_ids(), vec!["c", "d"]);

        let broken: Market = serde_json::from_value(json!({
            "version": "v1",
            "clobTokenIds": "not json"
        }))
        .unwrap();
        assert!(broken.asset_ids().is_empty());
    }

    #[test]
    fn filters_non_live_markets() {
        let live: Market = serde_json::from_value(json!({
            "active": true,
            "closed": false,
            "archived": false,
            "enableOrderBook": true,
            "acceptingOrders": true
        }))
        .unwrap();
        assert!(live.is_live());

        let closed: Market = serde_json::from_value(json!({
            "active": true,
            "closed": true,
            "enableOrderBook": true,
            "acceptingOrders": true
        }))
        .unwrap();
        assert!(!closed.is_live());
    }
}
