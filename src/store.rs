use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};

use crate::book::Book;
use crate::exchange::Update;
use crate::output::Output;
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

#[derive(Debug, PartialEq)]
pub struct Resync {
    pub socket: usize,
    pub symbol: String,
}

#[derive(Debug, Default, PartialEq)]
pub struct Applied {
    pub outputs: Vec<Output>,
    pub resync: Option<Resync>,
}

#[derive(Debug, Default, PartialEq)]
pub struct Reconciled {
    pub outputs: Vec<Output>,
    pub shards: Vec<Vec<String>>,
}

#[derive(Debug)]
struct SocketBooks {
    shard: usize,
    slot: usize,
    books: BTreeMap<String, Book>,
    last_exchange: BTreeMap<String, DateTime<Utc>>,
    connected: bool,
}

#[derive(Debug)]
pub struct Store {
    shard_count: usize,
    redundancy: usize,
    shards: BTreeMap<String, usize>,
    sources: Vec<usize>,
    sockets: Vec<SocketBooks>,
}

impl Store {
    pub fn new(shard_count: usize, redundancy: usize) -> Self {
        assert!(shard_count > 0, "shard count must be positive");
        assert!(redundancy > 0, "redundancy must be positive");
        let sockets = (0..shard_count)
            .flat_map(|shard| {
                (0..redundancy).map(move |slot| SocketBooks {
                    shard,
                    slot,
                    books: BTreeMap::new(),
                    last_exchange: BTreeMap::new(),
                    connected: false,
                })
            })
            .collect();
        Self {
            shard_count,
            redundancy,
            shards: BTreeMap::new(),
            sources: vec![0; shard_count],
            sockets,
        }
    }

    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    pub fn redundancy(&self) -> usize {
        self.redundancy
    }

    pub fn socket_count(&self) -> usize {
        self.sockets.len()
    }

    pub fn socket_id(&self, shard: usize, slot: usize) -> usize {
        shard * self.redundancy + slot
    }

    pub fn socket_shard(&self, socket: usize) -> usize {
        self.sockets[socket].shard
    }

    pub fn socket_slot(&self, socket: usize) -> usize {
        self.sockets[socket].slot
    }

    pub fn active_socket(&self, shard: usize) -> usize {
        self.socket_id(shard, self.sources[shard])
    }

    pub fn active_slot(&self, shard: usize) -> usize {
        self.sources[shard]
    }

    pub fn is_connected(&self, socket: usize) -> bool {
        self.sockets[socket].connected
    }

    pub fn set_connected(&mut self, socket: usize, connected: bool) {
        self.sockets[socket].connected = connected;
    }

    pub fn len(&self) -> usize {
        self.shards.len()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    pub fn shard_of(&self, symbol: &str) -> Option<usize> {
        self.shards.get(symbol).copied()
    }

    pub fn symbols(&self, shard: usize) -> Vec<String> {
        self.shards
            .iter()
            .filter(|(_, assigned)| **assigned == shard)
            .map(|(symbol, _)| symbol.clone())
            .collect()
    }

    pub fn apply(&mut self, update: Update, now: DateTime<Utc>) -> Applied {
        let Some(shard) = self.shards.get(update.symbol()).copied() else {
            return Applied::default();
        };
        let socket = self.active_socket(shard);
        self.apply_from(socket, update, now)
    }

    pub fn apply_from(&mut self, socket: usize, update: Update, now: DateTime<Utc>) -> Applied {
        let Some(shard) = self.shards.get(update.symbol()).copied() else {
            return Applied::default();
        };
        if self.sockets[socket].shard != shard {
            return Applied::default();
        }
        let active = self.sources[shard] == self.sockets[socket].slot;
        let state = &mut self.sockets[socket];

        match update {
            Update::Snapshot {
                symbol,
                exchange_timestamp,
                bids,
                asks,
            } => {
                let mut book = Book::default();
                book.replace(
                    bids.into_iter().map(|level| (level.price, level.quantity)),
                    asks.into_iter().map(|level| (level.price, level.quantity)),
                );
                let output = active
                    .then(|| Output::Snapshot(snapshot(&symbol, &book, exchange_timestamp, now)));
                state.books.insert(symbol.clone(), book);
                state.last_exchange.insert(symbol, exchange_timestamp);

                Applied {
                    outputs: output.into_iter().collect(),
                    resync: None,
                }
            }
            Update::Delta {
                symbol,
                exchange_timestamp,
                side,
                price,
                quantity,
            } => {
                let Some(book) = state.books.get_mut(&symbol) else {
                    return Applied {
                        outputs: Vec::new(),
                        resync: Some(Resync { socket, symbol }),
                    };
                };
                if !book.set(side, price, quantity) {
                    return Applied::default();
                }

                state
                    .last_exchange
                    .insert(symbol.clone(), exchange_timestamp);
                let outputs = if active {
                    vec![Output::Delta(PriceLevelUpdateDomain {
                        backtest_timestamp: now,
                        exchange_timestamp,
                        price_level: price,
                        quantity,
                        side,
                        symbol,
                    })]
                } else {
                    Vec::new()
                };
                Applied {
                    outputs,
                    resync: None,
                }
            }
        }
    }

    pub fn switch_source(&mut self, shard: usize, slot: usize, now: DateTime<Utc>) -> Vec<Output> {
        if self.sources[shard] == slot {
            return Vec::new();
        }
        self.sources[shard] = slot;
        self.snapshots_for_socket(self.socket_id(shard, slot), now)
    }

    pub fn disconnected(&mut self, socket: usize) {
        let state = &mut self.sockets[socket];
        state.books.clear();
        state.last_exchange.clear();
        state.connected = false;
    }

    pub fn snapshots(&self, now: DateTime<Utc>) -> Vec<Output> {
        let mut outputs = Vec::new();
        for shard in 0..self.shard_count {
            outputs.extend(self.snapshots_for_socket(self.active_socket(shard), now));
        }
        outputs
    }

    fn snapshots_for_socket(&self, socket: usize, now: DateTime<Utc>) -> Vec<Output> {
        let state = &self.sockets[socket];
        state
            .books
            .iter()
            .map(|(symbol, book)| {
                let exchange_timestamp = state.last_exchange.get(symbol).copied().unwrap_or(now);
                Output::Snapshot(snapshot(symbol, book, exchange_timestamp, now))
            })
            .collect()
    }

    pub fn reconcile(&mut self, symbols: &[String], now: DateTime<Utc>) -> Reconciled {
        let incoming: BTreeSet<&str> = symbols.iter().map(String::as_str).collect();
        let removed: Vec<String> = self
            .shards
            .keys()
            .filter(|symbol| !incoming.contains(symbol.as_str()))
            .cloned()
            .collect();

        let mut outputs = Vec::new();
        for symbol in removed {
            let Some(shard) = self.shards.remove(&symbol) else {
                continue;
            };
            let active = self.active_socket(shard);
            if let Some(mut book) = self.sockets[active].books.remove(&symbol) {
                outputs.extend(book.clear().into_iter().map(|(side, price_level)| {
                    Output::Delta(PriceLevelUpdateDomain {
                        backtest_timestamp: now,
                        exchange_timestamp: now,
                        price_level,
                        quantity: 0,
                        side,
                        symbol: symbol.clone(),
                    })
                }));
            }
            for socket in &mut self.sockets {
                socket.books.remove(&symbol);
                socket.last_exchange.remove(&symbol);
            }
        }

        for symbol in symbols {
            if self.shards.contains_key(symbol) {
                continue;
            }
            let shard = self.least_loaded_shard();
            self.shards.insert(symbol.clone(), shard);
        }

        let mut shards = vec![Vec::new(); self.shard_count];
        for (symbol, shard) in &self.shards {
            shards[*shard].push(symbol.clone());
        }

        Reconciled { outputs, shards }
    }

    fn least_loaded_shard(&self) -> usize {
        let mut loads = vec![0usize; self.shard_count];
        for shard in self.shards.values() {
            loads[*shard] += 1;
        }
        loads
            .into_iter()
            .enumerate()
            .min_by_key(|(_, load)| *load)
            .map(|(shard, _)| shard)
            .unwrap_or(0)
    }
}

fn snapshot(
    symbol: &str,
    book: &Book,
    exchange_timestamp: DateTime<Utc>,
    received_at: DateTime<Utc>,
) -> SnapshotDomain {
    SnapshotDomain {
        received_at,
        exchange_timestamp,
        bids: book
            .levels(crate::Side::Buy)
            .rev()
            .map(|(price_level, quantity)| SnapshotBidsEntryDomain {
                price_level,
                quantity,
            })
            .collect(),
        asks: book
            .levels(crate::Side::Sell)
            .map(|(price_level, quantity)| SnapshotAsksEntryDomain {
                price_level,
                quantity,
            })
            .collect(),
        symbol: symbol.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side;
    use crate::exchange::Level;

    fn timestamp(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis).unwrap()
    }

    fn snapshot_update(symbol: &str, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> Update {
        Update::Snapshot {
            symbol: symbol.into(),
            exchange_timestamp: timestamp(1_000),
            bids: bids
                .iter()
                .map(|(price, quantity)| Level {
                    price: *price,
                    quantity: *quantity,
                })
                .collect(),
            asks: asks
                .iter()
                .map(|(price, quantity)| Level {
                    price: *price,
                    quantity: *quantity,
                })
                .collect(),
        }
    }

    fn delta(symbol: &str, side: Side, price: i64, quantity: i64) -> Update {
        Update::Delta {
            symbol: symbol.into(),
            exchange_timestamp: timestamp(2_000),
            side,
            price,
            quantity,
        }
    }

    fn store_with(symbols: &[&str]) -> Store {
        store_with_redundancy(symbols, 1)
    }

    fn store_with_redundancy(symbols: &[&str], redundancy: usize) -> Store {
        let mut store = Store::new(2, redundancy);
        let symbols: Vec<String> = symbols.iter().map(|symbol| (*symbol).into()).collect();
        store.reconcile(&symbols, timestamp(0));
        store
    }

    fn only_snapshot(outputs: &[Output]) -> &SnapshotDomain {
        match outputs {
            [Output::Snapshot(snapshot)] => snapshot,
            other => panic!("expected one snapshot, got {other:?}"),
        }
    }

    fn only_delta(outputs: &[Output]) -> &PriceLevelUpdateDomain {
        match outputs {
            [Output::Delta(delta)] => delta,
            other => panic!("expected one delta, got {other:?}"),
        }
    }

    #[test]
    fn applies_snapshots_and_deltas() {
        let mut store = store_with(&["a"]);
        let applied = store.apply(
            snapshot_update("a", &[(100, 5)], &[(110, 7)]),
            timestamp(10),
        );
        let book = only_snapshot(&applied.outputs);
        assert_eq!(book.received_at, timestamp(10));
        assert_eq!(book.exchange_timestamp, timestamp(1_000));
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.bids[0].price_level, 100);
        assert_eq!(book.bids[0].quantity, 5);
        assert_eq!(book.asks[0].price_level, 110);

        let applied = store.apply(delta("a", Side::Buy, 100, 8), timestamp(20));
        let update = only_delta(&applied.outputs);
        assert_eq!(update.backtest_timestamp, timestamp(20));
        assert_eq!(update.exchange_timestamp, timestamp(2_000));
        assert_eq!(update.price_level, 100);
        assert_eq!(update.quantity, 8);
        assert_eq!(update.side, Side::Buy);

        let snapshots = store.snapshots(timestamp(30));
        let book = only_snapshot(&snapshots);
        assert_eq!(book.received_at, timestamp(30));
        assert_eq!(book.exchange_timestamp, timestamp(2_000));
        assert_eq!(book.bids[0].quantity, 8);
    }

    #[test]
    fn removes_levels_at_zero_quantity() {
        let mut store = store_with(&["a"]);
        store.apply(
            snapshot_update("a", &[(100, 5)], &[(110, 7)]),
            timestamp(10),
        );

        let applied = store.apply(delta("a", Side::Buy, 100, 0), timestamp(20));
        assert_eq!(only_delta(&applied.outputs).quantity, 0);
        let snapshots = store.snapshots(timestamp(30));
        assert!(only_snapshot(&snapshots).bids.is_empty());

        let applied = store.apply(delta("a", Side::Buy, 100, 0), timestamp(40));
        assert!(applied.outputs.is_empty());
    }

    #[test]
    fn ignores_duplicate_and_untracked_updates() {
        let mut store = store_with(&["a"]);
        store.apply(snapshot_update("a", &[(100, 5)], &[]), timestamp(10));

        let applied = store.apply(delta("a", Side::Buy, 100, 5), timestamp(20));
        assert!(applied.outputs.is_empty());
        assert_eq!(applied.resync, None);

        let applied = store.apply(delta("unknown", Side::Buy, 100, 5), timestamp(20));
        assert!(applied.outputs.is_empty());
        assert_eq!(applied.resync, None);

        let applied = store.apply(snapshot_update("unknown", &[(100, 5)], &[]), timestamp(20));
        assert!(applied.outputs.is_empty());
    }

    #[test]
    fn requests_a_resync_for_a_delta_without_a_snapshot() {
        let mut store = store_with(&["a"]);
        let shard = store.shard_of("a").unwrap();
        let socket = store.active_socket(shard);

        let applied = store.apply(delta("a", Side::Buy, 100, 5), timestamp(20));
        assert!(applied.outputs.is_empty());
        assert_eq!(
            applied.resync,
            Some(Resync {
                socket,
                symbol: "a".into()
            })
        );
        assert!(store.snapshots(timestamp(30)).is_empty());
    }

    #[test]
    fn drops_books_for_a_disconnected_shard() {
        let mut store = store_with(&["a", "b"]);
        let shard = store.shard_of("a").unwrap();
        let socket = store.active_socket(shard);
        store.apply(snapshot_update("a", &[(100, 5)], &[]), timestamp(10));
        store.apply(snapshot_update("b", &[(100, 5)], &[]), timestamp(10));

        store.disconnected(socket);

        let snapshots = store.snapshots(timestamp(20));
        assert_eq!(snapshots.len(), 1);
        assert_eq!(only_snapshot(&snapshots).symbol, "b");

        let applied = store.apply(delta("a", Side::Buy, 100, 6), timestamp(20));
        assert_eq!(
            applied.resync,
            Some(Resync {
                socket,
                symbol: "a".into()
            })
        );
    }

    #[test]
    fn standby_updates_are_silent() {
        let mut store = store_with_redundancy(&["a"], 2);
        let shard = store.shard_of("a").unwrap();
        let primary = store.socket_id(shard, 0);
        let standby = store.socket_id(shard, 1);

        let applied = store.apply_from(
            standby,
            snapshot_update("a", &[(100, 5)], &[]),
            timestamp(10),
        );
        assert!(applied.outputs.is_empty());
        assert!(store.snapshots(timestamp(20)).is_empty());

        let applied = store.apply_from(standby, delta("a", Side::Buy, 100, 6), timestamp(20));
        assert!(applied.outputs.is_empty());

        let applied = store.apply_from(
            primary,
            snapshot_update("a", &[(100, 5)], &[]),
            timestamp(30),
        );
        assert_eq!(only_snapshot(&applied.outputs).bids[0].quantity, 5);
    }

    #[test]
    fn switching_sources_emits_a_snapshot_from_the_standby() {
        let mut store = store_with_redundancy(&["a"], 2);
        let shard = store.shard_of("a").unwrap();
        let primary = store.socket_id(shard, 0);
        let standby = store.socket_id(shard, 1);

        store.apply_from(
            primary,
            snapshot_update("a", &[(100, 5)], &[(110, 7)]),
            timestamp(10),
        );
        store.apply_from(
            standby,
            snapshot_update("a", &[(100, 9)], &[(110, 8)]),
            timestamp(20),
        );

        let outputs = store.switch_source(shard, 1, timestamp(30));
        let snapshot = only_snapshot(&outputs);
        assert_eq!(snapshot.bids[0].quantity, 9);
        assert_eq!(snapshot.asks[0].quantity, 8);
        assert_eq!(snapshot.received_at, timestamp(30));
        assert_eq!(store.active_slot(shard), 1);

        let applied = store.apply_from(standby, delta("a", Side::Buy, 100, 10), timestamp(40));
        assert_eq!(only_delta(&applied.outputs).quantity, 10);
    }

    #[test]
    fn switching_to_the_current_source_is_a_noop() {
        let mut store = store_with_redundancy(&["a"], 2);
        let shard = store.shard_of("a").unwrap();
        assert!(store.switch_source(shard, 0, timestamp(10)).is_empty());
    }

    #[test]
    fn reconciles_additions_and_removals() {
        let mut store = store_with(&["a", "b"]);
        store.apply(
            snapshot_update("a", &[(100, 5)], &[(110, 7)]),
            timestamp(10),
        );
        store.apply(snapshot_update("b", &[(200, 5)], &[]), timestamp(10));

        let reconciled = store.reconcile(&["b".to_owned(), "c".to_owned()], timestamp(20));

        assert_eq!(store.len(), 2);
        assert!(store.shard_of("a").is_none());
        assert!(store.shard_of("c").is_some());
        assert_eq!(reconciled.outputs.len(), 2);
        let mut cleared: Vec<(Side, i64)> = reconciled
            .outputs
            .iter()
            .map(|output| {
                let delta = only_delta(std::slice::from_ref(output));
                assert_eq!(delta.quantity, 0);
                assert_eq!(delta.exchange_timestamp, timestamp(20));
                (delta.side, delta.price_level)
            })
            .collect();
        cleared.sort_by_key(|(side, price)| (*price, *side as u8));
        assert_eq!(cleared, vec![(Side::Buy, 100), (Side::Sell, 110)]);

        let total: usize = reconciled.shards.iter().map(Vec::len).sum();
        assert_eq!(total, 2);
        let snapshots = store.snapshots(timestamp(30));
        assert_eq!(snapshots.len(), 1);
        assert_eq!(only_snapshot(&snapshots).symbol, "b");
    }

    #[test]
    fn keeps_existing_shards_and_balances_new_symbols() {
        let mut store = Store::new(3, 1);
        let reconciled = store.reconcile(
            &[
                "a".to_owned(),
                "b".to_owned(),
                "c".to_owned(),
                "d".to_owned(),
            ],
            timestamp(0),
        );
        let before = store.symbols(0);
        assert_eq!(reconciled.shards.iter().map(Vec::len).max(), Some(2));

        let reconciled = store.reconcile(
            &[
                "a".to_owned(),
                "b".to_owned(),
                "c".to_owned(),
                "d".to_owned(),
                "e".to_owned(),
            ],
            timestamp(0),
        );
        assert_eq!(store.symbols(0), before);
        assert_eq!(reconciled.shards.iter().map(Vec::len).sum::<usize>(), 5);
    }

    #[test]
    fn reconcile_ignores_duplicate_symbols() {
        let mut store = Store::new(2, 1);
        let symbols = vec!["a".to_owned(), "a".to_owned(), "b".to_owned()];

        let reconciled = store.reconcile(&symbols, timestamp(0));
        assert_eq!(store.len(), 2);
        assert_eq!(reconciled.shards.iter().map(Vec::len).sum::<usize>(), 2);

        let reconciled = store.reconcile(&symbols, timestamp(0));
        assert!(reconciled.outputs.is_empty());
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn reconcile_with_no_symbols_clears_everything() {
        let mut store = store_with(&["a", "b"]);
        store.apply(snapshot_update("a", &[(100, 5)], &[]), timestamp(10));
        store.apply(snapshot_update("b", &[(200, 5)], &[]), timestamp(10));

        let reconciled = store.reconcile(&[], timestamp(20));
        assert!(store.is_empty());
        assert_eq!(reconciled.outputs.len(), 2);
        assert!(store.snapshots(timestamp(30)).is_empty());
    }
}
