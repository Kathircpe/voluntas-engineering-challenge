use std::{future::Future, sync::Arc, time::Duration};

use anyhow::Context;
use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval, timeout};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::exchange::Exchange;
use crate::output::Output;
use crate::socket::{self, Command, SocketEvent};
use crate::store::Store;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(1_800);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
const SOCKET_BUFFER: usize = 1_024;
const COMMAND_BUFFER: usize = 64;

pub async fn run(
    exchange: Arc<dyn Exchange>,
    config: &Config,
    output: &mpsc::Sender<Output>,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    info!(max_symbols = ?config.max_symbols, "discovering markets");
    tokio::pin!(shutdown);
    let symbols = tokio::select! {
        _ = &mut shutdown => {
            info!("shutting down");
            return Ok(());
        }
        result = discover(&exchange, config) => result?,
    };
    let shards = config
        .connections
        .max(symbols.len().div_ceil(config.symbols_per_connection));
    let redundancy = config.redundancy;
    info!(
        symbols = symbols.len(),
        shards,
        redundancy,
        sockets = shards * redundancy,
        "discovered markets"
    );

    let mut store = Store::new(shards, redundancy);
    let reconciled = store.reconcile(&symbols, Utc::now());
    write_outputs(output, reconciled.outputs).await?;
    let shard_symbols = reconciled.shards;

    let (events_tx, mut events) = mpsc::channel(SOCKET_BUFFER);
    let mut commands = Vec::with_capacity(store.socket_count());
    let mut tasks = JoinSet::new();

    for socket in 0..store.socket_count() {
        let (command_tx, command_rx) = mpsc::channel(COMMAND_BUFFER);
        commands.push(command_tx);
        let shard = store.socket_shard(socket);
        spawn_socket(
            &mut tasks,
            &exchange,
            config,
            shard,
            socket,
            shard_symbols[shard].clone(),
            events_tx.clone(),
            command_rx,
        );
    }

    let mut snapshots = interval(config.snapshot_interval);
    snapshots.set_missed_tick_behavior(MissedTickBehavior::Delay);
    snapshots.tick().await;
    let mut refresh = interval(config.refresh_interval);
    refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);
    refresh.tick().await;

    let mut discoveries = JoinSet::new();
    let mut last_data: Vec<Option<DateTime<Utc>>> = vec![None; store.socket_count()];
    let mut last_switch: Vec<Option<DateTime<Utc>>> = vec![None; store.shard_count()];

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutting down");
                break;
            }
            event = events.recv() => {
                let Some(event) = event else {
                    break;
                };
                match event {
                    SocketEvent::Connected { socket } => {
                        store.set_connected(socket, true);
                        last_data[socket] = Some(Utc::now());
                    }
                    SocketEvent::Updates { socket, received_at, updates } => {
                        store.set_connected(socket, true);
                        last_data[socket] = Some(received_at);
                        let shard = store.socket_shard(socket);
                        maybe_switch(
                            &mut store,
                            output,
                            shard,
                            socket,
                            &last_data,
                            &mut last_switch,
                            config,
                            received_at,
                        ).await?;
                        for update in updates {
                            let applied = store.apply_from(socket, update, received_at);
                            write_outputs(output, applied.outputs).await?;
                            if let Some(resync) = applied.resync {
                                let _ = commands[resync.socket]
                                    .send(Command::Resync(resync.symbol))
                                    .await;
                            }
                        }
                    }
                    SocketEvent::Disconnected { socket } => {
                        store.disconnected(socket);
                        last_data[socket] = None;
                        let shard = store.socket_shard(socket);
                        let slot = store.socket_slot(socket);
                        if store.active_slot(shard) == slot
                            && let Some(best) = best_standby(&store, shard, &last_data, slot)
                        {
                            let previous = store.active_slot(shard);
                            let outputs = store.switch_source(shard, best, Utc::now());
                            write_outputs(output, outputs).await?;
                            last_switch[shard] = Some(Utc::now());
                            info!(shard, from = previous, to = best, "switched websocket source");
                        }
                    }
                }
            }
            _ = snapshots.tick() => {
                write_outputs(output, store.snapshots(Utc::now())).await?;
            }
            _ = refresh.tick() => {
                if discoveries.is_empty() {
                    let exchange = Arc::clone(&exchange);
                    let config = config.clone();
                    discoveries.spawn(async move { discover(&exchange, &config).await });
                } else {
                    debug!("market refresh is already running");
                }
            }
            result = discoveries.join_next(), if !discoveries.is_empty() => {
                match result {
                    Some(Ok(Ok(symbols))) => {
                        let reconciled = store.reconcile(&symbols, Utc::now());
                        write_outputs(output, reconciled.outputs).await?;
                        for (shard, symbols) in reconciled.shards.into_iter().enumerate() {
                            let start = shard * redundancy;
                            for (socket, command) in
                                commands.iter().enumerate().skip(start).take(redundancy)
                            {
                                if command
                                    .send(Command::SetSymbols(symbols.clone()))
                                    .await
                                    .is_err()
                                {
                                    warn!(socket, "socket command channel closed");
                                }
                            }
                        }
                        info!(symbols = store.len(), "refreshed markets");
                    }
                    Some(Ok(Err(error))) => warn!(%error, "market refresh failed"),
                    Some(Err(error)) => warn!(%error, "market discovery task failed"),
                    None => {}
                }
            }
            joined = tasks.join_next(), if !tasks.is_empty() => {
                match joined {
                    Some(Ok(())) => warn!("socket task exited"),
                    Some(Err(error)) => warn!(%error, "socket task failed"),
                    None => {}
                }
                anyhow::bail!("a websocket task stopped unexpectedly");
            }
        }
    }

    for command in &commands {
        let _ = command.send(Command::Shutdown).await;
    }
    if timeout(SHUTDOWN_GRACE, async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

    Ok(())
}

fn best_standby(
    store: &Store,
    shard: usize,
    last_data: &[Option<DateTime<Utc>>],
    exclude: usize,
) -> Option<usize> {
    (0..store.redundancy())
        .filter(|slot| *slot != exclude)
        .filter(|slot| store.is_connected(store.socket_id(shard, *slot)))
        .max_by_key(|slot| last_data[store.socket_id(shard, *slot)])
}

#[allow(clippy::too_many_arguments)]
async fn maybe_switch(
    store: &mut Store,
    output: &mpsc::Sender<Output>,
    shard: usize,
    socket: usize,
    last_data: &[Option<DateTime<Utc>>],
    last_switch: &mut [Option<DateTime<Utc>>],
    config: &Config,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let slot = store.socket_slot(socket);
    if store.active_slot(shard) == slot {
        return Ok(());
    }
    let active = store.active_socket(shard);
    let switch = if !store.is_connected(active) || last_data[active].is_none() {
        true
    } else {
        let stall = now
            .signed_duration_since(last_data[active].unwrap())
            .num_milliseconds();
        stall >= config.source_stall_timeout.as_millis() as i64
            && !last_switch[shard].is_some_and(|last| {
                now.signed_duration_since(last).num_milliseconds()
                    < config.switch_cooldown.as_millis() as i64
            })
    };
    if !switch {
        return Ok(());
    }

    let previous = store.active_slot(shard);
    let outputs = store.switch_source(shard, slot, now);
    write_outputs(output, outputs).await?;
    last_switch[shard] = Some(now);
    info!(
        shard,
        from = previous,
        to = slot,
        "switched websocket source"
    );
    Ok(())
}

async fn discover(exchange: &Arc<dyn Exchange>, config: &Config) -> anyhow::Result<Vec<String>> {
    timeout(DISCOVERY_TIMEOUT, exchange.discover(config.max_symbols))
        .await
        .context("market discovery timed out")?
        .context("market discovery failed")
}

async fn write_outputs(sender: &mpsc::Sender<Output>, outputs: Vec<Output>) -> anyhow::Result<()> {
    for output in outputs {
        sender.send(output).await.context("output writer stopped")?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn spawn_socket(
    tasks: &mut JoinSet<()>,
    exchange: &Arc<dyn Exchange>,
    config: &Config,
    shard: usize,
    socket: usize,
    symbols: Vec<String>,
    events: mpsc::Sender<SocketEvent>,
    commands: mpsc::Receiver<Command>,
) {
    let exchange = Arc::clone(exchange);
    let settings = socket::Settings {
        ping_interval: config.ping_interval,
        idle_timeout: config.idle_timeout,
        data_timeout: config.data_timeout,
        subscribe_batch: config.subscribe_batch,
        symbol_idle_timeout: config.symbol_idle_timeout,
        symbol_resubscribe_cooldown: config.symbol_resubscribe_cooldown,
        subscription_check_interval: config.subscription_check_interval,
        reconnect_interval: config.reconnect_interval,
    };
    tasks.spawn(async move {
        socket::run(exchange, shard, socket, symbols, settings, events, commands).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::{Level, Update};
    use chrono::TimeZone;

    fn timestamp(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).unwrap()
    }

    fn snapshot(symbol: &str, quantity: i64) -> Update {
        Update::Snapshot {
            symbol: symbol.into(),
            exchange_timestamp: timestamp(1),
            bids: vec![Level {
                price: 100,
                quantity,
            }],
            asks: Vec::new(),
        }
    }

    #[test]
    fn chooses_the_freshest_connected_standby() {
        let mut store = Store::new(1, 3);
        store.set_connected(0, true);
        store.set_connected(1, true);
        store.set_connected(2, true);
        let last_data = [
            Some(timestamp(10)),
            Some(timestamp(30)),
            Some(timestamp(20)),
        ];

        assert_eq!(best_standby(&store, 0, &last_data, 0), Some(1));
        assert_eq!(best_standby(&store, 0, &last_data, 1), Some(2));

        store.set_connected(1, false);
        store.set_connected(2, false);
        assert_eq!(best_standby(&store, 0, &last_data, 0), None);
    }

    #[tokio::test]
    async fn switches_to_a_standby_and_emits_a_snapshot() {
        let config = Config::from_lookup(|_| None).unwrap();
        let mut store = Store::new(1, 2);
        store.reconcile(&["a".to_owned()], timestamp(0));
        store.set_connected(0, false);
        store.set_connected(1, true);
        store.apply_from(1, snapshot("a", 5), timestamp(10));

        let now = timestamp(20);
        let last_data = [None, Some(now)];
        let mut last_switch = [None];
        let (tx, mut rx) = mpsc::channel(8);

        maybe_switch(
            &mut store,
            &tx,
            0,
            1,
            &last_data,
            &mut last_switch,
            &config,
            now,
        )
        .await
        .unwrap();

        assert_eq!(store.active_slot(0), 1);
        assert_eq!(last_switch[0], Some(now));
        let Output::Snapshot(snapshot) = rx.try_recv().unwrap() else {
            panic!("expected a snapshot");
        };
        assert_eq!(snapshot.symbol, "a");
        assert_eq!(snapshot.bids[0].quantity, 5);
    }
}
