use std::{collections::BTreeMap, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, warn};

use crate::exchange::{Exchange, Update};

const MAX_BACKOFF: Duration = Duration::from_secs(30);
const RESYNC_COOLDOWN: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const BATCH_DELAY: Duration = Duration::from_millis(100);
const EMPTY_SHARD_WAIT: Duration = Duration::from_secs(60);

pub enum Command {
    SetSymbols(Vec<String>),
    Resync(String),
    Shutdown,
}

pub enum SocketEvent {
    Connected {
        socket: usize,
    },
    Updates {
        socket: usize,
        received_at: DateTime<Utc>,
        updates: Vec<Update>,
    },
    Disconnected {
        socket: usize,
    },
}

enum Session {
    Ended { connected: bool },
    Reconnect,
    Shutdown,
}

#[derive(Clone, Copy)]
pub struct Settings {
    pub ping_interval: Duration,
    pub idle_timeout: Duration,
    pub data_timeout: Duration,
    pub subscribe_batch: usize,
    pub symbol_idle_timeout: Duration,
    pub symbol_resubscribe_cooldown: Duration,
    pub subscription_check_interval: Duration,
    pub reconnect_interval: Duration,
}

pub async fn run(
    exchange: Arc<dyn Exchange>,
    shard: usize,
    socket: usize,
    mut symbols: Vec<String>,
    settings: Settings,
    events: mpsc::Sender<SocketEvent>,
    mut commands: mpsc::Receiver<Command>,
) {
    let mut backoff = Duration::from_secs(1);

    loop {
        if symbols.is_empty() {
            if wait(&mut commands, &mut symbols, EMPTY_SHARD_WAIT).await {
                return;
            }
            continue;
        }

        let (connected, delay) = match session(
            exchange.as_ref(),
            shard,
            socket,
            &mut symbols,
            settings,
            &events,
            &mut commands,
        )
        .await
        {
            Session::Shutdown => return,
            Session::Reconnect => (true, Duration::from_secs(1)),
            Session::Ended { connected } => (connected, backoff),
        };

        if events
            .send(SocketEvent::Disconnected { socket })
            .await
            .is_err()
        {
            return;
        }

        if wait(&mut commands, &mut symbols, delay).await {
            return;
        }
        backoff = if connected {
            Duration::from_secs(1)
        } else {
            (backoff * 2).min(MAX_BACKOFF)
        };
    }
}

async fn session(
    exchange: &dyn Exchange,
    shard: usize,
    socket: usize,
    symbols: &mut Vec<String>,
    settings: Settings,
    events: &mpsc::Sender<SocketEvent>,
    commands: &mut mpsc::Receiver<Command>,
) -> Session {
    let started = Instant::now();
    let stream = match timeout(
        CONNECT_TIMEOUT,
        tokio_tungstenite::connect_async(exchange.websocket_url()),
    )
    .await
    {
        Ok(Ok((stream, _))) => stream,
        Ok(Err(error)) => {
            warn!(shard, socket, %error, "websocket connection failed");
            return Session::Ended { connected: false };
        }
        Err(_) => {
            warn!(shard, socket, "websocket connection timed out");
            return Session::Ended { connected: false };
        }
    };
    if let Err(error) = stream.get_ref().get_ref().set_nodelay(true) {
        debug!(shard, socket, %error, "failed to enable TCP_NODELAY");
    }
    debug!(
        shard,
        socket,
        latency = ?started.elapsed(),
        "websocket connected"
    );

    let (mut sink, mut stream) = stream.split();
    if !subscribe_initial(
        exchange,
        shard,
        socket,
        symbols,
        settings.subscribe_batch,
        &mut sink,
    )
    .await
    {
        return Session::Ended { connected: false };
    }
    if events
        .send(SocketEvent::Connected { socket })
        .await
        .is_err()
    {
        return Session::Shutdown;
    }

    let mut ping = interval(settings.ping_interval);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ping.tick().await;
    let mut checks = interval(settings.subscription_check_interval);
    checks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    checks.tick().await;
    let mut idle = Box::pin(sleep(settings.idle_timeout));
    let mut data_idle = Box::pin(sleep(settings.data_timeout));
    let mut reconnect = Box::pin(sleep(
        settings.reconnect_interval + reconnect_jitter(settings.reconnect_interval, socket),
    ));
    let mut resyncs: BTreeMap<String, Instant> = BTreeMap::new();
    let mut last_update: BTreeMap<String, Instant> = symbols
        .iter()
        .map(|symbol| (symbol.clone(), Instant::now()))
        .collect();

    loop {
        tokio::select! {
            message = stream.next() => {
                let message = match message {
                    Some(Ok(message)) => message,
                    Some(Err(error)) => {
                        warn!(shard, socket, %error, "websocket read failed");
                        return Session::Ended { connected: true };
                    }
                    None => return Session::Ended { connected: true },
                };

                match message {
                    Message::Text(text) => {
                        idle.as_mut().reset(Instant::now() + settings.idle_timeout);
                        let received_at = Utc::now();
                        match handle_payload(exchange, shard, socket, text.as_str()).await {
                            Ok(updates) if !updates.is_empty() => {
                                record_updates(&updates, &mut last_update);
                                if events.send(SocketEvent::Updates { socket, received_at, updates }).await.is_err() {
                                    return Session::Shutdown;
                                }
                                data_idle.as_mut().reset(Instant::now() + settings.data_timeout);
                            }
                            Ok(_) => {}
                            Err(session) => return session,
                        }
                    }
                    Message::Binary(bytes) => {
                        idle.as_mut().reset(Instant::now() + settings.idle_timeout);
                        let Ok(payload) = std::str::from_utf8(&bytes) else {
                            warn!(shard, socket, "websocket sent invalid utf-8");
                            continue;
                        };
                        let received_at = Utc::now();
                        match handle_payload(exchange, shard, socket, payload).await {
                            Ok(updates) if !updates.is_empty() => {
                                record_updates(&updates, &mut last_update);
                                if events.send(SocketEvent::Updates { socket, received_at, updates }).await.is_err() {
                                    return Session::Shutdown;
                                }
                                data_idle.as_mut().reset(Instant::now() + settings.data_timeout);
                            }
                            Ok(_) => {}
                            Err(session) => return session,
                        }
                    }
                    Message::Ping(payload) => {
                        idle.as_mut().reset(Instant::now() + settings.idle_timeout);
                        if sink.send(Message::Pong(payload)).await.is_err() {
                            return Session::Ended { connected: true };
                        }
                    }
                    Message::Pong(_) => idle.as_mut().reset(Instant::now() + settings.idle_timeout),
                    Message::Close(_) => return Session::Ended { connected: true },
                    Message::Frame(_) => {}
                }
            }
            _ = ping.tick() => {
                let Some(heartbeat) = exchange.heartbeat() else {
                    continue;
                };
                if let Err(error) = sink.send(Message::Text(heartbeat.into())).await {
                    warn!(shard, socket, %error, "failed to send heartbeat");
                    return Session::Ended { connected: true };
                }
            }
            _ = checks.tick() => {
                let now = Instant::now();
                let quiet = quiet_symbols(
                    symbols,
                    &last_update,
                    &resyncs,
                    now,
                    settings.symbol_idle_timeout,
                    settings.symbol_resubscribe_cooldown,
                );
                if quiet.is_empty() {
                    continue;
                }
                for symbol in &quiet {
                    resyncs.insert(symbol.clone(), now);
                }
                if !resubscribe(exchange, shard, socket, &quiet, settings.subscribe_batch, &mut sink).await {
                    return Session::Ended { connected: true };
                }
                debug!(shard, socket, symbols = quiet.len(), "resubscribed quiet symbols");
            }
            _ = &mut reconnect => {
                debug!(shard, socket, "reconnecting on schedule");
                return Session::Reconnect;
            }
            _ = &mut idle => {
                warn!(shard, socket, "websocket went idle");
                return Session::Ended { connected: true };
            }
            _ = &mut data_idle => {
                warn!(shard, socket, "websocket stopped delivering market data");
                return Session::Ended { connected: true };
            }
            command = commands.recv() => {
                match command {
                    None | Some(Command::Shutdown) => {
                        let _ = sink.send(Message::Close(None)).await;
                        return Session::Shutdown;
                    }
                    Some(Command::SetSymbols(next)) => {
                        if !replace_symbols(exchange, shard, socket, symbols, next, settings.subscribe_batch, &mut sink).await {
                            return Session::Ended { connected: true };
                        }
                        let now = Instant::now();
                        last_update.retain(|symbol, _| symbols.contains(symbol));
                        for symbol in symbols.iter() {
                            last_update.entry(symbol.clone()).or_insert(now);
                        }
                        resyncs.retain(|symbol, _| symbols.contains(symbol));
                        if symbols.is_empty() {
                            let _ = sink.send(Message::Close(None)).await;
                            return Session::Ended { connected: true };
                        }
                    }
                    Some(Command::Resync(symbol)) => {
                        if !symbols.contains(&symbol) {
                            resyncs.remove(&symbol);
                            continue;
                        }
                        let now = Instant::now();
                        if resyncs
                            .get(&symbol)
                            .is_some_and(|last| now.duration_since(*last) < RESYNC_COOLDOWN)
                        {
                            continue;
                        }
                        resyncs.insert(symbol.clone(), now);
                        if !resubscribe(exchange, shard, socket, std::slice::from_ref(&symbol), settings.subscribe_batch, &mut sink).await {
                            return Session::Ended { connected: true };
                        }
                        debug!(shard, socket, symbol = %symbol, "requested a fresh snapshot");
                    }
                }
            }
        }
    }
}

async fn handle_payload(
    exchange: &dyn Exchange,
    shard: usize,
    socket: usize,
    payload: &str,
) -> Result<Vec<Update>, Session> {
    let payload = payload.trim();
    if payload == "PONG" {
        return Ok(Vec::new());
    }
    if payload == "INVALID OPERATION" {
        warn!(shard, socket, "server rejected an operation");
        return Ok(Vec::new());
    }

    match exchange.decode(payload) {
        Ok(updates) => Ok(updates),
        Err(error) => {
            let preview: String = payload.chars().take(160).collect();
            warn!(
                shard,
                socket,
                len = payload.len(),
                preview,
                %error,
                "discarding malformed websocket message"
            );
            Ok(Vec::new())
        }
    }
}

fn record_updates(updates: &[Update], last_update: &mut BTreeMap<String, Instant>) {
    let now = Instant::now();
    for update in updates {
        last_update.insert(update.symbol().to_owned(), now);
    }
}

fn quiet_symbols(
    symbols: &[String],
    last_update: &BTreeMap<String, Instant>,
    resyncs: &BTreeMap<String, Instant>,
    now: Instant,
    idle_timeout: Duration,
    cooldown: Duration,
) -> Vec<String> {
    symbols
        .iter()
        .filter(|symbol| {
            last_update
                .get(*symbol)
                .is_none_or(|last| now.duration_since(*last) >= idle_timeout)
                && resyncs
                    .get(*symbol)
                    .is_none_or(|last| now.duration_since(*last) >= cooldown)
        })
        .cloned()
        .collect()
}

fn reconnect_jitter(interval: Duration, socket: usize) -> Duration {
    let max = (interval.as_millis() / 10).min(30_000) as u64;
    if max == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis((socket as u64 * 7_919) % max)
}

async fn subscribe_initial<S>(
    exchange: &dyn Exchange,
    shard: usize,
    socket: usize,
    symbols: &[String],
    batch: usize,
    sink: &mut S,
) -> bool
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let mut chunks = symbols.chunks(batch);
    let Some(first) = chunks.next() else {
        return true;
    };
    if let Some(frame) = exchange.subscribe(first)
        && let Err(error) = sink.send(Message::Text(frame.into())).await
    {
        warn!(shard, socket, %error, "failed to subscribe");
        return false;
    }
    for chunk in chunks {
        sleep(BATCH_DELAY).await;
        if let Some(frame) = exchange.add(chunk)
            && let Err(error) = sink.send(Message::Text(frame.into())).await
        {
            warn!(shard, socket, %error, "failed to subscribe");
            return false;
        }
    }
    true
}

async fn replace_symbols<S>(
    exchange: &dyn Exchange,
    shard: usize,
    socket: usize,
    symbols: &mut Vec<String>,
    next: Vec<String>,
    batch: usize,
    sink: &mut S,
) -> bool
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let current: std::collections::BTreeSet<String> = symbols.iter().cloned().collect();
    let next: std::collections::BTreeSet<String> = next.into_iter().collect();
    let add: Vec<String> = next.difference(&current).cloned().collect();
    let remove: Vec<String> = current.difference(&next).cloned().collect();

    if !send_batches(exchange, shard, socket, &remove, batch, sink, true).await {
        return false;
    }
    if !send_batches(exchange, shard, socket, &add, batch, sink, false).await {
        return false;
    }

    *symbols = next.into_iter().collect();
    debug!(
        shard,
        socket,
        added = add.len(),
        removed = remove.len(),
        total = symbols.len(),
        "updated subscriptions"
    );
    true
}

async fn resubscribe<S>(
    exchange: &dyn Exchange,
    shard: usize,
    socket: usize,
    symbols: &[String],
    batch: usize,
    sink: &mut S,
) -> bool
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    if !send_batches(exchange, shard, socket, symbols, batch, sink, true).await {
        return false;
    }
    send_batches(exchange, shard, socket, symbols, batch, sink, false).await
}

async fn send_batches<S>(
    exchange: &dyn Exchange,
    shard: usize,
    socket: usize,
    symbols: &[String],
    batch: usize,
    sink: &mut S,
    remove: bool,
) -> bool
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    for (index, chunk) in symbols.chunks(batch).enumerate() {
        if index > 0 {
            sleep(BATCH_DELAY).await;
        }
        let frame = if remove {
            exchange.remove(chunk)
        } else {
            exchange.add(chunk)
        };
        if let Some(frame) = frame
            && let Err(error) = sink.send(Message::Text(frame.into())).await
        {
            warn!(shard, socket, %error, "failed to update subscriptions");
            return false;
        }
    }
    true
}

async fn wait(
    commands: &mut mpsc::Receiver<Command>,
    symbols: &mut Vec<String>,
    delay: Duration,
) -> bool {
    let timer = sleep(delay);
    tokio::pin!(timer);

    loop {
        tokio::select! {
            _ = &mut timer => return false,
            command = commands.recv() => match command {
                None | Some(Command::Shutdown) => return true,
                Some(Command::SetSymbols(next)) => {
                    *symbols = next;
                    if !symbols.is_empty() {
                        return false;
                    }
                }
                Some(Command::Resync(_)) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbols() -> Vec<String> {
        vec!["a".into(), "b".into(), "c".into()]
    }

    #[test]
    fn finds_symbols_idle_past_the_threshold() {
        let now = Instant::now();
        let symbols = symbols();
        let mut last_update = BTreeMap::new();
        last_update.insert("a".to_owned(), now);
        last_update.insert("b".to_owned(), now - Duration::from_secs(120));

        let quiet = quiet_symbols(
            &symbols,
            &last_update,
            &BTreeMap::new(),
            now,
            Duration::from_secs(60),
            Duration::from_secs(10),
        );
        assert_eq!(quiet, vec!["b".to_owned(), "c".to_owned()]);
    }

    #[test]
    fn skips_symbols_that_were_recently_resubscribed() {
        let now = Instant::now();
        let symbols = symbols();
        let mut last_update = BTreeMap::new();
        last_update.insert("a".to_owned(), now - Duration::from_secs(120));
        let mut resyncs = BTreeMap::new();
        resyncs.insert("a".to_owned(), now - Duration::from_secs(5));

        let quiet = quiet_symbols(
            &symbols,
            &last_update,
            &resyncs,
            now,
            Duration::from_secs(60),
            Duration::from_secs(10),
        );
        assert_eq!(quiet, vec!["b".to_owned(), "c".to_owned()]);
    }

    #[test]
    fn allows_resubscription_after_the_cooldown() {
        let now = Instant::now();
        let symbols = vec!["a".to_owned()];
        let mut last_update = BTreeMap::new();
        last_update.insert("a".to_owned(), now - Duration::from_secs(120));
        let mut resyncs = BTreeMap::new();
        resyncs.insert("a".to_owned(), now - Duration::from_secs(30));

        let quiet = quiet_symbols(
            &symbols,
            &last_update,
            &resyncs,
            now,
            Duration::from_secs(60),
            Duration::from_secs(10),
        );
        assert_eq!(quiet, vec!["a".to_owned()]);
    }
}
