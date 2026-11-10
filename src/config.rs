use std::{path::PathBuf, time::Duration};

use anyhow::{Context, bail};

const DEFAULT_MAX_SYMBOLS: Option<usize> = None;

#[derive(Debug, Clone)]
pub struct Config {
    pub connections: usize,
    pub redundancy: usize,
    pub symbols_per_connection: usize,
    pub subscribe_batch: usize,
    pub snapshot_interval: Duration,
    pub refresh_interval: Duration,
    pub ping_interval: Duration,
    pub idle_timeout: Duration,
    pub data_timeout: Duration,
    pub symbol_idle_timeout: Duration,
    pub symbol_resubscribe_cooldown: Duration,
    pub subscription_check_interval: Duration,
    pub reconnect_interval: Duration,
    pub source_stall_timeout: Duration,
    pub switch_cooldown: Duration,
    pub max_symbols: Option<usize>,
    pub output: PathBuf,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let connections = number(&lookup, "PM_CONNECTIONS", 8)?;
        if connections == 0 {
            bail!("PM_CONNECTIONS must be at least 1");
        }
        let redundancy = number(&lookup, "PM_REDUNDANCY", 2)?;
        if redundancy == 0 {
            bail!("PM_REDUNDANCY must be at least 1");
        }
        let symbols_per_connection = number(&lookup, "PM_SYMBOLS_PER_CONNECTION", 5_000)?;
        if symbols_per_connection == 0 {
            bail!("PM_SYMBOLS_PER_CONNECTION must be at least 1");
        }
        let subscribe_batch = number(&lookup, "PM_SUBSCRIBE_BATCH", 500)?;
        if subscribe_batch == 0 {
            bail!("PM_SUBSCRIBE_BATCH must be at least 1");
        }

        let snapshot_interval = seconds(&lookup, "PM_SNAPSHOT_SECS", 60)?;
        let refresh_interval = seconds(&lookup, "PM_REFRESH_SECS", 300)?;
        let ping_interval = seconds(&lookup, "PM_PING_SECS", 10)?;
        let idle_timeout = seconds(&lookup, "PM_IDLE_SECS", 30)?;
        let data_timeout = seconds(&lookup, "PM_DATA_IDLE_SECS", 120)?;
        let symbol_idle_timeout = seconds(&lookup, "PM_SYMBOL_IDLE_SECS", 300)?;
        let symbol_resubscribe_cooldown = seconds(&lookup, "PM_SYMBOL_RESUBSCRIBE_SECS", 60)?;
        let subscription_check_interval = seconds(&lookup, "PM_SUBSCRIPTION_CHECK_SECS", 30)?;
        let reconnect_interval = seconds(&lookup, "PM_RECONNECT_SECS", 3_600)?;
        let source_stall_timeout = seconds(&lookup, "PM_SOURCE_STALL_SECS", 10)?;
        let switch_cooldown = seconds(&lookup, "PM_SWITCH_COOLDOWN_SECS", 30)?;
        let max_symbols = match lookup("PM_MAX_SYMBOLS") {
            Some(value) if value.trim().eq_ignore_ascii_case("all") => None,
            Some(value) if !value.trim().is_empty() => {
                let max = number(&lookup, "PM_MAX_SYMBOLS", 0)?;
                if max == 0 {
                    bail!("PM_MAX_SYMBOLS must be positive or 'all'");
                }
                Some(max)
            }
            _ => DEFAULT_MAX_SYMBOLS,
        };
        let output = lookup("PM_OUTPUT")
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/dev/stdout"));

        Ok(Self {
            connections,
            redundancy,
            symbols_per_connection,
            subscribe_batch,
            snapshot_interval,
            refresh_interval,
            ping_interval,
            idle_timeout,
            data_timeout,
            symbol_idle_timeout,
            symbol_resubscribe_cooldown,
            subscription_check_interval,
            reconnect_interval,
            source_stall_timeout,
            switch_cooldown,
            max_symbols,
            output,
        })
    }
}

fn number(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
    default: usize,
) -> anyhow::Result<usize> {
    match lookup(key) {
        Some(value) => value
            .trim()
            .parse()
            .with_context(|| format!("{key} must be a non-negative integer")),
        None => Ok(default),
    }
}

fn seconds(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
    default: u64,
) -> anyhow::Result<Duration> {
    let seconds = match lookup(key) {
        Some(value) => value
            .trim()
            .parse()
            .with_context(|| format!("{key} must be a positive integer"))?,
        None => default,
    };
    if seconds == 0 {
        bail!("{key} must be greater than zero");
    }
    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn config(values: &[(&str, &str)]) -> anyhow::Result<Config> {
        let values: HashMap<String, String> = values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        Config::from_lookup(|key| values.get(key).cloned())
    }

    #[test]
    fn uses_defaults() {
        let config = config(&[]).unwrap();
        assert_eq!(config.connections, 8);
        assert_eq!(config.redundancy, 2);
        assert_eq!(config.symbols_per_connection, 5_000);
        assert_eq!(config.subscribe_batch, 500);
        assert_eq!(config.snapshot_interval, Duration::from_secs(60));
        assert_eq!(config.data_timeout, Duration::from_secs(120));
        assert_eq!(config.symbol_idle_timeout, Duration::from_secs(300));
        assert_eq!(config.reconnect_interval, Duration::from_secs(3_600));
        assert_eq!(config.output, PathBuf::from("/dev/stdout"));
        assert_eq!(config.max_symbols, DEFAULT_MAX_SYMBOLS);
    }

    #[test]
    fn reads_overrides() {
        let config = config(&[
            ("PM_CONNECTIONS", "2"),
            ("PM_REDUNDANCY", "3"),
            ("PM_SYMBOLS_PER_CONNECTION", "50"),
            ("PM_SUBSCRIBE_BATCH", "25"),
            ("PM_SNAPSHOT_SECS", "5"),
            ("PM_DATA_IDLE_SECS", "90"),
            ("PM_SYMBOL_IDLE_SECS", "120"),
            ("PM_RECONNECT_SECS", "600"),
            ("PM_SOURCE_STALL_SECS", "5"),
            ("PM_MAX_SYMBOLS", "10"),
            ("PM_OUTPUT", "/tmp/out.sbe"),
        ])
        .unwrap();
        assert_eq!(config.connections, 2);
        assert_eq!(config.redundancy, 3);
        assert_eq!(config.symbols_per_connection, 50);
        assert_eq!(config.subscribe_batch, 25);
        assert_eq!(config.snapshot_interval, Duration::from_secs(5));
        assert_eq!(config.data_timeout, Duration::from_secs(90));
        assert_eq!(config.symbol_idle_timeout, Duration::from_secs(120));
        assert_eq!(config.reconnect_interval, Duration::from_secs(600));
        assert_eq!(config.source_stall_timeout, Duration::from_secs(5));
        assert_eq!(config.max_symbols, Some(10));
        assert_eq!(config.output, PathBuf::from("/tmp/out.sbe"));
    }

    #[test]
    fn treats_blank_values_as_defaults() {
        let config = config(&[("PM_MAX_SYMBOLS", ""), ("PM_OUTPUT", "  ")]).unwrap();
        assert_eq!(config.max_symbols, DEFAULT_MAX_SYMBOLS);
        assert_eq!(config.output, PathBuf::from("/dev/stdout"));
    }

    #[test]
    fn accepts_all_symbols() {
        let config = config(&[("PM_MAX_SYMBOLS", "all")]).unwrap();
        assert!(config.max_symbols.is_none());
    }

    #[test]
    fn rejects_invalid_values() {
        assert!(config(&[("PM_CONNECTIONS", "0")]).is_err());
        assert!(config(&[("PM_CONNECTIONS", "x")]).is_err());
        assert!(config(&[("PM_REDUNDANCY", "0")]).is_err());
        assert!(config(&[("PM_SYMBOLS_PER_CONNECTION", "0")]).is_err());
        assert!(config(&[("PM_SUBSCRIBE_BATCH", "0")]).is_err());
        assert!(config(&[("PM_SNAPSHOT_SECS", "0")]).is_err());
        assert!(config(&[("PM_DATA_IDLE_SECS", "0")]).is_err());
        assert!(config(&[("PM_SYMBOL_IDLE_SECS", "0")]).is_err());
        assert!(config(&[("PM_MAX_SYMBOLS", "0")]).is_err());
        assert!(config(&[("PM_MAX_SYMBOLS", "x")]).is_err());
    }
}
