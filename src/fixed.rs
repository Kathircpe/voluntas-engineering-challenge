use anyhow::{Context, bail};

pub const SCALE: i64 = 1_000_000;
const PLACES: usize = 6;

pub fn parse(value: &str) -> anyhow::Result<i64> {
    let text = value.trim();
    if text.is_empty() {
        bail!("empty decimal");
    }
    if text.starts_with('-') || text.starts_with('+') {
        bail!("signed decimal is not supported: {value}");
    }

    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    if whole.is_empty() && fraction.is_empty() {
        bail!("invalid decimal: {value}");
    }
    if !whole.bytes().all(|b| b.is_ascii_digit()) || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        bail!("invalid decimal: {value}");
    }
    if fraction.len() > PLACES {
        bail!("decimal has more than {PLACES} places: {value}");
    }

    let whole: i64 = if whole.is_empty() {
        0
    } else {
        whole
            .parse()
            .with_context(|| format!("decimal is out of range: {value}"))?
    };
    let mut padded = fraction.to_owned();
    padded.extend(std::iter::repeat_n('0', PLACES - padded.len()));
    let fraction: i64 = padded
        .parse()
        .with_context(|| format!("invalid decimal: {value}"))?;

    whole
        .checked_mul(SCALE)
        .and_then(|scaled| scaled.checked_add(fraction))
        .with_context(|| format!("decimal is out of range: {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_whole_numbers() {
        assert_eq!(parse("0").unwrap(), 0);
        assert_eq!(parse("1").unwrap(), 1_000_000);
        assert_eq!(parse("42").unwrap(), 42_000_000);
    }

    #[test]
    fn parses_fractions() {
        assert_eq!(parse("0.08").unwrap(), 80_000);
        assert_eq!(parse("0.5").unwrap(), 500_000);
        assert_eq!(parse(".5").unwrap(), 500_000);
        assert_eq!(parse("10.").unwrap(), 10_000_000);
        assert_eq!(parse("33343.4").unwrap(), 33_343_400_000);
        assert_eq!(parse("219.217767").unwrap(), 219_217_767);
        assert_eq!(parse(" 1.25 ").unwrap(), 1_250_000);
    }

    #[test]
    fn rejects_bad_input() {
        for value in ["", " ", ".", "abc", "1.2.3", "-1", "+1", "1e6", "1.1234567"] {
            assert!(parse(value).is_err(), "{value}");
        }
    }

    #[test]
    fn rejects_overflow() {
        assert!(parse("9223372036855").is_err());
    }
}
