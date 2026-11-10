use std::collections::BTreeMap;

use crate::Side;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl Book {
    pub fn replace<B, A>(&mut self, bids: B, asks: A)
    where
        B: IntoIterator<Item = (i64, i64)>,
        A: IntoIterator<Item = (i64, i64)>,
    {
        self.bids = bids
            .into_iter()
            .filter(|(_, quantity)| *quantity > 0)
            .collect();
        self.asks = asks
            .into_iter()
            .filter(|(_, quantity)| *quantity > 0)
            .collect();
    }

    pub fn set(&mut self, side: Side, price: i64, quantity: i64) -> bool {
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if quantity <= 0 {
            levels.remove(&price).is_some()
        } else {
            levels.insert(price, quantity) != Some(quantity)
        }
    }

    pub fn levels(&self, side: Side) -> impl DoubleEndedIterator<Item = (i64, i64)> + '_ {
        let levels = match side {
            Side::Buy => self.bids.iter(),
            Side::Sell => self.asks.iter(),
        };
        levels.map(|(price, quantity)| (*price, *quantity))
    }

    pub fn clear(&mut self) -> Vec<(Side, i64)> {
        let mut removed = Vec::with_capacity(self.bids.len() + self.asks.len());
        removed.extend(self.bids.keys().map(|price| (Side::Buy, *price)));
        removed.extend(self.asks.keys().map(|price| (Side::Sell, *price)));
        self.bids.clear();
        self.asks.clear();
        removed
    }

    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_levels_and_drops_zero_quantities() {
        let mut book = Book::default();
        book.replace([(100, 5), (80, 1), (90, 0)], [(110, 7), (120, 3)]);

        assert_eq!(
            book.levels(Side::Buy).collect::<Vec<_>>(),
            vec![(80, 1), (100, 5)]
        );
        assert_eq!(
            book.levels(Side::Sell).collect::<Vec<_>>(),
            vec![(110, 7), (120, 3)]
        );
        assert_eq!(
            book.levels(Side::Buy).rev().collect::<Vec<_>>(),
            vec![(100, 5), (80, 1)]
        );
    }

    #[test]
    fn set_reports_changes() {
        let mut book = Book::default();
        assert!(book.set(Side::Buy, 100, 5));
        assert!(!book.set(Side::Buy, 100, 5));
        assert!(book.set(Side::Buy, 100, 6));
        assert!(book.set(Side::Buy, 100, 0));
        assert!(!book.set(Side::Buy, 100, 0));
        assert!(book.is_empty());
    }

    #[test]
    fn clear_returns_every_level() {
        let mut book = Book::default();
        book.replace([(100, 5)], [(110, 7)]);
        let mut removed = book.clear();
        removed.sort_by_key(|(side, price)| (*price, *side as u8));
        assert_eq!(removed, vec![(Side::Buy, 100), (Side::Sell, 110)]);
        assert!(book.is_empty());
    }
}
