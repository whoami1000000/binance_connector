use chrono::{MappedLocalTime, TimeZone, Utc};
use num::Zero;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::mem;

#[derive(Debug)]
pub enum OrderBookError {
    ZeroDepth,
    SnapshotExists(u64, u64),
    SnapshotTooOld(u64, u64),
    SnapshotTooEarly(u64),
    UpdateGaps(u64, u64),
}

impl Display for OrderBookError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderBookError::ZeroDepth => write!(f, "zero depth"),
            OrderBookError::SnapshotExists(last_update_id, snap_id) => {
                write!(f, "SnapshotExists [last_update_id={}][snap_id={}]", last_update_id, snap_id)
            }
            OrderBookError::SnapshotTooOld(first_update_id, snap_id) => {
                write!(f, "SnapshotTooOld [first_update_id={}][snap_id={}]", first_update_id, snap_id)
            }
            OrderBookError::SnapshotTooEarly(snap_id) => {
                write!(f, "snapshot [snap_id={}] too early", snap_id)
            }
            OrderBookError::UpdateGaps(last_update_id, first_update_id) => {
                write!(f, "UpdateGaps [last_update_id={}][first_update_id={}]", last_update_id, first_update_id)
            }
        }
    }
}

impl std::error::Error for OrderBookError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Update<P, Q> {
    #[serde(alias = "U")]
    first_update_id: u64,

    #[serde(alias = "u")]
    final_update_id: u64,

    #[serde(alias = "E")]
    event_time: i64,

    #[serde(alias = "b")]
    bids: Vec<(P, Q)>,

    #[serde(alias = "a")]
    asks: Vec<(P, Q)>,
}

#[derive(Debug, Deserialize)]
pub struct Snapshot<P, Q> {
    #[serde(alias = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<(P, Q)>,
    asks: Vec<(P, Q)>,
}

#[derive(Debug, Clone)]
pub struct OrderBook<P, Q> {
    symbol: String,
    depth: usize,
    has_snapshot: bool,
    snapshot_id: u64,
    last_update_id: u64,
    update_time: i64,
    updates: Vec<Update<P, Q>>,
    bids: BTreeMap<P, Q>,
    asks: BTreeMap<P, Q>,
}

impl<P: Ord + Clone, Q: Zero> OrderBook<P, Q> {
    pub fn build(symbol: &str, depth: usize) -> Result<Self, OrderBookError> {
        if depth == 0 {
            return Err(OrderBookError::ZeroDepth);
        }

        Ok(Self {
            symbol: symbol.to_string(),
            depth,
            has_snapshot: false,
            snapshot_id: 0,
            last_update_id: 0,
            update_time: 0,
            updates: vec![],
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        })
    }

    pub fn has_snapshot(&self) -> bool {
        self.has_snapshot
    }

    pub fn last_update_id(&self) -> u64 {
        self.last_update_id
    }

    pub fn best_price(&self) -> Option<(P, P)> {
        if self.bids.is_empty() || self.asks.is_empty() {
            return None;
        }

        Some((self.bids.last_key_value().unwrap().0.clone(), self.asks.first_key_value().unwrap().0.clone()))
    }

    pub fn process_update(&mut self, update: Update<P, Q>) -> Result<(), OrderBookError> {
        if self.has_snapshot {
            self.apply_update(update)
        } else {
            self.collect_update(update)
        }
    }

    pub fn process_snapshot(&mut self, snapshot: Snapshot<P, Q>) -> Result<(), OrderBookError> {
        if self.has_snapshot {
            return Err(OrderBookError::SnapshotExists(self.last_update_id, snapshot.last_update_id));
        }

        if self.updates.is_empty() {
            return Err(OrderBookError::SnapshotTooEarly(snapshot.last_update_id));
        }

        if !self.updates.is_empty() {
            let first_update_id = self.updates.first().unwrap().first_update_id;
            if snapshot.last_update_id < first_update_id {
                return Err(OrderBookError::SnapshotTooOld(first_update_id, snapshot.last_update_id));
            }
        }

        // apply snapshot
        Self::apply_layer(snapshot.bids, &mut self.bids);
        Self::apply_layer(snapshot.asks, &mut self.asks);

        // TODO: use binary search somehow
        let mut start_index = None;
        for (i, u) in self.updates.iter().enumerate() {
            if u.first_update_id <= snapshot.last_update_id && snapshot.last_update_id <= u.final_update_id {
                start_index = Some(i);
                break;
            }
        }

        if let Some(i) = start_index {
            let mut updates = mem::take(&mut self.updates);
            let updates = updates.drain(i..);
            for u in updates {
                self.apply_update(u)?;
            }
        } else {
            self.last_update_id = snapshot.last_update_id;
        }

        self.resize();

        self.has_snapshot = true;
        self.snapshot_id = snapshot.last_update_id;

        Ok(())
    }

    pub fn reset(&mut self) {
        self.has_snapshot = false;
        self.snapshot_id = 0;
        self.last_update_id = 0;
        self.updates.clear();
        self.bids.clear();
        self.asks.clear();
    }

    // private

    fn apply_update(&mut self, update: Update<P, Q>) -> Result<(), OrderBookError> {
        if update.final_update_id <= self.last_update_id {
            return Ok(()); // TODO: should we skip it ???
        }

        if self.last_update_id + 1 != update.first_update_id && self.last_update_id != self.snapshot_id {
            return Err(OrderBookError::UpdateGaps(self.last_update_id, update.first_update_id));
        }

        Self::apply_layer(update.bids, &mut self.bids);
        Self::apply_layer(update.asks, &mut self.asks);

        self.resize();

        self.last_update_id = update.final_update_id;
        self.update_time = update.event_time;

        Ok(())
    }

    fn collect_update(&mut self, update: Update<P, Q>) -> Result<(), OrderBookError> {
        if let Some(last) = self.updates.last() {
            if last.final_update_id + 1 != update.first_update_id {
                return Err(OrderBookError::UpdateGaps(last.final_update_id, update.first_update_id));
            }
        }
        self.updates.push(update);
        Ok(())
    }

    fn resize(&mut self) {
        while self.bids.len() > self.depth {
            self.bids.pop_first();
        }

        while self.asks.len() > self.depth {
            self.asks.pop_last();
        }
    }

    fn apply_layer(update: Vec<(P, Q)>, layer: &mut BTreeMap<P, Q>) {
        for (p, q) in update {
            if q.is_zero() {
                let _ = layer.remove(&p);
            } else {
                layer.insert(p, q);
            }
        }
    }
}

impl<P: Display + Ord + Clone, Q: Display + Zero> Display for OrderBook<P, Q> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Order Book\n")?;
        write!(f, "Symbol: {}\n", self.symbol)?;
        write!(f, "Id: {}\n", self.last_update_id)?;
        if let MappedLocalTime::Single(dt) = Utc.timestamp_millis_opt(self.update_time) {
            write!(f, "Time: {}\n", dt.to_rfc3339())?;
        }
        if let Some((b, a)) = self.best_price() {
            write!(f, "Best Price: Bid={} Ask={}\n", b, a)?;
        }
        write!(f, "Ask:\n")?;
        for (p, q) in self.asks.iter().rev() {
            write!(f, "{}: {}\n", p, q)?;
        }
        write!(f, "Bid:\n")?;
        for (p, q) in self.bids.iter().rev() {
            write!(f, "{}: {}\n", p, q)?;
        }
        write!(f, "\n")
    }
}
