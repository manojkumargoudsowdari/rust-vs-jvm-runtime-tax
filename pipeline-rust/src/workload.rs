//! The benchmark workload. Must stay semantically identical to
//! `pipeline-scala/src/main/scala/rvj/Workload.scala`; see PLAN.md §3.

use std::collections::HashMap;

pub const METRIC_THRESHOLD: f64 = 75.0;
pub const HOUR_MS: i64 = 3_600_000;
pub const TAG_LEN: usize = 8;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// One input row, fully owned (the payload is a heap-allocated copy).
pub struct Row {
    pub id: [u8; 16],
    pub ts: i64,
    pub payload: String,
    pub metric: f64,
}

/// A filtered and transformed row.
pub struct Enriched {
    pub id: [u8; 16],
    pub hour_bucket: i64,
    pub metric: f64,
    pub payload_hash: u64,
    pub tag: String,
}

impl Enriched {
    pub fn from_row(row: Row) -> Self {
        Enriched {
            id: row.id,
            hour_bucket: row.ts / HOUR_MS,
            metric: row.metric,
            payload_hash: fnv1a64(row.payload.as_bytes()),
            tag: row.payload[..TAG_LEN].to_ascii_uppercase(),
        }
    }

    /// Per-row checksum; folds in `tag` and `id` so that no part of the
    /// transform can be eliminated as dead code.
    pub fn row_hash(&self) -> u64 {
        let (lo, hi) = self.id.split_at(8);
        self.payload_hash
            ^ fnv1a64(self.tag.as_bytes())
            ^ u64::from_le_bytes(lo.try_into().unwrap())
            ^ u64::from_le_bytes(hi.try_into().unwrap())
    }
}

pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

#[derive(Clone, Debug, PartialEq)]
pub struct Agg {
    pub count: i64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub checksum: u64,
}

impl Agg {
    pub fn new() -> Self {
        Agg { count: 0, sum: 0.0, min: f64::INFINITY, max: f64::NEG_INFINITY, checksum: 0 }
    }

    pub fn add(&mut self, e: &Enriched) {
        self.count += 1;
        self.sum += e.metric;
        if e.metric < self.min {
            self.min = e.metric;
        }
        if e.metric > self.max {
            self.max = e.metric;
        }
        self.checksum ^= e.row_hash();
    }

    /// Merging must happen in unit order for `sum` to be reproducible.
    pub fn merge(&mut self, other: &Agg) {
        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
        self.checksum ^= other.checksum;
    }
}

impl Default for Agg {
    fn default() -> Self {
        Agg::new()
    }
}

/// Filter → transform → aggregate one materialized batch. Returns rows kept.
pub fn process_batch(rows: Vec<Row>, groups: &mut HashMap<i64, Agg>) -> u64 {
    let mut kept = 0;
    rows.into_iter()
        .filter(|r| r.metric > METRIC_THRESHOLD)
        .map(Enriched::from_row)
        .for_each(|e| {
            groups.entry(e.hour_bucket).or_default().add(&e);
            kept += 1;
        });
    kept
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a64_reference_vectors() {
        assert_eq!(fnv1a64(b""), 0xcbf29ce484222325);
        assert_eq!(fnv1a64(b"a"), 0xaf63dc4c8601ec8c);
        assert_eq!(fnv1a64(b"foobar"), 0x85944171f73967e8);
    }

    fn row(ts: i64, metric: f64, payload: &str) -> Row {
        let mut id = [0u8; 16];
        id[0] = 1;
        id[15] = 2;
        Row { id, ts, payload: payload.repeat(1024 / payload.len()), metric }
    }

    #[test]
    fn filters_buckets_and_aggregates() {
        let rows = vec![
            row(0, 80.0, "abcdefgh"),
            row(HOUR_MS - 1, 90.0, "abcdefgh"),
            row(HOUR_MS, 100.0, "zyxwvuts"),
            row(HOUR_MS, 75.0, "zyxwvuts"), // not > threshold
        ];
        let mut groups = HashMap::new();
        assert_eq!(process_batch(rows, &mut groups), 3);
        assert_eq!(groups.len(), 2);
        let g0 = &groups[&0];
        assert_eq!((g0.count, g0.sum, g0.min, g0.max), (2, 170.0, 80.0, 90.0));
        // Two identical row hashes cancel out under XOR.
        assert_eq!(g0.checksum, 0);
        assert_eq!(groups[&1].count, 1);
    }

    #[test]
    fn tag_is_uppercased_prefix() {
        let e = Enriched::from_row(row(0, 80.0, "abcDEfg-"));
        assert_eq!(e.tag, "ABCDEFG-");
    }
}
