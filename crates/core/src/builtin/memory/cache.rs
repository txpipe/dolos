//! The cache's memory tier: entries within a byte budget of their own,
//! evicted lowest cost per byte first.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    time::Instant,
};

use bytes::Bytes;

use crate::cache::{Cost, TierStats};

/// An entry's place in the eviction order: lowest cost per byte first, then
/// least recently used. `used` is unique, so no two ranks are equal.
#[derive(Debug, Clone, Copy)]
struct Rank {
    cost: Cost,
    size: u64,
    used: u64,
}

impl Ord for Rank {
    fn cmp(&self, other: &Self) -> Ordering {
        let mine = u128::from(self.cost) * u128::from(other.size.max(1));
        let theirs = u128::from(other.cost) * u128::from(self.size.max(1));

        mine.cmp(&theirs).then(self.used.cmp(&other.used))
    }
}

impl PartialOrd for Rank {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Rank {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Rank {}

struct Stored {
    value: Bytes,
    stored_at: Instant,
    rank: Rank,
}

#[derive(Default)]
struct Namespace {
    entries: HashMap<Bytes, Stored>,
    bytes: u64,
}

/// What an insert did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Inserted {
    pub kept: bool,
    /// The namespace of each entry it evicted.
    pub evicted: Vec<&'static str>,
}

/// Entries by namespace and key, within `budget` bytes of keys and values.
pub struct MemoryCacheTier {
    budget: u64,
    bytes: u64,
    clock: u64,
    namespaces: HashMap<&'static str, Namespace>,
    ranks: BTreeMap<Rank, (&'static str, Bytes)>,
}

impl MemoryCacheTier {
    pub fn new(budget: u64) -> Self {
        Self {
            budget,
            bytes: 0,
            clock: 0,
            namespaces: HashMap::new(),
            ranks: BTreeMap::new(),
        }
    }

    pub fn budget(&self) -> u64 {
        self.budget
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// The value under `key` and when it was stored. A hit makes the entry
    /// the most recently used.
    pub fn get(&mut self, namespace: &str, key: &[u8]) -> Option<(Bytes, Instant)> {
        let used = self.tick();
        let stored = self.namespaces.get_mut(namespace)?.entries.get_mut(key)?;

        let old = stored.rank;
        stored.rank.used = used;
        let rank = stored.rank;
        let found = (stored.value.clone(), stored.stored_at);

        if let Some(owner) = self.ranks.remove(&old) {
            self.ranks.insert(rank, owner);
        }

        Some(found)
    }

    /// Keeps `value` under `key` if it fits the budget, replacing what the key
    /// held. Room is made only from entries ranked below the new one, so a
    /// large entry cheap for its size never pushes out entries worth more per
    /// byte; when they cannot make room, it is not kept.
    pub fn insert(
        &mut self,
        namespace: &'static str,
        key: Bytes,
        value: Bytes,
        cost: Cost,
        now: Instant,
    ) -> Inserted {
        self.remove(namespace, &key);

        let size = (key.len() + value.len()) as u64;

        if size > self.budget {
            return Inserted::default();
        }

        let rank = Rank {
            cost,
            size,
            used: self.tick(),
        };

        let mut victims = Vec::new();
        let mut room = self.budget - self.bytes;

        for (victim, _) in self.ranks.range(..rank) {
            if room >= size {
                break;
            }

            victims.push(*victim);
            room += victim.size;
        }

        if room < size {
            return Inserted::default();
        }

        let mut evicted = Vec::with_capacity(victims.len());

        for victim in victims {
            if let Some((ns, key)) = self.ranks.remove(&victim) {
                self.take(ns, &key);
                evicted.push(ns);
            }
        }

        let ns = self.namespaces.entry(namespace).or_default();
        ns.bytes += size;
        ns.entries.insert(
            key.clone(),
            Stored {
                value,
                stored_at: now,
                rank,
            },
        );

        self.bytes += size;
        self.ranks.insert(rank, (namespace, key));

        Inserted {
            kept: true,
            evicted,
        }
    }

    pub fn remove(&mut self, namespace: &str, key: &[u8]) -> bool {
        let Some(rank) = self
            .namespaces
            .get(namespace)
            .and_then(|ns| ns.entries.get(key))
            .map(|stored| stored.rank)
        else {
            return false;
        };

        self.ranks.remove(&rank);
        self.take(namespace, key);

        true
    }

    /// Drops every entry of `namespace`.
    pub fn wipe(&mut self, namespace: &str) {
        let Some(ns) = self.namespaces.remove(namespace) else {
            return;
        };

        for stored in ns.entries.values() {
            self.ranks.remove(&stored.rank);
        }

        self.bytes -= ns.bytes;
    }

    pub fn usage(&self, namespace: &str) -> TierStats {
        self.namespaces
            .get(namespace)
            .map(|ns| TierStats {
                entries: ns.entries.len() as u64,
                bytes: ns.bytes,
            })
            .unwrap_or_default()
    }

    /// Removes an entry whose rank is already out of the eviction order.
    fn take(&mut self, namespace: &str, key: &[u8]) {
        let Some(ns) = self.namespaces.get_mut(namespace) else {
            return;
        };

        if let Some(stored) = ns.entries.remove(key) {
            ns.bytes -= stored.rank.size;
            self.bytes -= stored.rank.size;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert(tier: &mut MemoryCacheTier, key: &'static str, size: usize, cost: Cost) -> Inserted {
        let value = Bytes::from(vec![0; size - key.len()]);
        tier.insert("ns", Bytes::from(key), value, cost, Instant::now())
    }

    fn holds(tier: &mut MemoryCacheTier, key: &str) -> bool {
        tier.get("ns", key.as_bytes()).is_some()
    }

    #[test]
    fn a_large_cheap_entry_goes_before_small_costly_ones() {
        let mut tier = MemoryCacheTier::new(100);

        insert(&mut tier, "large", 60, 60);
        insert(&mut tier, "a", 10, 100);
        insert(&mut tier, "b", 10, 100);
        insert(&mut tier, "c", 10, 100);
        assert_eq!(tier.bytes(), 90);

        let inserted = insert(&mut tier, "d", 20, 200);
        assert!(inserted.kept);
        assert_eq!(inserted.evicted, ["ns"]);

        assert!(!holds(&mut tier, "large"));
        assert!(["a", "b", "c", "d"].iter().all(|k| holds(&mut tier, k)));
        assert_eq!(tier.bytes(), 50);
    }

    #[test]
    fn an_entry_worth_less_per_byte_pushes_nothing_out() {
        let mut tier = MemoryCacheTier::new(100);

        for key in ["a", "b", "c", "d", "e"] {
            insert(&mut tier, key, 20, 200);
        }

        let inserted = insert(&mut tier, "large", 50, 50);
        assert_eq!(inserted, Inserted::default());

        assert!(!holds(&mut tier, "large"));
        assert!(["a", "b", "c", "d", "e"]
            .iter()
            .all(|k| holds(&mut tier, k)));
    }

    #[test]
    fn the_least_recently_used_goes_first_among_equals() {
        let mut tier = MemoryCacheTier::new(30);

        insert(&mut tier, "a", 10, 10);
        insert(&mut tier, "b", 10, 10);
        insert(&mut tier, "c", 10, 10);
        assert!(holds(&mut tier, "a"));

        assert!(insert(&mut tier, "d", 10, 10).kept);

        assert!(!holds(&mut tier, "b"));
        assert!(["a", "c", "d"].iter().all(|k| holds(&mut tier, k)));
    }

    #[test]
    fn an_entry_over_the_budget_is_not_kept() {
        let mut tier = MemoryCacheTier::new(100);

        insert(&mut tier, "a", 10, 1);
        assert_eq!(
            insert(&mut tier, "huge", 101, u64::MAX),
            Inserted::default()
        );

        assert!(holds(&mut tier, "a"));
        assert_eq!(tier.bytes(), 10);
    }

    #[test]
    fn a_replacement_releases_the_old_bytes() {
        let mut tier = MemoryCacheTier::new(100);

        insert(&mut tier, "a", 40, 1);
        insert(&mut tier, "a", 70, 1);

        assert_eq!(tier.bytes(), 70);
        assert_eq!(tier.usage("ns").entries, 1);
    }

    #[test]
    fn a_wipe_drops_its_namespace_only() {
        let mut tier = MemoryCacheTier::new(100);
        let now = Instant::now();

        tier.insert("one", Bytes::from("a"), Bytes::from("1"), 1, now);
        tier.insert("one", Bytes::from("b"), Bytes::from("2"), 1, now);
        tier.insert("two", Bytes::from("a"), Bytes::from("3"), 1, now);

        tier.wipe("one");

        assert_eq!(tier.usage("one"), TierStats::default());
        assert_eq!(tier.get("two", b"a").unwrap().0, "3");
        assert_eq!(tier.bytes(), 2);
    }
}
