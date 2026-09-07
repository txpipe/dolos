//! Store-wide bounded caches over compressed segments.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::hash::Hash;
use std::io;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};

use super::dictionary::{DictionaryId, DictionarySource, PreparedDictionary};
use super::reader::{resolve_dictionary, SegmentIndex};

/// Upper bounds on what a [`ReadCache`] retains. Zero disables a cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheLimits {
    /// Decoded frame bytes kept across all segments.
    pub frame_bytes: usize,
    /// Decoded frames kept across all segments.
    pub frame_entries: usize,
    /// Parsed seek tables kept.
    pub index_entries: usize,
    /// Prepared dictionaries kept.
    pub dictionary_entries: usize,
    /// Open segment files kept.
    pub handles: usize,
    /// Frames decoding at once across all readers; zero leaves it unbounded.
    pub inflight_decodes: usize,
}

impl CacheLimits {
    /// Cache nothing; every read opens, parses and decodes afresh.
    pub const DISABLED: Self = Self {
        frame_bytes: 0,
        frame_entries: 0,
        index_entries: 0,
        dictionary_entries: 0,
        handles: 0,
        inflight_decodes: 0,
    };
}

impl Default for CacheLimits {
    fn default() -> Self {
        Self {
            frame_bytes: 64 << 20,
            frame_entries: 16_384,
            index_entries: 512,
            dictionary_entries: 8,
            handles: 64,
            inflight_decodes: 16,
        }
    }
}

/// A segment file as the cache keys it: the segment number plus a generation
/// the store bumps whenever the file behind that number changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentRef {
    pub segment_id: u32,
    pub generation: u64,
}

/// What a [`ReadCache`] currently retains.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub frame_bytes: usize,
    pub frame_entries: usize,
    pub index_entries: usize,
    pub dictionary_entries: usize,
    pub handles: usize,
    pub inflight_decodes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FrameKey {
    segment: SegmentRef,
    dictionary: Option<DictionaryId>,
    frame: u32,
}

/// Bounded caches for reading compressed segments from many threads.
///
/// Each cache is locked only to look up or insert; opening, parsing and
/// decoding happen outside the locks, with decoding further bounded by an
/// in-flight gate so a burst of readers cannot hold more than
/// `inflight_decodes` frames in working memory on top of the cache.
pub struct ReadCache {
    limits: CacheLimits,
    handles: Mutex<Lru<SegmentRef, Arc<File>>>,
    indexes: Mutex<Lru<SegmentRef, Arc<SegmentIndex>>>,
    dictionaries: Mutex<Lru<DictionaryId, Arc<PreparedDictionary>>>,
    frames: Mutex<Lru<FrameKey, Arc<[u8]>>>,
    gate: Gate,
}

impl ReadCache {
    pub fn new(limits: CacheLimits) -> Self {
        Self {
            limits,
            handles: Mutex::new(Lru::new(limits.handles, usize::MAX)),
            indexes: Mutex::new(Lru::new(limits.index_entries, usize::MAX)),
            dictionaries: Mutex::new(Lru::new(limits.dictionary_entries, usize::MAX)),
            frames: Mutex::new(Lru::new(limits.frame_entries, limits.frame_bytes)),
            gate: Gate::new(limits.inflight_decodes),
        }
    }

    pub fn limits(&self) -> CacheLimits {
        self.limits
    }

    /// Read `length` logical bytes at `offset` of the segment file at `path`.
    pub fn read(
        &self,
        segment: SegmentRef,
        path: &Path,
        offset: u64,
        length: u32,
        dictionaries: &dyn DictionarySource,
    ) -> io::Result<Vec<u8>> {
        let handle = self.handle(segment, path)?;
        let index = self.index_from(segment, &handle)?;
        let dictionary = match index.metadata().dictionary {
            Some(id) => Some(self.dictionary(id, dictionaries)?),
            None => None,
        };
        index.assemble(offset, length, |frame| {
            self.frame(segment, &index, &handle, dictionary.as_deref(), frame)
        })
    }

    /// The parsed index of the segment file at `path`.
    pub fn index(&self, segment: SegmentRef, path: &Path) -> io::Result<Arc<SegmentIndex>> {
        let handle = self.handle(segment, path)?;
        self.index_from(segment, &handle)
    }

    /// Forget everything cached for `segment_id`, whatever its generation.
    pub fn invalidate(&self, segment_id: u32) {
        self.handles
            .lock()
            .unwrap()
            .retain(|key| key.segment_id != segment_id);
        self.indexes
            .lock()
            .unwrap()
            .retain(|key| key.segment_id != segment_id);
        self.frames
            .lock()
            .unwrap()
            .retain(|key| key.segment.segment_id != segment_id);
    }

    /// Drop every cached entry.
    pub fn clear(&self) {
        self.handles.lock().unwrap().clear();
        self.indexes.lock().unwrap().clear();
        self.dictionaries.lock().unwrap().clear();
        self.frames.lock().unwrap().clear();
    }

    pub fn stats(&self) -> CacheStats {
        let frames = self.frames.lock().unwrap();
        CacheStats {
            frame_bytes: frames.bytes(),
            frame_entries: frames.len(),
            index_entries: self.indexes.lock().unwrap().len(),
            dictionary_entries: self.dictionaries.lock().unwrap().len(),
            handles: self.handles.lock().unwrap().len(),
            inflight_decodes: self.gate.inflight(),
        }
    }

    fn handle(&self, segment: SegmentRef, path: &Path) -> io::Result<Arc<File>> {
        if let Some(handle) = self.handles.lock().unwrap().get(&segment) {
            return Ok(handle);
        }
        let handle = Arc::new(File::open(path)?);
        self.handles
            .lock()
            .unwrap()
            .insert(segment, handle.clone(), 0);
        Ok(handle)
    }

    fn index_from(&self, segment: SegmentRef, handle: &File) -> io::Result<Arc<SegmentIndex>> {
        if let Some(index) = self.indexes.lock().unwrap().get(&segment) {
            return Ok(index);
        }
        let index = Arc::new(SegmentIndex::parse(handle)?);
        self.indexes
            .lock()
            .unwrap()
            .insert(segment, index.clone(), 0);
        Ok(index)
    }

    fn dictionary(
        &self,
        id: DictionaryId,
        source: &dyn DictionarySource,
    ) -> io::Result<Arc<PreparedDictionary>> {
        if let Some(dictionary) = self.dictionaries.lock().unwrap().get(&id) {
            return Ok(dictionary);
        }
        let dictionary = Arc::new(resolve_dictionary(id, source)?.prepare());
        self.dictionaries
            .lock()
            .unwrap()
            .insert(id, dictionary.clone(), 0);
        Ok(dictionary)
    }

    fn frame(
        &self,
        segment: SegmentRef,
        index: &SegmentIndex,
        handle: &File,
        dictionary: Option<&PreparedDictionary>,
        frame: usize,
    ) -> io::Result<Arc<[u8]>> {
        let key = FrameKey {
            segment,
            dictionary: dictionary.map(PreparedDictionary::id),
            frame: frame as u32,
        };
        if let Some(data) = self.frames.lock().unwrap().get(&key) {
            return Ok(data);
        }
        let data: Arc<[u8]> = {
            let _permit = self.gate.acquire();
            index.decode_frame(handle, frame, dictionary)?.into()
        };
        self.frames
            .lock()
            .unwrap()
            .insert(key, data.clone(), data.len());
        Ok(data)
    }
}

impl std::fmt::Debug for ReadCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadCache")
            .field("limits", &self.limits)
            .field("stats", &self.stats())
            .finish()
    }
}

/// Least-recently-used map bounded by entry count and total weight.
struct Lru<K, V> {
    slots: HashMap<K, Slot<V>>,
    order: BTreeMap<u64, K>,
    clock: u64,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

struct Slot<V> {
    value: V,
    tick: u64,
    weight: usize,
}

impl<K: Hash + Eq + Clone, V: Clone> Lru<K, V> {
    fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            slots: HashMap::new(),
            order: BTreeMap::new(),
            clock: 0,
            bytes: 0,
            max_entries,
            max_bytes,
        }
    }

    fn get(&mut self, key: &K) -> Option<V> {
        let slot = self.slots.get_mut(key)?;
        self.order.remove(&slot.tick);
        self.clock += 1;
        slot.tick = self.clock;
        self.order.insert(slot.tick, key.clone());
        Some(slot.value.clone())
    }

    /// Insert unless the entry can never fit; returns whether it was kept.
    fn insert(&mut self, key: K, value: V, weight: usize) -> bool {
        if self.max_entries == 0 || weight > self.max_bytes {
            return false;
        }
        self.remove(&key);
        while !self.slots.is_empty()
            && (self.slots.len() >= self.max_entries || self.bytes + weight > self.max_bytes)
        {
            let oldest = *self.order.keys().next().unwrap();
            let key = self.order.remove(&oldest).unwrap();
            if let Some(slot) = self.slots.remove(&key) {
                self.bytes -= slot.weight;
            }
        }
        self.clock += 1;
        self.order.insert(self.clock, key.clone());
        self.slots.insert(
            key,
            Slot {
                value,
                tick: self.clock,
                weight,
            },
        );
        self.bytes += weight;
        true
    }

    fn remove(&mut self, key: &K) {
        if let Some(slot) = self.slots.remove(key) {
            self.order.remove(&slot.tick);
            self.bytes -= slot.weight;
        }
    }

    fn retain(&mut self, mut keep: impl FnMut(&K) -> bool) {
        let doomed: Vec<K> = self.slots.keys().filter(|k| !keep(k)).cloned().collect();
        for key in &doomed {
            self.remove(key);
        }
    }

    fn clear(&mut self) {
        self.slots.clear();
        self.order.clear();
        self.bytes = 0;
    }

    fn len(&self) -> usize {
        self.slots.len()
    }

    fn bytes(&self) -> usize {
        self.bytes
    }
}

/// Counting gate over concurrent decodes; a limit of zero admits everyone.
struct Gate {
    limit: usize,
    inflight: Mutex<usize>,
    freed: Condvar,
}

struct Permit<'a>(&'a Gate);

impl Gate {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            inflight: Mutex::new(0),
            freed: Condvar::new(),
        }
    }

    fn acquire(&self) -> Option<Permit<'_>> {
        if self.limit == 0 {
            return None;
        }
        let mut inflight = self.inflight.lock().unwrap();
        while *inflight >= self.limit {
            inflight = self.freed.wait(inflight).unwrap();
        }
        *inflight += 1;
        Some(Permit(self))
    }

    fn inflight(&self) -> usize {
        *self.inflight.lock().unwrap()
    }
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        *self.0.inflight.lock().unwrap() -= 1;
        self.0.freed.notify_one();
    }
}
