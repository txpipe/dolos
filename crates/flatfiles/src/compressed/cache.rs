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
    /// Parsed seek-table bytes kept across all segments.
    pub index_bytes: usize,
    /// Parsed seek tables kept.
    pub index_entries: usize,
    /// Dictionary bytes and prepared decoder allocations kept.
    pub dictionary_bytes: usize,
    /// Prepared dictionaries kept.
    pub dictionary_entries: usize,
    /// Open segment files, including active reads. Zero disables retention
    /// and allows one transient handle.
    pub handles: usize,
    /// Reads or index loads in flight, including opening, parsing and decoding.
    /// Zero admits one operation at a time.
    pub inflight_reads: usize,
}

impl CacheLimits {
    /// Cache nothing; every read opens, parses and decodes afresh.
    pub const DISABLED: Self = Self {
        frame_bytes: 0,
        frame_entries: 0,
        index_bytes: 0,
        index_entries: 0,
        dictionary_bytes: 0,
        dictionary_entries: 0,
        handles: 0,
        inflight_reads: 1,
    };
}

impl Default for CacheLimits {
    fn default() -> Self {
        Self {
            frame_bytes: 64 << 20,
            frame_entries: 16_384,
            index_bytes: 32 << 20,
            index_entries: 512,
            dictionary_bytes: 8 << 20,
            dictionary_entries: 8,
            handles: 64,
            inflight_reads: 16,
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
    pub index_bytes: usize,
    pub index_entries: usize,
    pub dictionary_bytes: usize,
    pub dictionary_entries: usize,
    /// Handles retained in the cache.
    pub handles: usize,
    /// All open handles, including active and evicted readers.
    pub open_handles: usize,
    pub inflight_reads: usize,
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
/// decoding happen outside the locks. An operation permit covers the whole
/// read, including temporary index, dictionary and output allocations. A
/// separate permit follows each open file until its final reader releases it.
pub struct ReadCache {
    limits: CacheLimits,
    handles: Mutex<Lru<SegmentRef, Arc<ReadHandle>>>,
    indexes: Mutex<Lru<SegmentRef, Arc<SegmentIndex>>>,
    dictionaries: Mutex<Lru<DictionaryId, Arc<PreparedDictionary>>>,
    frames: Mutex<Lru<FrameKey, Arc<[u8]>>>,
    gate: Arc<Gate>,
    handle_gate: Arc<Gate>,
}

struct ReadHandle {
    file: File,
    _permit: Permit,
}

impl ReadCache {
    pub fn new(limits: CacheLimits) -> Self {
        Self {
            limits,
            handles: Mutex::new(Lru::new(limits.handles, usize::MAX)),
            indexes: Mutex::new(Lru::new(limits.index_entries, limits.index_bytes)),
            dictionaries: Mutex::new(Lru::new(limits.dictionary_entries, limits.dictionary_bytes)),
            frames: Mutex::new(Lru::new(limits.frame_entries, limits.frame_bytes)),
            gate: Arc::new(Gate::new(limits.inflight_reads)),
            handle_gate: Arc::new(Gate::new(limits.handles)),
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
        let _permit = self.gate.acquire();
        let handle = self.handle(segment, path)?;
        let index = self.index_from(segment, &handle.file)?;
        let dictionary = match index.metadata().dictionary {
            Some(id) => Some(self.dictionary(id, dictionaries)?),
            None => None,
        };
        index.assemble(offset, length, |frame| {
            self.frame(segment, &index, &handle.file, dictionary.as_deref(), frame)
        })
    }

    /// The parsed index of the segment file at `path`.
    pub fn index(&self, segment: SegmentRef, path: &Path) -> io::Result<Arc<SegmentIndex>> {
        let _permit = self.gate.acquire();
        let handle = self.handle(segment, path)?;
        self.index_from(segment, &handle.file)
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
        let indexes = self.indexes.lock().unwrap();
        let dictionaries = self.dictionaries.lock().unwrap();
        CacheStats {
            frame_bytes: frames.bytes(),
            frame_entries: frames.len(),
            index_bytes: indexes.bytes(),
            index_entries: indexes.len(),
            dictionary_bytes: dictionaries.bytes(),
            dictionary_entries: dictionaries.len(),
            handles: self.handles.lock().unwrap().len(),
            open_handles: self.handle_gate.inflight(),
            inflight_reads: self.gate.inflight(),
        }
    }

    fn handle(&self, segment: SegmentRef, path: &Path) -> io::Result<Arc<ReadHandle>> {
        loop {
            let revision = self.handle_gate.revision();
            let mut handles = self.handles.lock().unwrap();
            if let Some(handle) = handles.get(&segment) {
                return Ok(handle);
            }
            let permit = loop {
                if let Some(permit) = self.handle_gate.try_acquire() {
                    break Some(permit);
                }
                if !handles.evict_oldest() {
                    break None;
                }
            };
            drop(handles);
            if let Some(permit) = permit {
                let handle = Arc::new(ReadHandle {
                    file: File::open(path)?,
                    _permit: permit,
                });
                self.handles
                    .lock()
                    .unwrap()
                    .insert(segment, handle.clone(), 0);
                self.handle_gate.changed();
                return Ok(handle);
            }
            self.handle_gate.wait_changed(revision);
        }
    }

    fn index_from(&self, segment: SegmentRef, handle: &File) -> io::Result<Arc<SegmentIndex>> {
        if let Some(index) = self.indexes.lock().unwrap().get(&segment) {
            return Ok(index);
        }
        let index = Arc::new(SegmentIndex::parse(handle)?);
        self.indexes
            .lock()
            .unwrap()
            .insert(segment, index.clone(), index.memory_size());
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
        let dictionary = Arc::new(resolve_dictionary(id, source)?.prepare()?);
        self.dictionaries
            .lock()
            .unwrap()
            .insert(id, dictionary.clone(), dictionary.memory_size());
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
        let data: Arc<[u8]> = index.decode_frame(handle, frame, dictionary)?.into();
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
            && (self.slots.len() >= self.max_entries || weight > self.max_bytes - self.bytes)
        {
            self.evict_oldest();
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

    fn evict_oldest(&mut self) -> bool {
        let Some((_, key)) = self.order.pop_first() else {
            return false;
        };
        if let Some(slot) = self.slots.remove(&key) {
            self.bytes -= slot.weight;
        }
        true
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

/// Counting gate whose permits can follow an operation or an open file.
struct Gate {
    limit: usize,
    state: Mutex<GateState>,
    freed: Condvar,
}

#[derive(Default)]
struct GateState {
    inflight: usize,
    revision: u64,
}

struct Permit(Arc<Gate>);

impl Gate {
    fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            state: Mutex::new(GateState::default()),
            freed: Condvar::new(),
        }
    }

    fn acquire(self: &Arc<Self>) -> Permit {
        let mut state = self.state.lock().unwrap();
        while state.inflight >= self.limit {
            state = self.freed.wait(state).unwrap();
        }
        state.inflight += 1;
        Permit(self.clone())
    }

    fn try_acquire(self: &Arc<Self>) -> Option<Permit> {
        let mut state = self.state.lock().unwrap();
        if state.inflight >= self.limit {
            return None;
        }
        state.inflight += 1;
        Some(Permit(self.clone()))
    }

    fn revision(&self) -> u64 {
        self.state.lock().unwrap().revision
    }

    fn changed(&self) {
        let mut state = self.state.lock().unwrap();
        state.revision = state.revision.wrapping_add(1);
        self.freed.notify_all();
    }

    fn wait_changed(&self, revision: u64) {
        let mut state = self.state.lock().unwrap();
        while state.revision == revision {
            state = self.freed.wait(state).unwrap();
        }
    }

    fn inflight(&self) -> usize {
        self.state.lock().unwrap().inflight
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.inflight -= 1;
        state.revision = state.revision.wrapping_add(1);
        self.0.freed.notify_all();
    }
}
