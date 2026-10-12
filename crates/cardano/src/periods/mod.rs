//! Period logs (decision 0049): history folds the API serves without decoding
//! blocks.
//!
//! Roll visitors stage each block's contribution as entities in a state
//! namespace, undone through the WAL like any other. At each epoch boundary,
//! ESTART seals every staged epoch that can no longer roll back into archive
//! logs keyed `epoch_start(e)`, committing the archive before it deletes the
//! staged rows. Readers take the staged rows first and the sealed ones second,
//! and per epoch the sealed row wins, so a seal running beside a read leaves
//! its answer unchanged.

use std::{collections::BTreeMap, ops::Range};

use dolos_core::{
    ArchiveError, ArchiveStore, ArchiveWriter as _, ChainError, Domain, Entity, EntityDelta,
    EntityKey, EntityValue, Genesis, LogKey, Namespace, NsKey, StateStore, StateWriter as _,
    TemporalKey,
};
use pallas::{codec::minicbor, ledger::primitives::Epoch};
use serde::{Deserialize, Serialize};

use crate::{indexes::SlotOrder, ChainSummary, FixedNamespace};

pub mod first_mints;

/// Leading bytes of a staged key that name its group.
pub const GROUP_LEN: usize = 14;

const EPOCH_LEN: usize = 2;

/// Trailing bytes of a staged key that name a member of its group.
pub const MEMBER_LEN: usize = dolos_core::KEY_SIZE - GROUP_LEN - EPOCH_LEN;

/// Most entries one sealed list row holds; a longer list spans several rows.
pub const CHUNK_LEN: usize = 256;

/// Staged rows a seal handles per commit pair.
const SEAL_BATCH: usize = 50_000;

/// `group[..14] ‖ epoch ‖ member[..16]`: group-major, so one prefix scan
/// returns every staged epoch of a group.
///
/// The epoch is truncated to two bytes, which tells apart the few epochs
/// staged at any time; rows carry their full epoch.
pub fn staged_key(group: &[u8], epoch: Epoch, member: &[u8]) -> EntityKey {
    let mut key = [0u8; dolos_core::KEY_SIZE];
    key[..GROUP_LEN].copy_from_slice(&group[..GROUP_LEN]);
    key[GROUP_LEN..GROUP_LEN + EPOCH_LEN].copy_from_slice(&(epoch as u16).to_be_bytes());
    key[GROUP_LEN + EPOCH_LEN..].copy_from_slice(&member[..MEMBER_LEN]);
    EntityKey::from(&key)
}

/// Every staged key of one group.
pub fn group_range(group: &[u8]) -> Range<EntityKey> {
    let mut start = [0u8; dolos_core::KEY_SIZE];
    let mut end = [0xffu8; dolos_core::KEY_SIZE];
    start[..GROUP_LEN].copy_from_slice(&group[..GROUP_LEN]);
    end[..GROUP_LEN].copy_from_slice(&group[..GROUP_LEN]);
    EntityKey::from(&start)..EntityKey::from(&end)
}

/// Where a sealed row of `epoch` lives: `epoch_start(epoch) ‖ entity`.
pub fn sealed_key(chain: &ChainSummary, epoch: Epoch, entity: EntityKey) -> LogKey {
    LogKey::from((TemporalKey::from(chain.epoch_start(epoch)), entity))
}

/// A row staged in state until its epoch seals.
pub trait StagedRow: Entity + FixedNamespace + Clone + std::fmt::Debug {
    fn key(&self) -> EntityKey;

    fn epoch(&self) -> Epoch;
}

/// Stage a row unless its key is already staged: the first contribution of a
/// member in an epoch is the one kept.
///
/// A delta that finds the row present changes nothing but still lands in the
/// WAL, so visitors drop repeats within a block before emitting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageIfAbsent<E> {
    pub(crate) key: EntityKey,
    pub(crate) row: E,

    // undo
    pub(crate) was_new: bool,
}

impl<E: StagedRow> StageIfAbsent<E> {
    pub fn new(row: E) -> Self {
        Self {
            key: row.key(),
            row,
            was_new: false,
        }
    }
}

impl<E: StagedRow> EntityDelta for StageIfAbsent<E> {
    type Entity = E;

    fn key(&self) -> NsKey {
        NsKey(E::NS, self.key.clone())
    }

    fn apply(&mut self, entity: &mut Option<E>) {
        self.was_new = entity.is_none();

        if self.was_new {
            *entity = Some(self.row.clone());
        }
    }

    fn undo(&self, entity: &mut Option<E>) {
        if self.was_new {
            *entity = None;
        }
    }
}

/// Whether `epoch` can no longer roll back once epoch `boundary` opens: its
/// last slot is more than `mutable_slots` behind the boundary.
pub fn is_sealable(
    chain: &ChainSummary,
    mutable_slots: u64,
    epoch: Epoch,
    boundary: Epoch,
) -> bool {
    chain.epoch_start(epoch + 1) + mutable_slots <= chain.epoch_start(boundary)
}

/// Seal every staged epoch that is immutable at the boundary opening
/// `boundary`.
///
/// Runs from ESTART finalize, before its cursor commit, so a crash re-runs the
/// boundary and with it the seal, which writes the same rows again.
pub fn seal_staged<D: Domain>(
    state: &D::State,
    archive: &D::Archive,
    chain: &ChainSummary,
    genesis: &Genesis,
    boundary: Epoch,
) -> Result<(), ChainError> {
    let mutable_slots = crate::utils::mutable_slots(genesis);
    let sealable = |epoch| is_sealable(chain, mutable_slots, epoch, boundary);

    first_mints::seal::<D>(state, archive, chain, &sealable, SEAL_BATCH)
}

/// Seal staged rows in chunks of whole groups.
///
/// `rows` come in key order, and a chunk closes at the first group boundary
/// after it holds `batch` rows. `seal` writes the chunk's archive rows, which
/// commit before the chunk's staged rows are deleted. A crash between the two
/// leaves whole groups staged and sealed, and the re-run seals them again from
/// the same rows.
pub fn seal_groups<D, R>(
    state: &D::State,
    archive: &D::Archive,
    ns: Namespace,
    rows: impl IntoIterator<Item = Result<(EntityKey, R), ChainError>>,
    batch: usize,
    mut seal: impl FnMut(
        &[(EntityKey, R)],
        &<D::Archive as ArchiveStore>::Writer,
    ) -> Result<(), ChainError>,
) -> Result<(), ChainError>
where
    D: Domain,
{
    let mut chunk: Vec<(EntityKey, R)> = Vec::new();

    let mut flush = |chunk: &mut Vec<(EntityKey, R)>| -> Result<(), ChainError> {
        let writer = archive.start_writer()?;
        seal(chunk, &writer)?;
        writer.commit()?;

        let writer = state.start_writer()?;
        for (key, _) in chunk.drain(..) {
            writer.delete_entity(ns, &key)?;
        }
        writer.commit()?;

        Ok(())
    };

    for row in rows {
        let row = row?;

        let closes = chunk.last().is_some_and(|(last, _)| {
            chunk.len() >= batch && last.as_ref()[..GROUP_LEN] != row.0.as_ref()[..GROUP_LEN]
        });

        if closes {
            flush(&mut chunk)?;
        }

        chunk.push(row);
    }

    if !chunk.is_empty() {
        flush(&mut chunk)?;
    }

    Ok(())
}

/// Staged rows in `range`, by epoch, in key order within each.
pub fn read_staged<S, E>(
    state: &S,
    range: Range<EntityKey>,
) -> Result<BTreeMap<Epoch, Vec<E>>, ChainError>
where
    S: StateStore,
    E: StagedRow,
{
    let mut out: BTreeMap<Epoch, Vec<E>> = BTreeMap::new();

    for row in state.iter_entities_typed::<E>(E::NS, Some(range))? {
        let (_, row) = row?;
        out.entry(row.epoch()).or_default().push(row);
    }

    Ok(out)
}

/// The sealed rows at `entity(e)` of each of `epochs`, raw, in one batched
/// read.
pub fn read_sealed<A: ArchiveStore>(
    archive: &A,
    ns: Namespace,
    chain: &ChainSummary,
    epochs: impl IntoIterator<Item = Epoch>,
    entity: impl Fn(Epoch) -> EntityKey,
) -> Result<BTreeMap<Epoch, EntityValue>, ChainError> {
    let epochs: Vec<Epoch> = epochs.into_iter().collect();

    let keys: Vec<LogKey> = epochs
        .iter()
        .map(|&epoch| sealed_key(chain, epoch, entity(epoch)))
        .collect();

    let values = archive.read_logs(ns, &keys.iter().collect::<Vec<_>>())?;

    Ok(epochs
        .into_iter()
        .zip(values)
        .filter_map(|(epoch, value)| value.map(|value| (epoch, value)))
        .collect())
}

/// One epoch of a group, from wherever it lives.
#[derive(Debug)]
pub enum Period<E, V> {
    Staged(Vec<E>),
    Sealed(V),
}

/// Join staged and sealed rows by epoch, ascending; the sealed row wins.
///
/// The staged rows must be read before the sealed ones. A seal commits the
/// archive before it deletes the staged rows, so in that order a row missing
/// from the staged read is always in the sealed one.
pub fn join<E, V>(
    staged: BTreeMap<Epoch, Vec<E>>,
    sealed: BTreeMap<Epoch, V>,
) -> BTreeMap<Epoch, Period<E, V>> {
    let mut out: BTreeMap<_, _> = staged
        .into_iter()
        .map(|(epoch, rows)| (epoch, Period::Staged(rows)))
        .collect();

    out.extend(
        sealed
            .into_iter()
            .map(|(epoch, row)| (epoch, Period::Sealed(row))),
    );

    out
}

/// A sealed list row: one chunk of a group's entries for one epoch.
///
/// Field 0 is the group's entry count for the epoch, on every chunk, so a
/// reader takes it off the first chunk without decoding the entries.
pub trait ChunkRow: Entity + FixedNamespace {
    type Item;

    fn new(count: u64, items: Vec<Self::Item>) -> Self;

    fn into_items(self) -> Vec<Self::Item>;
}

/// Split one group's entries for one epoch into rows of [`CHUNK_LEN`].
pub fn chunk<R: ChunkRow>(items: Vec<R::Item>) -> Vec<R> {
    let count = items.len() as u64;
    let mut items = items.into_iter().peekable();
    let mut rows = Vec::new();

    while items.peek().is_some() {
        rows.push(R::new(count, items.by_ref().take(CHUNK_LEN).collect()));
    }

    rows
}

/// The entry count of a raw chunk row, read off its header.
pub fn chunk_count(raw: &[u8]) -> Result<u64, ChainError> {
    let mut decoder = minicbor::Decoder::new(raw);
    decoder.array()?;
    Ok(decoder.u64()?)
}

/// Chunks `chunks` of one sealed epoch, keyed `entity(i)`.
pub fn load_chunks<A, R>(
    archive: &A,
    chain: &ChainSummary,
    epoch: Epoch,
    chunks: Range<u32>,
    entity: impl Fn(u32) -> EntityKey,
) -> Result<Vec<R>, ChainError>
where
    A: ArchiveStore,
    R: ChunkRow,
{
    let keys: Vec<LogKey> = chunks
        .clone()
        .map(|i| sealed_key(chain, epoch, entity(i)))
        .collect();

    let rows = archive.read_logs_typed::<R>(R::NS, &keys.iter().collect::<Vec<_>>())?;

    rows.into_iter()
        .zip(chunks)
        .map(|(row, i)| {
            row.ok_or_else(|| {
                ArchiveError::InternalError(format!("{} epoch {epoch} lacks chunk {i}", R::NS))
                    .into()
            })
        })
        .collect()
}

/// The entries one epoch contributes to a list.
#[derive(Debug)]
pub enum Segment<T> {
    /// Unsealed: the entries, in order.
    Staged(Vec<T>),

    /// Sealed: the count off the first chunk's header, and that chunk raw.
    Sealed {
        epoch: Epoch,
        count: u64,
        first: EntityValue,
    },
}

impl<T> Segment<T> {
    pub fn len(&self) -> u64 {
        match self {
            Segment::Staged(items) => items.len() as u64,
            Segment::Sealed { count, .. } => *count,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The ascending index range each segment gives a page that skips `skip`
/// entries and takes `count` in `order`, segments in the page's order.
fn window(
    lens: &[u64],
    order: SlotOrder,
    mut skip: u64,
    mut count: u64,
) -> Vec<(usize, Range<u64>)> {
    let indexes: Box<dyn Iterator<Item = usize>> = match order {
        SlotOrder::Asc => Box::new(0..lens.len()),
        SlotOrder::Desc => Box::new((0..lens.len()).rev()),
    };

    let mut out = Vec::new();

    for index in indexes {
        if count == 0 {
            break;
        }

        let len = lens[index];

        if skip >= len {
            skip -= len;
            continue;
        }

        let take = (len - skip).min(count);

        let range = match order {
            SlotOrder::Asc => skip..skip + take,
            SlotOrder::Desc => len - skip - take..len - skip,
        };

        out.push((index, range));
        skip = 0;
        count -= take;
    }

    out
}

/// One page of a list spread over epoch segments, listed ascending.
///
/// Sealed segments the page doesn't reach are skipped on their count alone;
/// the ones it reaches read only the chunks covering it, through `load`.
pub fn page<R: ChunkRow>(
    segments: Vec<Segment<R::Item>>,
    order: SlotOrder,
    skip: usize,
    count: usize,
    mut load: impl FnMut(Epoch, Range<u32>) -> Result<Vec<R>, ChainError>,
) -> Result<Vec<R::Item>, ChainError> {
    let lens: Vec<u64> = segments.iter().map(Segment::len).collect();
    let mut segments: Vec<Option<Segment<R::Item>>> = segments.into_iter().map(Some).collect();

    let mut out = Vec::with_capacity(count);

    for (index, range) in window(&lens, order, skip as u64, count as u64) {
        let segment = segments[index]
            .take()
            .expect("a page visits each segment once");

        let mut items: Vec<R::Item> = match segment {
            Segment::Staged(items) => items
                .into_iter()
                .skip(range.start as usize)
                .take((range.end - range.start) as usize)
                .collect(),
            Segment::Sealed { epoch, first, .. } => {
                let chunk = CHUNK_LEN as u64;
                let from = (range.start / chunk) as u32;
                let to = range.end.div_ceil(chunk) as u32;

                let mut rows = Vec::new();

                if from == 0 {
                    rows.push(R::decode_entity(R::NS, &first)?);
                }

                rows.extend(load(epoch, from.max(1)..to)?);

                rows.into_iter()
                    .flat_map(ChunkRow::into_items)
                    .skip((range.start - from as u64 * chunk) as usize)
                    .take((range.end - range.start) as usize)
                    .collect()
            }
        };

        if order == SlotOrder::Desc {
            items.reverse();
        }

        out.append(&mut items);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_keys_group_by_prefix_and_sort_by_epoch() {
        let group = [7u8; 28];
        let key = |epoch| staged_key(&group, epoch, &[1u8; 32]);

        let range = group_range(&group);
        assert!(range.contains(&key(0)));
        assert!(range.contains(&key(65_535)));
        assert!(key(3) < key(4));

        let other = staged_key(&[8u8; 28], 3, &[0u8; 32]);
        assert!(!range.contains(&other));

        // a group shares its prefix only up to GROUP_LEN bytes
        let mut sibling = group;
        sibling[GROUP_LEN] = 9;
        assert!(range.contains(&staged_key(&sibling, 3, &[0u8; 32])));
    }

    /// Byron epochs are a sixth of mainnet's mutable window (`3k/f`), so each
    /// seals at the seventh boundary after it rather than at the second.
    #[test]
    fn byron_length_epochs_seal_once_the_mutable_window_passes() {
        let mut chain = ChainSummary::default();
        chain.append_era(
            0,
            crate::EraSummary {
                start: crate::EraBoundary {
                    epoch: 0,
                    slot: 0,
                    timestamp: 0,
                },
                end: None,
                epoch_length: 21_600,
                slot_length: 20,
                protocol: 0,
            },
        );

        let mutable = 129_600;

        for epoch in 0..30 {
            assert!(!is_sealable(&chain, mutable, epoch, epoch + 6), "{epoch}");
            assert!(is_sealable(&chain, mutable, epoch, epoch + 7), "{epoch}");
        }
    }

    #[test]
    fn window_walks_segments_in_page_order() {
        let lens = [3, 0, 5, 2];

        assert_eq!(
            window(&lens, SlotOrder::Asc, 0, 4),
            vec![(0, 0..3), (2, 0..1)]
        );
        assert_eq!(window(&lens, SlotOrder::Asc, 3, 4), vec![(2, 0..4)]);
        assert_eq!(window(&lens, SlotOrder::Asc, 9, 4), vec![(3, 1..2)]);
        assert_eq!(window(&lens, SlotOrder::Asc, 10, 4), vec![]);

        assert_eq!(
            window(&lens, SlotOrder::Desc, 0, 3),
            vec![(3, 0..2), (2, 4..5)]
        );
        assert_eq!(
            window(&lens, SlotOrder::Desc, 2, 6),
            vec![(2, 0..5), (0, 2..3)]
        );
        assert_eq!(window(&lens, SlotOrder::Desc, 7, 9), vec![(0, 0..3)]);
    }
}
