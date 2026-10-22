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
//!
//! A `Stats` type is one type in both stores, keyed epoch first. A `Set`
//! (decision 0057) lives in state only, one row per member keyed by a
//! [`GroupKey`], and its seal builds a `Chunk` in the archive: one list per
//! group and epoch. `PaymentStats` lives in state only too, sealing into the
//! same `Chunk` as its `Set`.

use std::{collections::BTreeMap, ops::Range};

use dolos_core::{
    ArchiveError, ArchiveStore, ArchiveWriter as _, ChainError, Domain, EntityDelta, EntityValue,
    FixedKey, Genesis, KeyBytes, KeyCodec, KeyError, LogEntity, LogKey, NsKey, RangeOf,
    StateEntity, StateStore, StateWriter as _,
};
use pallas::{codec::minicbor, ledger::primitives::Epoch};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::ChainSummary;
use dolos_core::SlotOrder;

pub mod account_addresses;
pub mod asset_mints;
pub mod first_mints;
pub mod first_scripts;
pub mod stats;

/// Most entries one sealed list row holds; a longer list spans several rows.
pub const CHUNK_LEN: usize = 256;

/// Staged rows a seal handles per commit pair.
const SEAL_BATCH: usize = 50_000;

/// The key of a `Set` row: `group ‖ epoch (8 B) ‖ member`.
///
/// Group-major, so one prefix scan over a group returns every staged epoch of
/// it, in epoch order. The key is exact: no two rows share it, and readers
/// take a group's rows without filtering.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupKey<G, M> {
    pub group: G,
    pub epoch: Epoch,
    pub member: M,
}

impl<G: FixedKey, M: KeyCodec> KeyCodec for GroupKey<G, M> {
    fn encode_key(&self, out: &mut KeyBytes) {
        self.group.encode_key(out);
        self.epoch.encode_key(out);
        self.member.encode_key(out);
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        let (group, (epoch, member)) = <(G, (u64, M))>::decode_key(bytes)?;

        Ok(Self {
            group,
            epoch,
            member,
        })
    }
}

/// The group of a `Set` whose members make one list per epoch: zero bytes, so
/// its rows key `epoch ‖ member`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Ungrouped;

impl KeyCodec for Ungrouped {
    fn encode_key(&self, _: &mut KeyBytes) {}

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        <()>::decode_key(bytes).map(|_| Self)
    }
}

impl FixedKey for Ungrouped {
    const LEN: usize = 0;
}

/// A group is the prefix of its staged keys. Declared per group kind: the
/// prefix trait is core's, so a blanket impl over every group is not one this
/// crate may write.
impl<M: KeyCodec> dolos_core::KeyPrefix<GroupKey<crate::PolicyKey, M>> for crate::PolicyKey {}
impl<M: KeyCodec> dolos_core::KeyPrefix<GroupKey<crate::AccountKey, M>> for crate::AccountKey {}
impl<M: KeyCodec> dolos_core::KeyPrefix<GroupKey<crate::PaymentKey, M>> for crate::PaymentKey {}
impl<M: KeyCodec> dolos_core::KeyPrefix<GroupKey<crate::AssetKey, M>> for crate::AssetKey {}

/// A `Set` row: one member of a group in an epoch, held in state under a
/// [`GroupKey`] until the epoch seals into a `Chunk`.
pub trait SetRow:
    StateEntity<Key: Clone + std::fmt::Debug + Serialize + DeserializeOwned> + Clone + std::fmt::Debug
{
    /// The epoch a staged key names.
    fn staged_epoch(key: &Self::Key) -> Epoch;
}

impl<E, G, M> SetRow for E
where
    E: StateEntity<Key = GroupKey<G, M>> + Clone + std::fmt::Debug,
    G: FixedKey + Clone + std::fmt::Debug + Serialize + DeserializeOwned,
    M: KeyCodec + Clone + std::fmt::Debug + Serialize + DeserializeOwned,
{
    fn staged_epoch(key: &GroupKey<G, M>) -> Epoch {
        key.epoch
    }
}

/// A `Set` row that holds its own key.
pub trait GroupRow: SetRow {
    fn key(&self) -> Self::Key;
}

/// Stage a row unless its key is already staged: the first contribution of a
/// member in an epoch is the one kept.
///
/// A delta that finds the row present changes nothing but still lands in the
/// WAL, so visitors drop repeats within a block before emitting.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(serialize = "E: Serialize", deserialize = "E: DeserializeOwned"))]
pub struct StageIfAbsent<E: SetRow> {
    pub(crate) key: E::Key,
    pub(crate) row: E,

    // undo
    pub(crate) was_new: bool,
}

impl<E: GroupRow> StageIfAbsent<E> {
    pub fn new(row: E) -> Self {
        Self::at(row.key(), row)
    }
}

impl<E: SetRow> StageIfAbsent<E> {
    /// Stage `row` at `key`, for a row whose value does not hold its key.
    pub fn at(key: E::Key, row: E) -> Self {
        Self {
            key,
            row,
            was_new: false,
        }
    }
}

impl<E: SetRow> EntityDelta for StageIfAbsent<E> {
    type Entity = E;

    fn key(&self) -> NsKey {
        NsKey::of::<E>(&self.key)
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

/// A staged row its contributions add up to.
pub trait SumRow:
    StateEntity<Key: Clone + std::fmt::Debug + Serialize + DeserializeOwned>
    + Clone
    + std::fmt::Debug
    + Default
{
    fn add(&mut self, other: &Self);

    /// Take `other` back out of a row it was added to; any part left at zero
    /// goes with it.
    fn sub(&mut self, other: &Self);

    fn is_zero(&self) -> bool;
}

/// Add a contribution to a staged sum row, creating it if absent.
///
/// Undo takes the contribution back out and removes a row left at zero, so a
/// zero row is never stored; a seal skips one all the same.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(serialize = "E: Serialize", deserialize = "E: DeserializeOwned"))]
pub struct StageSum<E: SumRow> {
    pub(crate) key: E::Key,
    pub(crate) delta: E,
}

impl<E: SumRow> StageSum<E> {
    pub fn new(key: E::Key, delta: E) -> Self {
        Self { key, delta }
    }
}

impl<E: SumRow> EntityDelta for StageSum<E> {
    type Entity = E;

    fn key(&self) -> NsKey {
        NsKey::of::<E>(&self.key)
    }

    fn apply(&mut self, entity: &mut Option<E>) {
        entity.get_or_insert_with(E::default).add(&self.delta);
    }

    fn undo(&self, entity: &mut Option<E>) {
        if let Some(row) = entity {
            row.sub(&self.delta);

            if row.is_zero() {
                *entity = None;
            }
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

    first_mints::seal::<D>(state, archive, chain, &sealable, SEAL_BATCH)?;
    first_scripts::seal::<D>(state, archive, chain, &sealable, SEAL_BATCH)?;
    asset_mints::seal::<D>(state, archive, chain, &sealable, SEAL_BATCH)?;
    account_addresses::seal::<D>(state, archive, chain, &sealable, SEAL_BATCH)?;
    stats::seal::<D>(state, archive, chain, &sealable, SEAL_BATCH)
}

/// Seal a `Set`'s rows in chunks of whole groups.
///
/// `rows` come in key order, and a chunk closes at the first group boundary
/// after it holds `batch` rows. `seal` writes the chunk's archive rows, which
/// commit before the chunk's staged rows are deleted. A crash between the two
/// leaves whole groups staged and sealed, and the re-run seals them again from
/// the same rows.
pub fn seal_groups<D, E, G, M, R>(
    state: &D::State,
    archive: &D::Archive,
    rows: impl IntoIterator<Item = Result<(GroupKey<G, M>, R), ChainError>>,
    batch: usize,
    mut seal: impl FnMut(
        &[(GroupKey<G, M>, R)],
        &<D::Archive as ArchiveStore>::Writer,
    ) -> Result<(), ChainError>,
) -> Result<(), ChainError>
where
    D: Domain,
    E: StateEntity<Key = GroupKey<G, M>>,
    G: FixedKey + PartialEq,
    M: KeyCodec,
{
    let mut chunk: Vec<(GroupKey<G, M>, R)> = Vec::new();

    let mut flush = |chunk: &mut Vec<(GroupKey<G, M>, R)>| -> Result<(), ChainError> {
        let writer = archive.start_writer()?;
        seal(chunk, &writer)?;
        writer.commit()?;

        let writer = state.start_writer()?;
        for (key, _) in chunk.drain(..) {
            writer.save_entity_typed::<E>(&key, None)?;
        }
        writer.commit()?;

        Ok(())
    };

    for row in rows {
        let row = row?;

        let closes = chunk
            .last()
            .is_some_and(|(last, _)| chunk.len() >= batch && last.group != row.0.group);

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

/// Seal an epoch-first period type: the rows of every epoch `sealable`
/// accepts move from state to the archive, the key `(e, key)` becoming
/// `epoch_start(e) ‖ key`.
///
/// Rows are keyed epoch first, so the sealable ones lead the namespace and
/// the scan stops at the first that isn't. They seal in chunks of `batch`
/// rows, each committing its archive rows before it deletes them from state.
/// A row seals on its own, so a chunk may close anywhere, even inside a
/// group's rows of one epoch; readers merge the two stores key by key. A
/// re-run writes the same rows again.
pub fn seal_epochs<D, E, K>(
    state: &D::State,
    archive: &D::Archive,
    chain: &ChainSummary,
    sealable: &dyn Fn(Epoch) -> bool,
    batch: usize,
) -> Result<(), ChainError>
where
    D: Domain,
    E: SumRow<Key = (Epoch, K)> + LogEntity<Key = K>,
    K: KeyCodec,
{
    let mut range = RangeOf::<(Epoch, K)>::all();

    loop {
        let mut chunk = Vec::new();
        let mut rest = None;

        for row in state.iter_entities_typed::<E>(Some(range.clone()))? {
            let (key, row) = row?;

            // epoch first: every row after an unsealable one is newer
            if !sealable(key.0) {
                break;
            }

            if chunk.len() == batch {
                rest = Some(key);
                break;
            }

            chunk.push((key, row));
        }

        if chunk.is_empty() {
            return Ok(());
        }

        let writer = archive.start_writer()?;

        for ((epoch, key), row) in &chunk {
            if !row.is_zero() {
                writer.write_log_typed::<E>(chain.epoch_start(*epoch), key, row)?;
            }
        }

        writer.commit()?;

        let writer = state.start_writer()?;

        for (key, _) in &chunk {
            writer.save_entity_typed::<E>(key, None)?;
        }

        writer.commit()?;

        match rest {
            Some(key) => range = range.starting_at(&key),
            None => return Ok(()),
        }
    }
}

/// The oldest epoch an epoch-first period type holds rows of in state: the
/// namespace's first key names it.
pub fn oldest_staged<S, E, K>(state: &S) -> Result<Option<Epoch>, ChainError>
where
    S: StateStore,
    E: StateEntity<Key = (Epoch, K)>,
    K: KeyCodec,
{
    let mut rows = state.iter_entities_typed::<E>(None)?;

    Ok(rows.next().transpose()?.map(|((epoch, _), _)| epoch))
}

/// Staged rows in `range`, by epoch, in key order within each.
pub fn read_staged<S, E>(
    state: &S,
    range: RangeOf<E::Key>,
) -> Result<BTreeMap<Epoch, Vec<E>>, ChainError>
where
    S: StateStore,
    E: SetRow,
{
    let mut out: BTreeMap<Epoch, Vec<E>> = BTreeMap::new();

    for row in state.iter_entities_typed::<E>(Some(range))? {
        let (key, row) = row?;
        out.entry(E::staged_epoch(&key)).or_default().push(row);
    }

    Ok(out)
}

/// The sealed rows of `R` at `key(e)` of each of `epochs`, raw, in one
/// batched read. A sealed row of epoch `e` lives at `epoch_start(e) ‖ key`.
pub fn read_sealed<A: ArchiveStore, R: LogEntity>(
    archive: &A,
    chain: &ChainSummary,
    epochs: impl IntoIterator<Item = Epoch>,
    key: impl Fn(Epoch) -> R::Key,
) -> Result<BTreeMap<Epoch, EntityValue>, ChainError> {
    let epochs: Vec<Epoch> = epochs.into_iter().collect();

    let keys: Vec<LogKey> = epochs
        .iter()
        .map(|&epoch| LogKey::of(chain.epoch_start(epoch), &key(epoch)))
        .collect();

    let values = archive.read_logs(R::NS, &keys.iter().collect::<Vec<_>>())?;

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
pub trait ChunkRow: LogEntity {
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

/// Chunks `chunks` of one sealed epoch, keyed `key(i)`.
pub fn load_chunks<A, R>(
    archive: &A,
    chain: &ChainSummary,
    epoch: Epoch,
    chunks: Range<u32>,
    key: impl Fn(u32) -> R::Key,
) -> Result<Vec<R>, ChainError>
where
    A: ArchiveStore,
    R: ChunkRow,
{
    let slot = chain.epoch_start(epoch);
    let keys: Vec<_> = chunks.clone().map(|i| (slot, key(i))).collect();

    let rows = archive.read_logs_typed::<R>(&keys)?;

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
    fn group_keys_are_exact_and_group_by_prefix() {
        use crate::{AssetKey, PolicyKey};

        type Key = GroupKey<PolicyKey, AssetKey>;

        let group = PolicyKey([7u8; 28].into());
        let key = |group, epoch| Key {
            group,
            epoch,
            member: AssetKey([1u8; 32].into()),
        };

        let raw = dolos_core::keys::encode(&key(group, 300));
        assert_eq!(raw.len(), 28 + 8 + 32);
        assert_eq!(raw[..28], [7u8; 28]);
        assert_eq!(raw[28..36], 300u64.to_be_bytes());
        assert_eq!(Key::decode_key(&raw), Ok(key(group, 300)));
        assert!(Key::decode_key(&raw[..67]).is_err());

        let range = RangeOf::<Key>::prefix(&group);
        assert!(range.contains(&key(group, 0)));
        assert!(range.contains(&key(group, u64::MAX)));

        let encoded = |epoch| dolos_core::keys::encode(&key(group, epoch));
        assert!(encoded(3) < encoded(4));
        assert!(encoded(255) < encoded(256));

        let other = PolicyKey([8u8; 28].into());
        assert!(!range.contains(&key(other, 3)));

        // a policy sharing all but its last byte is another group
        let mut sibling = [7u8; 28];
        sibling[27] = 9;
        assert!(!range.contains(&key(PolicyKey(sibling.into()), 3)));
    }

    proptest::proptest! {
        #[test]
        fn group_keys_round_trip_and_decode_exactly(
            group in proptest::prelude::any::<[u8; 28]>(),
            epoch in proptest::prelude::any::<u64>(),
            member in proptest::prelude::any::<[u8; 32]>(),
        ) {
            use crate::{AssetKey, PolicyKey};

            let key = GroupKey {
                group: PolicyKey(group.into()),
                epoch,
                member: AssetKey(member.into()),
            };

            let raw = dolos_core::keys::encode(&key);
            proptest::prop_assert_eq!(GroupKey::decode_key(&raw), Ok(key));
            proptest::prop_assert!(
                GroupKey::<PolicyKey, AssetKey>::decode_key(&raw[..raw.len() - 1]).is_err()
            );
            proptest::prop_assert!(
                GroupKey::<PolicyKey, AssetKey>::decode_key(&[raw.as_slice(), &[0]].concat())
                    .is_err()
            );
        }
    }

    /// Byron epochs are a sixth of mainnet's mutable window (`3k/f`), so each
    /// seals at the seventh boundary after it rather than at the second.
    #[test]
    fn byron_length_epochs_seal_once_the_mutable_window_passes() {
        let mut chain = ChainSummary::default();
        chain.append_era(
            0,
            crate::EraState {
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
