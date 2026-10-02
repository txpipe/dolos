use axum::http::StatusCode;
use itertools::Itertools as _;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use std::collections::{HashMap, HashSet};
use std::ops::{Range, RangeInclusive};

use dolos_core::async_query::BlockRefMeta;
use dolos_core::{
    ArchiveError, ArchiveStore as _, BlockSlot, Domain, StateStore as _, TxHash, TxoIdx, TxoRef,
};

use crate::{
    mapping::{IntoModel, UtxoOutputModelBuilder},
    pagination::{Order, Pagination},
    Facade,
};

/// Loads, sorts and paginates the page of UTxO models for `refs`.
///
/// `from` / `to` must be cleared by the caller: Blockfrost does not read them
/// on the address, script and account UTxO endpoints. Use
/// [`load_utxo_models_in_height_range`] for the endpoints that do.
pub async fn load_utxo_models<D, T>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    pagination: Pagination,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T>,
{
    let retention = Retention::read(domain).await?;

    load_utxo_models_in_slot_range(domain, refs, pagination, retention, None).await
}

/// Like [`load_utxo_models`] but honours `from` / `to` as an inclusive block
/// height range, the way Blockfrost's `/assets/{asset}/utxos` does. The
/// bounds are plain heights there: an `:index` is validated like on every
/// endpoint but cuts nothing.
///
/// An output whose creation block was pruned by `sync.max_history` has no
/// known height, so it can never be proven to sit inside the range, and the
/// model it feeds requires the block hash, height and time. Such rows are
/// left out rather than reported with made-up block data.
pub async fn load_utxo_models_in_height_range<D, T>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    mut pagination: Pagination,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T>,
{
    for bound in [&mut pagination.from, &mut pagination.to]
        .into_iter()
        .flatten()
    {
        bound.index = None;
    }

    let retention = Retention::read(domain).await?;

    // Heights are monotonic in slots, so the range is applied on slots. A
    // Byron epoch-boundary block blurs the edges: it takes the height of the
    // main block before it and the slot of the main block after it, and the
    // index keeps the boundary block's slot for that height. The main block
    // of height `h` therefore sits somewhere in `slot(h - 1)..=slot(h)`, and
    // `slot(h)` can also hold height `h + 1`. The rows of that zone at `from`,
    // and of `slot(to)` at `to`, are held against the exact heights later,
    // see [`drop_rows_past_height_bounds`].
    let mut edges = Vec::with_capacity(2);

    let start = match pagination.from.as_ref() {
        None => 0,
        Some(from) => match retention.locate(domain, from.number).await? {
            Height::Held(slot) => {
                let below = retention.slot_below(domain, from.number).await?;
                edges.push(below..=slot);
                below
            }
            // everything still held is newer than the pruned bound
            Height::Pruned => 0,
            // nothing held reaches that high
            Height::Beyond => return Ok(vec![]),
        },
    };

    let end = match pagination.to.as_ref() {
        None => u64::MAX,
        Some(to) => match retention.locate(domain, to.number).await? {
            Height::Held(slot) => {
                edges.push(slot..=slot);
                slot
            }
            // nothing still held is that old
            Height::Pruned => return Ok(vec![]),
            // Everything held is below the bound. The tip caps the span so a
            // block archived while this request runs cannot slip in above
            // the bound; one archived since the lookup that already passes
            // it gets the edge treatment.
            Height::Beyond => match read_end_block(domain, ArchiveEnd::Tip).await? {
                None => return Ok(vec![]),
                Some((slot, height)) if height <= to.number => slot,
                Some((slot, _)) => {
                    edges.push(retention.slot_below(domain, to.number).await?..=slot);
                    slot
                }
            },
        },
    };

    let bounds = SlotBounds {
        span: start..=end,
        edges,
    };

    load_utxo_models_in_slot_range(domain, refs, pagination, retention, Some(bounds)).await
}

/// The slot span of a height range, plus the edge zones whose rows may still
/// sit past the exact heights and have to be checked block by block.
struct SlotBounds {
    span: RangeInclusive<u64>,
    edges: Vec<RangeInclusive<u64>>,
}

/// Where a bound height stands against what the archive holds.
enum Height {
    /// The index holds its slot and the block is still retained.
    Held(BlockSlot),
    /// Below the oldest block still held.
    Pruned,
    /// Past the newest block held: not archived yet, or rolled back.
    Beyond,
}

/// One end of the archive.
#[derive(Clone, Copy)]
enum ArchiveEnd {
    Oldest,
    Tip,
}

/// The slot and height of the block at `end` of the archive, `None` for an
/// empty archive.
async fn read_end_block<D>(
    domain: &Facade<D>,
    end: ArchiveEnd,
) -> Result<Option<(BlockSlot, u64)>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    domain
        .query()
        .run_blocking(move |domain| {
            let block = match end {
                ArchiveEnd::Oldest => domain.archive().get_range(None, None)?.next(),
                ArchiveEnd::Tip => domain.archive().get_tip()?,
            };

            block
                .map(|(slot, body)| {
                    let height = MultiEraBlock::decode(&body)
                        .map_err(|e| ArchiveError::InternalError(e.to_string()))?
                        .number();
                    Ok((slot, height))
                })
                .transpose()
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// What the archive still holds: the oldest retained slot.
///
/// `sync.max_history` prunes block bodies every round but sweeps the index
/// entries only now and then, so a slot the index still knows can belong to
/// a block that is already gone. Every slot below `floor` is treated as
/// unknown for that reason.
#[derive(Clone, Copy)]
struct Retention {
    floor: BlockSlot,
}

impl Retention {
    async fn read<D>(domain: &Facade<D>) -> Result<Self, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
    {
        domain
            .query()
            .run_blocking(|domain| {
                let floor = domain
                    .archive()
                    .get_range(None, None)?
                    .next()
                    .map(|(slot, _)| slot)
                    .unwrap_or(0);

                Ok(Retention { floor })
            })
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }

    /// Where `height` stands. The index answers for a held height; one it
    /// does not know is told apart by the height of the oldest block held,
    /// read only then. A rollback drops the index entries before the bodies,
    /// so the tip alone could not tell a rolled-back height from a held one.
    async fn locate<D>(&self, domain: &Facade<D>, height: u64) -> Result<Height, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
    {
        if let Some(slot) = self.slot_of(domain, height).await? {
            return Ok(Height::Held(slot));
        }

        Ok(match read_end_block(domain, ArchiveEnd::Oldest).await? {
            Some((_, oldest)) if height < oldest => Height::Pruned,
            _ => Height::Beyond,
        })
    }

    /// The slot the index holds for `height`, `None` if the index does not
    /// know it or its block is no longer held.
    async fn slot_of<D>(
        &self,
        domain: &Facade<D>,
        height: u64,
    ) -> Result<Option<BlockSlot>, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
    {
        let slot = domain
            .query()
            .slot_by_number(height)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(self.retained(slot))
    }

    /// The slot of the block below `height`, where the zone that may hold
    /// `height` starts; `0` when there is none or it is not held, as nothing
    /// still held is older.
    async fn slot_below<D>(&self, domain: &Facade<D>, height: u64) -> Result<BlockSlot, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
    {
        match height.checked_sub(1) {
            Some(below) => Ok(self.slot_of(domain, below).await?.unwrap_or(0)),
            None => Ok(0),
        }
    }

    /// `slot` if its block is still held, `None` for a pruned or unknown one.
    fn retained(&self, slot: Option<BlockSlot>) -> Option<BlockSlot> {
        slot.filter(|slot| *slot >= self.floor)
    }
}

/// Positions the page so that only index lookups scale with the number of
/// refs; block bodies are read for a handful of slots only.
///
/// 1. The slot of every creating tx comes from the archive index, no block body
///    is read. Rows are sorted by slot, which fixes the order between blocks
///    and lets the page window be located.
/// 2. With `bounds`, the rows in its edge zones get their block decoded to be
///    held against the exact heights, see [`drop_rows_past_height_bounds`].
/// 3. The rows of the slots the window touches get their block decoded, which
///    settles the order inside a block (tx index, then output index). Each
///    block is decoded once per request, and rows that vanished mid-request
///    pull in the next window.
///
/// A row whose block was pruned has no position. With `bounds` such rows are
/// dropped, the model needs the block hash, height and time; without it they
/// stay and sort first, as the oldest.
///
/// `bounds` is the slot span of the `from` / `to` heights on `pagination`.
async fn load_utxo_models_in_slot_range<D, T>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    pagination: Pagination,
    retention: Retention,
    bounds: Option<SlotBounds>,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T>,
{
    let chain = domain.get_chain_summary()?;

    let tx_hashes: Vec<TxHash> = refs.iter().map(|txo_ref| txo_ref.0).unique().collect();
    let tx_slots: HashMap<TxHash, Option<u64>> = domain
        .query()
        .slots_by_tx_hashes(tx_hashes)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .collect();

    let mut rows: Vec<(Option<u64>, TxoRef)> = refs
        .into_iter()
        .map(|txo_ref| {
            let slot = retention.retained(tx_slots.get(&txo_ref.0).copied().flatten());
            (slot, txo_ref)
        })
        .filter(|(slot, _)| match (&bounds, slot) {
            (Some(bounds), Some(slot)) => bounds.span.contains(slot),
            (Some(_), None) => false,
            (None, _) => true,
        })
        .collect();
    // the map is as large as the ref set; do not carry it across the reads
    drop(tx_slots);

    // block metadata decoded so far, by tx; `None` for a tx whose block is gone
    let mut metas: HashMap<TxHash, Option<BlockRefMeta>> = HashMap::new();

    if let Some(bounds) = &bounds {
        drop_rows_past_height_bounds(domain, &mut rows, &bounds.edges, &pagination, &mut metas)
            .await?;
    }

    // the same order `page_sort_key` gives, minus the position inside a block
    rows.sort_by_key(|(slot, txo_ref)| (*slot, txo_ref.0, txo_ref.1));
    if let Order::Desc = pagination.order {
        rows.reverse();
    }

    let slots: Vec<Option<u64>> = rows.iter().map(|(slot, _)| *slot).collect();
    let mut window = slot_window(&slots, pagination.skip(), pagination.count);
    // rows of the window that sit before the page
    let mut to_skip = pagination.skip() - window.start;

    // A row can vanish between this request's tag scan and the reads below:
    // its block pruned or its UTxO spent. A vanished row of the page is
    // dropped and the page topped up from the rows after the window, so a
    // short page still means the end of the list. The offset counts rows as
    // the tag scan listed them, vanished or not, so a row that vanished
    // mid-request costs a repeated row on the next page rather than a lost
    // one. A row gone before the tag scan is simply not listed.
    let mut out = Vec::with_capacity(pagination.count);
    while !window.is_empty() {
        let chunk = &rows[window.clone()];
        resolve_block_meta(domain, chunk, &mut metas).await?;

        let mut positioned: Vec<_> = chunk
            .iter()
            .map(|(slot, txo_ref)| {
                let meta = metas.get(&txo_ref.0).cloned().flatten();
                (page_sort_key(meta.as_ref(), *slot, txo_ref), txo_ref, meta)
            })
            .collect();

        positioned.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));
        if let Order::Desc = pagination.order {
            positioned.reverse();
        }

        let skipped = to_skip.min(positioned.len());
        to_skip -= skipped;
        let mut candidates = positioned.into_iter().skip(skipped).peekable();

        while out.len() < pagination.count && candidates.peek().is_some() {
            let batch: Vec<_> = candidates
                .by_ref()
                .take(pagination.count - out.len())
                .collect();

            // with a height range a row whose block is gone has vanished too:
            // its UTxO is not even read, so it drops out with the spent ones
            let live: Vec<TxoRef> = batch
                .iter()
                .filter(|(_, _, meta)| bounds.is_none() || meta.is_some())
                .map(|(_, txo_ref, _)| (*txo_ref).clone())
                .collect();

            let utxos = domain
                .state()
                .get_utxos(live)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            for (_, txo_ref, meta) in batch {
                let Some(cbor) = utxos.get(txo_ref) else {
                    continue;
                };
                let output = MultiEraOutput::try_from(cbor.as_ref())
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                let builder = UtxoOutputModelBuilder::from_output(txo_ref.0, txo_ref.1, output);
                let builder = match meta {
                    Some(meta) => {
                        let block_time = chain.slot_time(meta.slot);
                        builder.with_block_data(meta).with_block_time(block_time)
                    }
                    None => builder,
                };

                out.push(<UtxoOutputModelBuilder<'_> as IntoModel<T>>::into_model(
                    builder,
                )?);
            }
        }

        if out.len() == pagination.count {
            break;
        }

        window = slot_window(&slots, window.end, pagination.count - out.len());
    }

    Ok(out)
}

/// Decodes the block metadata of the `rows` txs not in `metas` yet; each
/// block is read once, however many of the rows it holds.
async fn resolve_block_meta<D>(
    domain: &Facade<D>,
    rows: &[(Option<u64>, TxoRef)],
    metas: &mut HashMap<TxHash, Option<BlockRefMeta>>,
) -> Result<(), StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let located: Vec<(TxHash, Option<BlockSlot>)> = rows
        .iter()
        .filter(|(_, txo_ref)| !metas.contains_key(&txo_ref.0))
        .map(|(slot, txo_ref)| (txo_ref.0, *slot))
        .unique_by(|(tx_hash, _)| *tx_hash)
        .collect();

    if located.is_empty() {
        return Ok(());
    }

    let (resolved, _) = domain
        .query()
        .block_meta_by_located(located)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    metas.extend(resolved);

    Ok(())
}

/// Drops the rows in the `edges` zones that sit past the exact `from` / `to`
/// bounds on `pagination`.
///
/// A Byron epoch-boundary block can push the main block of a bound's height
/// anywhere between the slots of the heights below it and of the bound, and
/// share the bound's slot with the next height, so the zones cover those
/// slots. Every slot outside the zones is inside the heights by
/// monotonicity, so only the zone rows get their block decoded — a couple of
/// blocks per bound. The decoded metadata stays in `metas` for the page.
async fn drop_rows_past_height_bounds<D>(
    domain: &Facade<D>,
    rows: &mut Vec<(Option<u64>, TxoRef)>,
    edges: &[RangeInclusive<u64>],
    pagination: &Pagination,
    metas: &mut HashMap<TxHash, Option<BlockRefMeta>>,
) -> Result<(), StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let on_edge =
        |slot: &Option<u64>| slot.is_some_and(|slot| edges.iter().any(|edge| edge.contains(&slot)));

    let edge_rows: Vec<_> = rows
        .iter()
        .filter(|(slot, _)| on_edge(slot))
        .cloned()
        .collect();
    resolve_block_meta(domain, &edge_rows, metas).await?;

    rows.retain(|(slot, txo_ref)| {
        if !on_edge(slot) {
            return true;
        }

        match metas.get(&txo_ref.0).cloned().flatten() {
            Some(meta) => !pagination.should_skip(meta.height, meta.tx_index),
            // a block pruned since the slot lookup: the row keeps its place in
            // the offset and the page loop drops it, like a spent one
            None => true,
        }
    });

    Ok(())
}

/// The index range of `slots` (already in page order) that has to be
/// positioned exactly to serve the page `skip..skip + count`.
///
/// Rows of one slot are not in their final order yet, so the window grows
/// to the whole slot group at both ends. Rows without a slot are already in
/// their final order and never widen it.
fn slot_window(slots: &[Option<u64>], skip: usize, count: usize) -> Range<usize> {
    let len = slots.len();

    let start = skip.min(len);
    let end = skip.saturating_add(count).min(len);

    let mut start = start;
    if let Some(Some(slot)) = slots.get(start) {
        while start > 0 && slots[start - 1] == Some(*slot) {
            start -= 1;
        }
    }

    let mut end = end;
    if end > start {
        if let Some(Some(slot)) = slots.get(end - 1) {
            while end < len && slots[end] == Some(*slot) {
                end += 1;
            }
        }
    }

    start..end
}

/// The page order for a UTxO row: chain position first, `TxoRef` second.
///
/// Chain position is `None` for an output whose creation block was pruned by
/// `sync.max_history` — the block that carries its slot no longer exists, so
/// true chain order is unrecoverable for it. The `TxoRef` tie-breaker keeps
/// those rows in a deterministic order across requests; without it their
/// order came from `HashMap` iteration, and two page requests could slice two
/// different shufflings, duplicating or dropping rows.
///
/// `None` sorts before every known position, which approximates chain order:
/// a pruned creation block is older than every retained one.
///
/// A row whose block could not be read since its slot lookup (pruned in
/// between, or the tx missing from it) keeps its slot position, with the tx
/// index unknown.
fn page_sort_key(
    meta: Option<&BlockRefMeta>,
    slot: Option<u64>,
    txo_ref: &TxoRef,
) -> (Option<(u64, usize, u32)>, TxHash, TxoIdx) {
    let TxoRef(tx_hash, txo_idx) = txo_ref;

    let position = meta
        .map(|meta| (meta.slot, meta.tx_index, *txo_idx))
        .or_else(|| slot.map(|slot| (slot, 0, *txo_idx)));

    (position, *tx_hash, *txo_idx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas::crypto::hash::Hash;

    /// Pins the pruned-row ordering contract: no chain position means the
    /// `TxoRef` decides, deterministically, and the whole unknowable group
    /// sorts before any row with a known position.
    #[test]
    fn page_sort_key_orders_pruned_rows_by_txo_ref() {
        let low = TxoRef(Hash::from([0xaa; 32]), 1);
        let high = TxoRef(Hash::from([0xbb; 32]), 0);
        let low_later = TxoRef(Hash::from([0xaa; 32]), 2);

        // no block data: the TxoRef alone decides, tx hash before output index
        assert!(page_sort_key(None, None, &low) < page_sort_key(None, None, &high));
        assert!(page_sort_key(None, None, &low) < page_sort_key(None, None, &low_later));
        assert!(page_sort_key(None, None, &low_later) < page_sort_key(None, None, &high));

        // a known chain position sorts after the whole unknowable group,
        // regardless of its TxoRef
        let positioned = TxoRef(Hash::from([0x00; 32]), 0);
        let meta = BlockRefMeta {
            slot: 1,
            hash: Hash::from([0x11; 32]),
            height: 1,
            tx_hash: Hash::from([0x00; 32]),
            tx_index: 0,
        };

        assert!(page_sort_key(None, None, &high) < page_sort_key(Some(&meta), None, &positioned));
    }

    /// The window covers the page and grows to whole slot groups at both
    /// ends, because rows inside one slot are not ordered yet.
    #[test]
    fn slot_window_covers_whole_slot_groups() {
        // slots in page order, three rows in slot 20
        let slots = [Some(10), Some(20), Some(20), Some(20), Some(30), Some(40)];

        // page starts and ends inside the slot-20 group
        assert_eq!(slot_window(&slots, 2, 1), 1..4);
        // page ends in slot 20, starts on slot 10: grows only at the end
        assert_eq!(slot_window(&slots, 0, 2), 0..4);
        // page starts in slot 20, ends on slot 30: grows only at the start
        assert_eq!(slot_window(&slots, 3, 2), 1..5);
        // page past the end is empty
        assert_eq!(slot_window(&slots, 6, 3), 6..6);
        assert_eq!(slot_window(&slots, 99, 3), 6..6);
        // page reaching past the end clamps
        assert_eq!(slot_window(&slots, 4, 10), 4..6);
    }

    /// Rows with no slot are already in their final order and never widen
    /// the window.
    #[test]
    fn slot_window_ignores_pruned_rows() {
        let slots = [None, None, None, Some(10), Some(10)];

        assert_eq!(slot_window(&slots, 1, 1), 1..2);
        assert_eq!(slot_window(&slots, 2, 2), 2..5);
    }
}
