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
/// `from` / `to` on the pagination are ignored: Blockfrost does not accept
/// them on the address and script UTxO endpoints. Use
/// [`load_utxo_models_in_height_range`] for the endpoints that do.
pub async fn load_utxo_models<D, T>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    pagination: Pagination,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let retention = Retention::read(domain).await?;

    load_utxo_models_in_slot_range(domain, refs, pagination, retention, None).await
}

/// Like [`load_utxo_models`] but honours `from` / `to` as an inclusive block
/// height range, the way Blockfrost's `/assets/{asset}/utxos` does. An
/// optional `:index` on either bound also cuts inside that block by tx index,
/// as it does on the other endpoints that take `from` / `to`.
///
/// An output whose creation block was pruned by `sync.max_history` has no
/// known height, so it can never be proven to sit inside the range, and the
/// model it feeds requires the block hash, height and time. Such rows are
/// left out rather than reported with made-up block data.
pub async fn load_utxo_models_in_height_range<D, T>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    pagination: Pagination,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let retention = Retention::read(domain).await?;

    // heights are monotonic in slots, so the range is applied on slots and
    // the rows at its two edge slots are held against the exact bounds later
    let start = match pagination.from.as_ref() {
        Some(from) => match retention.locate(domain, from.number).await? {
            HeightPosition::Retained(slot) => slot,
            // everything still held is newer than the pruned bound
            HeightPosition::Pruned => 0,
            HeightPosition::AboveTip => return Ok(vec![]),
        },
        None => 0,
    };

    let end = match pagination.to.as_ref() {
        Some(to) => match retention.locate(domain, to.number).await? {
            HeightPosition::Retained(slot) => slot,
            // nothing still held is that old
            HeightPosition::Pruned => return Ok(vec![]),
            HeightPosition::AboveTip => u64::MAX,
        },
        None => u64::MAX,
    };

    load_utxo_models_in_slot_range(domain, refs, pagination, retention, Some(start..=end)).await
}

/// What the archive still holds: the oldest retained slot and the tip height.
///
/// `sync.max_history` prunes block bodies every round but sweeps the index
/// entries only now and then, so a slot the index still knows can belong to
/// a block that is already gone. Every slot below `floor` is treated as
/// unknown for that reason.
#[derive(Clone, Copy)]
struct Retention {
    floor: BlockSlot,
    tip_height: u64,
}

enum HeightPosition {
    Retained(BlockSlot),
    Pruned,
    AboveTip,
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

                let tip_height = match domain.archive().get_tip()? {
                    Some((_, body)) => MultiEraBlock::decode(&body)
                        .map_err(|e| ArchiveError::InternalError(e.to_string()))?
                        .number(),
                    None => 0,
                };

                Ok(Retention { floor, tip_height })
            })
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }

    /// The slot of `height`, or why there is none.
    async fn locate<D>(&self, domain: &Facade<D>, height: u64) -> Result<HeightPosition, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
    {
        if height > self.tip_height {
            return Ok(HeightPosition::AboveTip);
        }

        let slot = domain
            .query()
            .slot_by_number(height)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(match self.retained(slot) {
            Some(slot) => HeightPosition::Retained(slot),
            None => HeightPosition::Pruned,
        })
    }

    /// `slot` if its block is still held, `None` for a pruned or unknown one.
    fn retained(&self, slot: Option<BlockSlot>) -> Option<BlockSlot> {
        slot.filter(|slot| *slot >= self.floor)
    }
}

/// Positions the page in two passes so the cost stays proportional to the
/// number of refs for the cheap part only.
///
/// 1. The slot of every creating tx comes from the archive index, no block body
///    is read. Rows are sorted by slot, which fixes the order between blocks
///    and lets the page window be located.
/// 2. Only the rows of the slots the window touches get their block decoded,
///    which settles the order inside a block (tx index, then output index).
///
/// A row whose block was pruned has no position. With `range` such rows are
/// dropped, the model needs the block hash, height and time; without it they
/// stay and sort first, as the oldest.
///
/// `range` is the slot span of the `from` / `to` heights on `pagination`.
/// Only its two edge slots can hold rows outside the heights, see
/// [`drop_rows_past_height_bounds`].
async fn load_utxo_models_in_slot_range<D, T>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    pagination: Pagination,
    retention: Retention,
    range: Option<RangeInclusive<u64>>,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let chain = domain.get_chain_summary()?;

    let tx_hashes: Vec<TxHash> = refs.iter().map(|txo_ref| txo_ref.0).unique().collect();
    let slots: HashMap<TxHash, Option<u64>> = domain
        .query()
        .slots_by_tx_hashes(tx_hashes)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .collect();

    let mut rows: Vec<(Option<u64>, TxoRef)> = refs
        .into_iter()
        .map(|txo_ref| {
            let slot = retention.retained(slots.get(&txo_ref.0).copied().flatten());
            (slot, txo_ref)
        })
        .filter(|(slot, _)| match (&range, slot) {
            (Some(range), Some(slot)) => range.contains(slot),
            (Some(_), None) => false,
            (None, _) => true,
        })
        .collect();

    if let Some(range) = &range {
        drop_rows_past_height_bounds(domain, &mut rows, range, &pagination).await?;
    }

    // the same order `page_sort_key` gives, minus the position inside a block
    rows.sort_by_key(|(slot, txo_ref)| (*slot, txo_ref.0, txo_ref.1));
    if let Order::Desc = pagination.order {
        rows.reverse();
    }

    let window = slot_window(
        rows.iter().map(|(slot, _)| *slot),
        pagination.skip(),
        pagination.count,
    );
    let offset = pagination.skip() - window.start;
    let window: Vec<(Option<u64>, TxoRef)> = rows.drain(window).collect();

    // the slots are known already, so only the block bodies are read here
    let located: Vec<(TxHash, Option<BlockSlot>)> = window
        .iter()
        .map(|(slot, txo_ref)| (txo_ref.0, *slot))
        .unique_by(|(tx_hash, _)| *tx_hash)
        .collect();
    let (block_deps, _) = domain
        .query()
        .block_meta_by_located(located)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let block_deps: HashMap<TxHash, BlockRefMeta> = block_deps
        .into_iter()
        .filter_map(|(tx_hash, meta)| meta.map(|meta| (tx_hash, meta)))
        .collect();

    let mut positioned: Vec<_> = window
        .into_iter()
        .map(|(slot, txo_ref)| {
            let meta = block_deps.get(&txo_ref.0).cloned();
            (page_sort_key(meta.as_ref(), slot, &txo_ref), txo_ref, meta)
        })
        // a block pruned between the two reads is gone like a UTxO spent
        // between them: dropped from this page, not an error
        .filter(|(_, _, meta)| range.is_none() || meta.is_some())
        .collect();

    positioned.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));
    if let Order::Desc = pagination.order {
        positioned.reverse();
    }

    let page: Vec<_> = positioned
        .into_iter()
        .skip(offset)
        .take(pagination.count)
        .map(|(_, txo_ref, meta)| (txo_ref, meta))
        .collect();

    if page.is_empty() {
        return Ok(vec![]);
    }

    let utxos = domain
        .state()
        .get_utxos(page.iter().map(|(txo_ref, _)| txo_ref.clone()).collect())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut out = Vec::with_capacity(page.len());
    for (txo_ref, meta) in page {
        // the tag lookup and `get_utxos` read independent snapshots: a UTxO
        // spent between the two is simply gone from the map, not an error.
        let Some(cbor) = utxos.get(&txo_ref) else {
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

    Ok(out)
}

/// Drops the rows at the edge slots of `range` that sit past the exact
/// `from` / `to` bounds on `pagination`.
///
/// A slot is not always one block: a Byron epoch-boundary block shares its
/// slot with the main block that follows it, so `to` at a boundary block's
/// height maps to a slot that also holds the next height. A `:index` on
/// either bound cuts inside the edge block too. Every slot strictly between
/// the edges is inside the heights by monotonicity, so only the edge rows
/// get their block decoded — at most the two edge blocks.
async fn drop_rows_past_height_bounds<D>(
    domain: &Facade<D>,
    rows: &mut Vec<(Option<u64>, TxoRef)>,
    range: &RangeInclusive<u64>,
    pagination: &Pagination,
) -> Result<(), StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let edges = [*range.start(), *range.end()];

    let located: Vec<(TxHash, Option<BlockSlot>)> = rows
        .iter()
        .filter(|(slot, _)| slot.is_some_and(|slot| edges.contains(&slot)))
        .map(|(slot, txo_ref)| (txo_ref.0, *slot))
        .unique_by(|(tx_hash, _)| *tx_hash)
        .collect();

    if located.is_empty() {
        return Ok(());
    }

    let (metas, _) = domain
        .query()
        .block_meta_by_located(located)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let metas: HashMap<TxHash, Option<BlockRefMeta>> = metas.into_iter().collect();

    rows.retain(|(_, txo_ref)| match metas.get(&txo_ref.0) {
        // not on an edge slot
        None => true,
        Some(Some(meta)) => !pagination.should_skip(meta.height, meta.tx_index),
        // a block pruned since the slot lookup, dropped like the second pass
        // drops one
        Some(None) => false,
    });

    Ok(())
}

/// The index range of `slots` (already in page order) that has to be
/// positioned exactly to serve the page `skip..skip + count`.
///
/// Rows of one slot are not in their final order yet, so the window grows
/// to the whole slot group at both ends. Rows without a slot are already in
/// their final order and never widen it.
fn slot_window(
    slots: impl Iterator<Item = Option<u64>>,
    skip: usize,
    count: usize,
) -> Range<usize> {
    let slots: Vec<Option<u64>> = slots.collect();
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
/// A row whose block is still held but was not decoded for this page keeps
/// its slot position, with the tx index unknown.
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
        assert_eq!(slot_window(slots.iter().copied(), 2, 1), 1..4);
        // page ends in slot 20, starts on slot 10: grows only at the end
        assert_eq!(slot_window(slots.iter().copied(), 0, 2), 0..4);
        // page starts in slot 20, ends on slot 30: grows only at the start
        assert_eq!(slot_window(slots.iter().copied(), 3, 2), 1..5);
        // page past the end is empty
        assert_eq!(slot_window(slots.iter().copied(), 6, 3), 6..6);
        assert_eq!(slot_window(slots.iter().copied(), 99, 3), 6..6);
        // page reaching past the end clamps
        assert_eq!(slot_window(slots.iter().copied(), 4, 10), 4..6);
    }

    /// Rows with no slot are already in their final order and never widen
    /// the window.
    #[test]
    fn slot_window_ignores_pruned_rows() {
        let slots = [None, None, None, Some(10), Some(10)];

        assert_eq!(slot_window(slots.iter().copied(), 1, 1), 1..2);
        assert_eq!(slot_window(slots.iter().copied(), 2, 2), 2..5);
    }
}
