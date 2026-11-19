use axum::http::StatusCode;
use std::ops::RangeInclusive;

use dolos_core::{BlockSlot, Domain, Tag, TagEntry, TagOrder, TagStart, TxoRef, UtxoPosition};

use crate::{
    mapping::{IntoModel, UtxoOutputModelBuilder},
    pagination::Pagination,
    Facade,
};

use super::{
    models::into_models, page::tag_page_rows, rows::Rows, tag_order, walk::Walk, MAX_WINDOW,
};

/// A tag bound that every entry created before `slot` sorts before and every
/// entry created at `slot` or later sorts after. At slot 0 the entries without
/// a position (UTxOs no block produced) sort before it too.
fn before_slot(slot: BlockSlot) -> TagEntry {
    match slot.checked_sub(1) {
        Some(previous) => past_slot(previous),
        None => TagEntry::new(None, last_txo()),
    }
}

/// A tag bound that every entry created at `slot` or earlier sorts before and
/// every entry created later sorts after.
fn past_slot(slot: BlockSlot) -> TagEntry {
    let position = UtxoPosition::new(slot, UtxoPosition::MAX_TX_INDEX);

    TagEntry::new(Some(position), last_txo())
}

/// The ref every real entry at a position sorts before: no transaction hashes
/// to all ones.
fn last_txo() -> TxoRef {
    TxoRef([0xff; 32].into(), u32::MAX)
}

/// Loads one page of the UTxOs that carry `tag` and were created in a block
/// within `slots`, both ends inclusive, in creation order, as
/// [`load_tag_page`] does without the window. The rows carry their block's
/// time as well, for the models that report it.
///
/// The window bounds the tag walk itself: it starts just past the bound on the
/// walk's side and stops at the first entry past the other. The offset costs
/// tag entries, the page its rows and their blocks, as on a plain listing.
///
/// A height maps to one slot exactly, since an epoch-boundary block carries no
/// block number and no transaction, so a height range from
/// [`Pagination::start_and_end_slots`] bounds the rows without decoding a
/// block.
pub async fn load_tag_page_in_slots<D, T>(
    domain: &Facade<D>,
    tag: &Tag,
    pagination: &Pagination,
    slots: Option<RangeInclusive<BlockSlot>>,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let chain = domain.get_chain_summary()?;

    let Some(slots) = slots else {
        let rows = tag_page_rows(domain, tag, pagination).await?;
        return into_models(domain, rows, Some(&chain)).await;
    };

    let order = tag_order(&pagination.order);

    let (start, end) = match order {
        TagOrder::Asc => (before_slot(*slots.start()), past_slot(*slots.end())),
        TagOrder::Desc => (past_slot(*slots.end()), before_slot(*slots.start())),
    };

    let past_end = |entry: &TagEntry| match order {
        TagOrder::Asc => *entry > end,
        TagOrder::Desc => *entry < end,
    };

    let mut walk = Walk::tag(tag.clone(), order, TagStart::After(start));
    let mut rows = Rows::new(pagination.count);
    let mut skip = pagination.skip();

    while !rows.full() {
        let want = skip.saturating_add(rows.missing()).min(MAX_WINDOW);
        let mut entries = walk.next(domain, want).await?;

        let ended = match entries.iter().position(&past_end) {
            Some(at) => {
                entries.truncate(at);
                true
            }
            None => entries.is_empty(),
        };

        // The offset counts entries, their values unread. As the store's own
        // skip on a plain listing, it keeps no record of the refs it passes,
        // so a deep page holds the page's refs, not the offset's.
        let mut passed = rows.uncounted(entries);
        let page = passed.split_off(skip.min(passed.len()));
        skip -= passed.len();

        // the page's values only, a UTxO spent since the tag read giving its
        // place to the next entry
        let mut page = page.into_iter();

        while !rows.full() {
            let window: Vec<_> = page.by_ref().take(rows.missing()).collect();

            if window.is_empty() {
                break;
            }

            let loaded = rows.load(domain, window, false).await?;
            rows.rows.extend(loaded);
        }

        if ended {
            break;
        }
    }

    into_models(domain, rows.rows, Some(&chain)).await
}

#[cfg(test)]
mod tests {

    use super::*;
    use dolos_core::{TagEntry, TxoRef, UtxoPosition};

    /// The slot bounds sit between the entries of two slots: past every
    /// entry of the slots on one side, before every entry of the other.
    #[test]
    fn slot_bounds_split_the_entries_by_slot() {
        let entry = |position: Option<(u64, u16)>, index: u32| {
            TagEntry::new(
                position.map(|(slot, tx)| UtxoPosition::new(slot, tx)),
                TxoRef([0xfe; 32].into(), index),
            )
        };

        let unpositioned = entry(None, u32::MAX);
        let last_of_4 = entry(Some((4, UtxoPosition::MAX_TX_INDEX)), u32::MAX);
        let first_of_5 = entry(Some((5, 0)), 0);
        let last_of_5 = entry(Some((5, UtxoPosition::MAX_TX_INDEX)), u32::MAX);
        let first_of_6 = entry(Some((6, 0)), 0);

        assert!(last_of_4 < before_slot(5));
        assert!(before_slot(5) < first_of_5);

        assert!(last_of_5 < past_slot(5));
        assert!(past_slot(5) < first_of_6);

        // at slot 0 the entries without a position stay behind the bound
        assert!(unpositioned < before_slot(0));
        assert!(before_slot(0) < entry(Some((0, 0)), 0));
    }
}
