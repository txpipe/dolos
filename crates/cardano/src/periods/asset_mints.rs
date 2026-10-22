//! Asset mints: every mint and burn of each asset, serving
//! `/assets/{asset}/history`.
//!
//! A sealed epoch holds one list per asset it minted or burned, in chain
//! order, at `epoch_start(e) ‖ AssetKey ‖ chunk`. A reader point-reads chunk 0
//! of each epoch for its count and loads only the chunks its page covers, so
//! no page decodes a block.

use std::{collections::BTreeMap, ops::RangeInclusive};

use dolos_core::{ArchiveWriter as _, ChainError, Domain, EntityValue, RangeOf, StateStore as _};
use pallas::{crypto::hash::Hash, ledger::primitives::Epoch};

use super::{Period, Segment};
use crate::{AssetKey, AssetMintChunk, AssetMintSet, ChainSummary};
use dolos_core::SlotOrder;

/// Seal the staged mints of every epoch `sealable` accepts.
///
/// Each (asset, epoch) list depends on its group's staged rows alone, and a
/// group never splits across seal chunks, so the rows seal chunk by chunk and
/// a re-run writes the same lists again.
pub(crate) fn seal<D: Domain>(
    state: &D::State,
    archive: &D::Archive,
    chain: &ChainSummary,
    sealable: &dyn Fn(Epoch) -> bool,
    batch: usize,
) -> Result<(), ChainError> {
    let mut rows = Vec::new();

    for row in state.iter_entities_typed::<AssetMintSet>(None)? {
        let (key, row) = row?;

        if sealable(key.epoch) {
            rows.push(Ok((key, row)));
        }
    }

    super::seal_groups::<D, AssetMintSet, _, _, _>(state, archive, rows, batch, |chunk, writer| {
        let mut mints = Vec::new();

        // rows come in key order: asset, then epoch, then chain order
        for (i, (key, row)) in chunk.iter().enumerate() {
            mints.push((row.tx_hash, row.amount));

            let closes = chunk
                .get(i + 1)
                .is_none_or(|(next, _)| next.group != key.group || next.epoch != key.epoch);

            if closes {
                let slot = chain.epoch_start(key.epoch);

                for (i, list) in super::chunk::<AssetMintChunk>(std::mem::take(&mut mints))
                    .into_iter()
                    .enumerate()
                {
                    writer.write_log_typed(slot, &(key.group, i as u32), &list)?;
                }
            }
        }

        Ok(())
    })
}

/// Per-epoch segments of an asset's list: staged epochs keep their mints in
/// chain order, sealed ones their count.
fn segments(
    periods: BTreeMap<Epoch, Period<AssetMintSet, EntityValue>>,
) -> Result<Vec<Segment<(Hash<32>, i64)>>, ChainError> {
    periods
        .into_iter()
        .map(|(epoch, period)| match period {
            Period::Staged(rows) => Ok(Segment::Staged(
                rows.into_iter()
                    .map(|row| (row.tx_hash, row.amount))
                    .collect(),
            )),
            Period::Sealed(first) => Ok(Segment::Sealed {
                epoch,
                count: super::chunk_count(&first)?,
                first,
            }),
        })
        .collect()
}

/// One page of `asset`'s mints and burns over `epochs`, as `(tx hash,
/// amount)` in chain order.
///
/// Staged rows are read first and sealed ones after, per epoch the sealed
/// list winning, so a seal running beside the read leaves the page unchanged.
pub fn page<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    asset: &AssetKey,
    order: SlotOrder,
    skip: usize,
    count: usize,
) -> Result<Vec<(Hash<32>, i64)>, ChainError> {
    let (state, archive) = (domain.state(), domain.archive());

    let mut staged = super::read_staged::<_, AssetMintSet>(state, RangeOf::prefix(asset))?;
    staged.retain(|epoch, _| epochs.contains(epoch));

    let sealed = super::read_sealed::<_, AssetMintChunk>(archive, chain, epochs, |_| (*asset, 0))?;

    let segments = segments(super::join(staged, sealed))?;

    super::page::<AssetMintChunk>(segments, order, skip, count, |epoch, chunks| {
        super::load_chunks(archive, chain, epoch, chunks, |i| (*asset, i))
    })
}

#[cfg(test)]
mod tests {
    use dolos_core::{
        ArchiveStore as _, BlockSlot, Domain as _, StateStore as _, StateWriter as _,
    };
    use dolos_testing::{
        faults::{FaultyToyDomain, TestFault},
        toy_domain::ToyDomain,
    };

    use super::*;
    use crate::periods::{is_sealable, GroupKey, CHUNK_LEN};
    use crate::MintPosition;

    /// The epoch the sealing boundary opens: 5 and 6 are immutable at it, 7 is
    /// not.
    const BOUNDARY: Epoch = 8;

    /// `A` mints and burns in epochs 5 to 7, more than a sealed chunk holds in
    /// epoch 6. `B` sorts right after it and mints once in each epoch.
    const A: [u8; 32] = [0x11; 32];
    const B: [u8; 32] = [0x12; 32];

    fn chain(domain: &ToyDomain) -> ChainSummary {
        crate::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap()
    }

    fn tx(asset: [u8; 32], epoch: Epoch, i: u32) -> Hash<32> {
        let mut hash = [0u8; 32];
        hash[0] = asset[0];
        hash[1] = epoch as u8;
        hash[28..].copy_from_slice(&i.to_be_bytes());
        hash.into()
    }

    fn seeded() -> ToyDomain {
        let domain = ToyDomain::new(None, None);
        let chain = chain(&domain);

        let writer = domain.state().start_writer().unwrap();

        let row = |asset: [u8; 32], epoch: Epoch, i: u32, amount: i64| {
            let key = GroupKey {
                group: AssetKey(asset.into()),
                epoch,
                member: (chain.epoch_start(epoch) + u64::from(i / 3), i % 3),
            };
            let row = AssetMintSet {
                tx_hash: tx(asset, epoch, i),
                amount,
            };
            writer.write_entity_typed(&key, &row).unwrap();
        };

        for i in 0..4 {
            row(A, 5, i, if i % 2 == 0 { 10 } else { -3 });
        }
        for i in 0..(CHUNK_LEN as u32 + 40) {
            row(A, 6, i, i64::from(i) + 1);
        }
        row(A, 7, 0, -1);
        row(A, 7, 1, 2);

        for epoch in 5..=7 {
            row(B, epoch, 0, 1);
        }

        writer.commit().unwrap();

        domain
    }

    fn sealable(domain: &ToyDomain) -> impl Fn(Epoch) -> bool {
        let chain = chain(domain);
        let mutable = crate::utils::mutable_slots(&domain.genesis());
        move |epoch| is_sealable(&chain, mutable, epoch, BOUNDARY)
    }

    fn seal_as<D: Domain>(domain: &D, at: &ToyDomain) -> Result<(), ChainError> {
        seal::<D>(
            domain.state(),
            domain.archive(),
            &chain(at),
            &sealable(at),
            2,
        )
    }

    type Logs = Vec<(BlockSlot, (AssetKey, u32), AssetMintChunk)>;

    fn logs(domain: &ToyDomain) -> Logs {
        domain
            .archive()
            .iter_logs_typed::<AssetMintChunk>(None)
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn staged(domain: &ToyDomain) -> Vec<(GroupKey<AssetKey, MintPosition>, AssetMintSet)> {
        domain
            .state()
            .iter_entities_typed::<AssetMintSet>(None)
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn page_of(
        domain: &ToyDomain,
        asset: [u8; 32],
        order: SlotOrder,
        skip: usize,
        count: usize,
    ) -> Vec<(Hash<32>, i64)> {
        page(
            domain,
            &chain(domain),
            0..=BOUNDARY,
            &AssetKey(asset.into()),
            order,
            skip,
            count,
        )
        .unwrap()
    }

    /// Every page both lists can answer, in both orders.
    fn pages(domain: &ToyDomain) -> Vec<Vec<(Hash<32>, i64)>> {
        let mut out = Vec::new();

        for order in [SlotOrder::Asc, SlotOrder::Desc] {
            for count in [7, 100] {
                for skip in (0..320).step_by(count) {
                    for asset in [A, B] {
                        out.push(page_of(domain, asset, order, skip, count));
                    }
                }
            }
        }

        out
    }

    #[test]
    fn a_seal_moves_immutable_epochs_to_the_archive() {
        let domain = seeded();
        let before = pages(&domain);

        seal_as(&domain, &domain).unwrap();

        // only epoch 7 stays staged
        assert!(staged(&domain).iter().all(|(key, _)| key.epoch == 7));
        assert_eq!(staged(&domain).len(), 3);

        // A: one chunk in epoch 5, two in epoch 6; B: one in each
        let chunks = logs(&domain);
        assert_eq!(chunks.len(), 1 + 2 + 2);

        let counts: Vec<_> = chunks
            .iter()
            .map(|(_, (asset, i), chunk)| (asset.0[0], *i, chunk.count, chunk.mints.len()))
            .collect();
        assert!(counts.contains(&(0x11, 0, CHUNK_LEN as u64 + 40, CHUNK_LEN)));
        assert!(counts.contains(&(0x11, 1, CHUNK_LEN as u64 + 40, 40)));

        assert_eq!(pages(&domain), before);

        // chain order, both orders, signed amounts kept
        let all = page_of(&domain, A, SlotOrder::Asc, 0, 1_000);
        assert_eq!(all.len(), 4 + CHUNK_LEN + 40 + 2);
        assert_eq!(all[..2], [(tx(A, 5, 0), 10), (tx(A, 5, 1), -3)]);
        assert_eq!(all[4], (tx(A, 6, 0), 1));
        assert_eq!(all[all.len() - 2..], [(tx(A, 7, 0), -1), (tx(A, 7, 1), 2)]);

        let mut desc = page_of(&domain, A, SlotOrder::Desc, 0, 1_000);
        desc.reverse();
        assert_eq!(desc, all);

        // a page inside epoch 6's second chunk
        assert_eq!(
            page_of(&domain, A, SlotOrder::Asc, 4 + CHUNK_LEN + 1, 2),
            [
                (tx(A, 6, CHUNK_LEN as u32 + 1), CHUNK_LEN as i64 + 2),
                (tx(A, 6, CHUNK_LEN as u32 + 2), CHUNK_LEN as i64 + 3),
            ]
        );

        assert_eq!(page_of(&domain, B, SlotOrder::Asc, 0, 10).len(), 3);
    }

    /// Whichever commit the seal dies after, a re-run writes the same chunk
    /// rows and leaves no staged row of a sealed epoch.
    #[test]
    fn a_crash_at_any_commit_seals_the_same_rows() {
        let clean = seeded();
        seal_as(&clean, &clean).unwrap();

        let expected = (logs(&clean), staged(&clean));

        let mut crash_after = 0;

        loop {
            let domain = seeded();
            let crashing =
                FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(crash_after));

            if seal_as(&crashing, &domain).is_ok() {
                break;
            }

            seal_as(&domain, &domain).expect("re-run");

            assert_eq!(
                (logs(&domain), staged(&domain)),
                expected,
                "after a crash following commit {crash_after}"
            );

            crash_after += 1;
        }

        // an archive and a state commit per group chunk
        assert!(crash_after >= 2, "only {crash_after} commits");
    }

    /// A read landing between any two of the seal's commits answers every page
    /// as a read before it.
    #[test]
    fn a_read_beside_a_seal_answers_the_same_pages() {
        let before = pages(&seeded());

        let mut crash_after = 0;

        loop {
            let domain = seeded();
            let crashing =
                FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(crash_after));
            let done = seal_as(&crashing, &domain).is_ok();

            assert_eq!(
                pages(&domain),
                before,
                "between commits {crash_after} and next"
            );

            if done {
                break;
            }

            crash_after += 1;
        }
    }
}
