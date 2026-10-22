//! First scripts: the scripts each epoch saw first, serving `/scripts`
//! (decision 0057).
//!
//! A sealed epoch holds one list, in first-seen order, at
//! `epoch_start(e) ‖ chunk`. The `scripts` witness decides at roll time which
//! sighting is a script's first, so every staged row is listed.

use std::{collections::BTreeMap, ops::RangeInclusive};

use dolos_core::{ArchiveWriter as _, BlockSlot, ChainError, Domain, StateStore};
use pallas::{crypto::hash::Hash, ledger::primitives::Epoch};

use super::{Period, Segment};
use crate::{ChainSummary, FirstScriptChunk, FirstScriptSet};
use dolos_core::SlotOrder;

/// Seal the staged first scripts of every epoch `sealable` accepts.
///
/// Rows are keyed epoch first, so the sealable ones lead the namespace. Their
/// group is [`super::Ungrouped`], which never closes a `seal_groups` chunk
/// early: the seal is one archive commit, then one state commit. An epoch's
/// list depends on every staged row of it, so that is the split it needs, and
/// a re-run after a crash between the two writes the same chunks again.
pub(crate) fn seal<D: Domain>(
    state: &D::State,
    archive: &D::Archive,
    chain: &ChainSummary,
    sealable: &dyn Fn(Epoch) -> bool,
    batch: usize,
) -> Result<(), ChainError> {
    let mut rows = Vec::new();

    for row in state.iter_entities_typed::<FirstScriptSet>(None)? {
        let (key, row) = row?;

        // epoch first: every row after an unsealable one is newer
        if !sealable(key.epoch) {
            break;
        }

        rows.push(Ok((key, row)));
    }

    super::seal_groups::<D, FirstScriptSet, _, _, _>(
        state,
        archive,
        rows,
        batch,
        |chunk, writer| {
            let mut epochs: BTreeMap<Epoch, Vec<(FirstScriptSet, Hash<28>)>> = BTreeMap::new();

            for (key, row) in chunk {
                epochs
                    .entry(key.epoch)
                    .or_default()
                    .push((*row, key.member.0));
            }

            for (epoch, mut scripts) in epochs {
                scripts.sort();
                let hashes = scripts.into_iter().map(|(_, hash)| hash).collect();
                let slot = chain.epoch_start(epoch);

                for (i, chunk) in super::chunk::<FirstScriptChunk>(hashes)
                    .into_iter()
                    .enumerate()
                {
                    writer.write_log_typed(slot, &(i as u32), &chunk)?;
                }
            }

            Ok(())
        },
    )
}

/// The staged first scripts in `epochs`, by epoch, in first-seen order.
///
/// A row whose block is past `archived` is left out: the roll commits state
/// before it hands the batch's blocks to the archive, and `/scripts/{hash}`
/// has nothing to serve for a script until its block is there.
fn read_staged<S: StateStore>(
    state: &S,
    epochs: &RangeInclusive<Epoch>,
    archived: BlockSlot,
) -> Result<BTreeMap<Epoch, Vec<Hash<28>>>, ChainError> {
    let mut out: BTreeMap<Epoch, Vec<(FirstScriptSet, Hash<28>)>> = BTreeMap::new();

    for row in state.iter_entities_typed::<FirstScriptSet>(None)? {
        let (key, row) = row?;

        if epochs.contains(&key.epoch) && row.slot <= archived {
            out.entry(key.epoch).or_default().push((row, key.member.0));
        }
    }

    Ok(out
        .into_iter()
        .map(|(epoch, mut scripts)| {
            scripts.sort();
            (epoch, scripts.into_iter().map(|(_, hash)| hash).collect())
        })
        .collect())
}

/// One page of the scripts first seen over `epochs`, in first-seen order,
/// up to the archive tip `archived`.
///
/// A pruned node holds the sealed chunks of its archive window only, so it
/// lists the scripts first seen in that window.
pub fn all_page<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    archived: BlockSlot,
    order: SlotOrder,
    skip: usize,
    count: usize,
) -> Result<Vec<Hash<28>>, ChainError> {
    let (state, archive) = (domain.state(), domain.archive());

    let staged = read_staged(state, &epochs, archived)?;
    let sealed = super::read_sealed::<_, FirstScriptChunk>(archive, chain, epochs, |_| 0)?;

    let mut segments = Vec::new();

    for (epoch, period) in super::join(staged, sealed) {
        segments.push(match period {
            Period::Staged(hashes) => Segment::Staged(hashes),
            Period::Sealed(first) => Segment::Sealed {
                epoch,
                count: super::chunk_count(&first)?,
                first,
            },
        });
    }

    super::page::<FirstScriptChunk>(segments, order, skip, count, |epoch, chunks| {
        super::load_chunks(archive, chain, epoch, chunks, |i| i)
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
    use crate::{
        periods::{is_sealable, GroupKey, Ungrouped},
        FirstScriptKey, ScriptKey,
    };

    /// The epoch the sealing boundary opens: 5 and 6 are immutable at it, 7 is
    /// not.
    const BOUNDARY: Epoch = 8;

    fn chain(domain: &ToyDomain) -> ChainSummary {
        crate::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap()
    }

    fn hash(epoch: Epoch, i: u32) -> Hash<28> {
        let mut bytes = [0u8; 28];
        bytes[0] = epoch as u8;
        bytes[24..].copy_from_slice(&i.to_be_bytes());

        // keyed by hash, so hash order runs against first-seen order
        bytes.iter_mut().for_each(|x| *x = !*x);
        Hash::new(bytes)
    }

    /// Every script staged in epochs 5 to 7, in first-seen order: epoch 6
    /// holds more than a sealed chunk does.
    fn listing(domain: &ToyDomain) -> Vec<(Epoch, Hash<28>, FirstScriptSet)> {
        let chain = chain(domain);
        let mut out = Vec::new();

        for (epoch, scripts) in [(5, 7u32), (6, 300), (7, 3)] {
            for i in 0..scripts {
                let row = FirstScriptSet {
                    slot: chain.epoch_start(epoch) + u64::from(i / 10),
                    tx_index: (i % 10) / 3,
                    order: i % 3,
                };

                out.push((epoch, hash(epoch, i), row));
            }
        }

        out
    }

    fn seeded() -> ToyDomain {
        let domain = ToyDomain::new(None, None);
        let writer = domain.state().start_writer().unwrap();

        for (epoch, hash, row) in listing(&domain) {
            let key = GroupKey {
                group: Ungrouped,
                epoch,
                member: ScriptKey(hash),
            };

            writer.write_entity_typed(&key, &row).unwrap();
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

    fn logs(domain: &ToyDomain) -> Vec<(BlockSlot, u32, FirstScriptChunk)> {
        domain
            .archive()
            .iter_logs_typed::<FirstScriptChunk>(None)
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn staged(domain: &ToyDomain) -> Vec<(FirstScriptKey, FirstScriptSet)> {
        domain
            .state()
            .iter_entities_typed::<FirstScriptSet>(None)
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn page(domain: &ToyDomain, order: SlotOrder, skip: usize, count: usize) -> Vec<Hash<28>> {
        all_page(
            domain,
            &chain(domain),
            0..=BOUNDARY,
            u64::MAX,
            order,
            skip,
            count,
        )
        .unwrap()
    }

    /// Every page the list can answer, in both orders.
    fn pages(domain: &ToyDomain) -> Vec<Vec<Hash<28>>> {
        let mut out = Vec::new();

        for order in [SlotOrder::Asc, SlotOrder::Desc] {
            for count in [5, 300] {
                for skip in (0..320).step_by(count) {
                    out.push(page(domain, order, skip, count));
                }
            }
        }

        out
    }

    /// Both orders, across sealed epochs and an unsealed one, list every
    /// script in first-seen order.
    #[test]
    fn pages_list_first_seen_order_across_sealed_and_staged_epochs() {
        let domain = seeded();
        seal_as(&domain, &domain).unwrap();

        assert!(staged(&domain).iter().all(|(key, _)| key.epoch == 7));
        assert_eq!(
            logs(&domain).len(),
            1 + 2,
            "epoch 5 in one chunk, epoch 6 in two"
        );

        let mut expected = listing(&domain);
        expected.sort_by_key(|(_, _, row)| *row);
        let expected: Vec<Hash<28>> = expected.into_iter().map(|(_, hash, _)| hash).collect();

        assert_eq!(page(&domain, SlotOrder::Asc, 0, 1_000), expected);

        let reversed: Vec<_> = expected.iter().rev().copied().collect();
        assert_eq!(page(&domain, SlotOrder::Desc, 0, 1_000), reversed);

        // a page straddling the sealed epoch 6 and the staged epoch 7
        assert_eq!(page(&domain, SlotOrder::Asc, 305, 4), expected[305..309]);
        assert_eq!(page(&domain, SlotOrder::Desc, 1, 4), reversed[1..5]);

        // a page inside the second chunk of epoch 6
        assert_eq!(page(&domain, SlotOrder::Asc, 270, 5), expected[270..275]);
    }

    /// A staged row whose block the archive does not hold yet is left out.
    #[test]
    fn staged_rows_past_the_archive_tip_are_left_out() {
        let domain = seeded();
        let chain = chain(&domain);
        let tip = chain.epoch_start(7);

        let page = all_page(&domain, &chain, 0..=BOUNDARY, tip, SlotOrder::Asc, 0, 1_000).unwrap();

        // epoch 7's scripts are all at its first slot
        assert_eq!(page.len(), 7 + 300 + 3);

        let page = all_page(
            &domain,
            &chain,
            0..=BOUNDARY,
            tip - 1,
            SlotOrder::Asc,
            0,
            1_000,
        )
        .unwrap();

        assert_eq!(page.len(), 7 + 300);
    }

    /// The seal interrupted between its archive commit and its state delete:
    /// the re-run writes the same chunk rows and leaves no set row of a
    /// sealed epoch, and a read at any point answers the same pages.
    #[test]
    fn a_crash_between_the_seal_commits_reseals_the_same_rows() {
        let clean = seeded();
        let before = pages(&clean);
        seal_as(&clean, &clean).unwrap();

        let expected = (logs(&clean), staged(&clean));
        assert_eq!(pages(&clean), before);

        let mut crash_after = 0;

        loop {
            let domain = seeded();
            let crashing =
                FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(crash_after));

            if seal_as(&crashing, &domain).is_ok() {
                break;
            }

            assert_eq!(pages(&domain), before, "a read after commit {crash_after}");

            seal_as(&domain, &domain).expect("re-run");

            assert_eq!(
                (logs(&domain), staged(&domain)),
                expected,
                "after a crash following commit {crash_after}"
            );

            crash_after += 1;
        }

        // the archive commit, then the state delete
        assert_eq!(crash_after, 2);
    }
}
