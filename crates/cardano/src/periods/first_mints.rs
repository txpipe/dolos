//! First mints: the assets each epoch minted for the first time, overall and
//! per policy, serving `/assets` and `/assets/policy/{policy}`.
//!
//! A sealed epoch holds two lists, in mint order: every subject it first
//! minted, at `epoch_start(e) ‖ chunk`, and each policy's names, at
//! `epoch_start(e) ‖ policy ‖ chunk`. The roll's witness decides which mint
//! is an asset's first, so every staged row is listed, and the seal records
//! each one in the archive's entity index, where the witness finds it once
//! the staged row is gone.

use std::{collections::BTreeMap, ops::RangeInclusive};

use dolos_core::{
    ArchiveStore, ArchiveWriter as _, ChainError, Domain, EntityValue, LogKey, RangeOf, StateStore,
};
use pallas::{crypto::hash::Hash, ledger::primitives::Epoch};

use super::{Period, Segment};
use crate::{
    indexes::{entities, SlotOrder},
    ChainSummary, FirstMintChunk, FirstMintSet, FixedNamespace as _, PolicyKey, PolicyMintChunk,
};

fn policy_key(policy: &Hash<28>, chunk: u32) -> (PolicyKey, u32) {
    (PolicyKey(*policy), chunk)
}

/// Seal the staged first mints of every epoch `sealable` accepts.
///
/// The all-subjects list of an epoch depends on every staged row of it, so it
/// is written first, whole and in one archive commit, before any of those rows
/// is deleted; a re-run that finds it sealed leaves it alone. The per-policy
/// lists depend on their group only, and seal chunk by chunk.
///
/// Each chunk's archive commit also writes its rows' entity index entries,
/// before the state commit deletes the rows, so the roll's witness finds every
/// sealed first mint in one of the two.
pub(crate) fn seal<D: Domain>(
    state: &D::State,
    archive: &D::Archive,
    chain: &ChainSummary,
    sealable: &dyn Fn(Epoch) -> bool,
    batch: usize,
) -> Result<(), ChainError> {
    let mut rows = Vec::new();

    for row in state.iter_entities_typed::<FirstMintSet>(None)? {
        let (key, row) = row?;

        if sealable(row.epoch) {
            rows.push((key, row));
        }
    }

    if rows.is_empty() {
        return Ok(());
    }

    let mut epochs: BTreeMap<Epoch, Vec<&FirstMintSet>> = BTreeMap::new();

    for (_, row) in &rows {
        epochs.entry(row.epoch).or_default().push(row);
    }

    for (epoch, mut mints) in epochs {
        let slot = chain.epoch_start(epoch);
        let head = LogKey::of(slot, &0u32);

        if archive.read_logs(FirstMintChunk::NS, &[&head])?[0].is_some() {
            continue;
        }

        mints.sort_by_key(|row| row.mint_order());
        let subjects = mints.into_iter().map(FirstMintSet::subject).collect();

        let writer = archive.start_writer()?;

        for (i, chunk) in super::chunk::<FirstMintChunk>(subjects)
            .into_iter()
            .enumerate()
        {
            writer.write_log_typed(slot, &(i as u32), &chunk)?;
        }

        writer.commit()?;
    }

    let rows = rows.into_iter().map(Ok);

    super::seal_groups::<D, FirstMintSet, _, _, _>(state, archive, rows, batch, |chunk, writer| {
        let mut groups: BTreeMap<(Epoch, Hash<28>), Vec<&FirstMintSet>> = BTreeMap::new();

        for (key, row) in chunk {
            writer.write_entity(entities::ASSET, key.member.0.as_slice(), row.slot)?;
            groups.entry((row.epoch, row.policy)).or_default().push(row);
        }

        for ((epoch, policy), mut mints) in groups {
            mints.sort_by_key(|row| row.mint_order());
            let names = mints.into_iter().map(|row| row.name.clone()).collect();

            for (i, chunk) in super::chunk::<PolicyMintChunk>(names)
                .into_iter()
                .enumerate()
            {
                let slot = chain.epoch_start(epoch);
                writer.write_log_typed(slot, &policy_key(&policy, i as u32), &chunk)?;
            }
        }

        Ok(())
    })
}

/// Per-epoch segments of one list: staged epochs keep their first mints in
/// mint order, sealed ones their count.
fn segments(
    periods: BTreeMap<Epoch, Period<FirstMintSet, EntityValue>>,
    item: impl Fn(FirstMintSet) -> Vec<u8>,
) -> Result<Vec<Segment<Vec<u8>>>, ChainError> {
    let mut out = Vec::with_capacity(periods.len());

    for (epoch, period) in periods {
        let segment = match period {
            Period::Staged(mut mints) => {
                mints.sort_by_key(|row| row.mint_order());

                Segment::Staged(mints.into_iter().map(&item).collect())
            }
            Period::Sealed(first) => Segment::Sealed {
                epoch,
                count: super::chunk_count(&first)?,
                first,
            },
        };

        out.push(segment);
    }

    Ok(out)
}

/// One page of the subjects first minted over `epochs`, in mint order.
pub fn all_page<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    order: SlotOrder,
    skip: usize,
    count: usize,
) -> Result<Vec<Vec<u8>>, ChainError> {
    let (state, archive) = (domain.state(), domain.archive());

    let mut staged = super::read_staged::<_, FirstMintSet>(state, RangeOf::all())?;
    staged.retain(|epoch, _| epochs.contains(epoch));

    let sealed = super::read_sealed::<_, FirstMintChunk>(archive, chain, epochs, |_| 0)?;

    let segments = segments(super::join(staged, sealed), |row| row.subject())?;

    super::page::<FirstMintChunk>(segments, order, skip, count, |epoch, chunks| {
        super::load_chunks(archive, chain, epoch, chunks, |i| i)
    })
}

/// One page of the subjects `policy` first minted over `epochs`, in mint
/// order, or `None` where it never minted.
pub fn policy_page<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    policy: &Hash<28>,
    order: SlotOrder,
    skip: usize,
    count: usize,
) -> Result<Option<Vec<Vec<u8>>>, ChainError> {
    let (state, archive) = (domain.state(), domain.archive());

    let mut staged =
        super::read_staged::<_, FirstMintSet>(state, RangeOf::prefix(&PolicyKey(*policy)))?;

    staged.retain(|epoch, _| epochs.contains(epoch));

    let sealed = super::read_sealed::<_, PolicyMintChunk>(archive, chain, epochs, |_| {
        policy_key(policy, 0)
    })?;

    let segments = segments(super::join(staged, sealed), |row| row.name)?;

    if segments.iter().all(Segment::is_empty) {
        return Ok(None);
    }

    let names = super::page::<PolicyMintChunk>(segments, order, skip, count, |epoch, chunks| {
        super::load_chunks(archive, chain, epoch, chunks, |i| policy_key(policy, i))
    })?;

    Ok(Some(
        names
            .into_iter()
            .map(|name| [policy.as_slice(), &name].concat())
            .collect(),
    ))
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
    use crate::periods::{is_sealable, GroupKey, GroupRow as _};
    use crate::AssetKey;

    /// The epoch the sealing boundary opens: 5 and 6 are immutable at it, 7 is
    /// not.
    const BOUNDARY: Epoch = 8;

    const P: [u8; 28] = [0x11; 28];
    const Q: [u8; 28] = [0x22; 28];

    /// Shares all of `P` but its last byte, so its staged keys sort right
    /// after `P`'s.
    fn sibling() -> [u8; 28] {
        let mut policy = P;
        policy[27] = 0x99;
        policy
    }

    fn chain(domain: &ToyDomain) -> ChainSummary {
        crate::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap()
    }

    /// Staged first mints in epochs 5 to 7, one row per asset as the roll's
    /// witness stages them. `Q` mints more names in epoch 6 than a sealed
    /// chunk holds.
    fn seeded() -> ToyDomain {
        let domain = ToyDomain::new(None, None);
        let chain = chain(&domain);

        let mut rows = Vec::new();
        let mut row = |policy: [u8; 28], name: String, epoch: Epoch, offset: u64, tx: u32| {
            rows.push(FirstMintSet {
                policy: policy.into(),
                name: name.into_bytes(),
                epoch,
                slot: chain.epoch_start(epoch) + offset,
                tx_index: tx,
            });
        };

        for (i, name) in ["E", "B", "A", "D", "C"].into_iter().enumerate() {
            row(P, name.into(), 5, 10 + i as u64 / 2, i as u32 % 2);
        }
        row(sibling(), "A".into(), 5, 10, 1);
        row(Q, "X".into(), 5, 30, 0);
        row(P, "F".into(), 6, 5, 0);
        row(sibling(), "B".into(), 6, 5, 0);
        for i in 0..300 {
            row(Q, format!("N{i:03}"), 6, 7 + i / 100, (i % 100) as u32);
        }
        row(P, "G".into(), 7, 1, 0);
        row(Q, "Y".into(), 7, 1, 1);

        let writer = domain.state().start_writer().unwrap();

        for row in &rows {
            writer.write_entity_typed(&row.key(), row).unwrap();
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

    type Logs = (
        Vec<(BlockSlot, u32, FirstMintChunk)>,
        Vec<(BlockSlot, (PolicyKey, u32), PolicyMintChunk)>,
    );

    fn logs(domain: &ToyDomain) -> Logs {
        let archive = domain.archive();

        let all = archive
            .iter_logs_typed::<FirstMintChunk>(None)
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let policies = archive
            .iter_logs_typed::<PolicyMintChunk>(None)
            .unwrap()
            .map(Result::unwrap)
            .collect();

        (all, policies)
    }

    fn staged(domain: &ToyDomain) -> Vec<(GroupKey<PolicyKey, AssetKey>, FirstMintSet)> {
        domain
            .state()
            .iter_entities_typed::<FirstMintSet>(None)
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    /// Every page the two lists can answer, in both orders.
    fn pages(domain: &ToyDomain) -> Vec<Option<Vec<Vec<u8>>>> {
        let chain = chain(domain);
        let mut out = Vec::new();

        for order in [SlotOrder::Asc, SlotOrder::Desc] {
            for count in [5, 300] {
                for skip in (0..330).step_by(count) {
                    out.push(Some(
                        all_page(domain, &chain, 0..=BOUNDARY, order, skip, count).unwrap(),
                    ));

                    for policy in [P, Q, sibling()] {
                        out.push(
                            policy_page(
                                domain,
                                &chain,
                                0..=BOUNDARY,
                                &policy.into(),
                                order,
                                skip,
                                count,
                            )
                            .unwrap(),
                        );
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
        assert!(staged(&domain).iter().all(|(_, row)| row.epoch == 7));
        assert_eq!(staged(&domain).len(), 2);

        let (all, policies) = logs(&domain);
        assert_eq!(all.len(), 1 + 2, "epoch 5 in one chunk, epoch 6 in two");
        assert!(policies.len() > 4);

        assert_eq!(pages(&domain), before);

        // epoch 5 in mint order: slot, tx index, subject, where the sibling's
        // "A" follows P's "B" in the same tx
        let page = all_page(&domain, &chain(&domain), 5..=5, SlotOrder::Asc, 0, 10).unwrap();
        let names: Vec<_> = page.iter().map(|s| (s[0], s[28..].to_vec())).collect();
        assert_eq!(page[2][..28], sibling());
        assert_eq!(
            names,
            vec![
                (0x11, b"E".to_vec()),
                (0x11, b"B".to_vec()),
                (0x11, b"A".to_vec()),
                (0x11, b"A".to_vec()),
                (0x11, b"D".to_vec()),
                (0x11, b"C".to_vec()),
                (0x22, b"X".to_vec()),
            ]
        );

        // every staged row is listed: the roll's witness stages first mints
        // only, so the seal applies none
        let p = policy_page(
            &domain,
            &chain(&domain),
            0..=BOUNDARY,
            &P.into(),
            SlotOrder::Asc,
            0,
            100,
        )
        .unwrap()
        .unwrap();
        let names: Vec<_> = p.iter().map(|s| s[28..].to_vec()).collect();
        assert_eq!(names, [b"E", b"B", b"A", b"D", b"C", b"F", b"G"]);
    }

    /// Each staged row's first-mint slot in the entity index, `None` where
    /// the index holds none.
    fn indexed(domain: &ToyDomain, rows: &[FirstMintSet]) -> Vec<Option<BlockSlot>> {
        let keys: Vec<AssetKey> = rows.iter().map(FirstMintSet::asset_key).collect();
        let keys: Vec<&[u8]> = keys.iter().map(|k| k.0.as_slice()).collect();

        domain
            .archive()
            .entity_slots(entities::ASSET, &keys)
            .unwrap()
    }

    /// The seal records every sealed first mint's slot in the entity index, in
    /// the archive commit that seals its group, and a crash at any commit
    /// leaves the same entries after the re-run. Between the commits an asset
    /// is in the index or staged, never in neither.
    #[test]
    fn the_seal_indexes_each_sealed_asset_once_whatever_commit_it_dies_at() {
        let clean = seeded();
        let rows: Vec<FirstMintSet> = staged(&clean).into_iter().map(|(_, r)| r).collect();
        seal_as(&clean, &clean).unwrap();

        let expected: Vec<_> = rows
            .iter()
            .map(|r| (r.epoch < 7).then_some(r.slot))
            .collect();
        assert_eq!(indexed(&clean, &rows), expected);

        let mut crash_after = 0;

        loop {
            let domain = seeded();
            let crashing =
                FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(crash_after));

            if seal_as(&crashing, &domain).is_ok() {
                break;
            }

            let still: std::collections::HashSet<_> =
                staged(&domain).into_iter().map(|(k, _)| k.member).collect();

            for (row, slot) in rows.iter().zip(indexed(&domain, &rows)) {
                assert!(
                    slot.is_some() || still.contains(&row.asset_key()),
                    "lost {} after commit {crash_after}",
                    hex::encode(row.subject())
                );
            }

            seal_as(&domain, &domain).expect("re-run");
            assert_eq!(
                indexed(&domain, &rows),
                expected,
                "crash after commit {crash_after}"
            );

            crash_after += 1;
        }

        // the epoch-wide commit plus a pair per group chunk
        assert!(crash_after > 4, "only {crash_after} commits");
    }

    /// Done criterion 1, seal idempotence: whichever commit the seal dies
    /// after, a re-run leaves exactly the rows of a seal that never crashed and
    /// nothing staged for the sealed epochs.
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

        // the epoch-wide commit plus a pair per group chunk
        assert!(crash_after > 4, "only {crash_after} commits");
    }

    /// Done criterion 1, concurrent seal: a read holding staged rows from
    /// before a seal and sealed rows from after it, or one landing between any
    /// two of its commits, answers every page as a read before or after it.
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

        // the staged half read before the seal, the sealed half after it
        let fresh = seeded();
        let domain = seeded();
        let chain = chain(&domain);
        let (state, archive) = (domain.state(), domain.archive());

        let staged_all =
            crate::periods::read_staged::<_, FirstMintSet>(state, RangeOf::all()).unwrap();
        let staged_q = crate::periods::read_staged::<_, FirstMintSet>(
            state,
            RangeOf::prefix(&PolicyKey(Q.into())),
        )
        .unwrap();

        seal_as(&domain, &domain).unwrap();

        let sealed_all =
            crate::periods::read_sealed::<_, FirstMintChunk>(archive, &chain, 0..=BOUNDARY, |_| 0)
                .unwrap();
        let sealed_q = crate::periods::read_sealed::<_, PolicyMintChunk>(
            archive,
            &chain,
            0..=BOUNDARY,
            |_| policy_key(&Q.into(), 0),
        )
        .unwrap();

        let all = segments(crate::periods::join(staged_all, sealed_all), |row| {
            row.subject()
        })
        .unwrap();
        let all = crate::periods::page::<FirstMintChunk>(all, SlotOrder::Asc, 0, 400, |e, c| {
            crate::periods::load_chunks(archive, &chain, e, c, |i| i)
        })
        .unwrap();
        let expected = all_page(&fresh, &chain, 0..=BOUNDARY, SlotOrder::Asc, 0, 400).unwrap();
        assert_eq!(all, expected);

        let q = segments(crate::periods::join(staged_q, sealed_q), |row| row.name).unwrap();
        let q = crate::periods::page::<PolicyMintChunk>(q, SlotOrder::Desc, 3, 290, |e, c| {
            crate::periods::load_chunks(archive, &chain, e, c, |i| policy_key(&Q.into(), i))
        })
        .unwrap();
        let q: Vec<Vec<u8>> = q
            .into_iter()
            .map(|name| [Q.as_slice(), &name].concat())
            .collect();
        let expected = policy_page(
            &fresh,
            &chain,
            0..=BOUNDARY,
            &Q.into(),
            SlotOrder::Desc,
            3,
            290,
        )
        .unwrap()
        .unwrap();
        assert_eq!(q, expected);
    }
}
