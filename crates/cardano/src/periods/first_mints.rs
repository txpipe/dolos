//! First mints: the assets each epoch minted for the first time, overall and
//! per policy, serving `/assets` and `/assets/policy/{policy}`.
//!
//! A sealed epoch holds two lists, in mint order: every subject it first
//! minted, at `epoch_start(e) ‖ chunk`, and each policy's names, at
//! `epoch_start(e) ‖ policy ‖ chunk`. `AssetState.initial_slot` is the witness
//! that keeps re-mints out, at seal time and when reading staged rows.

use std::{collections::BTreeMap, ops::RangeInclusive};

use dolos_core::{
    ArchiveStore, ArchiveWriter as _, ChainError, Domain, EntityKey, EntityValue, StateStore,
};
use pallas::{crypto::hash::Hash, ledger::primitives::Epoch};

use super::{Period, Segment};
use crate::{
    indexes::SlotOrder, AssetState, ChainSummary, FirstMintChunk, FixedNamespace as _,
    PolicyMintChunk, StagedFirstMint,
};

/// Asset states read per state call when telling first mints from re-mints.
const LOOKUP_BATCH: usize = 1_000;

fn all_key(chunk: u32) -> EntityKey {
    EntityKey::from(&chunk.to_be_bytes())
}

fn policy_key(policy: &Hash<28>, chunk: u32) -> EntityKey {
    EntityKey::from([policy.as_slice(), &chunk.to_be_bytes()].concat())
}

/// Whether each staged row is its asset's first mint ever: the asset's
/// `initial_slot` is the row's slot.
fn first_mint_flags<'a, S: StateStore>(
    state: &S,
    rows: impl IntoIterator<Item = &'a StagedFirstMint>,
) -> Result<Vec<bool>, ChainError> {
    let rows: Vec<&StagedFirstMint> = rows.into_iter().collect();
    let mut flags = Vec::with_capacity(rows.len());

    for batch in rows.chunks(LOOKUP_BATCH) {
        let keys: Vec<EntityKey> = batch.iter().map(|row| row.asset_key()).collect();
        let assets = state
            .read_entities_typed::<AssetState>(AssetState::NS, &keys.iter().collect::<Vec<_>>())?;

        for (row, asset) in batch.iter().zip(assets) {
            flags.push(asset.and_then(|x| x.initial_slot) == Some(row.slot));
        }
    }

    Ok(flags)
}

/// Seal the staged first mints of every epoch `sealable` accepts.
///
/// The all-subjects list of an epoch depends on every staged row of it, so it
/// is written first, whole and in one archive commit, before any of those rows
/// is deleted; a re-run that finds it sealed leaves it alone. The per-policy
/// lists depend on their group only, and seal chunk by chunk.
pub(crate) fn seal<D: Domain>(
    state: &D::State,
    archive: &D::Archive,
    chain: &ChainSummary,
    sealable: &dyn Fn(Epoch) -> bool,
    batch: usize,
) -> Result<(), ChainError> {
    let mut rows = Vec::new();

    for row in state.iter_entities_typed::<StagedFirstMint>(StagedFirstMint::NS, None)? {
        let (key, row) = row?;

        if sealable(row.epoch) {
            rows.push((key, row));
        }
    }

    if rows.is_empty() {
        return Ok(());
    }

    let flags = first_mint_flags(state, rows.iter().map(|(_, row)| row))?;

    let mut epochs: BTreeMap<Epoch, Vec<&StagedFirstMint>> = BTreeMap::new();

    for ((_, row), first) in rows.iter().zip(&flags) {
        if *first {
            epochs.entry(row.epoch).or_default().push(row);
        }
    }

    for (epoch, mut mints) in epochs {
        let head = super::sealed_key(chain, epoch, all_key(0));

        if archive.read_logs(FirstMintChunk::NS, &[&head])?[0].is_some() {
            continue;
        }

        mints.sort_by_key(|row| row.mint_order());
        let subjects = mints.into_iter().map(StagedFirstMint::subject).collect();

        let writer = archive.start_writer()?;

        for (i, chunk) in super::chunk::<FirstMintChunk>(subjects)
            .into_iter()
            .enumerate()
        {
            let key = super::sealed_key(chain, epoch, all_key(i as u32));
            writer.write_log_typed(&key, &chunk)?;
        }

        writer.commit()?;
    }

    let rows = rows
        .into_iter()
        .zip(flags)
        .map(|((key, row), first)| Ok((key, (row, first))));

    super::seal_groups::<D, _>(
        state,
        archive,
        StagedFirstMint::NS,
        rows,
        batch,
        |chunk, writer| {
            let mut groups: BTreeMap<(Epoch, Hash<28>), Vec<&StagedFirstMint>> = BTreeMap::new();

            for (_, (row, first)) in chunk {
                if *first {
                    groups.entry((row.epoch, row.policy)).or_default().push(row);
                }
            }

            for ((epoch, policy), mut mints) in groups {
                mints.sort_by_key(|row| row.mint_order());
                let names = mints.into_iter().map(|row| row.name.clone()).collect();

                for (i, chunk) in super::chunk::<PolicyMintChunk>(names)
                    .into_iter()
                    .enumerate()
                {
                    let key = super::sealed_key(chain, epoch, policy_key(&policy, i as u32));
                    writer.write_log_typed(&key, &chunk)?;
                }
            }

            Ok(())
        },
    )
}

/// Per-epoch segments of one list: staged epochs keep their first mints in
/// mint order, sealed ones their count.
fn segments<S: StateStore>(
    state: &S,
    periods: BTreeMap<Epoch, Period<StagedFirstMint, EntityValue>>,
    item: impl Fn(StagedFirstMint) -> Vec<u8>,
) -> Result<Vec<Segment<Vec<u8>>>, ChainError> {
    let mut out = Vec::with_capacity(periods.len());

    for (epoch, period) in periods {
        let segment = match period {
            Period::Staged(rows) => {
                let flags = first_mint_flags(state, &rows)?;

                let mut mints: Vec<StagedFirstMint> = rows
                    .into_iter()
                    .zip(flags)
                    .filter_map(|(row, first)| first.then_some(row))
                    .collect();

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
pub fn all_page<S, A>(
    state: &S,
    archive: &A,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    order: SlotOrder,
    skip: usize,
    count: usize,
) -> Result<Vec<Vec<u8>>, ChainError>
where
    S: StateStore,
    A: ArchiveStore,
{
    let mut staged = super::read_staged::<S, StagedFirstMint>(state, EntityKey::full_range())?;
    staged.retain(|epoch, _| epochs.contains(epoch));

    let sealed = super::read_sealed(archive, FirstMintChunk::NS, chain, epochs, |_| all_key(0))?;

    let segments = segments(state, super::join(staged, sealed), |row| row.subject())?;

    super::page::<FirstMintChunk>(segments, order, skip, count, |epoch, chunks| {
        super::load_chunks(archive, chain, epoch, chunks, all_key)
    })
}

/// One page of the subjects `policy` first minted over `epochs`, in mint
/// order, or `None` where it never minted.
pub fn policy_page<S, A>(
    state: &S,
    archive: &A,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    policy: &Hash<28>,
    order: SlotOrder,
    skip: usize,
    count: usize,
) -> Result<Option<Vec<Vec<u8>>>, ChainError>
where
    S: StateStore,
    A: ArchiveStore,
{
    let mut staged =
        super::read_staged::<S, StagedFirstMint>(state, super::group_range(policy.as_slice()))?;

    staged.retain(|epoch, _| epochs.contains(epoch));

    // the group prefix is shorter than a policy id
    for rows in staged.values_mut() {
        rows.retain(|row| row.policy == *policy);
    }

    let sealed = super::read_sealed(archive, PolicyMintChunk::NS, chain, epochs, |_| {
        policy_key(policy, 0)
    })?;

    let segments = segments(state, super::join(staged, sealed), |row| row.name)?;

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
    use dolos_core::{ArchiveStore as _, Domain as _, LogKey, StateStore as _, StateWriter as _};
    use dolos_testing::{
        faults::{FaultyToyDomain, TestFault},
        toy_domain::ToyDomain,
    };

    use super::*;
    use crate::periods::{is_sealable, StagedRow as _};

    /// The epoch the sealing boundary opens: 5 and 6 are immutable at it, 7 is
    /// not.
    const BOUNDARY: Epoch = 8;

    const P: [u8; 28] = [0x11; 28];
    const Q: [u8; 28] = [0x22; 28];

    /// Shares `P`'s staged-key prefix.
    fn sibling() -> [u8; 28] {
        let mut policy = P;
        policy[super::super::GROUP_LEN..].fill(0x99);
        policy
    }

    fn chain(domain: &ToyDomain) -> ChainSummary {
        crate::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap()
    }

    /// Staged first mints in epochs 5 to 7, the asset states that witness
    /// them, and one re-mint of an asset first minted in epoch 5. `Q` mints
    /// more names in epoch 6 than a sealed chunk holds.
    fn seeded() -> ToyDomain {
        let domain = ToyDomain::new(None, None);
        let chain = chain(&domain);

        let mut rows = Vec::new();
        let mut row = |policy: [u8; 28], name: String, epoch: Epoch, offset: u64, tx: u32| {
            rows.push(StagedFirstMint {
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

            let asset = AssetState {
                initial_slot: Some(row.slot),
                ..Default::default()
            };
            writer.write_entity_typed(&row.asset_key(), &asset).unwrap();
        }

        // P's "A" again in epoch 6: staged, but its asset was first minted in 5
        let remint = StagedFirstMint {
            policy: P.into(),
            name: b"A".to_vec(),
            epoch: 6,
            slot: chain.epoch_start(6) + 1,
            tx_index: 0,
        };
        writer.write_entity_typed(&remint.key(), &remint).unwrap();

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
        Vec<(LogKey, FirstMintChunk)>,
        Vec<(LogKey, PolicyMintChunk)>,
    );

    fn logs(domain: &ToyDomain) -> Logs {
        let archive = domain.archive();

        let all = archive
            .iter_logs_typed::<FirstMintChunk>(FirstMintChunk::NS, None)
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let policies = archive
            .iter_logs_typed::<PolicyMintChunk>(PolicyMintChunk::NS, None)
            .unwrap()
            .map(Result::unwrap)
            .collect();

        (all, policies)
    }

    fn staged(domain: &ToyDomain) -> Vec<(EntityKey, StagedFirstMint)> {
        domain
            .state()
            .iter_entities_typed::<StagedFirstMint>(StagedFirstMint::NS, None)
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    /// Every page the two lists can answer, in both orders.
    fn pages(domain: &ToyDomain) -> Vec<Option<Vec<Vec<u8>>>> {
        let chain = chain(domain);
        let (state, archive) = (domain.state(), domain.archive());
        let mut out = Vec::new();

        for order in [SlotOrder::Asc, SlotOrder::Desc] {
            for count in [5, 300] {
                for skip in (0..330).step_by(count) {
                    out.push(Some(
                        all_page(state, archive, &chain, 0..=BOUNDARY, order, skip, count).unwrap(),
                    ));

                    for policy in [P, Q, sibling()] {
                        out.push(
                            policy_page(
                                state,
                                archive,
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
        let page = all_page(
            domain.state(),
            domain.archive(),
            &chain(&domain),
            5..=5,
            SlotOrder::Asc,
            0,
            10,
        )
        .unwrap();
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

        // the re-mint of P's "A" is in no list
        let p = policy_page(
            domain.state(),
            domain.archive(),
            &chain(&domain),
            0..=BOUNDARY,
            &P.into(),
            SlotOrder::Asc,
            0,
            100,
        )
        .unwrap()
        .unwrap();
        assert_eq!(p.iter().filter(|s| s[28..] == *b"A").count(), 1);
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
            crate::periods::read_staged::<_, StagedFirstMint>(state, EntityKey::full_range())
                .unwrap();
        let staged_q = crate::periods::read_staged::<_, StagedFirstMint>(
            state,
            crate::periods::group_range(&Q),
        )
        .unwrap();

        seal_as(&domain, &domain).unwrap();

        let sealed_all =
            crate::periods::read_sealed(archive, FirstMintChunk::NS, &chain, 0..=BOUNDARY, |_| {
                all_key(0)
            })
            .unwrap();
        let sealed_q =
            crate::periods::read_sealed(archive, PolicyMintChunk::NS, &chain, 0..=BOUNDARY, |_| {
                policy_key(&Q.into(), 0)
            })
            .unwrap();

        let all = segments(state, crate::periods::join(staged_all, sealed_all), |row| {
            row.subject()
        })
        .unwrap();
        let all = crate::periods::page::<FirstMintChunk>(all, SlotOrder::Asc, 0, 400, |e, c| {
            crate::periods::load_chunks(archive, &chain, e, c, all_key)
        })
        .unwrap();
        let expected = all_page(
            fresh.state(),
            fresh.archive(),
            &chain,
            0..=BOUNDARY,
            SlotOrder::Asc,
            0,
            400,
        )
        .unwrap();
        assert_eq!(all, expected);

        let q = segments(state, crate::periods::join(staged_q, sealed_q), |row| {
            row.name
        })
        .unwrap();
        let q = crate::periods::page::<PolicyMintChunk>(q, SlotOrder::Desc, 3, 290, |e, c| {
            crate::periods::load_chunks(archive, &chain, e, c, |i| policy_key(&Q.into(), i))
        })
        .unwrap();
        let q: Vec<Vec<u8>> = q
            .into_iter()
            .map(|name| [Q.as_slice(), &name].concat())
            .collect();
        let expected = policy_page(
            fresh.state(),
            fresh.archive(),
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
