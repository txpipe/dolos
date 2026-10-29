//! Account addresses: the addresses each account saw in each epoch, serving
//! `/accounts/{stake}/addresses`.
//!
//! A sealed epoch holds one row per account that saw an address in it, at
//! `epoch_start(e) ‖ account`, listing its addresses in order of first
//! appearance within the epoch. An address active in several epochs is in
//! each of their rows, so readers keep its first appearance and drop the
//! rest.
//!
//! The same seal records each account's first slot in the archive's entity
//! index, under the stake address the `stake` tag files its addresses under:
//! the existence witness `/accounts/{stake}` reads ([`stake_seen`]).

use std::{
    collections::{BTreeMap, HashSet},
    ops::RangeInclusive,
};

use dolos_core::{
    ArchiveStore, ArchiveWriter as _, BlockSlot, ChainError, Domain, Entity as _, RangeOf,
    StateStore,
};
use pallas::{
    codec::minicbor::bytes::ByteVec,
    ledger::{
        addresses::{Network, StakeAddress},
        primitives::Epoch,
    },
};

use super::{GroupKey, Period};
use crate::{
    indexes::{entities, CardanoArchiveIndexExt as _, SlotOrder},
    pallas_extras, AccountAddressChunk, AccountAddressSet, AccountKey, AddressKey, ChainSummary,
    FixedNamespace as _,
};

/// Epochs read from the archive per call while walking an account's history.
const EPOCH_BATCH: usize = 64;

/// One epoch's addresses, in order of first appearance within it.
fn staged_addresses(mut rows: Vec<AccountAddressSet>) -> Vec<Vec<u8>> {
    rows.sort_by_key(AccountAddressSet::chain_order);
    rows.into_iter().map(|row| row.address).collect()
}

/// The stake address the `stake` tag files a staged row's address under: the
/// row's account, on the network its address's header names. `None` for an
/// empty address, which no row holds.
fn stake_address(row: &AccountAddressSet) -> Option<StakeAddress> {
    let header = row.address.first()?;
    let network = Network::from(header & 0x0f);

    Some(pallas_extras::stake_credential_to_address(
        network,
        &row.account,
    ))
}

/// Record in the entity index the first slot each stake address of `chunk`
/// was seen at, unless the index holds it at that slot or earlier.
///
/// A chunk holds every sealed row of its accounts, and earlier chunks have
/// committed, so the read finds whatever an earlier seal recorded.
fn index_stakes<A: ArchiveStore>(
    archive: &A,
    chunk: &[(GroupKey<AccountKey, AddressKey>, AccountAddressSet)],
    writer: &A::Writer,
) -> Result<(), ChainError> {
    let mut first: BTreeMap<Vec<u8>, BlockSlot> = BTreeMap::new();

    for (_, row) in chunk {
        let Some(stake) = stake_address(row) else {
            continue;
        };

        first
            .entry(stake.to_vec())
            .and_modify(|slot| *slot = (*slot).min(row.slot))
            .or_insert(row.slot);
    }

    let keys: Vec<&[u8]> = first.keys().map(Vec::as_slice).collect();
    let indexed = archive.entity_slots(entities::STAKE, &keys)?;

    for ((stake, slot), indexed) in first.iter().zip(indexed) {
        if indexed.is_none_or(|indexed| *slot < indexed) {
            writer.write_entity(entities::STAKE, stake, *slot)?;
        }
    }

    Ok(())
}

/// Seal the account addresses of every epoch `sealable` accepts.
///
/// Each (epoch, account) row depends on its group's staged rows alone, so the
/// rows seal chunk by chunk, and a re-run writes the same row again. The
/// chunk's archive commit also records its accounts' first slots
/// ([`index_stakes`]), before the state commit deletes their staged rows.
pub(crate) fn seal<D: Domain>(
    state: &D::State,
    archive: &D::Archive,
    chain: &ChainSummary,
    sealable: &dyn Fn(Epoch) -> bool,
    batch: usize,
) -> Result<(), ChainError> {
    let mut rows = Vec::new();

    for row in state.iter_entities_typed::<AccountAddressSet>(None)? {
        let (key, row) = row?;

        if sealable(row.epoch) {
            rows.push(Ok((key, row)));
        }
    }

    super::seal_groups::<D, AccountAddressSet, _, _, _>(
        state,
        archive,
        rows,
        batch,
        |chunk, writer| {
            let mut group: Vec<AccountAddressSet> = Vec::new();

            // rows come in key order: account, then epoch
            for (i, (key, row)) in chunk.iter().enumerate() {
                group.push(row.clone());

                let closes = chunk
                    .get(i + 1)
                    .is_none_or(|(next, _)| next.group != key.group || next.epoch != key.epoch);

                if closes {
                    let addresses = staged_addresses(std::mem::take(&mut group))
                        .into_iter()
                        .map(ByteVec::from)
                        .collect();

                    writer.write_log_typed(
                        chain.epoch_start(key.epoch),
                        &key.group,
                        &AccountAddressChunk { addresses },
                    )?;
                }
            }

            index_stakes(archive, chunk, writer)
        },
    )
}

/// Whether the chain ever filed an address under `stake`, a stake address's
/// bytes, as the `stake` tag does: an output paid to an address under it, or
/// an input spending one.
///
/// The entity index answers for the epochs the seal took. For the epochs it
/// left staged ([`super::staged_epochs`]), where the index holds nothing yet,
/// the `stake` tag answers from the first of them on. The seal commits its
/// entries before the ledger leaves their epochs staged, so after a miss in
/// the tag the index is read again: an entry the seal committed between the
/// first index read and the tag read is found then.
pub fn stake_seen<D: Domain>(domain: &D, stake: &[u8]) -> Result<bool, ChainError> {
    let indexed = || -> Result<bool, ChainError> {
        let slots = domain.archive().entity_slots(entities::STAKE, &[stake])?;

        Ok(slots.into_iter().next().flatten().is_some())
    };

    if indexed()? {
        return Ok(true);
    }

    let state = domain.state();

    let Some(cursor) = state.read_cursor()? else {
        return Ok(false);
    };

    let chain = crate::eras::load_era_summary::<D>(state)?;
    let (tip, _) = chain.slot_epoch(cursor.slot());
    let ledger = crate::load_epoch::<D>(state)?.number;
    let mutable_slots = crate::utils::mutable_slots(&domain.genesis());

    let staged = super::staged_epochs(&chain, mutable_slots, ledger, tip);

    let tagged = domain
        .archive()
        .slots_by_stake(stake, chain.epoch_start(*staged.start()), BlockSlot::MAX)?
        .next()
        .transpose()?
        .is_some();

    if tagged {
        return Ok(true);
    }

    indexed()
}

/// Every address `account` saw over `epochs`, in order of first appearance,
/// stopping once `until` are known.
///
/// Staged rows are read first and sealed ones after, per epoch the sealed row
/// winning, so a seal running beside the read leaves the answer unchanged.
fn first_appearances<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    account: &AccountKey,
    until: Option<usize>,
) -> Result<Vec<Vec<u8>>, ChainError> {
    let (state, archive) = (domain.state(), domain.archive());

    let mut staged = super::read_staged::<_, AccountAddressSet>(state, RangeOf::prefix(account))?;

    staged.retain(|epoch, _| epochs.contains(epoch));

    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut out = Vec::new();

    let epochs: Vec<Epoch> = epochs.collect();

    for batch in epochs.chunks(EPOCH_BATCH) {
        let sealed = super::read_sealed::<_, AccountAddressChunk>(
            archive,
            chain,
            batch.iter().copied(),
            |_| account.clone(),
        )?;

        let mut here = BTreeMap::new();

        for epoch in batch {
            if let Some(rows) = staged.remove(epoch) {
                here.insert(*epoch, rows);
            }
        }

        for (_, period) in super::join(here, sealed) {
            let addresses = match period {
                Period::Staged(rows) => staged_addresses(rows),
                Period::Sealed(raw) => {
                    AccountAddressChunk::decode_entity(AccountAddressChunk::NS, &raw)?
                        .addresses
                        .into_iter()
                        .map(Vec::from)
                        .collect()
                }
            };

            for address in addresses {
                if seen.insert(address.clone()) {
                    out.push(address);
                }
            }
        }

        if until.is_some_and(|until| out.len() >= until) {
            break;
        }
    }

    Ok(out)
}

/// One page of the addresses `account` saw over `epochs`, ordered by first
/// appearance on chain.
///
/// `asc` walks the epochs forward and stops once the page is covered; `desc`
/// needs every address before it can count from the end.
pub fn page<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    account: &AccountKey,
    order: SlotOrder,
    skip: usize,
    count: usize,
) -> Result<Vec<Vec<u8>>, ChainError> {
    let until = match order {
        SlotOrder::Asc => Some(skip.saturating_add(count)),
        SlotOrder::Desc => None,
    };

    let all = first_appearances(domain, chain, epochs, account, until)?;

    let page = match order {
        SlotOrder::Asc => all.into_iter().skip(skip).take(count).collect(),
        SlotOrder::Desc => all.into_iter().rev().skip(skip).take(count).collect(),
    };

    Ok(page)
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
    use pallas::ledger::primitives::StakeCredential;

    use super::*;
    use crate::periods::{is_sealable, GroupKey, GroupRow as _};
    use crate::AddressKey;

    /// The epoch the sealing boundary opens: 5 and 6 are immutable at it, 7 is
    /// not.
    const BOUNDARY: Epoch = 8;

    fn a() -> StakeCredential {
        StakeCredential::AddrKeyhash([0x11; 28].into())
    }

    fn b() -> StakeCredential {
        StakeCredential::ScriptHash([0x11; 28].into())
    }

    fn chain(domain: &ToyDomain) -> ChainSummary {
        crate::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap()
    }

    /// Staged addresses of two accounts in epochs 5 to 7. Within an epoch they
    /// appear against byte order, and some return in later epochs.
    fn seeded() -> ToyDomain {
        let domain = ToyDomain::new(None, None);
        let chain = chain(&domain);

        let mut rows = Vec::new();
        let mut row =
            |account: StakeCredential, address: u8, epoch: Epoch, offset: u64, tx: u32| {
                rows.push(AccountAddressSet {
                    account,
                    address: vec![0x01, address],
                    epoch,
                    slot: chain.epoch_start(epoch) + offset,
                    tx_index: tx,
                    output_index: address as u32 % 3,
                });
            };

        for (i, address) in [9u8, 3, 7, 1].into_iter().enumerate() {
            row(a(), address, 5, 10 + i as u64 / 2, i as u32 % 2);
        }
        row(b(), 4, 5, 10, 0);
        for i in 0..40u8 {
            row(a(), 100 + i, 6, 300 - i as u64, 0);
        }
        row(a(), 3, 6, 1, 0);
        row(b(), 4, 6, 2, 0);
        row(a(), 9, 7, 1, 0);
        row(a(), 2, 7, 2, 0);
        row(b(), 5, 7, 1, 1);

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

    fn logs(domain: &ToyDomain) -> Vec<(BlockSlot, AccountKey, AccountAddressChunk)> {
        domain
            .archive()
            .iter_logs_typed::<AccountAddressChunk>(None)
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    /// The stake address the seeded rows file `account` under: their
    /// addresses' header names network 1.
    fn stake(account: &StakeCredential) -> Vec<u8> {
        pallas_extras::stake_credential_to_address(Network::Mainnet, account).to_vec()
    }

    /// The entity index's slot for each of `accounts`.
    fn indexed(domain: &ToyDomain, accounts: &[StakeCredential]) -> Vec<Option<BlockSlot>> {
        let keys: Vec<Vec<u8>> = accounts.iter().map(stake).collect();
        let keys: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();

        domain
            .archive()
            .entity_slots(entities::STAKE, &keys)
            .unwrap()
    }

    fn staged(domain: &ToyDomain) -> Vec<(GroupKey<AccountKey, AddressKey>, AccountAddressSet)> {
        domain
            .state()
            .iter_entities_typed::<AccountAddressSet>(None)
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    /// Every page both accounts can answer, in both orders.
    fn pages(domain: &ToyDomain) -> Vec<Vec<Vec<u8>>> {
        let chain = chain(domain);
        let mut out = Vec::new();

        for order in [SlotOrder::Asc, SlotOrder::Desc] {
            for count in [1, 7, 100] {
                for skip in (0..50).step_by(count) {
                    for account in [a(), b()] {
                        out.push(
                            page(
                                domain,
                                &chain,
                                0..=BOUNDARY,
                                &AccountKey(account),
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
        assert_eq!(staged(&domain).len(), 3);

        // one row per (epoch, account)
        assert_eq!(logs(&domain).len(), 4);

        assert_eq!(pages(&domain), before);

        // first appearance: chain order within an epoch, repeats dropped
        // across them, newest first in `desc`
        let all = |order| {
            page(
                &domain,
                &chain(&domain),
                0..=BOUNDARY,
                &AccountKey(a()),
                order,
                0,
                100,
            )
            .unwrap()
        };

        let asc = all(SlotOrder::Asc);
        let tails: Vec<u8> = asc.iter().map(|x| x[1]).collect();
        let mut expected = vec![9, 3, 7, 1];
        expected.extend((100..140).rev());
        expected.push(2);
        assert_eq!(tails, expected);

        let mut desc = all(SlotOrder::Desc);
        desc.reverse();
        assert_eq!(desc, asc);
    }

    /// The seal records each account under its stake address at the first
    /// slot its sealed rows name. A later seal keeps that slot, and records an
    /// account the later epoch saw first.
    #[test]
    fn a_seal_indexes_each_account_at_its_first_slot() {
        let domain = seeded();
        let chain = chain(&domain);

        seal_as(&domain, &domain).unwrap();

        let first = chain.epoch_start(5) + 10;
        assert_eq!(indexed(&domain, &[a(), b()]), [Some(first), Some(first)]);

        // epoch 7 stays staged, so nothing it alone saw is indexed yet
        let c = StakeCredential::AddrKeyhash([0x22; 28].into());
        let row = AccountAddressSet {
            account: c.clone(),
            address: vec![0x01, 0x42],
            epoch: 7,
            slot: chain.epoch_start(7) + 5,
            tx_index: 0,
            output_index: 0,
        };

        let writer = domain.state().start_writer().unwrap();
        writer.write_entity_typed(&row.key(), &row).unwrap();
        writer.commit().unwrap();

        assert_eq!(indexed(&domain, std::slice::from_ref(&c)), [None]);

        let mutable = crate::utils::mutable_slots(&domain.genesis());
        let later = |epoch| is_sealable(&chain, mutable, epoch, BOUNDARY + 1);
        assert!(later(7));

        seal::<ToyDomain>(domain.state(), domain.archive(), &chain, &later, 2).unwrap();

        assert!(staged(&domain).is_empty());
        assert_eq!(
            indexed(&domain, &[a(), b(), c]),
            [Some(first), Some(first), Some(row.slot)]
        );
    }

    /// Done criterion 1 of the framework, seal idempotence: whichever commit
    /// the seal dies after, a re-run leaves exactly the rows and index entries
    /// of a seal that never crashed, and nothing staged for the sealed epochs.
    #[test]
    fn a_crash_at_any_commit_seals_the_same_rows() {
        let clean = seeded();
        seal_as(&clean, &clean).unwrap();

        let entries = |domain: &ToyDomain| indexed(domain, &[a(), b()]);
        let expected = (logs(&clean), staged(&clean), entries(&clean));

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
                (logs(&domain), staged(&domain), entries(&domain)),
                expected,
                "after a crash following commit {crash_after}"
            );

            crash_after += 1;
        }

        // a pair per group chunk
        assert!(crash_after >= 2, "only {crash_after} commits");
    }

    /// A read landing between any two commits of a seal answers every page
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
