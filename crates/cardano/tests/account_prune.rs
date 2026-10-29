//! Dead accounts through the real ESTART: a shard deletes an account that is
//! dead once rotated, and a pruned account that carried history adds it to
//! the credential's `pruned-accounts` row in the same commit. The account
//! rows are built by the roll's own deltas, the harness is `ToyDomain` on the
//! devnet genesis, and every work unit runs through `execute_work_unit`, the
//! executor the node runs.
//!
//! The rollback cases at the end roll real blocks through the WAL and every
//! boundary instead, and roll back across ESTART.

use std::sync::Arc;

use dolos_cardano::{
    estart::EstartWorkUnit, AccountKey, AccountState, AssignRewards, CardanoWorkUnit,
    ControlledAmountDec, ControlledAmountInc, EpochState, PrunedAccountState, StakeDelegation,
    StakeDeregistration, StakeRegistration, WithdrawalInc,
};
use dolos_core::{
    sync::{execute_work_unit, SyncExt as _},
    ChainPoint, Domain, DomainError, EntityDelta, FixedNamespace, Genesis, Namespace, RangeOf,
    StateStore as _, StateWriter as _,
};
use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    toy_domain::ToyDomain,
};
use pallas::ledger::primitives::{Epoch, StakeCredential};

fn devnet() -> Arc<Genesis> {
    Arc::new(dolos_cardano::include::devnet::load())
}

fn credential(byte: u8) -> StakeCredential {
    StakeCredential::AddrKeyhash([byte; 28].into())
}

fn epoch<D: Domain>(domain: &D) -> Epoch {
    dolos_cardano::load_epoch::<D>(domain.state())
        .unwrap()
        .number
}

fn epoch_start<D: Domain>(domain: &D, epoch: Epoch) -> u64 {
    dolos_cardano::eras::load_era_summary::<D>(domain.state())
        .unwrap()
        .epoch_start(epoch)
}

/// The slot `offset` slots into the current epoch.
fn slot(domain: &ToyDomain, offset: u64) -> u64 {
    epoch_start(domain, epoch(domain)) + offset
}

/// Run ESTART over the boundary that opens the next epoch.
fn open_next_epoch<D>(domain: &D) -> Result<(), DomainError>
where
    D: Domain<WorkUnit = CardanoWorkUnit>,
{
    let slot = epoch_start(domain, epoch(domain) + 1);

    let mut work = CardanoWorkUnit::Estart(Box::new(EstartWorkUnit::new(slot, domain.genesis())));

    execute_work_unit(domain, &mut work)
}

fn account(domain: &ToyDomain, credential: &StakeCredential) -> Option<AccountState> {
    domain
        .state()
        .read_entity_typed::<AccountState>(&AccountKey::from(credential))
        .unwrap()
}

fn remainder(domain: &ToyDomain, credential: &StakeCredential) -> Option<PrunedAccountState> {
    domain
        .state()
        .read_entity_typed::<PrunedAccountState>(&AccountKey::from(credential))
        .unwrap()
}

fn remainders(domain: &ToyDomain) -> Vec<(AccountKey, PrunedAccountState)> {
    domain
        .state()
        .iter_entities_typed::<PrunedAccountState>(None)
        .unwrap()
        .map(|x| x.unwrap())
        .collect()
}

/// Apply one of the roll's account deltas to the stored row, as the roll
/// does: a delta that creates the row inserts it.
fn roll<T>(domain: &ToyDomain, credential: &StakeCredential, mut delta: T)
where
    T: EntityDelta<Entity = AccountState>,
{
    let key = AccountKey::from(credential);
    let mut row = account(domain, credential);

    delta.apply(&mut row);

    let writer = domain.state().start_writer().unwrap();
    writer.save_entity_typed(&key, row.as_ref()).unwrap();
    writer.commit().unwrap();
}

/// One registered life of `credential` in the current epoch, from `offset`
/// slots into it: register, fund, earn `rewards`, withdraw them, deregister
/// ten slots later and empty. Returns the deregistration slot.
fn live_and_leave(
    domain: &ToyDomain,
    credential: &StakeCredential,
    offset: u64,
    rewards: u64,
) -> u64 {
    let epoch = epoch(domain);
    let key = AccountKey::from(credential);
    let registered = slot(domain, offset);

    roll(
        domain,
        credential,
        StakeRegistration::new(credential.clone(), registered, epoch, 2_000_000),
    );
    roll(
        domain,
        credential,
        ControlledAmountInc::new(credential.clone(), false, 7_000_000, epoch),
    );
    roll(domain, credential, AssignRewards::new(key, rewards));
    roll(
        domain,
        credential,
        WithdrawalInc::new(credential.clone(), rewards),
    );
    roll(
        domain,
        credential,
        StakeDeregistration::new(credential.clone(), registered + 10, epoch),
    );
    roll(
        domain,
        credential,
        ControlledAmountDec::new(credential.clone(), false, 7_000_000),
    );

    registered + 10
}

/// A store in its first epoch.
fn fresh() -> ToyDomain {
    ToyDomain::new_with_genesis(devnet(), None, None)
}

/// An account that deregisters and empties is dead, but the boundary that
/// closes the epoch of its deregistration keeps it: a rollback across that
/// boundary could undo the deregistration. The next boundary prunes it, and
/// its history stays in a remainder.
#[test]
fn an_emptied_deregistered_account_leaves_a_remainder_a_boundary_later() {
    let domain = fresh();
    let leaving = credential(0x31);

    let deregistered = live_and_leave(&domain, &leaving, 100, 500_000);
    assert!(account(&domain, &leaving).unwrap().is_dead());

    open_next_epoch(&domain).unwrap();

    let kept = account(&domain, &leaving).expect("deregistered in the closing epoch");
    assert!(kept.is_dead());
    assert_eq!(remainder(&domain, &leaving), None);

    open_next_epoch(&domain).unwrap();

    assert_eq!(account(&domain, &leaving), None);
    assert_eq!(
        remainder(&domain, &leaving),
        Some(PrunedAccountState {
            lifetime_rewards: 500_000,
            deregistered_at: Some(deregistered),
        })
    );
}

#[test]
fn a_recreated_account_pruned_again_keeps_one_remainder() {
    let domain = fresh();
    let leaving = credential(0x32);

    live_and_leave(&domain, &leaving, 100, 500_000);
    open_next_epoch(&domain).unwrap();
    open_next_epoch(&domain).unwrap();
    assert_eq!(account(&domain, &leaving), None);

    // funded again, the credential starts a fresh row, then lives and leaves
    // once more
    roll(
        &domain,
        &leaving,
        ControlledAmountInc::new(leaving.clone(), false, 1_000_000, epoch(&domain)),
    );

    let fresh_row = account(&domain, &leaving).unwrap();
    assert_eq!(fresh_row.deregistered_at, None);
    assert_eq!(fresh_row.stake.live().unwrap().rewards_sum, 0);

    roll(
        &domain,
        &leaving,
        ControlledAmountDec::new(leaving.clone(), false, 1_000_000),
    );
    let deregistered = live_and_leave(&domain, &leaving, 900, 300_000);

    open_next_epoch(&domain).unwrap();
    assert!(account(&domain, &leaving).is_some());
    open_next_epoch(&domain).unwrap();

    assert_eq!(account(&domain, &leaving), None);
    assert_eq!(
        remainders(&domain),
        vec![(
            AccountKey::from(&leaving),
            PrunedAccountState {
                lifetime_rewards: 800_000,
                deregistered_at: Some(deregistered),
            }
        )]
    );
}

#[test]
fn a_never_registered_account_that_empties_leaves_no_remainder() {
    let domain = fresh();
    let passing = credential(0x33);

    roll(
        &domain,
        &passing,
        ControlledAmountInc::new(passing.clone(), false, 3_000_000, epoch(&domain)),
    );
    roll(
        &domain,
        &passing,
        ControlledAmountDec::new(passing.clone(), false, 3_000_000),
    );

    open_next_epoch(&domain).unwrap();

    assert_eq!(account(&domain, &passing), None);
    assert_eq!(remainder(&domain, &passing), None);
}

/// A row stays while anything the ledger reads is in it: a registration, a
/// balance, at a pointer address too, or a stake or delegation snapshot the
/// reward calculation still reaches. A deregistered delegator goes once its
/// delegation has rotated out of `go`.
#[test]
fn a_row_stays_until_every_snapshot_is_empty() {
    let domain = fresh();

    let registered = credential(0x41);
    let funded = credential(0x42);
    let delegated = credential(0x43);
    let pointed = credential(0x44);

    let at = epoch(&domain);

    roll(
        &domain,
        &registered,
        StakeRegistration::new(registered.clone(), slot(&domain, 50), at, 2_000_000),
    );
    roll(
        &domain,
        &funded,
        ControlledAmountInc::new(funded.clone(), false, 1, at),
    );
    roll(
        &domain,
        &pointed,
        ControlledAmountInc::new(pointed.clone(), true, 1, at),
    );

    // delegated in this epoch, deregistered and emptied in the next one
    roll(
        &domain,
        &delegated,
        StakeRegistration::new(delegated.clone(), slot(&domain, 60), at, 2_000_000),
    );
    roll(
        &domain,
        &delegated,
        StakeDelegation::new(delegated.clone(), [0xa1; 28].into(), at),
    );

    open_next_epoch(&domain).unwrap();

    let at = epoch(&domain);
    let deregistered = slot(&domain, 70);
    roll(
        &domain,
        &delegated,
        StakeDeregistration::new(delegated.clone(), deregistered, at),
    );

    // its delegation sits in `mark`, rotates to `set` and `go`, and out at the
    // third boundary
    for _ in 0..2 {
        assert!(!account(&domain, &delegated).unwrap().is_dead());
        open_next_epoch(&domain).unwrap();
    }

    assert!(!account(&domain, &delegated).unwrap().is_dead());
    open_next_epoch(&domain).unwrap();

    assert_eq!(account(&domain, &delegated), None);
    assert_eq!(
        remainder(&domain, &delegated),
        Some(PrunedAccountState {
            lifetime_rewards: 0,
            deregistered_at: Some(deregistered),
        })
    );

    for staying in [&registered, &funded, &pointed] {
        assert!(account(&domain, staying).is_some(), "{staying:?}");
        assert_eq!(remainder(&domain, staying), None);
    }
}

/// Every stored row of `ns`, key and value bytes, in key order.
fn rows(domain: &ToyDomain, ns: Namespace) -> Vec<(Vec<u8>, Vec<u8>)> {
    domain
        .state()
        .iter_entities(ns, RangeOf::<AccountKey>::all().into_raw())
        .unwrap()
        .map(|x| {
            let (key, value) = x.unwrap();
            (key.as_ref().to_vec(), value)
        })
        .collect()
}

/// A boundary interrupted after any number of commits, and resumed, prunes
/// the same rows and writes the same remainders as one that ran straight
/// through, a credential pruned before included: each pruned life adds to
/// its remainder once.
#[test]
fn a_resumed_boundary_prunes_the_same_rows() {
    // spread over the account shards by their first key byte
    let bytes = [0x05, 0x37, 0x6a, 0x90, 0xc4, 0xfe];

    let prepare = || {
        let domain = fresh();

        let first: Vec<_> = bytes
            .iter()
            .enumerate()
            .map(|(i, byte)| live_and_leave(&domain, &credential(*byte), 100 + i as u64, 1_000))
            .collect();

        // kept by the boundary closing their deregistrations, pruned by the
        // next one
        open_next_epoch(&domain).unwrap();
        open_next_epoch(&domain).unwrap();

        // half of them come back and leave again, kept by the boundary
        // closing this epoch
        let mut deregistered = first;
        for (i, byte) in bytes.iter().enumerate().step_by(2) {
            deregistered[i] = live_and_leave(&domain, &credential(*byte), 500 + i as u64, 2_000);
        }

        open_next_epoch(&domain).unwrap();

        // beside a never-registered one that empties and a live one that stays
        let passing = credential(0x77);
        let at = epoch(&domain);
        roll(
            &domain,
            &passing,
            ControlledAmountInc::new(passing.clone(), false, 5, at),
        );
        roll(
            &domain,
            &passing,
            ControlledAmountDec::new(passing.clone(), false, 5),
        );

        let staying = credential(0xb0);
        roll(
            &domain,
            &staying,
            ControlledAmountInc::new(staying.clone(), false, 5, at),
        );

        (domain, deregistered)
    };

    let snapshot = |domain: &ToyDomain| {
        (
            rows(domain, AccountState::NS),
            rows(domain, PrunedAccountState::NS),
        )
    };

    let (clean, deregistered) = prepare();
    open_next_epoch(&clean).unwrap();
    let expected = snapshot(&clean);

    assert_eq!(account(&clean, &credential(0x77)), None);
    assert!(account(&clean, &credential(0xb0)).is_some());
    assert_eq!(expected.1.len(), bytes.len());
    for (i, byte) in bytes.iter().enumerate() {
        assert_eq!(account(&clean, &credential(*byte)), None);

        let lifetime_rewards = match i % 2 {
            0 => 3_000,
            _ => 1_000,
        };

        assert_eq!(
            remainder(&clean, &credential(*byte)),
            Some(PrunedAccountState {
                lifetime_rewards,
                deregistered_at: Some(deregistered[i]),
            })
        );
    }

    let mut crash_after = 0;

    loop {
        let (domain, _) = prepare();

        let crashing =
            FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(crash_after));

        if open_next_epoch(&crashing).is_ok() {
            // past the last commit, so every crash point has been covered
            break;
        }

        open_next_epoch(&domain).expect("resume");

        assert_eq!(
            snapshot(&domain),
            expected,
            "rows after a crash following commit {crash_after}"
        );

        crash_after += 1;
    }

    assert!(
        crash_after > 1,
        "the boundary committed too little to crash between shards"
    );
}

/// A fresh node's first rollback is to the origin, before genesis: genesis
/// waits for the first block, and the peer's first reply rolls back to the
/// intersection. It undoes nothing, so it reads none of the state genesis
/// writes. `ToyDomain` runs genesis eagerly, so its epoch is removed first.
#[test]
fn a_rollback_before_genesis_reads_no_epoch() {
    let domain = fresh();

    let writer = domain.state().start_writer().unwrap();
    writer.save_entity_typed::<EpochState>(&(), None).unwrap();
    writer.commit().unwrap();

    domain.rollback(&ChainPoint::Origin).unwrap();
}

/// Rollbacks across ESTART on a chain of real blocks: the roll writes the
/// WAL, RUPD, EWRAP and ESTART run on the first block past each boundary, and
/// `rollback` undoes the blocks after its target.
///
/// Every case is ignored under `strict`: undoing a block of the closing epoch
/// trips `EpochStatsUpdate::undo`'s epoch check on the rotated rolling stats,
/// a defect `main` shares.
mod rollback {
    use std::sync::Arc;

    use dolos_cardano::{AccountKey, AccountState, EpochState, PrunedAccountState};
    use dolos_core::{
        config::CustomUtxo, sync::SyncExt as _, BootstrapExt as _, ChainPoint, Domain as _,
        Genesis, RawBlock, StateStore as _, StateWriter as _, TxoRef, WalStore as _,
    };
    use dolos_testing::{
        blocks::make_conway_block_with_txs_at,
        faults::{FaultyToyDomain, TestFault},
        toy_domain::ToyDomain,
    };
    use pallas::{
        codec::utils::{KeepRaw, Set},
        ledger::{
            addresses::{Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart},
            primitives::{
                conway::{
                    Certificate, PostAlonzoTransactionOutput, TransactionBody, TransactionInput,
                    TransactionOutput, Value,
                },
                NonEmptySet, StakeCredential,
            },
            traverse::{ComputeHash as _, Era},
        },
    };

    const ADA: u64 = 1_000_000;
    const C: [u8; 28] = [0xc1; 28];
    const SEED: [u8; 32] = [0xee; 32];

    fn cred() -> StakeCredential {
        StakeCredential::AddrKeyhash(C.into())
    }

    fn shelley(payment: u8, delegation: ShelleyDelegationPart) -> Vec<u8> {
        ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::Key([payment; 28].into()),
            delegation,
        )
        .to_vec()
    }

    /// A base address `C` stakes.
    fn staked(payment: u8) -> Vec<u8> {
        staked_to(C, payment)
    }

    fn staked_to(credential: [u8; 28], payment: u8) -> Vec<u8> {
        shelley(payment, ShelleyDelegationPart::Key(credential.into()))
    }

    fn enterprise(payment: u8) -> Vec<u8> {
        shelley(payment, ShelleyDelegationPart::Null)
    }

    fn output(address: Vec<u8>, coin: u64) -> TransactionOutput<'static> {
        TransactionOutput::PostAlonzo(KeepRaw::from(PostAlonzoTransactionOutput {
            address: address.into(),
            value: Value::Coin(coin),
            datum_option: None,
            script_ref: None,
        }))
    }

    fn tx(
        inputs: &[TxoRef],
        outputs: Vec<(Vec<u8>, u64)>,
        fee: u64,
        certs: Vec<Certificate>,
    ) -> TransactionBody<'static> {
        TransactionBody {
            inputs: Set::from(
                inputs
                    .iter()
                    .map(|txo| TransactionInput {
                        transaction_id: txo.0,
                        index: txo.1 as u64,
                    })
                    .collect::<Vec<_>>(),
            ),
            outputs: outputs
                .into_iter()
                .map(|(address, coin)| output(address, coin))
                .collect(),
            fee,
            ttl: None,
            certificates: NonEmptySet::try_from(certs).ok(),
            withdrawals: None,
            auxiliary_data_hash: None,
            validity_interval_start: None,
            mint: None,
            script_data_hash: None,
            collateral: None,
            required_signers: None,
            network_id: None,
            collateral_return: None,
            total_collateral: None,
            reference_inputs: None,
            voting_procedures: None,
            proposal_procedures: None,
            treasury_value: None,
            donation: None,
        }
    }

    fn seed(index: u32) -> TxoRef {
        TxoRef(SEED.into(), index)
    }

    /// `C` funded with 10 ADA from seed 0, and that output spent again.
    fn fund_and_spend() -> [TransactionBody<'static>; 2] {
        let fund = tx(&[seed(0)], vec![(staked(0x01), 10 * ADA)], 10 * ADA, vec![]);
        let spend = tx(
            &[TxoRef(fund.compute_hash(), 0)],
            vec![(enterprise(0x02), 9_900_000)],
            100_000,
            vec![],
        );

        [fund, spend]
    }

    /// The devnet with 500-slot Conway epochs, a 300-slot mutable window, and
    /// `seeds` UTxOs of 20 ADA each at an enterprise address.
    fn domain(seeds: u32) -> ToyDomain {
        let mut genesis = dolos_cardano::include::devnet::load();
        genesis.force_protocol = Some(9);
        genesis.shelley.epoch_length = Some(500);
        genesis.shelley.security_param = Some(5);
        genesis.byron.protocol_consts.k = 5;
        let genesis: Arc<Genesis> = Arc::new(genesis);

        let mut config = dolos_cardano::CardanoLogicConfig::default();

        for i in 0..seeds {
            let cbor = pallas::codec::minicbor::to_vec(output(enterprise(0x0f), 20 * ADA)).unwrap();
            config.custom_utxos.push(CustomUtxo {
                ref_: seed(i),
                era: Some(Era::Conway.into()),
                cbor,
            });
        }

        assert_eq!(dolos_cardano::mutable_slots(&genesis), 300);

        ToyDomain::new_with_genesis_and_config(genesis, config, None, None)
    }

    /// One block per slot, each holding its transactions.
    fn blocks(chain: Vec<(u64, Vec<TransactionBody<'static>>)>) -> Vec<(ChainPoint, RawBlock)> {
        let mut prev = None;
        let mut out = Vec::new();

        for (i, (slot, txs)) in chain.into_iter().enumerate() {
            let txs = txs.into_iter().map(|tx| (tx, true)).collect();
            let (point, raw) = make_conway_block_with_txs_at(slot, i as u64, prev, txs, None);
            prev = point.hash();
            out.push((point, raw));
        }

        out
    }

    fn account(domain: &ToyDomain) -> Option<AccountState> {
        account_of(domain, C)
    }

    fn account_of(domain: &ToyDomain, credential: [u8; 28]) -> Option<AccountState> {
        let credential = StakeCredential::AddrKeyhash(credential.into());

        domain
            .state()
            .read_entity_typed::<AccountState>(&AccountKey::from(&credential))
            .unwrap()
    }

    fn remainder(domain: &ToyDomain) -> Option<PrunedAccountState> {
        remainder_of(domain, C)
    }

    fn remainder_of(domain: &ToyDomain, credential: [u8; 28]) -> Option<PrunedAccountState> {
        let credential = StakeCredential::AddrKeyhash(credential.into());

        domain
            .state()
            .read_entity_typed::<PrunedAccountState>(&AccountKey::from(&credential))
            .unwrap()
    }

    fn epoch(domain: &ToyDomain) -> u64 {
        dolos_cardano::load_epoch::<ToyDomain>(domain.state())
            .unwrap()
            .number
    }

    /// Roll back to `to`, its stats raised first ([`raise_rolling_stats`]).
    fn roll_back(domain: &ToyDomain, to: &ChainPoint) {
        raise_rolling_stats(domain);
        domain.rollback(to).unwrap();
    }

    /// Raise the opening epoch's rolling stats, which hold none of the closing
    /// epoch's blocks, so that a debug build's undo of those blocks' stats
    /// takes the path of a release build, whose subtractions in
    /// `EpochStatsUpdate::undo` wrap.
    fn raise_rolling_stats(domain: &ToyDomain) {
        let mut state = domain
            .state()
            .read_entity_typed::<EpochState>(&())
            .unwrap()
            .unwrap();
        let number = state.number;
        let stats = state.rolling.live_mut(number).as_mut().unwrap();
        stats.blocks_minted += 1_000;
        stats.non_overlay_blocks_minted += 1_000;
        stats.produced_utxos += 1_000_000 * ADA;
        stats.consumed_utxos += 1_000_000 * ADA;
        stats.gathered_fees += 1_000_000 * ADA;
        stats.new_accounts += 1_000;
        stats.removed_accounts += 1_000;
        stats.withdrawals += 1_000_000 * ADA;
        stats.drep_deposits += 1_000_000 * ADA;
        stats.proposal_deposits += 1_000_000 * ADA;
        stats.drep_refunds += 1_000_000 * ADA;
        stats.treasury_donations += 1_000_000 * ADA;
        stats.tx_count += 1_000;
        stats.output = dolos_core::cbor::U128(stats.output.0 + 1_000_000_000_000_000u128);

        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed::<EpochState>(&(), &state)
            .unwrap();
        writer.commit().unwrap();
    }

    /// A never-registered credential funded and spent again in the closing
    /// epoch's last blocks is pruned at the boundary. A rollback to the block
    /// that funded it restores its row at the opening epoch, holding the UTxO
    /// again, as on `main`, and a deeper one removes the row with the block
    /// that created it.
    #[test]
    #[cfg_attr(feature = "strict", ignore = "EpochStatsUpdate::undo, see the module")]
    fn a_rollback_across_the_boundary_restores_a_pruned_account() {
        let [fund, spend] = fund_and_spend();

        // 420 crosses RUPD (4k/f = 400), 510 opens epoch 1
        let blocks = blocks(vec![
            (220, vec![]),
            (260, vec![fund]),
            (300, vec![spend]),
            (420, vec![]),
            (510, vec![]),
        ]);

        let domain = domain(1);

        for (_, raw) in &blocks {
            domain.roll_forward(raw.clone()).unwrap();
        }

        assert_eq!(epoch(&domain), 1);
        assert_eq!(account(&domain), None, "pruned at the boundary");

        // undoes the spending block on the pruned row
        roll_back(&domain, &blocks[1].0);

        let row = account(&domain).expect("restored");
        assert_eq!(row.stake.live().unwrap().utxo_sum, 10 * ADA);
        assert!(!row.is_registered());
        assert_eq!(row.deregistered_at, None);
        assert_eq!(remainder(&domain), None);

        let opening = epoch(&domain);
        assert!(row.stake.is_at_epoch(opening), "{:?}", row.stake);
        assert!(row.pool.is_at_epoch(opening), "{:?}", row.pool);
        assert!(row.drep.is_at_epoch(opening), "{:?}", row.drep);

        // and the block that created it
        roll_back(&domain, &blocks[0].0);

        assert_eq!(account(&domain), None);
        assert_eq!(remainder(&domain), None);
    }

    /// A pruned credential funded again in the opening epoch has a fresh row
    /// by the time the rollback starts. Undoing that block removes the fresh
    /// row, and the undo of the closing epoch's spend restores the pruned one.
    #[test]
    #[cfg_attr(feature = "strict", ignore = "EpochStatsUpdate::undo, see the module")]
    fn a_rollback_restores_a_pruned_account_behind_its_fresh_row() {
        let [fund, spend] = fund_and_spend();
        let again = tx(&[seed(1)], vec![(staked(0x03), 5 * ADA)], 15 * ADA, vec![]);

        let blocks = blocks(vec![
            (260, vec![fund]),
            (300, vec![spend]),
            (420, vec![]),
            (510, vec![]),
            (530, vec![again]),
        ]);

        let domain = domain(2);

        for (_, raw) in &blocks {
            domain.roll_forward(raw.clone()).unwrap();
        }

        assert_eq!(epoch(&domain), 1);
        let fresh = account(&domain).expect("funded again");
        assert_eq!(fresh.stake.live().unwrap().utxo_sum, 5 * ADA);

        roll_back(&domain, &blocks[0].0);

        let row = account(&domain).expect("restored");
        assert_eq!(row.stake.live().unwrap().utxo_sum, 10 * ADA);
        assert_eq!(remainder(&domain), None);
    }

    /// A credential that registers and deregisters in the closing epoch is
    /// dead at the boundary but stays, so a rollback across it undoes the
    /// deregistration on the row itself, as on `main`.
    #[test]
    #[cfg_attr(feature = "strict", ignore = "EpochStatsUpdate::undo, see the module")]
    fn a_deregistration_in_the_closing_epoch_survives_a_rollback_across_the_boundary() {
        let register = tx(
            &[seed(0)],
            vec![(enterprise(0x02), 17_900_000)],
            100_000,
            vec![Certificate::StakeRegistration(cred())],
        );
        let deregister = tx(
            &[seed(1)],
            vec![(enterprise(0x03), 21_900_000)],
            100_000,
            vec![Certificate::StakeDeregistration(cred())],
        );

        let blocks = blocks(vec![
            (260, vec![register]),
            (300, vec![deregister]),
            (420, vec![]),
            (510, vec![]),
        ]);

        let domain = domain(2);

        for (_, raw) in &blocks {
            domain.roll_forward(raw.clone()).unwrap();
        }

        assert_eq!(epoch(&domain), 1);
        let kept = account(&domain).expect("deregistered in the closing epoch");
        assert!(kept.is_dead());
        assert_eq!(kept.deregistered_at, Some(300));

        roll_back(&domain, &blocks[0].0);

        let row = account(&domain).unwrap();
        assert!(row.is_registered());
        assert_eq!(row.registered_at, Some(260));
        assert_eq!(row.deregistered_at, None);
        assert_eq!(remainder(&domain), None);
    }

    /// `C`'s live value, `(utxo_sum, rewards_sum, withdrawals_sum)`.
    fn sums(domain: &ToyDomain) -> Option<(u64, u64, u64)> {
        sums_of(domain, C)
    }

    fn sums_of(domain: &ToyDomain, credential: [u8; 28]) -> Option<(u64, u64, u64)> {
        account_of(domain, credential)
            .and_then(|x| x.stake.live().cloned())
            .map(|x| (x.utxo_sum, x.rewards_sum, x.withdrawals_sum))
    }

    /// The lifetime rewards `/accounts/{stake}` serves for `C`: its row's and
    /// its remainder's ([`PrunedAccountState::restore`]).
    fn served(domain: &ToyDomain) -> u64 {
        let row = sums(domain).map(|(_, rewards, _)| rewards).unwrap_or(0);
        let pruned = remainder(domain).map(|x| x.lifetime_rewards).unwrap_or(0);

        row + pruned
    }

    /// A credential with a pruned life behind it, whose rewards are in its
    /// remainder, funded and spent again in the closing epoch: the rollback
    /// restores its row, which the undo sets to that life's lifetime sums,
    /// and strips them from it again, so the remainder alone counts them.
    #[test]
    #[cfg_attr(feature = "strict", ignore = "EpochStatsUpdate::undo, see the module")]
    fn a_rollback_strips_the_restored_life_from_the_row() {
        const REWARDS: u64 = 450_000;

        let [fund, spend] = fund_and_spend();

        // epoch 1 crosses RUPD at 900, the value comes and goes, and 1010
        // opens epoch 2
        let blocks = blocks(vec![
            (420, vec![]),
            (510, vec![]),
            (920, vec![]),
            (940, vec![fund]),
            (960, vec![spend]),
            (1010, vec![]),
        ]);

        let domain = domain(1);

        domain.roll_forward(blocks[0].1.clone()).unwrap();
        domain.roll_forward(blocks[1].1.clone()).unwrap();
        assert_eq!(epoch(&domain), 1);

        // a life that earned and withdrew rewards and deregistered in epoch 0
        let mut row = AccountState::new(1, cred());
        let live = row.stake.unwrap_live_mut();
        live.rewards_sum = REWARDS;
        live.withdrawals_sum = REWARDS;
        row.deregistered_at = Some(300);

        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(&AccountKey::from(&cred()), &row)
            .unwrap();
        writer.commit().unwrap();

        for (_, raw) in &blocks[2..] {
            domain.roll_forward(raw.clone()).unwrap();
        }

        assert_eq!(epoch(&domain), 2);
        assert_eq!(account(&domain), None, "pruned at the boundary");

        let pruned = PrunedAccountState {
            lifetime_rewards: REWARDS,
            deregistered_at: Some(300),
        };
        assert_eq!(remainder(&domain), Some(pruned.clone()));

        // undoes the spending block on the pruned row
        roll_back(&domain, &blocks[3].0);

        assert_eq!(sums(&domain), Some((10 * ADA, 0, 0)), "restored, stripped");
        assert_eq!(remainder(&domain), Some(pruned.clone()));
        assert_eq!(served(&domain), REWARDS);

        // and the funding block, on the restored row, which it sets to the
        // life's sums again
        roll_back(&domain, &blocks[2].0);

        assert_eq!(sums(&domain), Some((0, 0, 0)), "stripped again");
        assert_eq!(remainder(&domain), Some(pruned));
        assert_eq!(served(&domain), REWARDS, "counted once");
    }

    /// An earlier life of `C`, pruned before the chain starts.
    const EARLIER: u64 = 1_000_000;

    /// The life ESTART prunes.
    const PRUNED: u64 = 450_000;

    /// Both lives, in the remainder after the prune, with the newest
    /// deregistration. Every rollback below leaves it so.
    fn both_lives() -> Option<PrunedAccountState> {
        Some(PrunedAccountState {
            lifetime_rewards: EARLIER + PRUNED,
            deregistered_at: Some(300),
        })
    }

    /// B1 (940) pays `C` and B2 (960) spends it in epoch 1, ESTART at 1010
    /// prunes the row, and with `fund_again` B3 (1030) pays `C` again, so its
    /// WAL entry records `was_new`. Before B1, the row holds a life with
    /// [`PRUNED`] rewards, deregistered at 300, and the remainder an earlier
    /// one with [`EARLIER`], deregistered at 100.
    fn two_lives(fund_again: bool) -> (ToyDomain, Vec<(ChainPoint, RawBlock)>) {
        let [fund, spend] = fund_and_spend();

        let mut chain = vec![
            (420, vec![]),
            (510, vec![]),
            (920, vec![]),
            (940, vec![fund]),
            (960, vec![spend]),
            (1010, vec![]),
        ];

        if fund_again {
            let again = tx(&[seed(1)], vec![(staked(0x03), 5 * ADA)], 15 * ADA, vec![]);
            chain.push((1030, vec![again]));
        }

        let blocks = blocks(chain);
        let domain = domain(2);

        domain.roll_forward(blocks[0].1.clone()).unwrap();
        domain.roll_forward(blocks[1].1.clone()).unwrap();
        assert_eq!(epoch(&domain), 1);

        let mut row = AccountState::new(1, cred());
        let live = row.stake.unwrap_live_mut();
        live.rewards_sum = PRUNED;
        live.withdrawals_sum = PRUNED;
        row.deregistered_at = Some(300);

        let earlier = PrunedAccountState {
            lifetime_rewards: EARLIER,
            deregistered_at: Some(100),
        };

        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(&AccountKey::from(&cred()), &row)
            .unwrap();
        writer
            .write_entity_typed(&AccountKey::from(&cred()), &earlier)
            .unwrap();
        writer.commit().unwrap();

        for (_, raw) in &blocks[2..] {
            domain.roll_forward(raw.clone()).unwrap();
        }

        assert_eq!(epoch(&domain), 2);
        assert_eq!(
            remainder(&domain),
            both_lives(),
            "both lives in the remainder after the prune"
        );

        (domain, blocks)
    }

    /// A rollback to before B1 commits its state and dies before the WAL
    /// truncation. Returns the tip the restart's catch-up replays to.
    fn crash_before_truncation(
        domain: &ToyDomain,
        blocks: &[(ChainPoint, RawBlock)],
    ) -> ChainPoint {
        let to = blocks[2].0.clone();
        let tip = blocks.last().unwrap().0.clone();

        // the state commit lands, the archive commit fails
        raise_rolling_stats(domain);
        let crashing = FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(1));
        crashing
            .rollback(&to)
            .expect_err("dies after the state commit");

        assert_eq!(domain.state().read_cursor().unwrap(), Some(to));
        assert_eq!(
            domain.wal().find_tip().unwrap().map(|(point, _)| point),
            Some(tip.clone()),
            "the WAL keeps the undone blocks"
        );

        tip
    }

    /// The crashed rollback to before B1, and the restart's catch-up, which
    /// replays the logged blocks onto the restored row.
    fn crash_rolling_back_to_b1(domain: &ToyDomain, blocks: &[(ChainPoint, RawBlock)]) {
        let tip = crash_before_truncation(domain, blocks);

        assert_eq!(sums(domain), Some((0, 0, 0)), "restored, stripped");
        assert_eq!(remainder(domain), both_lives(), "the rollback leaves it");

        domain.bootstrap().unwrap();

        assert_eq!(domain.state().read_cursor().unwrap(), Some(tip));
        assert_eq!(remainder(domain), both_lives());
    }

    /// The rollback strips the restored life from the row, and the earlier
    /// life stays in the remainder beside it.
    #[test]
    #[cfg_attr(feature = "strict", ignore = "EpochStatsUpdate::undo, see the module")]
    fn a_rollback_leaves_both_lives_in_the_remainder() {
        let (domain, blocks) = two_lives(false);

        roll_back(&domain, &blocks[2].0);

        assert_eq!(sums(&domain), Some((0, 0, 0)));
        assert_eq!(remainder(&domain), both_lives());
        assert_eq!(served(&domain), EARLIER + PRUNED);
    }

    /// After the crashed rollback, catch-up replays B1..B3 onto the stripped
    /// row, and the same rollback runs again: B3's logged `was_new` removes
    /// the row, B2's undo restores it a second time, and the rollback strips
    /// it again.
    #[test]
    #[cfg_attr(feature = "strict", ignore = "EpochStatsUpdate::undo, see the module")]
    fn a_retried_rollback_leaves_the_remainder_as_one_rollback_does() {
        let (domain, blocks) = two_lives(true);

        assert_eq!(sums(&domain), Some((5 * ADA, 0, 0)), "funded again");

        crash_rolling_back_to_b1(&domain, &blocks);
        assert_eq!(sums(&domain), Some((5 * ADA, 0, 0)), "replayed");

        // the peer is still on the fork
        roll_back(&domain, &blocks[2].0);

        assert_eq!(sums(&domain), Some((0, 0, 0)), "restored again, stripped");
        assert_eq!(remainder(&domain), both_lives());
        assert_eq!(served(&domain), EARLIER + PRUNED);
    }

    /// Without B3, the replayed row survives B2's undo and nothing is
    /// restored. B2's and B1's undos still set the row to the pruned life's
    /// sums, and the rollback strips them.
    #[test]
    #[cfg_attr(feature = "strict", ignore = "EpochStatsUpdate::undo, see the module")]
    fn without_a_fresh_row_a_retried_rollback_strips_the_replayed_row() {
        let (domain, blocks) = two_lives(false);

        assert_eq!(account(&domain), None, "pruned at the boundary");

        crash_rolling_back_to_b1(&domain, &blocks);
        assert_eq!(sums(&domain), Some((0, 0, 0)), "replayed");

        roll_back(&domain, &blocks[2].0);

        assert_eq!(sums(&domain), Some((0, 0, 0)));
        assert_eq!(remainder(&domain), both_lives());
        assert_eq!(served(&domain), EARLIER + PRUNED);
    }

    /// After the crashed rollback, the peer keeps the boundary but not B3,
    /// and the node rolls back only that far. B3's logged `was_new` removes
    /// the replayed row, as it removes the fresh row without the crash, and
    /// both lives stay in the remainder.
    #[test]
    #[cfg_attr(feature = "strict", ignore = "EpochStatsUpdate::undo, see the module")]
    fn a_shallower_rollback_after_a_crashed_one_keeps_both_lives() {
        for crash in [false, true] {
            let (domain, blocks) = two_lives(true);

            if crash {
                crash_rolling_back_to_b1(&domain, &blocks);
            }

            roll_back(&domain, &blocks[5].0);

            assert_eq!(account(&domain), None, "crash: {crash}");
            assert_eq!(remainder(&domain), both_lives(), "crash: {crash}");
            assert_eq!(served(&domain), EARLIER + PRUNED, "crash: {crash}");
        }
    }

    /// The same shallower rollback, then B3' at 1031 on the new fork pays `C`
    /// again, and a rollback to before B1 removes that fresh row, restores
    /// the pruned one behind it and strips it.
    #[test]
    #[cfg_attr(feature = "strict", ignore = "EpochStatsUpdate::undo, see the module")]
    fn a_deep_rollback_on_the_new_fork_keeps_both_lives() {
        for crash in [false, true] {
            let (domain, blocks) = two_lives(true);

            if crash {
                crash_rolling_back_to_b1(&domain, &blocks);
            }

            roll_back(&domain, &blocks[5].0);

            let again = tx(&[seed(1)], vec![(staked(0x04), 6 * ADA)], 14 * ADA, vec![]);
            // the mock header hashes only slot, number and prev: 1031 tells
            // it apart from B3
            let (point, raw) = make_conway_block_with_txs_at(
                1031,
                6,
                blocks[5].0.hash(),
                vec![(again, true)],
                None,
            );
            assert_ne!(point, blocks[6].0);
            domain.roll_forward(raw).unwrap();
            assert_eq!(sums(&domain), Some((6 * ADA, 0, 0)), "crash: {crash}");

            roll_back(&domain, &blocks[2].0);

            assert_eq!(sums(&domain), Some((0, 0, 0)), "crash: {crash}");
            assert_eq!(remainder(&domain), both_lives(), "crash: {crash}");
            assert_eq!(served(&domain), EARLIER + PRUNED, "crash: {crash}");
        }
    }

    /// The rewards of the registered row below, before the boundary.
    const REWARDS: u64 = 120_000;

    /// What the boundary's EWRAP pays it.
    const PAID: u64 = 30_000;

    /// The earlier life of the registered row below, in its remainder.
    fn earlier_life() -> Option<PrunedAccountState> {
        Some(PrunedAccountState {
            lifetime_rewards: EARLIER,
            deregistered_at: Some(300),
        })
    }

    /// A registered row that recreated a pruned credential, earned
    /// [`REWARDS`] and withdrew `withdrawn` of them. B1 (940) pays `C`, B2
    /// (960) spends it, and the boundary at 1010 keeps the row and pays it
    /// [`PAID`]. With `fund_again`, B3 (1030) pays `C` again, so its WAL entry
    /// captures the paid value.
    fn paid_at_the_boundary(
        withdrawn: u64,
        fund_again: bool,
    ) -> (ToyDomain, Vec<(ChainPoint, RawBlock)>) {
        let [fund, spend] = fund_and_spend();

        let mut chain = vec![
            (420, vec![]),
            (510, vec![]),
            (920, vec![]),
            (940, vec![fund]),
            (960, vec![spend]),
            (1010, vec![]),
        ];

        if fund_again {
            let again = tx(&[seed(1)], vec![(staked(0x03), 5 * ADA)], 15 * ADA, vec![]);
            chain.push((1030, vec![again]));
        }

        let blocks = blocks(chain);
        let domain = domain(2);

        domain.roll_forward(blocks[0].1.clone()).unwrap();
        domain.roll_forward(blocks[1].1.clone()).unwrap();
        assert_eq!(epoch(&domain), 1);

        let mut row = AccountState::new(1, cred());
        row.registered_at = Some(600);
        let live = row.stake.unwrap_live_mut();
        live.rewards_sum = REWARDS;
        live.withdrawals_sum = withdrawn;

        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(&AccountKey::from(&cred()), &row)
            .unwrap();
        writer
            .write_entity_typed(&AccountKey::from(&cred()), &earlier_life().unwrap())
            .unwrap();
        writer.commit().unwrap();

        for (_, raw) in &blocks[2..6] {
            domain.roll_forward(raw.clone()).unwrap();
        }

        assert_eq!(epoch(&domain), 2);
        let mut kept = account(&domain).unwrap();
        assert!(kept.is_registered(), "kept");

        // what the boundary's EWRAP pays the live value, ahead of the rotation
        kept.stake.unwrap_live_mut().rewards_sum += PAID;
        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(&AccountKey::from(&cred()), &kept)
            .unwrap();
        writer.commit().unwrap();

        for (_, raw) in &blocks[6..] {
            domain.roll_forward(raw.clone()).unwrap();
        }

        let funded = if fund_again { 5 * ADA } else { 0 };
        assert_eq!(sums(&domain), Some((funded, REWARDS + PAID, withdrawn)));
        assert_eq!(remainder(&domain), earlier_life());

        (domain, blocks)
    }

    /// The registered row funded and spent across the boundary: the rollback
    /// undoes the values on the row itself and restores nothing. The
    /// boundary's payment goes with the undone values, as on `main`, and the
    /// remainder of the earlier life stays as it was.
    #[test]
    #[cfg_attr(feature = "strict", ignore = "EpochStatsUpdate::undo, see the module")]
    fn a_rollback_over_a_recreated_row_leaves_its_remainder() {
        const WITHDRAWN: u64 = 100_000;

        let (domain, blocks) = paid_at_the_boundary(WITHDRAWN, false);

        roll_back(&domain, &blocks[2].0);

        assert_eq!(sums(&domain), Some((0, REWARDS, WITHDRAWN)));
        assert_eq!(remainder(&domain), earlier_life());
    }

    /// After a crashed rollback to before B1, the peer keeps the boundary but
    /// not B3, and the node rolls back only that far. B2's undo took the
    /// payment from the row (`main`'s defect), and catch-up replayed B1..B3
    /// without the boundary, so B3's undo gives the payment back, as on
    /// `main`. The boundary rotated the row, so ESTART never pruned it and the
    /// settle leaves it, whatever was withdrawn. With every reward withdrawn,
    /// the rotated `mark` holds no balance.
    #[test]
    #[cfg_attr(feature = "strict", ignore = "EpochStatsUpdate::undo, see the module")]
    fn a_shallower_rollback_after_a_crashed_one_keeps_the_boundary_payment() {
        for withdrawn in [100_000, 10_000, 0, REWARDS] {
            for crash in [false, true] {
                let case = format!("withdrawn {withdrawn}, crash: {crash}");
                let (domain, blocks) = paid_at_the_boundary(withdrawn, true);

                if crash {
                    let tip = crash_before_truncation(&domain, &blocks);

                    assert_eq!(sums(&domain), Some((0, REWARDS, withdrawn)), "{case}");

                    domain.bootstrap().unwrap();

                    assert_eq!(domain.state().read_cursor().unwrap(), Some(tip));
                    assert_eq!(
                        sums(&domain),
                        Some((5 * ADA, REWARDS, withdrawn)),
                        "replayed without the payment, {case}"
                    );
                }

                roll_back(&domain, &blocks[5].0);

                let live = account(&domain).unwrap().stake.live().cloned().unwrap();

                assert_eq!(
                    live.withdrawable(),
                    REWARDS + PAID - withdrawn,
                    "ledger balance, {case}"
                );
                assert_eq!(
                    sums(&domain),
                    Some((0, REWARDS + PAID, withdrawn)),
                    "lifetime sums, {case}"
                );
                assert_eq!(remainder(&domain), earlier_life(), "{case}");
            }
        }
    }

    /// A credential with no row before the closing epoch.
    const W: [u8; 28] = [0xa1; 28];

    /// How a closing-epoch block creates `W`'s row, and how B3 touches it
    /// again.
    #[derive(Clone, Copy, Debug)]
    enum NewRow {
        /// B1 registers `W`, and B3 pays an address `W` stakes 5 ADA.
        RegisteredPaidAgain,
        /// B1 registers `W`, and B3 withdraws the boundary's payment.
        RegisteredWithdrawn,
        /// B1 pays `W` 10 ADA, B2 registers it, and B3 pays it 5 ADA more.
        FundedPaidAgain,
    }

    struct NewRowChain {
        domain: ToyDomain,
        blocks: Vec<(ChainPoint, RawBlock)>,
        /// `W`'s live sums before B3, which B3's undo gives back.
        before_b3: (u64, u64, u64),
        /// `W`'s live sums as catch-up replays B1..B3, without the boundary.
        replayed: (u64, u64, u64),
    }

    /// B1 (940) creates `W`'s row, its delta `was_new`, the boundary at 1010
    /// keeps and rotates the row and pays it [`PAID`], and B3 (1030) touches
    /// it again, so its WAL entry captures the paid value.
    fn new_row_paid_at_the_boundary(case: NewRow) -> NewRowChain {
        let stake = StakeCredential::AddrKeyhash(W.into());

        let (b1, b2) = match case {
            NewRow::FundedPaidAgain => {
                let fund = tx(
                    &[seed(0)],
                    vec![
                        (staked_to(W, 0x01), 10 * ADA),
                        (enterprise(0x02), 9_900_000),
                    ],
                    100_000,
                    vec![],
                );
                let register = tx(
                    &[TxoRef(fund.compute_hash(), 1)],
                    vec![(enterprise(0x04), 7_800_000)],
                    100_000,
                    vec![Certificate::StakeRegistration(stake.clone())],
                );
                (fund, vec![register])
            }
            NewRow::RegisteredPaidAgain | NewRow::RegisteredWithdrawn => {
                let register = tx(
                    &[seed(0)],
                    vec![(enterprise(0x02), 17_900_000)],
                    100_000,
                    vec![Certificate::StakeRegistration(stake.clone())],
                );
                (register, vec![])
            }
        };

        let b3 = match case {
            NewRow::RegisteredWithdrawn => {
                let mut withdraw = tx(
                    &[seed(1)],
                    vec![(enterprise(0x03), 20 * ADA + PAID - 100_000)],
                    100_000,
                    vec![],
                );
                let mut reward_account = vec![0xe0];
                reward_account.extend_from_slice(&W);
                withdraw.withdrawals = Some(
                    [(reward_account.into(), PAID)]
                        .into_iter()
                        .collect::<std::collections::BTreeMap<_, _>>(),
                );
                withdraw
            }
            NewRow::RegisteredPaidAgain | NewRow::FundedPaidAgain => tx(
                &[seed(1)],
                vec![(staked_to(W, 0x03), 5 * ADA)],
                15 * ADA,
                vec![],
            ),
        };

        let held = match case {
            NewRow::FundedPaidAgain => 10 * ADA,
            NewRow::RegisteredPaidAgain | NewRow::RegisteredWithdrawn => 0,
        };

        let (after_b3, replayed) = match case {
            NewRow::RegisteredPaidAgain => ((5 * ADA, PAID, 0), (5 * ADA, 0, 0)),
            NewRow::RegisteredWithdrawn => ((0, PAID, PAID), (0, 0, PAID)),
            NewRow::FundedPaidAgain => ((15 * ADA, PAID, 0), (15 * ADA, 0, 0)),
        };

        let blocks = blocks(vec![
            (420, vec![]),
            (510, vec![]),
            (920, vec![]),
            (940, vec![b1]),
            (960, b2),
            (1010, vec![]),
            (1030, vec![b3]),
        ]);

        let domain = domain(2);

        for (_, raw) in &blocks[..6] {
            domain.roll_forward(raw.clone()).unwrap();
        }

        assert_eq!(epoch(&domain), 2);

        let mut kept = account_of(&domain, W).expect("kept by the boundary");
        assert!(kept.is_registered(), "{case:?}");
        assert!(kept.stake.mark().is_some(), "rotated, {case:?}");
        assert_eq!(sums_of(&domain, W), Some((held, 0, 0)), "{case:?}");

        // what the boundary's EWRAP pays it: a pool deposit refund or a leader
        // reward to the account registered that epoch
        kept.stake.unwrap_live_mut().rewards_sum += PAID;
        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(&AccountKey::from(&stake), &kept)
            .unwrap();
        writer.commit().unwrap();

        domain.roll_forward(blocks[6].1.clone()).unwrap();

        assert_eq!(sums_of(&domain, W), Some(after_b3), "{case:?}");
        assert_eq!(remainder_of(&domain, W), None, "{case:?}");

        NewRowChain {
            domain,
            blocks,
            before_b3: (held, PAID, 0),
            replayed,
        }
    }

    /// After a crashed rollback to before B1, B1's `was_new` undo has deleted
    /// `W`'s row, and catch-up creates it again at the closing epoch, with no
    /// `mark` and without the boundary's payment. The peer keeps the boundary
    /// but not B3, and B3's undo gives the payment back, as on `main`: the
    /// boundary kept the row, so the settle leaves it.
    #[test]
    #[cfg_attr(feature = "strict", ignore = "EpochStatsUpdate::undo, see the module")]
    fn a_shallower_rollback_after_a_crashed_one_keeps_a_new_rows_boundary_payment() {
        for case in [
            NewRow::RegisteredPaidAgain,
            NewRow::RegisteredWithdrawn,
            NewRow::FundedPaidAgain,
        ] {
            for crash in [false, true] {
                let label = format!("{case:?}, crash: {crash}");

                let NewRowChain {
                    domain,
                    blocks,
                    before_b3,
                    replayed,
                } = new_row_paid_at_the_boundary(case);

                if crash {
                    let tip = crash_before_truncation(&domain, &blocks);

                    assert_eq!(account_of(&domain, W), None, "B1 undone, {label}");

                    domain.bootstrap().unwrap();

                    assert_eq!(domain.state().read_cursor().unwrap(), Some(tip));

                    let row = account_of(&domain, W).expect("B1 replayed");
                    assert!(row.is_registered(), "{label}");
                    assert!(row.stake.mark().is_none(), "{label}");
                    assert!(row.stake.is_at_epoch(1), "{:?}, {label}", row.stake);
                    assert_eq!(
                        sums_of(&domain, W),
                        Some(replayed),
                        "replayed without the payment, {label}"
                    );
                }

                roll_back(&domain, &blocks[5].0);

                let live = account_of(&domain, W)
                    .unwrap()
                    .stake
                    .live()
                    .cloned()
                    .unwrap();

                assert_eq!(live.withdrawable(), PAID, "ledger balance, {label}");
                assert_eq!(
                    sums_of(&domain, W),
                    Some(before_b3),
                    "lifetime sums, {label}"
                );
                assert_eq!(remainder_of(&domain, W), None, "{label}");
            }
        }
    }

    /// One rollback over five credentials: `C` and `C2` with pruned lives,
    /// `D` registered with rewards, `E` holding value through the boundary,
    /// and `F`, never registered, pruned with no history. Each rollback
    /// strips both restored lives and leaves every other row as on `main`.
    #[test]
    #[cfg_attr(feature = "strict", ignore = "EpochStatsUpdate::undo, see the module")]
    fn a_rollback_strips_each_restored_life_and_no_other_row() {
        const C2: [u8; 28] = [0xc2; 28];
        const D: [u8; 28] = [0xd1; 28];
        const E: [u8; 28] = [0xe1; 28];
        const F: [u8; 28] = [0xf1; 28];
        const PRUNED2: u64 = 280_000;
        const WITHDRAWN: u64 = 100_000;

        let fund = tx(
            &[seed(0)],
            vec![
                (staked_to(C, 0x01), 4 * ADA),
                (staked_to(C2, 0x01), 4 * ADA),
                (staked_to(D, 0x01), 4 * ADA),
                (staked_to(E, 0x01), 4 * ADA),
                (staked_to(F, 0x01), 3_900_000),
            ],
            100_000,
            vec![],
        );
        let funded = fund.compute_hash();
        let spend = tx(
            &[0, 1, 2, 4].map(|index| TxoRef(funded, index)),
            vec![(staked_to(E, 0x02), ADA), (enterprise(0x02), 14_800_000)],
            100_000,
            vec![],
        );

        let blocks = blocks(vec![
            (420, vec![]),
            (510, vec![]),
            (920, vec![]),
            (940, vec![fund]),
            (960, vec![spend]),
            (1010, vec![]),
        ]);

        let domain = domain(1);

        domain.roll_forward(blocks[0].1.clone()).unwrap();
        domain.roll_forward(blocks[1].1.clone()).unwrap();
        assert_eq!(epoch(&domain), 1);

        // two lives that earned, withdrew and deregistered in epoch 0, and a
        // registered row with rewards
        let writer = domain.state().start_writer().unwrap();

        for (credential, rewards, withdrawn, registered_at, deregistered_at) in [
            (C, PRUNED, PRUNED, None, Some(300)),
            (C2, PRUNED2, PRUNED2, None, Some(300)),
            (D, REWARDS, WITHDRAWN, Some(600), None),
        ] {
            let mut row = AccountState::new(1, StakeCredential::AddrKeyhash(credential.into()));
            row.registered_at = registered_at;
            row.deregistered_at = deregistered_at;
            let live = row.stake.unwrap_live_mut();
            live.rewards_sum = rewards;
            live.withdrawals_sum = withdrawn;

            writer
                .write_entity_typed(&AccountKey::from(&row.credential), &row)
                .unwrap();
        }

        writer.commit().unwrap();

        for (_, raw) in &blocks[2..] {
            domain.roll_forward(raw.clone()).unwrap();
        }

        assert_eq!(epoch(&domain), 2);

        for credential in [C, C2, F] {
            assert_eq!(account_of(&domain, credential), None, "pruned");
        }

        for credential in [D, E] {
            let kept = account_of(&domain, credential).expect("kept");
            assert!(kept.stake.mark().is_some(), "rotated");
        }

        let remainders = |domain: &ToyDomain| {
            [C, C2, D, E, F].map(|x| remainder_of(domain, x).map(|x| x.lifetime_rewards))
        };

        let pruned = [Some(PRUNED), Some(PRUNED2), None, None, None];
        assert_eq!(remainders(&domain), pruned);

        // undoes the spend, restoring C, C2 and F
        roll_back(&domain, &blocks[3].0);

        assert_eq!(sums_of(&domain, C), Some((4 * ADA, 0, 0)));
        assert_eq!(sums_of(&domain, C2), Some((4 * ADA, 0, 0)));
        assert_eq!(sums_of(&domain, D), Some((4 * ADA, REWARDS, WITHDRAWN)));
        assert_eq!(sums_of(&domain, E), Some((4 * ADA, 0, 0)));
        assert_eq!(sums_of(&domain, F), Some((3_900_000, 0, 0)));
        assert_eq!(remainders(&domain), pruned);

        // and the funding, on the restored rows
        roll_back(&domain, &blocks[2].0);

        assert_eq!(sums_of(&domain, C), Some((0, 0, 0)));
        assert_eq!(sums_of(&domain, C2), Some((0, 0, 0)));
        assert_eq!(sums_of(&domain, D), Some((0, REWARDS, WITHDRAWN)));
        assert_eq!(account_of(&domain, E), None);
        assert_eq!(account_of(&domain, F), None);
        assert_eq!(remainders(&domain), pruned);
    }
}
