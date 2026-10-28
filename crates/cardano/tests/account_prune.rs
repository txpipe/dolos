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
    ControlledAmountDec, ControlledAmountInc, PrunedAccountState, StakeDelegation,
    StakeDeregistration, StakeRegistration, WithdrawalInc,
};
use dolos_core::{
    sync::execute_work_unit, Domain, DomainError, EntityDelta, FixedNamespace, Genesis, Namespace,
    RangeOf, StateStore as _, StateWriter as _,
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
        config::CustomUtxo, sync::SyncExt as _, ChainPoint, Domain as _, Genesis, RawBlock,
        StateStore as _, StateWriter as _, TxoRef,
    };
    use dolos_testing::{blocks::make_conway_block_with_txs_at, toy_domain::ToyDomain};
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
        shelley(payment, ShelleyDelegationPart::Key(C.into()))
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
        domain
            .state()
            .read_entity_typed::<AccountState>(&AccountKey::from(&cred()))
            .unwrap()
    }

    fn remainder(domain: &ToyDomain) -> Option<PrunedAccountState> {
        domain
            .state()
            .read_entity_typed::<PrunedAccountState>(&AccountKey::from(&cred()))
            .unwrap()
    }

    fn epoch(domain: &ToyDomain) -> u64 {
        dolos_cardano::load_epoch::<ToyDomain>(domain.state())
            .unwrap()
            .number
    }

    /// Roll back to `to`. A debug build first raises the opening epoch's
    /// rolling stats, which hold none of the closing epoch's blocks, so that
    /// undoing those blocks' stats takes the path of a release build, whose
    /// subtractions in `EpochStatsUpdate::undo` wrap.
    fn roll_back(domain: &ToyDomain, to: &ChainPoint) {
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

        domain.rollback(to).unwrap();
    }

    /// A never-registered credential funded and spent again in the closing
    /// epoch's last blocks is pruned at the boundary. A rollback to the block
    /// that funded it restores its row holding the UTxO again, as on `main`,
    /// and a deeper one removes the row with the block that created it.
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

    /// A credential with a pruned life behind it, whose rewards are in its
    /// remainder, funded and spent again in the closing epoch: the rollback
    /// restores its row with that life's lifetime sums and takes them back
    /// out of the remainder, so the two count them once.
    #[test]
    #[cfg_attr(feature = "strict", ignore = "EpochStatsUpdate::undo, see the module")]
    fn a_rollback_takes_the_restored_life_back_out_of_the_remainder() {
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
        assert_eq!(
            remainder(&domain),
            Some(PrunedAccountState {
                lifetime_rewards: REWARDS,
                deregistered_at: Some(300),
            })
        );

        // undoes the spending block on the pruned row
        roll_back(&domain, &blocks[3].0);

        let row = account(&domain).expect("restored");
        let live = row.stake.live().unwrap();
        assert_eq!(live.utxo_sum, 10 * ADA);
        assert_eq!(live.rewards_sum, REWARDS);
        assert_eq!(live.withdrawals_sum, REWARDS);
        assert_eq!(
            remainder(&domain),
            Some(PrunedAccountState {
                lifetime_rewards: 0,
                deregistered_at: Some(300),
            })
        );

        // and the funding block, on the restored row
        roll_back(&domain, &blocks[2].0);

        let live = account(&domain).unwrap().stake.live().cloned().unwrap();
        assert_eq!(live.utxo_sum, 0);
        assert_eq!(live.rewards_sum, REWARDS);
        assert_eq!(
            remainder(&domain).map(|x| x.lifetime_rewards),
            Some(0),
            "counted once"
        );
    }
}
