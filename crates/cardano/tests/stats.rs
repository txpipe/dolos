//! Stats through the real pipeline: every total the period logs answer equals
//! the lifted fold over the same blocks, whether its epochs are sealed or
//! still staged, and rollback takes a block's contributions back out exactly.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use dolos_cardano::{
    model::stats::split_payment,
    owned::OwnedMultiEraOutput,
    periods::stats::{
        account_totals, address_totals, payment_totals, sum_account_block_txs, sum_block_txs,
    },
    AccountKey, AddressKey, AddressStats, CardanoDelta, ChainSummary, PaymentKey,
    StagedAddressStats, UnitSums,
};
use dolos_core::{
    config::{CardanoConfig, CustomUtxo},
    import::ImportExt as _,
    sync::SyncExt as _,
    ChainError, ChainPoint, Domain as _, EraCbor, Genesis, RawBlock, StateStore as _, TxoRef,
    WalStore as _,
};
use dolos_testing::{blocks::make_conway_block_with_txs_at, toy_domain::ToyDomain};
use pallas::{
    codec::utils::{Bytes, KeepRaw, Set},
    crypto::hash::Hash,
    ledger::{
        addresses::{
            Address, Network, Pointer, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
        },
        primitives::{
            conway::{
                PositiveCoin, PostAlonzoTransactionOutput, TransactionBody, TransactionInput,
                TransactionOutput, Value,
            },
            StakeCredential,
        },
        traverse::{ComputeHash as _, Era, MultiEraBlock, MultiEraInput, MultiEraOutput},
    },
};

const ADA: u64 = 1_000_000;
const MAX: u64 = u64::MAX;

const A: [u8; 28] = [0xa0; 28];
const B: [u8; 28] = [0xb0; 28];

fn shelley(payment: u8, delegation: ShelleyDelegationPart) -> Vec<u8> {
    ShelleyAddress::new(
        Network::Testnet,
        ShelleyPaymentPart::Key([payment; 28].into()),
        delegation,
    )
    .to_vec()
}

fn staked(payment: u8, stake: [u8; 28]) -> Vec<u8> {
    shelley(payment, ShelleyDelegationPart::Key(stake.into()))
}

fn enterprise(payment: u8) -> Vec<u8> {
    shelley(payment, ShelleyDelegationPart::Null)
}

/// A pointer the ledger's mainnet table resolves to no credential.
fn pointer(payment: u8) -> Vec<u8> {
    shelley(
        payment,
        ShelleyDelegationPart::Pointer(Pointer::new(13200, 526450, 149104513)),
    )
}

fn byron() -> Vec<u8> {
    let address = pallas::ledger::addresses::ByronAddress::from_base58(
        "Ae2tdPwUPEZ4YjgvykNpoFeYUxoyhNj2kg8KfKWN2FizsSpLUPv68MpTVDo",
    )
    .unwrap();

    Address::Byron(address).to_vec()
}

/// The addresses of the chain below. Payment hash 0x01 is shared by a base,
/// an enterprise and a pointer address, 0x02 by a base and an enterprise one.
fn a1() -> Vec<u8> {
    staked(0x01, A)
}

fn a2() -> Vec<u8> {
    staked(0x02, A)
}

fn a4() -> Vec<u8> {
    staked(0x04, A)
}

fn b3() -> Vec<u8> {
    staked(0x03, B)
}

fn e1() -> Vec<u8> {
    enterprise(0x01)
}

fn e2() -> Vec<u8> {
    enterprise(0x02)
}

fn p1() -> Vec<u8> {
    pointer(0x01)
}

fn asset(name: &str) -> (Hash<28>, Bytes) {
    (
        Hash::from([0x55; 28]),
        Bytes::from(name.as_bytes().to_vec()),
    )
}

fn value(coin: u64, assets: &[(&str, u64)]) -> Value {
    if assets.is_empty() {
        return Value::Coin(coin);
    }

    let mut multiasset: BTreeMap<Hash<28>, BTreeMap<Bytes, PositiveCoin>> = BTreeMap::new();

    for (name, quantity) in assets {
        let (policy, name) = asset(name);
        multiasset
            .entry(policy)
            .or_default()
            .insert(name, PositiveCoin::try_from(*quantity).unwrap());
    }

    Value::Multiasset(coin, multiasset)
}

fn output(address: Vec<u8>, value: Value) -> TransactionOutput<'static> {
    TransactionOutput::PostAlonzo(KeepRaw::from(PostAlonzoTransactionOutput {
        address: address.into(),
        value,
        datum_option: None,
        script_ref: None,
    }))
}

fn input(txo: &TxoRef) -> TransactionInput {
    TransactionInput {
        transaction_id: txo.0,
        index: txo.1 as u64,
    }
}

fn body(inputs: &[TxoRef], outputs: Vec<(Vec<u8>, Value)>, fee: u64) -> TransactionBody<'static> {
    TransactionBody {
        inputs: Set::from(inputs.iter().map(input).collect::<Vec<_>>()),
        outputs: outputs
            .into_iter()
            .map(|(address, value)| output(address, value))
            .collect(),
        fee,
        ttl: None,
        certificates: None,
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

const SEED: [u8; 32] = [0xee; 32];

fn seed(index: u32) -> TxoRef {
    TxoRef(SEED.into(), index)
}

/// What the funding tx hands out; the seeds hold the same at the faucet, plus
/// its fee share.
fn funds() -> Vec<(Vec<u8>, Value)> {
    vec![
        (a1(), value(10 * ADA, &[("X", MAX - 5), ("Y", 7)])),
        (a2(), value(20 * ADA, &[("X", MAX - 3)])),
        (e1(), value(5 * ADA, &[])),
        (b3(), value(30 * ADA, &[("Y", 100)])),
        (a4(), value(50 * ADA, &[])),
        (byron(), value(8 * ADA, &[])),
    ]
}

const FEE_SHARE: u64 = 100_000;

/// The seeds sit at an enterprise faucet: an input at a stake address needs
/// its account, which a received output creates.
fn faucet() -> Vec<u8> {
    enterprise(0x0f)
}

fn seeds() -> Vec<(Vec<u8>, Value)> {
    funds()
        .into_iter()
        .map(|(_, value)| {
            let value = match value {
                Value::Coin(coin) => Value::Coin(coin + FEE_SHARE),
                Value::Multiasset(coin, assets) => Value::Multiasset(coin + FEE_SHARE, assets),
            };

            (faucet(), value)
        })
        .collect()
}

fn produced(tx: &TransactionBody, index: u32) -> TxoRef {
    TxoRef(tx.compute_hash(), index)
}

/// Two blocks in epoch 0, then one per epoch of 500 slots up to epoch 6; on
/// the devnet's mutable window of 300 slots, epochs 0 to 4 seal and 5 and 6
/// stay staged.
///
/// - T0 funds the addresses from the faucet.
/// - T1 sends an address's output back to it.
/// - T2 spreads one account address over two more of the same account.
/// - T3 is phase-2-invalid: it moves its collateral and its collateral return,
///   never its inputs or outputs.
/// - T4 touches two of the three addresses of one payment hash.
/// - T5 moves a token between two accounts, T6 spends a Byron output.
/// - T7 spends two outputs of the first block, sending sums past u64 from one
///   address in one tx.
/// - T8 and T9 land in later epochs, T9 in one still staged.
fn chain() -> Vec<Vec<(TransactionBody<'static>, bool)>> {
    let seeds: Vec<TxoRef> = (0..funds().len() as u32).map(seed).collect();
    let t0 = body(&seeds, funds(), FEE_SHARE * seeds.len() as u64);
    let funded = |index: u32| produced(&t0, index);

    let t1 = body(
        &[funded(0)],
        vec![(a1(), value(9_800_000, &[("X", MAX - 5), ("Y", 7)]))],
        200_000,
    );

    let t2 = body(
        &[funded(1)],
        vec![
            (a1(), value(10 * ADA, &[("X", MAX - 3)])),
            (a4(), value(9_900_000, &[])),
        ],
        100_000,
    );

    let t3 = TransactionBody {
        collateral: Some(vec![input(&funded(4))].try_into().unwrap()),
        collateral_return: Some(output(a4(), value(49 * ADA, &[]))),
        total_collateral: Some(ADA),
        ..body(
            &[funded(3)],
            vec![(b3(), value(29_800_000, &[("Y", 100)]))],
            200_000,
        )
    };

    let t4 = body(
        &[funded(2)],
        vec![(p1(), value(2 * ADA, &[])), (b3(), value(2_900_000, &[]))],
        100_000,
    );

    let t5 = body(
        &[funded(3)],
        vec![
            (a2(), value(10 * ADA, &[("Y", 60)])),
            (b3(), value(19_800_000, &[("Y", 40)])),
        ],
        200_000,
    );

    let t6 = body(&[funded(5)], vec![(a1(), value(7_900_000, &[]))], 100_000);

    let t7 = body(
        &[produced(&t1, 0), produced(&t2, 0)],
        vec![
            (a2(), value(9_700_000, &[("X", MAX - 5)])),
            (e2(), value(10 * ADA, &[("X", MAX - 3), ("Y", 7)])),
        ],
        100_000,
    );

    let t8 = body(
        &[produced(&t4, 0)],
        vec![(p1(), value(1_900_000, &[]))],
        100_000,
    );

    let t9 = body(
        &[produced(&t7, 0)],
        vec![(a2(), value(9_600_000, &[("X", MAX - 5)]))],
        100_000,
    );

    vec![
        vec![(t0, true)],
        vec![(t1, true), (t2, true)],
        vec![(t3, false), (t4, true)],
        vec![(t5, true), (t6, true)],
        vec![(t7, true)],
        vec![(t8, true)],
        vec![(t9, true)],
        vec![],
    ]
}

/// The first two blocks land in epoch 0, every other one mid-epoch in the
/// next.
fn slot(block: usize) -> u64 {
    match block {
        0 => 10,
        1 => 200,
        _ => (block as u64 - 1) * 500 + 120,
    }
}

fn raw_blocks() -> Vec<(ChainPoint, RawBlock)> {
    let mut prev = None;
    let mut out = Vec::new();

    for (i, txs) in chain().into_iter().enumerate() {
        let (point, raw) = make_conway_block_with_txs_at(slot(i), i as u64, prev, txs, None);
        prev = point.hash();
        out.push((point, raw));
    }

    out
}

fn config() -> CardanoConfig {
    let mut config = CardanoConfig::default();

    for (i, (address, value)) in seeds().into_iter().enumerate() {
        let cbor = pallas::codec::minicbor::to_vec(output(address, value)).unwrap();

        config.custom_utxos.push(CustomUtxo {
            ref_: seed(i as u32),
            era: Some(Era::Conway.into()),
            cbor,
        });
    }

    config
}

/// The devnet with 500-slot Conway epochs and a 300-slot mutable window.
fn genesis() -> Arc<Genesis> {
    let mut genesis = dolos_cardano::include::devnet::load();
    genesis.force_protocol = Some(9);
    genesis.shelley.epoch_length = Some(500);
    genesis.shelley.security_param = Some(5);
    genesis.byron.protocol_consts.k = 5;
    Arc::new(genesis)
}

/// The lifted fold over every block, resolving inputs from every output
/// the seeds and the chain ever held.
struct Oracle {
    blocks: Vec<RawBlock>,
    outputs: HashMap<TxoRef, OwnedMultiEraOutput>,
}

impl Oracle {
    fn new(blocks: &[(ChainPoint, RawBlock)]) -> Self {
        let mut outputs = HashMap::new();

        let mut hold = |txo: TxoRef, cbor: Vec<u8>| {
            let output =
                OwnedMultiEraOutput::decode(Arc::new(EraCbor(Era::Conway.into(), cbor))).unwrap();
            outputs.insert(txo, output);
        };

        for (i, (address, value)) in seeds().into_iter().enumerate() {
            hold(
                seed(i as u32),
                pallas::codec::minicbor::to_vec(output(address, value)).unwrap(),
            );
        }

        for (_, raw) in blocks {
            let block = MultiEraBlock::decode(raw).unwrap();

            for tx in block.txs() {
                for (index, output) in tx.produces() {
                    hold(TxoRef(tx.hash(), index as u32), output.encode());
                }
            }
        }

        Self {
            blocks: blocks.iter().map(|(_, raw)| raw.clone()).collect(),
            outputs,
        }
    }

    fn fold(
        &self,
        mut fold: impl FnMut(
            &mut AddressStats,
            &MultiEraBlock,
            &dyn Fn(
                &MultiEraInput,
                &mut dyn FnMut(&MultiEraOutput) -> Result<(), ChainError>,
            ) -> Result<(), ChainError>,
        ),
    ) -> AddressStats {
        let mut totals = AddressStats::default();

        let consumed = |input: &MultiEraInput,
                        visit: &mut dyn FnMut(&MultiEraOutput) -> Result<(), ChainError>|
         -> Result<(), ChainError> {
            match self.outputs.get(&TxoRef::from(input)) {
                Some(output) => output.with_dependent(|_, output| visit(output)),
                None => Ok(()),
            }
        };

        for raw in &self.blocks {
            let block = MultiEraBlock::decode(raw).unwrap();
            fold(&mut totals, &block, &consumed);
        }

        totals
    }

    fn address(&self, address: &[u8]) -> AddressStats {
        self.fold(|totals, block, consumed| {
            sum_block_txs(totals, block, consumed, |x| x.to_vec() == address).unwrap()
        })
    }

    fn payment(&self, payment: u8) -> AddressStats {
        self.fold(|totals, block, consumed| {
            sum_block_txs(totals, block, consumed, |x| {
                split_payment(x).is_some_and(|(hash, _)| hash == Hash::from([payment; 28]))
            })
            .unwrap()
        })
    }

    fn account(&self, stake: [u8; 28]) -> AddressStats {
        let account = StakeCredential::AddrKeyhash(stake.into());

        self.fold(|totals, block, consumed| {
            sum_account_block_txs(totals, block, consumed, &account).unwrap()
        })
    }
}

fn summary(domain: &ToyDomain) -> ChainSummary {
    dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap()
}

fn addresses() -> Vec<Vec<u8>> {
    vec![
        a1(),
        a2(),
        a4(),
        b3(),
        e1(),
        e2(),
        p1(),
        byron(),
        staked(0x09, B),
    ]
}

const PAYMENTS: [u8; 7] = [0x01, 0x02, 0x03, 0x04, 0x0f, 0x05, 0x09];

const ACCOUNTS: [[u8; 28]; 3] = [A, B, [0xc0; 28]];

/// Every total the period logs answer, by address, payment and account.
fn totals(domain: &ToyDomain) -> Vec<AddressStats> {
    let chain = summary(domain);
    let epochs = || 0..=6;

    let mut out = Vec::new();

    for address in addresses() {
        let key = AddressKey::of(&Address::from_bytes(&address).unwrap());
        out.push(address_totals(domain, &chain, epochs(), &key).unwrap());
    }

    for payment in PAYMENTS {
        let payment = PaymentKey([payment; 28].into());
        out.push(payment_totals(domain, &chain, epochs(), &payment).unwrap());
    }

    for stake in ACCOUNTS {
        let account = AccountKey(StakeCredential::AddrKeyhash(stake.into()));
        out.push(account_totals(domain, &chain, epochs(), &account).unwrap());
    }

    out
}

fn oracle_totals(oracle: &Oracle) -> Vec<AddressStats> {
    let mut out: Vec<AddressStats> = addresses()
        .iter()
        .map(|address| oracle.address(address))
        .collect();

    out.extend(PAYMENTS.iter().map(|payment| oracle.payment(*payment)));
    out.extend(ACCOUNTS.iter().map(|stake| oracle.account(*stake)));

    out
}

fn sums(stats: &AddressStats, unit: &[u8]) -> UnitSums {
    stats
        .units
        .get(&Bytes::from(unit.to_vec()))
        .copied()
        .unwrap_or_default()
}

fn x() -> Vec<u8> {
    let (policy, name) = asset("X");
    [policy.as_slice(), &name].concat()
}

/// Done criterion 1: by address, payment credential and stake, the totals
/// read from sealed and staged epochs equal the lifted fold.
#[test]
fn totals_equal_the_lifted_fold() {
    let blocks = raw_blocks();
    let oracle = Oracle::new(&blocks);

    let domain = ToyDomain::new_with_genesis_and_config(genesis(), config(), None, None);
    domain
        .import_blocks(blocks.iter().map(|(_, raw)| raw.clone()).collect())
        .unwrap();

    // the read spans both stores
    let staged: Vec<u64> = domain
        .state()
        .iter_entities_typed::<StagedAddressStats>(None)
        .unwrap()
        .map(|row| row.unwrap().0 .0)
        .collect();
    assert!(!staged.is_empty() && staged.iter().all(|epoch| *epoch >= 5));

    let expected = oracle_totals(&oracle);
    assert_eq!(totals(&domain), expected);

    let by_address: HashMap<Vec<u8>, &AddressStats> =
        addresses().into_iter().zip(&expected).collect();

    // a self-transfer receives and sends, and counts once
    let a1 = by_address[&a1()];
    assert_eq!(a1.tx_count, 5);
    assert_eq!(sums(a1, &[]).sent.0, u128::from(10 * ADA + 19_800_000));

    // X sums past u64, everything a1 received of it sent on
    let x_sums = sums(a1, &x());
    assert_eq!(
        x_sums.received.0,
        u128::from(MAX - 5) * 2 + u128::from(MAX - 3)
    );
    assert_eq!(x_sums.sent.0, x_sums.received.0);

    // the invalid tx moved its collateral and collateral return only
    let a4 = by_address[&a4()];
    assert_eq!(a4.tx_count, 3);
    assert_eq!(sums(a4, &[]).sent.0, u128::from(50 * ADA));
    assert_eq!(
        sums(a4, &[]).received.0,
        u128::from(50 * ADA + 9_900_000 + 49 * ADA)
    );
    assert_eq!(by_address[&b3()].tx_count, 3);

    // the account counts a tx touching several of its addresses once
    let account_a = &expected[addresses().len() + PAYMENTS.len()];
    assert_eq!(account_a.tx_count, 8);

    // a payment hash counts a tx touching several of its addresses once
    let payment_01 = &expected[addresses().len()];
    assert_eq!(payment_01.tx_count, 7);

    // nothing for what the chain never touched
    assert_eq!(by_address[&staked(0x09, B)], &AddressStats::default());
    assert_eq!(expected.last().unwrap(), &AddressStats::default());
}

/// Rollback: undoing a block takes exactly its contributions back out of
/// every total, and a key a block touches many times costs one WAL delta.
#[test]
fn rollback_takes_a_block_back_out_of_every_total() {
    let blocks = raw_blocks();

    let domain = ToyDomain::new_with_genesis_and_config(
        Arc::new(dolos_cardano::include::preview::load()),
        config(),
        None,
        None,
    );

    let mut after = Vec::new();

    for (_, raw) in &blocks {
        domain.roll_forward(raw.clone()).unwrap();
        after.push(totals(&domain));
    }

    // the second block touches three addresses, a1 twice
    let entry = domain.wal().read_entry(&blocks[1].0).unwrap().unwrap();
    let staged = entry
        .delta
        .iter()
        .filter(|delta| matches!(delta, CardanoDelta::StageAddressStats(_)))
        .count();
    assert_eq!(staged, 3);

    for i in (0..blocks.len() - 1).rev() {
        domain.rollback(&blocks[i].0).unwrap();
        assert_eq!(totals(&domain), after[i], "after rolling back to block {i}");
    }
}
