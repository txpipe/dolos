//! Witness-datum refcounts through the real pipeline and its WAL: a datum
//! stays readable while any live UTxO references its hash, whichever of those
//! UTxOs carried the datum in its witness set, and rollback restores every
//! row the block before held, back to the bootstrap state.

use std::sync::Arc;

use dolos_cardano::{indexes::AsyncCardanoQueryExt as _, DatumKey, DatumState};
use dolos_core::{
    config::CustomUtxo, sync::SyncExt as _, AsyncQueryFacade, ChainPoint, Domain as _, RawBlock,
    StateStore as _, TxoRef,
};
use dolos_testing::{
    blocks::{empty_witness_set, make_conway_block_with_witnessed_txs_at},
    toy_domain::ToyDomain,
};
use pallas::{
    codec::{
        minicbor,
        utils::{Int, KeepRaw, NonEmptySet, Set},
    },
    crypto::hash::Hash,
    ledger::{
        addresses::{Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart},
        primitives::{
            conway::{
                DatumOption, PlutusData, PostAlonzoTransactionOutput, TransactionBody,
                TransactionInput, TransactionOutput, Value, WitnessSet,
            },
            BigInt,
        },
        traverse::{ComputeHash as _, Era},
    },
};

const ADA: u64 = 1_000_000;

const SEED: [u8; 32] = [0xee; 32];

fn seed(index: u32) -> TxoRef {
    TxoRef(SEED.into(), index)
}

fn address(payment: u8) -> Vec<u8> {
    ShelleyAddress::new(
        Network::Testnet,
        ShelleyPaymentPart::Key([payment; 28].into()),
        ShelleyDelegationPart::Null,
    )
    .to_vec()
}

fn datum() -> PlutusData {
    PlutusData::BigInt(BigInt::Int(Int::from(42)))
}

fn datum_hash() -> Hash<32> {
    datum().compute_hash()
}

fn datum_bytes() -> Vec<u8> {
    minicbor::to_vec(datum()).unwrap()
}

fn output(datum_hash: Option<Hash<32>>) -> TransactionOutput<'static> {
    TransactionOutput::PostAlonzo(KeepRaw::from(PostAlonzoTransactionOutput {
        address: address(0x01).into(),
        value: Value::Coin(2 * ADA),
        datum_option: datum_hash.map(|hash| KeepRaw::from(DatumOption::Hash(hash))),
        script_ref: None,
    }))
}

fn body(input: &TxoRef, datum_hash: Option<Hash<32>>) -> TransactionBody<'static> {
    body_from(std::slice::from_ref(input), datum_hash)
}

fn body_from(inputs: &[TxoRef], datum_hash: Option<Hash<32>>) -> TransactionBody<'static> {
    TransactionBody {
        inputs: Set::from(
            inputs
                .iter()
                .map(|input| TransactionInput {
                    transaction_id: input.0,
                    index: input.1 as u64,
                })
                .collect::<Vec<_>>(),
        ),
        outputs: vec![output(datum_hash)],
        fee: 100_000,
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

fn witnessing_datum() -> WitnessSet<'static> {
    WitnessSet {
        plutus_data: Some(KeepRaw::from(
            NonEmptySet::try_from(vec![KeepRaw::from(datum())]).unwrap(),
        )),
        ..empty_witness_set()
    }
}

/// A tx from `input` to one output locked by the datum's hash, carrying the
/// datum in its witness set when `witnessed`.
fn lock(input: &TxoRef, witnessed: bool) -> (TransactionBody<'static>, WitnessSet<'static>) {
    let witness_set = if witnessed {
        witnessing_datum()
    } else {
        empty_witness_set()
    };

    (body(input, Some(datum_hash())), witness_set)
}

/// A tx spending `input` to one output locked by the datum's hash, without
/// the datum in its witness set.
fn relock(input: &TxoRef) -> (TransactionBody<'static>, WitnessSet<'static>) {
    (body(input, Some(datum_hash())), empty_witness_set())
}

/// A tx spending `input` to an output with no datum.
fn spend(input: &TxoRef) -> (TransactionBody<'static>, WitnessSet<'static>) {
    (body(input, None), empty_witness_set())
}

/// A tx spending every one of `inputs` to one output with no datum.
fn spend_all(inputs: &[TxoRef]) -> (TransactionBody<'static>, WitnessSet<'static>) {
    (body_from(inputs, None), empty_witness_set())
}

fn produced(tx: &TransactionBody) -> TxoRef {
    TxoRef(tx.compute_hash(), 0)
}

/// A domain whose configured UTxOs are the seeds, the one at each index
/// carrying the datum hash given for it.
fn domain(seeds: &[Option<Hash<32>>]) -> ToyDomain {
    let mut config = dolos_cardano::CardanoLogicConfig::default();

    for (index, datum_hash) in seeds.iter().enumerate() {
        let cbor = minicbor::to_vec(TransactionOutput::PostAlonzo(KeepRaw::from(
            PostAlonzoTransactionOutput {
                address: address(0x0f).into(),
                value: Value::Coin(10 * ADA),
                datum_option: datum_hash.map(|hash| KeepRaw::from(DatumOption::Hash(hash))),
                script_ref: None,
            },
        )))
        .unwrap();

        config.custom_utxos.push(CustomUtxo {
            ref_: seed(index as u32),
            era: Some(Era::Conway.into()),
            cbor,
        });
    }

    ToyDomain::new_with_genesis_and_config(
        Arc::new(dolos_cardano::include::preview::load()),
        config,
        None,
        None,
    )
}

type Tx = (TransactionBody<'static>, WitnessSet<'static>);

/// One block per entry, each holding its txs in order.
fn blocks(chain: Vec<Vec<Tx>>) -> Vec<(ChainPoint, RawBlock)> {
    let mut prev = None;

    chain
        .into_iter()
        .enumerate()
        .map(|(i, txs)| {
            let txs = txs
                .into_iter()
                .map(|(body, witness_set)| (body, witness_set, true))
                .collect();
            let (point, raw) = make_conway_block_with_witnessed_txs_at(
                10 + 10 * i as u64,
                i as u64,
                prev,
                txs,
                None,
            );
            prev = point.hash();
            (point, raw)
        })
        .collect()
}

fn row(domain: &ToyDomain) -> Option<DatumState> {
    domain
        .state()
        .read_entity_typed::<DatumState>(&DatumKey(datum_hash()))
        .unwrap()
}

/// What a datum lookup answers.
async fn lookup(domain: &ToyDomain) -> Option<Vec<u8>> {
    AsyncQueryFacade::new(domain.clone())
        .get_datum(&datum_hash())
        .await
        .unwrap()
}

fn known(refcount: u64) -> Option<DatumState> {
    Some(DatumState {
        refcount,
        bytes: datum_bytes(),
    })
}

fn unknown(refcount: u64) -> Option<DatumState> {
    Some(DatumState {
        refcount,
        bytes: vec![],
    })
}

/// Seeds with no datum, for chains that lock their own.
const PLAIN_SEEDS: [Option<Hash<32>>; 2] = [None, None];

/// Rolls `chain` forward one tx per block; see `roll_blocks`.
async fn roll(seeds: &[Option<Hash<32>>], chain: Vec<Tx>, expected: &[Option<DatumState>]) {
    let chain = chain.into_iter().map(|tx| vec![tx]).collect();
    roll_blocks(seeds, chain, expected).await;
}

/// Rolls `chain` forward from a domain configured with `seeds`, checking the
/// row and the lookup after each block against `expected`, then rolls it
/// back, checking each block's state is restored exactly, down to the row
/// bootstrap left.
async fn roll_blocks(
    seeds: &[Option<Hash<32>>],
    chain: Vec<Vec<Tx>>,
    expected: &[Option<DatumState>],
) {
    let domain = domain(seeds);
    let blocks = blocks(chain);
    assert_eq!(blocks.len(), expected.len());

    let bootstrapped = row(&domain);

    for (i, ((_, raw), expected)) in blocks.iter().zip(expected).enumerate() {
        domain.roll_forward(raw.clone()).unwrap();

        let readable = expected.as_ref().filter(|state| !state.bytes.is_empty());
        assert_eq!(
            lookup(&domain).await,
            readable.map(|state| state.bytes.clone()),
            "lookup after block {i}"
        );
        assert_eq!(&row(&domain), expected, "row after block {i}");
    }

    for i in (0..blocks.len() - 1).rev() {
        domain.rollback(&blocks[i].0).unwrap();
        assert_eq!(
            row(&domain),
            expected[i],
            "row after rolling back to block {i}"
        );
    }

    domain.rollback(&ChainPoint::Origin).unwrap();
    assert_eq!(
        row(&domain),
        bootstrapped,
        "row after rolling back to Origin"
    );
}

/// Output A carries the datum's hash and the datum; output B, in a later tx,
/// only the hash. Spending A leaves the datum readable while B is live, and
/// spending B then removes it.
#[tokio::test]
async fn spending_the_witnessed_output_keeps_the_datum_for_another() {
    let a = lock(&seed(0), true);
    let b = lock(&seed(1), false);
    let spend_a = spend(&produced(&a.0));
    let spend_b = spend(&produced(&b.0));

    roll(
        &PLAIN_SEEDS,
        vec![a, b, spend_a, spend_b],
        &[known(1), known(2), known(1), None],
    )
    .await;
}

/// The same with B first: before any tx witnesses the datum its hash is
/// counted but the datum is not readable; the witness of A fills it in, and
/// spending B leaves it readable while A is live.
#[tokio::test]
async fn spending_an_unwitnessed_output_keeps_the_datum_for_another() {
    let b = lock(&seed(1), false);
    let a = lock(&seed(0), true);
    let spend_b = spend(&produced(&b.0));
    let spend_a = spend(&produced(&a.0));

    roll(
        &PLAIN_SEEDS,
        vec![b, a, spend_b, spend_a],
        &[unknown(1), known(2), known(1), None],
    )
    .await;
}

/// A configured UTxO S carrying the datum's hash is counted at bootstrap, so
/// spending it leaves the datum readable for an output C that a later tx
/// locked and witnessed.
#[tokio::test]
async fn spending_a_configured_output_keeps_the_datum_for_another() {
    let seeds = [None, Some(datum_hash())];
    assert_eq!(row(&domain(&seeds)), unknown(1), "row after bootstrap");

    let c = lock(&seed(0), true);
    let spend_s = spend(&seed(1));
    let spend_c = spend(&produced(&c.0));

    roll(
        &seeds,
        vec![c, spend_s, spend_c],
        &[known(2), known(1), None],
    )
    .await;
}

/// One tx spends A, the only UTxO referencing the datum, and produces B
/// with the datum's hash but not the datum. Its increment comes before its
/// decrement, so the row keeps the datum's bytes while B is live.
#[tokio::test]
async fn relocking_the_only_output_in_one_tx_keeps_the_datum() {
    let a = lock(&seed(0), true);
    let relock_a = relock(&produced(&a.0));
    let spend_b = spend(&produced(&relock_a.0));

    roll(
        &PLAIN_SEEDS,
        vec![a, relock_a, spend_b],
        &[known(1), known(1), None],
    )
    .await;
}

/// Two configured UTxOs carry the datum's hash, so bootstrap counts both and
/// spending each in turn leaves the datum readable for C until C is spent.
#[tokio::test]
async fn every_configured_output_counts_toward_the_datum() {
    let seeds = [None, Some(datum_hash()), Some(datum_hash())];
    assert_eq!(row(&domain(&seeds)), unknown(2), "row after bootstrap");

    let c = lock(&seed(0), true);
    let spend_s1 = spend(&seed(1));
    let spend_s2 = spend(&seed(2));
    let spend_c = spend(&produced(&c.0));

    roll(
        &seeds,
        vec![c, spend_s1, spend_s2, spend_c],
        &[known(3), known(2), known(1), None],
    )
    .await;
}

/// One tx spending both UTxOs that reference the datum decrements once for
/// each, removing the row.
#[tokio::test]
async fn spending_two_outputs_in_one_tx_decrements_the_datum_for_each() {
    let a = lock(&seed(0), true);
    let b = lock(&seed(1), false);
    let spend_ab = spend_all(&[produced(&a.0), produced(&b.0)]);

    roll(
        &PLAIN_SEEDS,
        vec![a, b, spend_ab],
        &[known(1), known(2), None],
    )
    .await;
}

/// In one block, T1 spends A, the only UTxO referencing the datum, and a
/// later T2 locks another output with the hash but not the datum. T1's
/// decrement applies before T2's increment, as it would with each in its own
/// block: the row is removed, then counted again without the datum.
#[tokio::test]
async fn a_block_applies_its_txs_datum_deltas_in_order() {
    let a = lock(&seed(0), true);
    let spend_a = spend(&produced(&a.0));
    let b = lock(&seed(1), false);
    let spend_b = spend(&produced(&b.0));

    roll_blocks(
        &PLAIN_SEEDS,
        vec![vec![a], vec![spend_a, b], vec![spend_b]],
        &[known(1), unknown(1), None],
    )
    .await;
}
