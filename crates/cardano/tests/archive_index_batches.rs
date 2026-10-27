//! Archive index records are a function of the chain, not of how its blocks
//! were split into work batches.
//!
//! A phase-2-invalid transaction consumes its collateral, not its ordinary
//! inputs, so dependency loading never fetches those inputs from state. An
//! indexer that walked the ordinary inputs tagged them only when the batch
//! happened to hold them anyway — produced earlier in the same batch (preprod
//! epoch 259) or loaded for a valid consumer later in it (preview epoch 1165).
//! The chain below carries both shapes at distances that fit one 500-block
//! batch but never one 100-block batch, and every partition has to write the
//! same records.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use dolos_cardano::{
    indexes::{archive_dimensions, CardanoArchiveIndexExt as _},
    CardanoLogic,
};
use dolos_core::{
    config::CustomUtxo, import::ImportExt as _, ArchiveIndexDelta, ArchiveStore as _, BlockSlot,
    ChainLogic as _, ChainPoint, Domain as _, EraCbor, ExactRecord, RawBlock, Tag, TagRecord,
    TxoRef, UtxoValue,
};
use dolos_testing::{blocks::make_conway_block_with_txs_at, toy_domain::ToyDomain};
use pallas::{
    codec::utils::{KeepRaw, NonEmptySet, Set},
    crypto::hash::Hash,
    ledger::{
        addresses::{Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart},
        primitives::conway::{
            DatumOption, PostAlonzoTransactionOutput, TransactionBody, TransactionInput,
            TransactionOutput, Value,
        },
        traverse::{ComputeHash as _, Era, MultiEraBlock},
    },
};

const ADA: u64 = 1_000_000;

/// Blocks in the chain, one slot apart, all inside preview's first epoch.
const BLOCKS: usize = 600;

/// Shape one: an output produced before the invalid tx that names it.
const PRODUCER: usize = 10;
const INVALID_AFTER_PRODUCER: usize = PRODUCER + 415;

/// Shape two: an input an invalid tx names, validly consumed later.
const INVALID_BEFORE_SPENDER: usize = 300;
const SPENDER: usize = INVALID_BEFORE_SPENDER + 150;

const SEED: [u8; 32] = [0xee; 32];
const DATUM: [u8; 32] = [0xda; 32];

fn seed(index: u32) -> TxoRef {
    TxoRef(SEED.into(), index)
}

fn enterprise(payment: u8) -> Vec<u8> {
    ShelleyAddress::new(
        Network::Testnet,
        ShelleyPaymentPart::Key([payment; 28].into()),
        ShelleyDelegationPart::Null,
    )
    .to_vec()
}

fn faucet() -> Vec<u8> {
    enterprise(0x0f)
}

/// Where the producer pays the output the first invalid tx names.
fn produced_address() -> Vec<u8> {
    enterprise(0x01)
}

/// Where the seed the second invalid tx names sits.
fn named_address() -> Vec<u8> {
    enterprise(0x02)
}

fn collateral_address(shape: u8) -> Vec<u8> {
    enterprise(0x10 + shape)
}

fn output(address: Vec<u8>, coin: u64, datum: Option<Hash<32>>) -> TransactionOutput<'static> {
    TransactionOutput::PostAlonzo(KeepRaw::from(PostAlonzoTransactionOutput {
        address: address.into(),
        value: Value::Coin(coin),
        datum_option: datum.map(|hash| KeepRaw::from(DatumOption::Hash(hash))),
        script_ref: None,
    }))
}

fn input(txo: &TxoRef) -> TransactionInput {
    TransactionInput {
        transaction_id: txo.0,
        index: txo.1 as u64,
    }
}

fn body(
    inputs: &[TxoRef],
    outputs: Vec<TransactionOutput<'static>>,
    collateral: Option<TxoRef>,
) -> TransactionBody<'static> {
    TransactionBody {
        inputs: Set::from(inputs.iter().map(input).collect::<Vec<_>>()),
        outputs,
        fee: 200_000,
        ttl: None,
        certificates: None,
        withdrawals: None,
        auxiliary_data_hash: None,
        validity_interval_start: None,
        mint: None,
        script_data_hash: None,
        collateral: collateral
            .as_ref()
            .map(|x| NonEmptySet::try_from(vec![input(x)]).unwrap()),
        required_signers: None,
        network_id: None,
        collateral_return: collateral.map(|_| output(faucet(), ADA, None)),
        total_collateral: None,
        reference_inputs: None,
        voting_procedures: None,
        proposal_procedures: None,
        treasury_value: None,
        donation: None,
    }
}

/// The seeds: the producer's funding, the input the second invalid tx names,
/// and one collateral per invalid tx.
struct Seeds;

impl Seeds {
    const FUNDING: u32 = 0;
    const NAMED: u32 = 1;
    const COLLATERAL_ONE: u32 = 2;
    const COLLATERAL_TWO: u32 = 3;

    fn all() -> Vec<(TxoRef, Vec<u8>)> {
        vec![
            (seed(Self::FUNDING), faucet()),
            (seed(Self::NAMED), named_address()),
            (seed(Self::COLLATERAL_ONE), collateral_address(1)),
            (seed(Self::COLLATERAL_TWO), collateral_address(2)),
        ]
    }
}

struct Chain {
    blocks: Vec<(BlockSlot, RawBlock)>,
    /// The output the first invalid tx names but never consumes.
    produced: TxoRef,
}

fn slot(block: usize) -> BlockSlot {
    10 + block as u64
}

fn chain() -> Chain {
    let producer = body(
        &[seed(Seeds::FUNDING)],
        vec![output(produced_address(), 10 * ADA, Some(DATUM.into()))],
        None,
    );
    let produced = TxoRef(producer.compute_hash(), 0);

    let mut txs: BTreeMap<usize, Vec<(TransactionBody<'static>, bool)>> = BTreeMap::new();

    txs.insert(PRODUCER, vec![(producer, true)]);

    txs.insert(
        INVALID_AFTER_PRODUCER,
        vec![(
            body(
                std::slice::from_ref(&produced),
                vec![output(faucet(), 9 * ADA, None)],
                Some(seed(Seeds::COLLATERAL_ONE)),
            ),
            false,
        )],
    );

    txs.insert(
        INVALID_BEFORE_SPENDER,
        vec![(
            body(
                &[seed(Seeds::NAMED)],
                vec![output(faucet(), 4 * ADA, None)],
                Some(seed(Seeds::COLLATERAL_TWO)),
            ),
            false,
        )],
    );

    txs.insert(
        SPENDER,
        vec![(
            body(
                &[seed(Seeds::NAMED)],
                vec![output(faucet(), 4 * ADA, None)],
                None,
            ),
            true,
        )],
    );

    let mut prev = None;
    let mut blocks = Vec::with_capacity(BLOCKS);

    for i in 0..BLOCKS {
        let (point, raw) = make_conway_block_with_txs_at(
            slot(i),
            i as u64,
            prev,
            txs.remove(&i).unwrap_or_default(),
            None,
        );
        prev = point.hash();
        blocks.push((slot(i), raw));
    }

    Chain { blocks, produced }
}

fn config() -> dolos_cardano::CardanoLogicConfig {
    let mut config = dolos_cardano::CardanoLogicConfig::default();

    for (txo, address) in Seeds::all() {
        let cbor = pallas::codec::minicbor::to_vec(output(address, 5 * ADA, None)).unwrap();

        config.custom_utxos.push(CustomUtxo {
            ref_: txo,
            era: Some(Era::Conway.into()),
            cbor,
        });
    }

    config
}

/// A node that imported the chain `partition` blocks at a time, each
/// `import_blocks` call one work batch.
fn import(chain: &Chain, partition: usize) -> ToyDomain {
    let domain = ToyDomain::new_with_genesis_and_config(
        Arc::new(dolos_cardano::include::preview::load()),
        config(),
        None,
        None,
    );

    for blocks in chain.blocks.chunks(partition) {
        domain
            .import_blocks(blocks.iter().map(|(_, raw)| raw.clone()).collect())
            .unwrap();
    }

    domain
}

fn tags_of(domain: &ToyDomain) -> Vec<TagRecord> {
    let mut found: Vec<TagRecord> = domain
        .archive()
        .iter_archive_tags(&archive_dimensions::ALL, 0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    found.sort();
    found
}

fn exact_of(domain: &ToyDomain) -> Vec<ExactRecord> {
    let mut found: Vec<ExactRecord> = domain
        .archive()
        .iter_exact_records(0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    found.sort();
    found
}

fn address_slots(domain: &ToyDomain, address: &[u8]) -> Vec<BlockSlot> {
    domain
        .archive()
        .slots_by_address(address, 0, u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn spent_slots(domain: &ToyDomain, txo: &TxoRef) -> Vec<BlockSlot> {
    let key: Vec<u8> = txo.clone().into();

    domain
        .archive()
        .slots_by_spent_txo(&key, 0, u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn datum_slots(domain: &ToyDomain) -> Vec<BlockSlot> {
    domain
        .archive()
        .slots_by_datum(&DATUM, 0, u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// The 100- and 500-block partitions, plus the extremes — every block its own
/// batch, as after a restore at each block, and the whole chain in one.
#[test]
fn every_import_partition_writes_the_same_index_records() {
    let chain = chain();
    let reference = import(&chain, 1);

    let tags = tags_of(&reference);
    let exact = exact_of(&reference);
    assert!(!tags.is_empty() && !exact.is_empty());

    for partition in [100, 500, BLOCKS] {
        let domain = import(&chain, partition);

        assert_eq!(tags_of(&domain), tags, "tags, {partition}-block partition");
        assert_eq!(
            exact_of(&domain),
            exact,
            "exact, {partition}-block partition"
        );
    }
}

/// Shape one: the 500-block batch holds the producer, so the invalid tx's
/// ordinary input sits in memory, and still contributes nothing.
#[test]
fn an_input_produced_before_an_invalid_tx_is_not_tagged_by_it() {
    let chain = chain();
    let domain = import(&chain, 500);
    let invalid = slot(INVALID_AFTER_PRODUCER);

    assert_eq!(
        address_slots(&domain, &produced_address()),
        vec![slot(PRODUCER)]
    );
    assert_eq!(datum_slots(&domain), vec![slot(PRODUCER)]);
    assert!(spent_slots(&domain, &chain.produced).is_empty());

    // it is tagged by what it consumes, its collateral
    assert_eq!(
        address_slots(&domain, &collateral_address(1)),
        vec![invalid]
    );
    assert_eq!(
        spent_slots(&domain, &seed(Seeds::COLLATERAL_ONE)),
        vec![invalid]
    );
}

/// Shape two: the 500-block batch loads the named input for the valid spender
/// after the invalid tx; only the spender is tagged by it.
#[test]
fn an_input_consumed_after_an_invalid_tx_is_tagged_by_its_spender_only() {
    let chain = chain();
    let domain = import(&chain, 500);

    assert_eq!(
        address_slots(&domain, &named_address()),
        vec![slot(SPENDER)]
    );
    assert_eq!(
        spent_slots(&domain, &seed(Seeds::NAMED)),
        vec![slot(SPENDER)]
    );
    assert_eq!(
        spent_slots(&domain, &seed(Seeds::COLLATERAL_TWO)),
        vec![slot(INVALID_BEFORE_SPENDER)]
    );
}

fn era_cbor(output: TransactionOutput<'static>) -> Arc<UtxoValue> {
    let cbor = pallas::codec::minicbor::to_vec(output).unwrap();
    Arc::new(UtxoValue::new(EraCbor(Era::Conway.into(), cbor), None))
}

fn tags(deltas: &[ArchiveIndexDelta]) -> Vec<(BlockSlot, Vec<Tag>)> {
    deltas.iter().map(|x| (x.slot, x.tags.clone())).collect()
}

/// WAL replay rebuilds a block's entries from the inputs its entry carries,
/// which are the whole batch's: the catch-up after a restart (and the restored
/// node's own WAL) and the rollback have to write what the narrowest batch
/// wrote.
#[test]
fn wal_replay_ignores_inputs_the_block_does_not_consume() {
    let chain = chain();
    let (block_slot, raw) = &chain.blocks[INVALID_AFTER_PRODUCER];
    let point = ChainPoint::Specific(*block_slot, MultiEraBlock::decode(raw).unwrap().hash());

    let narrow: HashMap<TxoRef, Arc<UtxoValue>> = HashMap::from([(
        seed(Seeds::COLLATERAL_ONE),
        era_cbor(output(collateral_address(1), 5 * ADA, None)),
    )]);

    let mut wide = narrow.clone();
    wide.insert(
        chain.produced.clone(),
        era_cbor(output(produced_address(), 10 * ADA, Some(DATUM.into()))),
    );

    let catchup = |inputs| CardanoLogic::compute_catchup(raw, inputs, point.clone()).unwrap();
    let undo = |inputs| CardanoLogic::compute_undo(raw, inputs, point.clone()).unwrap();

    let expected = tags(&catchup(&narrow).archive_index_deltas);
    assert!(!expected[0].1.is_empty());

    assert_eq!(tags(&catchup(&wide).archive_index_deltas), expected);
    assert_eq!(tags(&undo(&narrow).archive_index_deltas), expected);
    assert_eq!(tags(&undo(&wide).archive_index_deltas), expected);
}
