use std::{collections::BTreeMap, ops::Range, str::FromStr, time::Duration};

use pallas::{
    codec::minicbor,
    crypto::hash::Hash,
    ledger::{
        addresses::Address,
        primitives::{
            babbage::GenTransactionOutput,
            conway::{PostAlonzoTransactionOutput, Value},
        },
        traverse::{Era, MultiEraOutput},
    },
};

use rand::Rng;

use dolos_core::*;

pub mod blocks;

#[cfg(feature = "toy-domain")]
pub mod toy_domain;

pub trait UtxoGenerator {
    fn generate(&self, address: &TestAddress) -> EraCbor;
}

impl<F> UtxoGenerator for F
where
    F: Fn(&TestAddress) -> EraCbor,
{
    fn generate(&self, address: &TestAddress) -> EraCbor {
        self(address)
    }
}

#[derive(Clone)]
pub enum TestAddress {
    Alice,
    Bob,
    Carol,
    Dave,
    Eve,
    // Fred,
    // George,
    // Harry,
    Custom(String),
}

pub const ADDRESS_TEST_VECTORS: [&str; 5] = [
    // a Shelley address with both payment and stake parts
    "addr1q9dhugez3ka82k2kgh7r2lg0j7aztr8uell46kydfwu3vk6n8w2cdu8mn2ha278q6q25a9rc6gmpfeekavuargcd32vsvxhl7e",
    // a Shelley address with only payment part
    "addr1vx2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzers66hrl8",
    // a Shelley stake address
    "stake178phkx6acpnf78fuvxn0mkew3l0fd058hzquvz7w36x4gtcccycj5",
    // a Shelley script address
    "addr1w9jx45flh83z6wuqypyash54mszwmdj8r64fydafxtfc6jgrw4rm3",
    // a Byron address
    "37btjrVyb4KDXBNC4haBVPCrro8AQPHwvCMp3RFhhSVWwfFmZ6wwzSK6JK1hY6wHNmtrpTf1kdbva8TCneM2YsiXT7mrzT21EacHnPpz5YyUdj64na",
];

impl TestAddress {
    pub fn everyone() -> Vec<Self> {
        vec![
            TestAddress::Alice,
            TestAddress::Bob,
            TestAddress::Carol,
            TestAddress::Dave,
            TestAddress::Eve,
            // TestAddress::Fred,
            // TestAddress::George,
            // TestAddress::Harry,
        ]
    }

    pub fn ordinal(&self) -> usize {
        match self {
            TestAddress::Alice => 0,
            TestAddress::Bob => 1,
            TestAddress::Carol => 2,
            TestAddress::Dave => 3,
            TestAddress::Eve => 4,
            // TestAddress::Fred => 5,
            // TestAddress::George => 6,
            // TestAddress::Harry => 7,
            TestAddress::Custom(_) => 8,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            TestAddress::Custom(addr) => addr,
            x => ADDRESS_TEST_VECTORS[x.ordinal()],
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        Address::from_str(self.as_str()).unwrap().to_vec()
    }
}

impl From<TestAddress> for Vec<u8> {
    fn from(addr: TestAddress) -> Self {
        addr.to_bytes()
    }
}

impl From<&str> for TestAddress {
    fn from(value: &str) -> Self {
        TestAddress::Custom(value.to_owned())
    }
}

impl From<String> for TestAddress {
    fn from(value: String) -> Self {
        TestAddress::Custom(value)
    }
}

impl From<&TestAddress> for TestAddress {
    fn from(value: &TestAddress) -> Self {
        value.clone()
    }
}

pub enum TestAsset {
    Hosky,
    Snek,
    NikePig,
    Custom(&'static str, &'static str),
}

impl TestAsset {
    pub fn policy_hex(&self) -> &str {
        match self {
            TestAsset::Hosky => "a0028f350aaabe0545fdcb56b039bfb08e4bb4d8c4d7c3c7d481c235",
            TestAsset::Snek => "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f",
            TestAsset::NikePig => "c881c20e49dbaca3ff6cef365969354150983230c39520b917f5cf7c",
            TestAsset::Custom(policy, _) => policy,
        }
    }

    pub fn ticker(&self) -> &str {
        match self {
            TestAsset::Hosky => "HOSKY",
            TestAsset::Snek => "SNEK",
            TestAsset::NikePig => "NIKEPIG",
            TestAsset::Custom(_, name) => name,
        }
    }

    pub fn name(&self) -> Option<&[u8]> {
        Some(self.ticker().as_bytes())
    }

    pub fn policy(&self) -> Hash<28> {
        Hash::from_str(self.policy_hex()).unwrap()
    }
}

pub fn genesis_tx_hash() -> Hash<32> {
    Hash::from_str("0000000000000000000000000000000000000000000000000000000000000000").unwrap()
}

pub fn slot_to_hash(slot: u64) -> BlockHash {
    let mut hasher = pallas::crypto::hash::Hasher::<256>::new();
    hasher.input(&(slot as i32).to_le_bytes());
    hasher.finalize()
}

pub fn slot_to_chainpoint(slot: u64) -> ChainPoint {
    ChainPoint::Specific(slot, slot_to_hash(slot))
}

pub fn tx_sequence_to_hash(sequence: u64) -> TxHash {
    let mut hasher = pallas::crypto::hash::Hasher::<256>::new();
    hasher.input(&sequence.to_le_bytes());
    hasher.finalize()
}

pub fn fake_genesis_utxo(
    address: impl Into<TestAddress>,
    ordinal: usize,
    amount: u64,
) -> (TxoRef, EraCbor) {
    let tx_hash = genesis_tx_hash();
    let txoref = TxoRef(tx_hash, ordinal as u32);
    (txoref, utxo_with_value(address, Value::Coin(amount)))
}

pub fn replace_utxo_address(utxo: EraCbor, new_address: TestAddress) -> EraCbor {
    let EraCbor(_, cbor) = utxo;

    let output = MultiEraOutput::decode(Era::Conway, &cbor).unwrap();

    let Some(GenTransactionOutput::PostAlonzo(mut output)) = output.as_conway().cloned() else {
        unreachable!()
    };

    output.address = new_address.to_bytes().into();

    EraCbor(Era::Conway.into(), minicbor::to_vec(&output).unwrap())
}

pub fn replace_utxo_map_address(utxos: UtxoMap, new_address: TestAddress) -> UtxoMap {
    utxos
        .into_iter()
        .map(|(k, v)| (k, replace_utxo_address(v, new_address.clone())))
        .collect()
}

pub fn replace_utxo_map_txhash(utxos: UtxoMap, tx_sequence: u64) -> UtxoMap {
    let new_txhash = tx_sequence_to_hash(tx_sequence);

    utxos
        .into_iter()
        .map(|(k, v)| (TxoRef(new_txhash, k.1), v))
        .collect()
}

pub fn get_utxo_address_and_value(utxo: &EraCbor) -> (Vec<u8>, u64) {
    let EraCbor(_, cbor) = utxo;

    let output = MultiEraOutput::decode(Era::Conway, cbor).unwrap();

    let Some(GenTransactionOutput::PostAlonzo(output)) = output.as_conway() else {
        unreachable!()
    };

    (
        output.address.as_slice().to_vec(),
        match output.value {
            Value::Coin(value) => value,
            _ => unreachable!(),
        },
    )
}

pub fn assert_utxo_address_and_value(utxo: &EraCbor, address: impl Into<Vec<u8>>, value: u64) {
    let (output_address, output_value) = get_utxo_address_and_value(utxo);

    assert_eq!(output_address, address.into());
    assert_eq!(output_value, value);
}

pub fn assert_utxo_map_address_and_value<A>(utxos: &UtxoMap, address: A, value: u64)
where
    A: Into<Vec<u8>> + Clone,
{
    for utxo in utxos.values() {
        assert_utxo_address_and_value(utxo, address.clone(), value);
    }
}

pub fn print_utxo(txoref: &TxoRef, utxo: &EraCbor) {
    let (output_address, output_value) = get_utxo_address_and_value(utxo);

    let bech32 = Address::from_bytes(&output_address).unwrap().to_string();

    println!("{}#{} -> {} = {}", txoref.0, txoref.1, bech32, output_value);
}

pub fn print_utxo_map(utxos: &UtxoMap) {
    for (txoref, utxo) in utxos {
        print_utxo(txoref, utxo);
    }
}

pub fn fake_genesis_delta(initial_amount: u64) -> LedgerDelta {
    LedgerDelta {
        new_position: Some(ChainPoint::Origin),
        produced_utxo: TestAddress::everyone()
            .into_iter()
            .enumerate()
            .map(|(ordinal, addr)| fake_genesis_utxo(addr, ordinal, initial_amount))
            .collect(),
        ..Default::default()
    }
}

pub fn forward_delta_from_slot(slot: u64) -> LedgerDelta {
    LedgerDelta {
        new_position: Some(slot_to_chainpoint(slot)),
        ..Default::default()
    }
}

pub fn undo_delta_from_slot(slot: u64) -> LedgerDelta {
    LedgerDelta {
        undone_position: Some(slot_to_chainpoint(slot)),
        ..Default::default()
    }
}

pub fn revert_delta(delta: LedgerDelta) -> LedgerDelta {
    LedgerDelta {
        undone_position: delta.new_position,
        recovered_stxi: delta.consumed_utxo,
        undone_utxo: delta.produced_utxo,
        ..Default::default()
    }
}

pub fn make_move_utxo_delta(
    utxos: UtxoMap,
    slot: u64,
    tx_seq: u64,
    to: TestAddress,
) -> LedgerDelta {
    let moved = utxos.clone();
    let moved = replace_utxo_map_address(moved, to);
    let moved = replace_utxo_map_txhash(moved, tx_seq);

    let mut delta = forward_delta_from_slot(slot);
    delta.consumed_utxo = utxos;
    delta.produced_utxo = moved;
    delta
}

pub fn make_custom_utxo_delta<G>(
    slot: u64,
    addresses: impl IntoIterator<Item = TestAddress>,
    utxos_per_address: Range<u64>,
    utxo_generator: G,
) -> LedgerDelta
where
    G: UtxoGenerator,
{
    let addresses = addresses.into_iter().collect::<Vec<_>>();

    let mut utxos = UtxoMap::new();

    for (tx, address) in addresses.iter().enumerate() {
        let utxo_count = rand::rng().random_range(utxos_per_address.clone());

        for ordinal in 0..utxo_count {
            let tx = tx_sequence_to_hash(tx as u64);

            let key = TxoRef(tx, ordinal as u32);
            let cbor = utxo_generator.generate(address);

            utxos.insert(key, cbor);
        }
    }

    let mut delta = forward_delta_from_slot(slot);
    delta.produced_utxo = utxos;

    delta
}

pub fn utxo_with_value(address: impl Into<TestAddress>, value: Value) -> EraCbor {
    let output = pallas::ledger::primitives::conway::TransactionOutput::PostAlonzo(
        PostAlonzoTransactionOutput {
            address: address.into().to_bytes().into(),
            value,
            datum_option: None,
            script_ref: None,
        }
        .into(),
    );

    EraCbor(
        pallas::ledger::traverse::Era::Conway.into(),
        pallas::codec::minicbor::to_vec(&output).unwrap(),
    )
}

pub fn utxo_with_random_amount(address: impl Into<TestAddress>, amount: Range<u64>) -> EraCbor {
    let amount = rand::rng().random_range(amount);

    utxo_with_value(address, Value::Coin(amount))
}

pub const MIN_UTXO_AMOUNT: u64 = 1_111_111;

pub fn utxo_with_random_asset(
    address: impl Into<TestAddress>,
    asset: impl Into<TestAsset>,
    asset_amount: Range<u64>,
) -> EraCbor {
    let rnd_amount = rand::rng().random_range(asset_amount);

    let asset: TestAsset = asset.into();

    let multi_assets = BTreeMap::from_iter(vec![(
        asset.policy(),
        BTreeMap::from_iter(vec![(
            asset.name().unwrap().to_vec().into(),
            pallas::ledger::primitives::conway::PositiveCoin::try_from(rnd_amount).unwrap(),
        )]),
    )]);

    let value =
        pallas::ledger::primitives::conway::Value::Multiasset(MIN_UTXO_AMOUNT, multi_assets);

    utxo_with_value(address, value)
}

#[derive(Clone, Default)]
/// Cancel token that cancels after a set amount of time.
pub struct ToyCancelToken {
    duration: Duration,
}

impl ToyCancelToken {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

impl CancelToken for ToyCancelToken {
    async fn cancelled(&self) {
        tokio::time::sleep(self.duration).await;
    }
}
