use std::str::FromStr;

use pallas::{
    codec::minicbor,
    crypto::hash::Hash,
    ledger::{
        addresses::Address,
        primitives::{
            babbage::GenTransactionOutput,
            conway::{PostAlonzoTransactionOutput, Value},
        },
        traverse::Era,
    },
};

use crate::*;

#[derive(Clone)]
pub enum FakeAddress {
    Alice,
    Bob,
    Carol,
    Dave,
    Eve,
    Fred,
    George,
    Harry,
    Custom(String),
}

const HARDCODED_ADDRESS: &str = "addr_test1qruhen60uwzpwnnr7gjs50z2v8u9zyfw6zunet4k42zrpr54mrlv55f93rs6j48wt29w90hlxt4rvpvshe55k5r9mpvqjv2wt4";

impl FakeAddress {
    pub fn everyone() -> Vec<Self> {
        vec![
            FakeAddress::Alice,
            FakeAddress::Bob,
            FakeAddress::Carol,
            FakeAddress::Dave,
            FakeAddress::Eve,
            FakeAddress::Fred,
            FakeAddress::George,
            FakeAddress::Harry,
        ]
    }

    pub fn ordinal(&self) -> usize {
        match self {
            FakeAddress::Alice => 0,
            FakeAddress::Bob => 1,
            FakeAddress::Carol => 2,
            FakeAddress::Dave => 3,
            FakeAddress::Eve => 4,
            FakeAddress::Fred => 5,
            FakeAddress::George => 6,
            FakeAddress::Harry => 7,
            FakeAddress::Custom(_) => 8,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            FakeAddress::Alice => HARDCODED_ADDRESS,
            FakeAddress::Bob => "addr_test1wr4c5ruvn9ss5r4davqh8nf964c8t2hu7kl8cqmxt42hdwqhuqp46",
            FakeAddress::Carol => {
                "addr_test1qq969yp0wz6qw9kcfh3ansqamsvm29x337dkjyf3nfefqrtwu22xqq55v3vnm4fu69p4qf0zu4s57c97qcgyc495wt4smkcg42"
            }
            FakeAddress::Dave => HARDCODED_ADDRESS,
            FakeAddress::Eve => HARDCODED_ADDRESS,
            FakeAddress::Fred => HARDCODED_ADDRESS,
            FakeAddress::George => HARDCODED_ADDRESS,
            FakeAddress::Harry => HARDCODED_ADDRESS,
            FakeAddress::Custom(addr) => addr,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        Address::from_str(self.as_str()).unwrap().to_vec()
    }
}

impl Into<Vec<u8>> for FakeAddress {
    fn into(self) -> Vec<u8> {
        self.to_bytes()
    }
}

impl From<&str> for FakeAddress {
    fn from(value: &str) -> Self {
        FakeAddress::Custom(value.to_owned())
    }
}

impl From<String> for FakeAddress {
    fn from(value: String) -> Self {
        FakeAddress::Custom(value)
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

pub fn fake_utxo(
    tx_hash: Hash<32>,
    txo_idx: u32,
    address: impl Into<FakeAddress>,
    amount: u64,
) -> (TxoRef, EraCbor) {
    let txoref = TxoRef(tx_hash, txo_idx);

    let output = pallas::ledger::primitives::conway::TransactionOutput::PostAlonzo(
        PostAlonzoTransactionOutput {
            address: address.into().to_bytes().into(),
            value: pallas::ledger::primitives::conway::Value::Coin(amount),
            datum_option: None,
            script_ref: None,
        }
        .into(),
    );

    (
        txoref,
        EraCbor(
            pallas::ledger::traverse::Era::Conway.into(),
            pallas::codec::minicbor::to_vec(&output).unwrap(),
        ),
    )
}

pub fn fake_genesis_utxo(
    address: impl Into<FakeAddress>,
    ordinal: usize,
    amount: u64,
) -> (TxoRef, EraCbor) {
    fake_utxo(genesis_tx_hash(), ordinal as u32, address, amount)
}

pub fn replace_utxo_address(utxo: EraCbor, new_address: FakeAddress) -> EraCbor {
    let EraCbor(_, cbor) = utxo;

    let output = MultiEraOutput::decode(Era::Conway, &cbor).unwrap();

    let Some(GenTransactionOutput::PostAlonzo(mut output)) = output.as_conway().cloned() else {
        unreachable!()
    };

    output.address = new_address.to_bytes().into();

    EraCbor(Era::Conway.into(), minicbor::to_vec(&output).unwrap())
}

pub fn replace_utxo_map_address(utxos: UtxoMap, new_address: FakeAddress) -> UtxoMap {
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

pub fn assert_utxo_address_and_value(utxo: &EraCbor, address: impl Into<Vec<u8>>, value: u64) {
    let EraCbor(_, cbor) = utxo;
    let output = MultiEraOutput::decode(Era::Conway, &cbor).unwrap();

    let Some(GenTransactionOutput::PostAlonzo(output)) = output.as_conway() else {
        unreachable!()
    };

    assert_eq!(output.address.as_slice(), address.into());
    assert_eq!(output.value, Value::Coin(value));
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
    let EraCbor(_, cbor) = utxo;
    let output = MultiEraOutput::decode(Era::Conway, &cbor).unwrap();

    let Some(GenTransactionOutput::PostAlonzo(output)) = output.as_conway() else {
        unreachable!()
    };

    let bech32 = Address::from_bytes(&output.address)
        .unwrap()
        .to_bech32()
        .unwrap();

    let value = match output.value {
        Value::Coin(value) => value,
        _ => unreachable!(),
    };

    println!("{}#{} -> {} = {}", txoref.0, txoref.1, bech32, value);
}

pub fn print_utxo_map(utxos: &UtxoMap) {
    for (txoref, utxo) in utxos {
        print_utxo(txoref, utxo);
    }
}

pub fn fake_genesis_delta(initial_amount: u64) -> LedgerDelta {
    LedgerDelta {
        new_position: Some(ChainPoint::Origin),
        produced_utxo: FakeAddress::everyone()
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
    to: FakeAddress,
) -> LedgerDelta {
    let moved = utxos.clone();
    let moved = replace_utxo_map_address(moved, to);
    let moved = replace_utxo_map_txhash(moved, tx_seq);

    let mut delta = forward_delta_from_slot(slot);
    delta.consumed_utxo = utxos;
    delta.produced_utxo = moved;
    delta
}
