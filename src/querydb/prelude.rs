use pallas;
use redb::{MultimapTableDefinition, TableDefinition};
// use std::error::Error;

// Given a block, table "block" maps its slot to its CBOR representation
pub type BlockKeyType<'a> = u64;
pub type BlockValueType<'a> = &'a [u8];
pub type BlockResultType = Vec<u8>;
pub const BLOCK_TABLE: TableDefinition<BlockKeyType, BlockValueType> =
    TableDefinition::new("block");

// Given a block, table "block_by_hash" maps its hash to its slot.
pub type BlockByHashKeyType<'a> = &'a [u8; 32];
pub type BlockByHashValueType<'a> = u64;
pub const BLOCK_BY_HASH_TABLE: TableDefinition<BlockByHashKeyType, BlockByHashValueType> =
    TableDefinition::new("block_by_hash");

// Given a transaction, table "tx" maps its hash to an encoding representation
// of it
// NOTE: transactions don't have a precise CBOR representation, so we use
// a library encoded representation instead
pub type TxKeyType<'a> = &'a [u8; 32];
pub type TxValueType<'a> = &'a [u8];
pub type TxResultType = Vec<u8>;
pub const TX_TABLE: TableDefinition<TxKeyType, TxValueType> = TableDefinition::new("tx");

// Given a UTxO, table "utxo" maps its output reference (a pair composed of the
// hash of the transaction that produced the UTxO and the index in the list of
// transaction outputs corresponding to it) to the result of encoding said UTxO
// NOTE: Just like transactions, UTxO's don't have a precise CBOR
// representation.
pub type UTxOKeyType<'a> = (&'a [u8], u8);
pub type UTxOValueType<'a> = &'a [u8];
pub type UTxOResultType = Vec<u8>;
pub const UTXO_TABLE: TableDefinition<UTxOKeyType, UTxOValueType> = TableDefinition::new("utxo");

// Given an address, table "utxo_by_addr" maps it to a list of pairs of a tx
// hash and an (output) index (each one representing a UTxO sitting at that
// address)
pub type UTxOByAddrKeyType<'a> = &'a [u8];
pub type UTxOByAddrValueType<'a> = (&'a [u8], u8);
pub type UTxOByAddrResultType = (Vec<u8>, u8);
pub const UTXO_BY_ADDR_TABLE: MultimapTableDefinition<UTxOByAddrKeyType, UTxOByAddrValueType> =
    MultimapTableDefinition::new("utxo_by_addr");

// Given a minting policy, table "utxo_by_beacon" maps it to a list of pairs of
// a tx hash and an (output) index (each one representing a UTxO containing a
// token of that policy)
pub type UTxOByBeaconKeyType<'a> = &'a [u8; 28];
pub type UTxOByBeaconValueType<'a> = (&'a [u8], u8);
pub type UTxOByBeaconResultType = (Vec<u8>, u8);
pub const UTXO_BY_BEACON_TABLE: MultimapTableDefinition<
    UTxOByBeaconKeyType,
    UTxOByBeaconValueType,
> = MultimapTableDefinition::new("utxo_by_beacon");

// Table "prot_params" stores only the latest protocol parameters.
pub type ProtParamsKeyType = ();
pub type ProtParamsValueType<'a> = &'a [u8];
pub type ProtParamsResultType = Vec<u8>;
pub const PROT_PARAMS_TABLE: TableDefinition<ProtParamsKeyType, ProtParamsValueType> =
    TableDefinition::new("prot_params");

pub enum Error {
    AddressDecoding(pallas::ledger::addresses::Error),
    BlockDecoding(pallas::ledger::traverse::Error),
    KeyNotFound,
    OutputDecoding(pallas::codec::minicbor::decode::Error),
    UTxOTableInvariantBroken,
    ReDBError(Box<dyn std::error::Error>),
}
