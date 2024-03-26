use pallas;
use redb::{MultimapTableDefinition, StorageError, TableDefinition, TableError, TransactionError};

// Given a block, table "block" maps its hash to its CBOR representation
pub type BlockKeyType<'a> = &'a [u8; 32];
pub type BlockValueType<'a> = &'a [u8];
pub const BLOCK_TABLE: TableDefinition<BlockKeyType, BlockValueType> =
    TableDefinition::new("block");
// "chain_tip" stores the hash of the last applied block
pub type ChainTipKeyType = u64;
pub type ChainTipValueType<'a> = &'a [u8; 32];
pub type ChainTipResultType = Vec<u8>;
pub const CHAIN_TIP_TABLE: TableDefinition<ChainTipKeyType, ChainTipValueType> =
    TableDefinition::new("chain_tip");
// Given a transaction, table "tx" maps its hash to an encoding representation
// of it
// NOTE: transactions don't have a precise CBOR representation, so we use
// a library encoded representation instead
pub type TxTableKeyType<'a> = &'a [u8; 32];
pub type TxTableValueType<'a> = &'a [u8];
pub const TX_TABLE: TableDefinition<TxTableKeyType, TxTableValueType> = TableDefinition::new("tx");
// Given a UTxO, table "utxo" maps its output reference (a pair composed of the
// hash of the transaction that produced the UTxO and the index in the list of
// transaction outputs corresponding to it) to the result of encoding said UTxO
// NOTE: Just like transactions, UTxO's don't have a precise CBOR
// representation.
pub type UTxOKeyType<'a> = (&'a [u8], u8);
pub type UTxOValueType<'a> = &'a [u8];
pub type UTxOResultType<'a> = Vec<u8>;
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
pub const UTXO_BY_BEACON_TABLE: MultimapTableDefinition<
    UTxOByBeaconKeyType,
    UTxOByBeaconValueType,
> = MultimapTableDefinition::new("utxo_by_beacon");

pub enum StoreError {
    AddressDecoding(pallas::ledger::addresses::Error),
    BlockDecoding(pallas::ledger::traverse::Error),
    ReDBError(ReDBError),
}

pub enum ReDBError {
    CommitError(redb::CommitError),
    DatabaseInitilization(redb::DatabaseError),
    InsertError(redb::StorageError),
    TableError(redb::TableError),
    TransactionError(redb::TransactionError),
}

pub enum ReadError {
    ChainTipNotFound,
    TransactionError(TransactionError),
    TableError(TableError),
    StorageError(StorageError),
}
