use ::redb::{ReadTransaction, TableDefinition, WriteTransaction};
use redb_extras::roaring::{RoaringValue, RoaringValueReadOnlyTable, RoaringValueTable};
use std::collections::HashMap;
use xxhash_rust::xxh3::xxh3_64;

use dolos_core::{BlockSlot, ChainPoint, SlotTags};

type Error = super::RedbArchiveError;

pub struct SlotKeyIterator {
    iter: Box<dyn Iterator<Item = u64>>,
}

impl SlotKeyIterator {
    pub fn new<I>(iter: I) -> Self
    where
        I: Iterator<Item = u64> + 'static,
    {
        Self {
            iter: Box::new(iter),
        }
    }
}

impl Iterator for SlotKeyIterator {
    type Item = Result<u64, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(Ok)
    }
}

impl DoubleEndedIterator for SlotKeyIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        // Note: roaring bitmap iterators may not support reverse iteration
        // This is a limitation we accept for the performance benefits
        None
    }
}

pub struct AddressApproxIndexTable;

impl AddressApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, RoaringValue> = TableDefinition::new("byaddress");

    pub fn compute_key(address: &Vec<u8>) -> u64 {
        xxh3_64(address.as_slice())
    }

    pub fn iter_by_address(rx: &ReadTransaction, address: &[u8]) -> Result<SlotKeyIterator, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&address.to_vec());
        let bitmap = table.get_bitmap(key)?;
        let values: Vec<u64> = bitmap.iter().collect();
        Ok(SlotKeyIterator::new(values.into_iter()))
    }
}

pub struct AddressPaymentPartApproxIndexTable;

impl AddressPaymentPartApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, RoaringValue> = TableDefinition::new("bypayment");

    pub fn compute_key(address: &Vec<u8>) -> u64 {
        xxh3_64(address.as_slice())
    }

    pub fn get_by_address_payment_part(
        rx: &ReadTransaction,
        address_payment_part: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&address_payment_part.to_vec());
        let bitmap = table.get_bitmap(key)?;
        Ok(bitmap.iter().collect::<Vec<_>>())
    }

    pub fn iter_by_payment(rx: &ReadTransaction, payment: &[u8]) -> Result<SlotKeyIterator, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&payment.to_vec());
        let bitmap = table.get_bitmap(key)?;
        let values: Vec<u64> = bitmap.iter().collect();
        Ok(SlotKeyIterator::new(values.into_iter()))
    }
}

pub struct AddressStakePartApproxIndexTable;

impl AddressStakePartApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, RoaringValue> = TableDefinition::new("bystake");

    pub fn compute_key(address_stake_part: &Vec<u8>) -> u64 {
        xxh3_64(address_stake_part.as_slice())
    }

    pub fn get_by_address_stake_part(
        rx: &ReadTransaction,
        address_stake_part: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&address_stake_part.to_vec());
        let bitmap = table.get_bitmap(key)?;
        Ok(bitmap.iter().collect::<Vec<_>>())
    }

    pub fn iter_by_stake(rx: &ReadTransaction, stake: &[u8]) -> Result<SlotKeyIterator, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&stake.to_vec());
        let bitmap = table.get_bitmap(key)?;
        let values: Vec<u64> = bitmap.iter().collect();
        Ok(SlotKeyIterator::new(values.into_iter()))
    }
}

pub struct AssetApproxIndexTable;

impl AssetApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, RoaringValue> = TableDefinition::new("byasset");

    pub fn compute_key(asset: &Vec<u8>) -> u64 {
        xxh3_64(asset.as_slice())
    }

    pub fn get_by_asset(rx: &ReadTransaction, asset: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&asset.to_vec());
        let bitmap = table.get_bitmap(key)?;
        Ok(bitmap.iter().collect::<Vec<_>>())
    }

    pub fn iter_by_asset(rx: &ReadTransaction, asset: &[u8]) -> Result<SlotKeyIterator, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&asset.to_vec());
        let bitmap = table.get_bitmap(key)?;
        let values: Vec<u64> = bitmap.iter().collect();
        Ok(SlotKeyIterator::new(values.into_iter()))
    }
}

pub struct BlockHashApproxIndexTable;

impl BlockHashApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, RoaringValue> =
        TableDefinition::new("byblockhash");

    pub fn compute_key(block_hash: &Vec<u8>) -> u64 {
        xxh3_64(block_hash.as_slice())
    }

    pub fn get_by_block_hash(
        rx: &ReadTransaction,
        block_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&block_hash.to_vec());
        let bitmap = table.get_bitmap(key)?;
        Ok(bitmap.iter().collect::<Vec<_>>())
    }
}

pub struct BlockNumberApproxIndexTable;

impl BlockNumberApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, RoaringValue> =
        TableDefinition::new("byblocknumber");

    pub fn compute_key(block_number: &u64) -> u64 {
        // Left for readability
        *block_number
    }

    pub fn get_by_block_number(
        rx: &ReadTransaction,
        block_number: &u64,
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(block_number);
        let bitmap = table.get_bitmap(key)?;
        Ok(bitmap.iter().collect::<Vec<_>>())
    }
}

pub struct DatumHashApproxIndexTable;

impl DatumHashApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, RoaringValue> = TableDefinition::new("bydatum");

    pub fn compute_key(datum_hash: &Vec<u8>) -> u64 {
        xxh3_64(datum_hash.as_slice())
    }

    pub fn get_by_datum_hash(
        rx: &ReadTransaction,
        datum_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&datum_hash.to_vec());
        let bitmap = table.get_bitmap(key)?;
        Ok(bitmap.iter().collect::<Vec<_>>())
    }
}

pub struct MetadataApproxIndexTable;

impl MetadataApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, RoaringValue> = TableDefinition::new("bymetadata");

    pub fn compute_key(metadata: &u64) -> u64 {
        // Left for readability
        *metadata
    }

    pub fn iter_by_metadata(
        rx: &ReadTransaction,
        metadata: &u64,
    ) -> Result<SlotKeyIterator, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(metadata);
        let bitmap = table.get_bitmap(key)?;
        let values: Vec<u64> = bitmap.iter().collect();
        Ok(SlotKeyIterator::new(values.into_iter()))
    }
}

pub struct PolicyApproxIndexTable;

impl PolicyApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, RoaringValue> = TableDefinition::new("bypolicy");

    pub fn compute_key(policy: &Vec<u8>) -> u64 {
        xxh3_64(policy.as_slice())
    }

    pub fn get_by_policy(rx: &ReadTransaction, policy: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&policy.to_vec());
        let bitmap = table.get_bitmap(key)?;
        Ok(bitmap.iter().collect::<Vec<_>>())
    }
}

pub struct ScriptHashApproxIndexTable;

impl ScriptHashApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, RoaringValue> = TableDefinition::new("byscript");

    pub fn compute_key(script_hash: &Vec<u8>) -> u64 {
        xxh3_64(script_hash.as_slice())
    }

    pub fn get_by_script_hash(
        rx: &ReadTransaction,
        script_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&script_hash.to_vec());
        let bitmap = table.get_bitmap(key)?;
        Ok(bitmap.iter().collect::<Vec<_>>())
    }
}

pub struct SpentTxoApproxIndexTable;

impl SpentTxoApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, RoaringValue> = TableDefinition::new("byspenttxo");

    pub fn compute_key(spent_txo: &Vec<u8>) -> u64 {
        xxh3_64(spent_txo.as_slice())
    }

    pub fn get_by_spent_txo(
        rx: &ReadTransaction,
        spent_txo: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&spent_txo.to_vec());
        let bitmap = table.get_bitmap(key)?;
        Ok(bitmap.iter().collect::<Vec<_>>())
    }
}

pub struct AccountCertsApproxIndexTable;

impl AccountCertsApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, RoaringValue> =
        TableDefinition::new("bystakeactions");

    pub fn compute_key(account: &Vec<u8>) -> u64 {
        xxh3_64(account.as_slice())
    }

    pub fn get_by_account(rx: &ReadTransaction, account: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&account.to_vec());
        let bitmap = table.get_bitmap(key)?;
        Ok(bitmap.iter().collect::<Vec<_>>())
    }

    pub fn iter_by_account_certs(
        rx: &ReadTransaction,
        account: &[u8],
    ) -> Result<SlotKeyIterator, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&account.to_vec());
        let bitmap = table.get_bitmap(key)?;
        let values: Vec<u64> = bitmap.iter().collect();
        Ok(SlotKeyIterator::new(values.into_iter()))
    }
}

pub struct TxHashApproxIndexTable;

impl TxHashApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, RoaringValue> = TableDefinition::new("bytx");

    pub fn compute_key(tx_hash: &Vec<u8>) -> u64 {
        xxh3_64(tx_hash.as_slice())
    }

    pub fn get_by_tx_hash(rx: &ReadTransaction, tx_hash: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let key = Self::compute_key(&tx_hash.to_vec());
        let bitmap = table.get_bitmap(key)?;
        Ok(bitmap.iter().collect::<Vec<_>>())
    }
}

pub struct Indexes;

impl Indexes {
    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(AddressApproxIndexTable::DEF)?;
        wx.open_table(AddressPaymentPartApproxIndexTable::DEF)?;
        wx.open_table(AddressStakePartApproxIndexTable::DEF)?;
        wx.open_table(AssetApproxIndexTable::DEF)?;
        wx.open_table(BlockHashApproxIndexTable::DEF)?;
        wx.open_table(BlockNumberApproxIndexTable::DEF)?;
        wx.open_table(DatumHashApproxIndexTable::DEF)?;
        wx.open_table(PolicyApproxIndexTable::DEF)?;
        wx.open_table(ScriptHashApproxIndexTable::DEF)?;
        wx.open_table(SpentTxoApproxIndexTable::DEF)?;
        wx.open_table(AccountCertsApproxIndexTable::DEF)?;
        wx.open_table(TxHashApproxIndexTable::DEF)?;
        wx.open_table(MetadataApproxIndexTable::DEF)?;

        Ok(())
    }

    pub fn iter_by_address(rx: &ReadTransaction, address: &[u8]) -> Result<SlotKeyIterator, Error> {
        AddressApproxIndexTable::iter_by_address(rx, address)
    }

    pub fn iter_by_asset(rx: &ReadTransaction, asset: &[u8]) -> Result<SlotKeyIterator, Error> {
        AssetApproxIndexTable::iter_by_asset(rx, asset)
    }

    pub fn iter_by_payment(rx: &ReadTransaction, payment: &[u8]) -> Result<SlotKeyIterator, Error> {
        AddressPaymentPartApproxIndexTable::iter_by_payment(rx, payment)
    }

    pub fn iter_by_stake(rx: &ReadTransaction, stake: &[u8]) -> Result<SlotKeyIterator, Error> {
        AddressStakePartApproxIndexTable::iter_by_stake(rx, stake)
    }

    pub fn iter_by_metadata(
        rx: &ReadTransaction,
        metadata: &u64,
    ) -> Result<SlotKeyIterator, Error> {
        MetadataApproxIndexTable::iter_by_metadata(rx, metadata)
    }

    pub fn iter_by_account_certs(
        rx: &ReadTransaction,
        account: &[u8],
    ) -> Result<SlotKeyIterator, Error> {
        AccountCertsApproxIndexTable::iter_by_account_certs(rx, account)
    }

    pub fn get_by_address_payment_part(
        rx: &ReadTransaction,
        address_payment_part: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        AddressPaymentPartApproxIndexTable::get_by_address_payment_part(rx, address_payment_part)
    }

    pub fn get_by_address_stake_part(
        rx: &ReadTransaction,
        address_stake_part: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        AddressStakePartApproxIndexTable::get_by_address_stake_part(rx, address_stake_part)
    }

    pub fn get_by_asset(rx: &ReadTransaction, asset: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        AssetApproxIndexTable::get_by_asset(rx, asset)
    }

    pub fn get_by_block_hash(
        rx: &ReadTransaction,
        block_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        BlockHashApproxIndexTable::get_by_block_hash(rx, block_hash)
    }

    pub fn get_by_block_number(
        rx: &ReadTransaction,
        block_number: &u64,
    ) -> Result<Vec<BlockSlot>, Error> {
        BlockNumberApproxIndexTable::get_by_block_number(rx, block_number)
    }

    pub fn get_by_datum_hash(
        rx: &ReadTransaction,
        datum_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        DatumHashApproxIndexTable::get_by_datum_hash(rx, datum_hash)
    }

    pub fn get_by_policy(rx: &ReadTransaction, policy: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        PolicyApproxIndexTable::get_by_policy(rx, policy)
    }

    pub fn get_by_script_hash(
        rx: &ReadTransaction,
        script_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        ScriptHashApproxIndexTable::get_by_script_hash(rx, script_hash)
    }

    pub fn get_by_spent_txo(
        rx: &ReadTransaction,
        spent_txo: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        SpentTxoApproxIndexTable::get_by_spent_txo(rx, spent_txo)
    }

    pub fn get_by_account(rx: &ReadTransaction, account: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        AccountCertsApproxIndexTable::get_by_account(rx, account)
    }

    pub fn get_by_tx_hash(rx: &ReadTransaction, tx_hash: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        TxHashApproxIndexTable::get_by_tx_hash(rx, tx_hash)
    }

    pub fn copy(rx: &ReadTransaction, wx: &WriteTransaction) -> Result<(), Error> {
        Self::copy_table(AddressApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(AddressPaymentPartApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(AddressStakePartApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(AssetApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(BlockHashApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(BlockNumberApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(DatumHashApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(PolicyApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(ScriptHashApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(SpentTxoApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(AccountCertsApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(TxHashApproxIndexTable::DEF, rx, wx)?;

        Ok(())
    }

    pub fn apply(wx: &WriteTransaction, point: &ChainPoint, tags: &SlotTags) -> Result<(), Error> {
        let slot = point.slot();

        if let Some(hash) = point.hash() {
            Self::insert(
                wx,
                BlockHashApproxIndexTable::DEF,
                BlockHashApproxIndexTable::compute_key,
                vec![hash.to_vec()],
                slot,
            )?;
        }

        if let Some(number) = tags.number {
            Self::insert(
                wx,
                BlockNumberApproxIndexTable::DEF,
                BlockNumberApproxIndexTable::compute_key,
                vec![number],
                slot,
            )?;
        }

        Self::insert(
            wx,
            TxHashApproxIndexTable::DEF,
            TxHashApproxIndexTable::compute_key,
            tags.tx_hashes.clone(),
            slot,
        )?;

        Self::insert(
            wx,
            ScriptHashApproxIndexTable::DEF,
            ScriptHashApproxIndexTable::compute_key,
            tags.scripts.clone(),
            slot,
        )?;

        Self::insert(
            wx,
            DatumHashApproxIndexTable::DEF,
            DatumHashApproxIndexTable::compute_key,
            tags.datums.clone(),
            slot,
        )?;

        Self::insert(
            wx,
            AddressPaymentPartApproxIndexTable::DEF,
            AddressPaymentPartApproxIndexTable::compute_key,
            tags.payment_addresses.clone(),
            slot,
        )?;

        Self::insert(
            wx,
            AddressStakePartApproxIndexTable::DEF,
            AddressStakePartApproxIndexTable::compute_key,
            tags.stake_addresses.clone(),
            slot,
        )?;

        Self::insert(
            wx,
            AddressApproxIndexTable::DEF,
            AddressApproxIndexTable::compute_key,
            tags.full_addresses.clone(),
            slot,
        )?;

        Self::insert(
            wx,
            PolicyApproxIndexTable::DEF,
            PolicyApproxIndexTable::compute_key,
            tags.policies.clone(),
            slot,
        )?;

        Self::insert(
            wx,
            AssetApproxIndexTable::DEF,
            AssetApproxIndexTable::compute_key,
            tags.assets.clone(),
            slot,
        )?;

        Self::insert(
            wx,
            SpentTxoApproxIndexTable::DEF,
            SpentTxoApproxIndexTable::compute_key,
            tags.spent_txo.clone(),
            slot,
        )?;

        Self::insert(
            wx,
            AccountCertsApproxIndexTable::DEF,
            AccountCertsApproxIndexTable::compute_key,
            tags.account_certs.clone(),
            slot,
        )?;

        Self::insert(
            wx,
            MetadataApproxIndexTable::DEF,
            MetadataApproxIndexTable::compute_key,
            tags.metadata.clone(),
            slot,
        )?;

        Ok(())
    }

    pub fn undo(wx: &WriteTransaction, point: &ChainPoint, tags: &SlotTags) -> Result<(), Error> {
        let slot = point.slot();

        if let Some(hash) = point.hash() {
            Self::insert(
                wx,
                BlockHashApproxIndexTable::DEF,
                BlockHashApproxIndexTable::compute_key,
                vec![hash.to_vec()],
                slot,
            )?;
        }

        if let Some(number) = tags.number {
            Self::insert(
                wx,
                BlockNumberApproxIndexTable::DEF,
                BlockNumberApproxIndexTable::compute_key,
                vec![number],
                slot,
            )?;
        }

        Self::remove(
            wx,
            TxHashApproxIndexTable::DEF,
            TxHashApproxIndexTable::compute_key,
            tags.tx_hashes.clone(),
            slot,
        )?;

        Self::remove(
            wx,
            ScriptHashApproxIndexTable::DEF,
            ScriptHashApproxIndexTable::compute_key,
            tags.scripts.clone(),
            slot,
        )?;

        Self::remove(
            wx,
            DatumHashApproxIndexTable::DEF,
            DatumHashApproxIndexTable::compute_key,
            tags.datums.clone(),
            slot,
        )?;

        Self::remove(
            wx,
            AddressApproxIndexTable::DEF,
            AddressApproxIndexTable::compute_key,
            tags.full_addresses.clone(),
            slot,
        )?;

        Self::remove(
            wx,
            AddressPaymentPartApproxIndexTable::DEF,
            AddressPaymentPartApproxIndexTable::compute_key,
            tags.payment_addresses.clone(),
            slot,
        )?;

        Self::remove(
            wx,
            AddressStakePartApproxIndexTable::DEF,
            AddressStakePartApproxIndexTable::compute_key,
            tags.stake_addresses.clone(),
            slot,
        )?;

        Self::remove(
            wx,
            PolicyApproxIndexTable::DEF,
            PolicyApproxIndexTable::compute_key,
            tags.policies.clone(),
            slot,
        )?;

        Self::remove(
            wx,
            AssetApproxIndexTable::DEF,
            AssetApproxIndexTable::compute_key,
            tags.assets.clone(),
            slot,
        )?;

        Self::remove(
            wx,
            SpentTxoApproxIndexTable::DEF,
            SpentTxoApproxIndexTable::compute_key,
            tags.spent_txo.clone(),
            slot,
        )?;

        Self::remove(
            wx,
            AccountCertsApproxIndexTable::DEF,
            AccountCertsApproxIndexTable::compute_key,
            tags.account_certs.clone(),
            slot,
        )?;

        Self::remove(
            wx,
            MetadataApproxIndexTable::DEF,
            MetadataApproxIndexTable::compute_key,
            tags.metadata.clone(),
            slot,
        )?;

        Ok(())
    }

    pub fn insert<T>(
        wx: &WriteTransaction,
        table: TableDefinition<'static, u64, RoaringValue>,
        compute_key: fn(&T) -> u64,
        inputs: Vec<T>,
        slot: u64,
    ) -> Result<(), Error> {
        let mut table = wx.open_table(table)?;

        // Group by key for batch insert
        let mut key_groups: HashMap<u64, Vec<u64>> = HashMap::new();
        for x in inputs {
            let key = compute_key(&x);
            key_groups.entry(key).or_default().push(slot);
        }

        // Use batch insert from redb-extras
        for (key, slots) in key_groups {
            table.insert_members(key, slots)?;
        }

        Ok(())
    }

    pub fn remove<T>(
        wx: &WriteTransaction,
        table: TableDefinition<'static, u64, RoaringValue>,
        compute_key: fn(&T) -> u64,
        inputs: Vec<T>,
        slot: u64,
    ) -> Result<(), Error> {
        let mut table = wx.open_table(table)?;

        // Group by key for batch remove
        let mut key_groups: HashMap<u64, Vec<u64>> = HashMap::new();
        for x in inputs {
            let key = compute_key(&x);
            key_groups.entry(key).or_default().push(slot);
        }

        // Use batch remove from redb-extras
        for (key, slots) in key_groups {
            table.remove_members(key, slots)?;
        }

        Ok(())
    }

    fn copy_table(
        table: TableDefinition<'static, u64, RoaringValue>,
        rx: &ReadTransaction,
        wx: &WriteTransaction,
    ) -> Result<(), Error> {
        let source = rx.open_table(table)?;
        let mut target = wx.open_table(table)?;

        let all = source.range::<u64>(..)?;
        for entry in all {
            let (key, bitmap) = entry?;
            target.insert(key.value(), bitmap.value())?;
        }

        Ok(())
    }
}
