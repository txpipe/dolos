use ::redb::{MultimapTableDefinition, ReadTransaction, WriteTransaction};
use redb::MultimapValue;
use std::hash::{DefaultHasher, Hash as _, Hasher};

use dolos_core::{BlockSlot, ChainPoint, SlotTags};

type Error = super::RedbArchiveError;

pub struct SlotKeyIterator {
    range: MultimapValue<'static, u64>,
}

impl SlotKeyIterator {
    pub fn new(range: MultimapValue<'static, u64>) -> Self {
        Self { range }
    }
}

impl Iterator for SlotKeyIterator {
    type Item = Result<u64, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.range.next()?;
        let res = next.map(|x| x.value()).map_err(Error::from);
        Some(res)
    }
}

impl DoubleEndedIterator for SlotKeyIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        let next = self.range.next_back()?;
        let res = next.map(|x| x.value()).map_err(Error::from);
        Some(res)
    }
}

pub struct AddressApproxIndexTable;

impl AddressApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, u64, u64> =
        MultimapTableDefinition::new("byaddress");

    pub fn compute_key(address: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        address.hash(&mut hasher);
        hasher.finish()
    }

    pub fn iter_by_address(rx: &ReadTransaction, address: &[u8]) -> Result<SlotKeyIterator, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(&address.to_vec());
        let range = table.get(key)?;
        Ok(SlotKeyIterator::new(range))
    }
}

pub struct AddressPaymentPartApproxIndexTable;

impl AddressPaymentPartApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, u64, u64> =
        MultimapTableDefinition::new("bypayment");

    pub fn compute_key(address: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        address.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_address_payment_part(
        rx: &ReadTransaction,
        address_payment_part: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(&address_payment_part.to_vec());
        let mut out = vec![];
        for slot in table.get(key)? {
            out.push(slot?.value());
        }
        Ok(out)
    }

    pub fn iter_by_payment(rx: &ReadTransaction, payment: &[u8]) -> Result<SlotKeyIterator, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(&payment.to_vec());
        let range = table.get(key)?;
        Ok(SlotKeyIterator::new(range))
    }
}

pub struct AddressStakePartApproxIndexTable;

impl AddressStakePartApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, u64, u64> =
        MultimapTableDefinition::new("bystake");

    pub fn compute_key(address_stake_part: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        address_stake_part.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_address_stake_part(
        rx: &ReadTransaction,
        address_stake_part: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(&address_stake_part.to_vec());
        let mut out = vec![];
        for slot in table.get(key)? {
            out.push(slot?.value());
        }
        Ok(out)
    }
}

pub struct AssetApproxIndexTable;

impl AssetApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, u64, u64> =
        MultimapTableDefinition::new("byasset");

    pub fn compute_key(asset: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        asset.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_asset(rx: &ReadTransaction, asset: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(&asset.to_vec());
        let mut out = vec![];
        for slot in table.get(key)? {
            out.push(slot?.value());
        }
        Ok(out)
    }

    pub fn iter_by_asset(rx: &ReadTransaction, asset: &[u8]) -> Result<SlotKeyIterator, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(&asset.to_vec());
        let range = table.get(key)?;
        Ok(SlotKeyIterator::new(range))
    }
}

pub struct BlockHashApproxIndexTable;

impl BlockHashApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, u64, u64> =
        MultimapTableDefinition::new("byblockhash");

    pub fn compute_key(block_hash: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        block_hash.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_block_hash(
        rx: &ReadTransaction,
        block_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(&block_hash.to_vec());
        let mut out = vec![];
        for slot in table.get(key)? {
            out.push(slot?.value());
        }
        Ok(out)
    }
}

pub struct BlockNumberApproxIndexTable;

impl BlockNumberApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, u64, u64> =
        MultimapTableDefinition::new("byblocknumber");

    pub fn compute_key(block_number: &u64) -> u64 {
        // Left for readability
        *block_number
    }

    pub fn get_by_block_number(
        rx: &ReadTransaction,
        block_number: &u64,
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(block_number);
        let mut out = vec![];
        for slot in table.get(key)? {
            out.push(slot?.value());
        }
        Ok(out)
    }
}

pub struct DatumHashApproxIndexTable;

impl DatumHashApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, u64, u64> =
        MultimapTableDefinition::new("bydatum");

    pub fn compute_key(datum_hash: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        datum_hash.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_datum_hash(
        rx: &ReadTransaction,
        datum_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(&datum_hash.to_vec());
        let mut out = vec![];
        for slot in table.get(key)? {
            out.push(slot?.value());
        }
        Ok(out)
    }
}

pub struct PolicyApproxIndexTable;

impl PolicyApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, u64, u64> =
        MultimapTableDefinition::new("bypolicy");

    pub fn compute_key(policy: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        policy.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_policy(rx: &ReadTransaction, policy: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(&policy.to_vec());
        let mut out = vec![];
        for slot in table.get(key)? {
            out.push(slot?.value());
        }
        Ok(out)
    }
}

pub struct ScriptHashApproxIndexTable;

impl ScriptHashApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, u64, u64> =
        MultimapTableDefinition::new("byscript");

    pub fn compute_key(script_hash: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        script_hash.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_script_hash(
        rx: &ReadTransaction,
        script_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(&script_hash.to_vec());
        let mut out = vec![];
        for slot in table.get(key)? {
            out.push(slot?.value());
        }
        Ok(out)
    }
}

pub struct TxHashApproxIndexTable;

impl TxHashApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, u64, u64> =
        MultimapTableDefinition::new("bytx");

    pub fn compute_key(tx_hash: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        tx_hash.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_tx_hash(rx: &ReadTransaction, tx_hash: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_multimap_table(Self::DEF)?;
        let key = Self::compute_key(&tx_hash.to_vec());
        let mut out = vec![];
        for slot in table.get(key)? {
            out.push(slot?.value());
        }
        Ok(out)
    }
}

pub struct Indexes;

impl Indexes {
    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_multimap_table(AddressApproxIndexTable::DEF)?;
        wx.open_multimap_table(AddressPaymentPartApproxIndexTable::DEF)?;
        wx.open_multimap_table(AddressStakePartApproxIndexTable::DEF)?;
        wx.open_multimap_table(AssetApproxIndexTable::DEF)?;
        wx.open_multimap_table(BlockHashApproxIndexTable::DEF)?;
        wx.open_multimap_table(BlockNumberApproxIndexTable::DEF)?;
        wx.open_multimap_table(DatumHashApproxIndexTable::DEF)?;
        wx.open_multimap_table(PolicyApproxIndexTable::DEF)?;
        wx.open_multimap_table(ScriptHashApproxIndexTable::DEF)?;
        wx.open_multimap_table(TxHashApproxIndexTable::DEF)?;

        Ok(())
    }

    pub fn iter_by_address(rx: &ReadTransaction, address: &[u8]) -> Result<SlotKeyIterator, Error> {
        AddressApproxIndexTable::iter_by_address(rx, address)
    }

    pub fn iter_by_asset(rx: &ReadTransaction, asset: &[u8]) -> Result<SlotKeyIterator, Error> {
        AssetApproxIndexTable::iter_by_asset(rx, asset)
    }

    pub fn iter_by_payment(rx: &ReadTransaction, address: &[u8]) -> Result<SlotKeyIterator, Error> {
        AddressPaymentPartApproxIndexTable::iter_by_payment(rx, address)
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

        Ok(())
    }

    pub fn insert<T>(
        wx: &WriteTransaction,
        table: MultimapTableDefinition<'static, u64, u64>,
        compute_key: fn(&T) -> u64,
        inputs: Vec<T>,
        slot: u64,
    ) -> Result<(), Error> {
        let mut table = wx.open_multimap_table(table)?;
        for x in inputs {
            let key = compute_key(&x);
            let _ = table.insert(key, slot)?;
        }

        Ok(())
    }

    pub fn remove<T>(
        wx: &WriteTransaction,
        table: MultimapTableDefinition<'static, u64, u64>,
        compute_key: fn(&T) -> u64,
        inputs: Vec<T>,
        slot: u64,
    ) -> Result<(), Error> {
        let mut table = wx.open_multimap_table(table)?;
        for x in inputs {
            let key = compute_key(&x);
            let _ = table.remove(key, slot)?;
        }

        Ok(())
    }

    fn copy_table(
        table: MultimapTableDefinition<'static, u64, u64>,
        rx: &ReadTransaction,
        wx: &WriteTransaction,
    ) -> Result<(), Error> {
        let source = rx.open_multimap_table(table)?;
        let mut target = wx.open_multimap_table(table)?;

        let all = source.range::<u64>(..)?;
        for entry in all {
            let (key, values) = entry?;
            for value in values {
                let value = value?;
                target.insert(key.value(), value.value())?;
            }
        }

        Ok(())
    }
}
