use ::redb::{
    ReadOnlyTable, ReadTransaction, ReadableTable as _, TableDefinition, WriteTransaction,
};
use redb_extras::buckets::{BucketError, BucketedKey, KeyBuilder};
use redb_extras::roaring::RoaringValue;
use roaring::RoaringTreemap;
use xxhash_rust::xxh3::xxh3_64;

use dolos_core::{BlockSlot, ChainPoint, SlotTags};

type Error = super::RedbArchiveError;

const BUCKET_SIZE: u64 = 432_000;

fn key_builder() -> KeyBuilder {
    KeyBuilder::new(BUCKET_SIZE).expect("bucket size must be positive")
}

struct BucketRangeRoaringIterator {
    table: ReadOnlyTable<BucketedKey<u64>, RoaringValue>,
    base_key: u64,
    front_bucket: i64,
    back_bucket: i64,
    finished: bool,
}

impl BucketRangeRoaringIterator {
    fn new(
        table: ReadOnlyTable<BucketedKey<u64>, RoaringValue>,
        key_builder: &KeyBuilder,
        base_key: u64,
        start_sequence: u64,
        end_sequence: u64,
    ) -> Result<Self, BucketError> {
        if start_sequence > end_sequence {
            return Err(BucketError::InvalidRange {
                start: start_sequence,
                end: end_sequence,
            });
        }

        let bucket_size = key_builder.bucket_size();
        let start_bucket = start_sequence / bucket_size;
        let end_bucket = end_sequence / bucket_size;

        Ok(Self {
            table,
            base_key,
            front_bucket: start_bucket as i64,
            back_bucket: end_bucket as i64,
            finished: false,
        })
    }
}

impl Iterator for BucketRangeRoaringIterator {
    type Item = Result<RoaringTreemap, BucketError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        while self.front_bucket <= self.back_bucket {
            let bucket = self.front_bucket as u64;
            self.front_bucket += 1;

            match self.table.get(&BucketedKey::new(self.base_key, bucket)) {
                Ok(Some(value_guard)) => return Some(Ok(value_guard.value().clone())),
                Ok(None) => continue,
                Err(err) => {
                    self.finished = true;
                    return Some(Err(BucketError::IterationError(format!(
                        "Database error during point lookup: {}",
                        err
                    ))));
                }
            }
        }

        self.finished = true;
        None
    }
}

impl DoubleEndedIterator for BucketRangeRoaringIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        while self.front_bucket <= self.back_bucket {
            let bucket = self.back_bucket as u64;
            self.back_bucket -= 1;

            match self.table.get(&BucketedKey::new(self.base_key, bucket)) {
                Ok(Some(value_guard)) => return Some(Ok(value_guard.value().clone())),
                Ok(None) => continue,
                Err(err) => {
                    self.finished = true;
                    return Some(Err(BucketError::IterationError(format!(
                        "Database error during point lookup: {}",
                        err
                    ))));
                }
            }
        }

        self.finished = true;
        None
    }
}

pub struct SlotKeyIterator {
    inner: BucketRangeRoaringIterator,
    start_slot: BlockSlot,
    end_slot: BlockSlot,
    front_values: Option<roaring::treemap::IntoIter>,
    back_values: Option<roaring::treemap::IntoIter>,
    finished: bool,
}

impl SlotKeyIterator {
    fn new(inner: BucketRangeRoaringIterator, start_slot: BlockSlot, end_slot: BlockSlot) -> Self {
        Self {
            inner,
            start_slot,
            end_slot,
            front_values: None,
            back_values: None,
            finished: false,
        }
    }
}

fn next_slot_in_range(
    iter: &mut roaring::treemap::IntoIter,
    start_slot: BlockSlot,
    end_slot: BlockSlot,
) -> Option<BlockSlot> {
    while let Some(slot) = iter.next() {
        if slot < start_slot {
            continue;
        }

        if slot > end_slot {
            return None;
        }

        return Some(slot);
    }

    None
}

fn next_back_slot_in_range(
    iter: &mut roaring::treemap::IntoIter,
    start_slot: BlockSlot,
    end_slot: BlockSlot,
) -> Option<BlockSlot> {
    while let Some(slot) = iter.next_back() {
        if slot > end_slot {
            continue;
        }

        if slot < start_slot {
            return None;
        }

        return Some(slot);
    }

    None
}

impl Iterator for SlotKeyIterator {
    type Item = Result<u64, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            if let Some(values) = self.front_values.as_mut() {
                if let Some(slot) = next_slot_in_range(values, self.start_slot, self.end_slot) {
                    return Some(Ok(slot));
                }
                self.front_values = None;
            }

            let next_bucket = self.inner.next();
            let bucket = match next_bucket {
                Some(Ok(bucket)) => bucket,
                Some(Err(err)) => {
                    self.finished = true;
                    return Some(Err(err.into()));
                }
                None => {
                    self.finished = true;
                    return None;
                }
            };

            self.front_values = Some(bucket.into_iter());
        }
    }
}

impl DoubleEndedIterator for SlotKeyIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            if let Some(values) = self.back_values.as_mut() {
                if let Some(slot) = next_back_slot_in_range(values, self.start_slot, self.end_slot)
                {
                    return Some(Ok(slot));
                }
                self.back_values = None;
            }

            let next_bucket = self.inner.next_back();
            let bucket = match next_bucket {
                Some(Ok(bucket)) => bucket,
                Some(Err(err)) => {
                    self.finished = true;
                    return Some(Err(err.into()));
                }
                None => {
                    self.finished = true;
                    return None;
                }
            };

            self.back_values = Some(bucket.into_iter());
        }
    }
}

fn slot_iterator(
    rx: &ReadTransaction,
    table: TableDefinition<'static, BucketedKey<u64>, RoaringValue>,
    base_key: u64,
    start_slot: u64,
    end_slot: u64,
) -> Result<SlotKeyIterator, Error> {
    let table = rx.open_table(table)?;
    let key_builder = key_builder();
    let iter =
        BucketRangeRoaringIterator::new(table, &key_builder, base_key, start_slot, end_slot)?;

    Ok(SlotKeyIterator::new(iter, start_slot, end_slot))
}

pub struct AddressApproxIndexTable;

impl AddressApproxIndexTable {
    pub const DEF: TableDefinition<'static, BucketedKey<u64>, RoaringValue> =
        TableDefinition::new("byaddress");

    pub fn compute_key(address: &Vec<u8>) -> u64 {
        xxh3_64(address.as_slice())
    }

    pub fn iter_by_address(
        rx: &ReadTransaction,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        let key = Self::compute_key(&address.to_vec());
        slot_iterator(rx, Self::DEF, key, start_slot, end_slot)
    }
}

pub struct AddressPaymentPartApproxIndexTable;

impl AddressPaymentPartApproxIndexTable {
    pub const DEF: TableDefinition<'static, BucketedKey<u64>, RoaringValue> =
        TableDefinition::new("bypayment");

    pub fn compute_key(address: &Vec<u8>) -> u64 {
        xxh3_64(address.as_slice())
    }

    pub fn get_by_address_payment_part(
        rx: &ReadTransaction,
        address_payment_part: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        let key = Self::compute_key(&address_payment_part.to_vec());
        let iter = slot_iterator(rx, Self::DEF, key, start_slot, end_slot)?;

        iter.collect::<Result<_, _>>()
    }

    pub fn iter_by_payment(
        rx: &ReadTransaction,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        let key = Self::compute_key(&payment.to_vec());
        slot_iterator(rx, Self::DEF, key, start_slot, end_slot)
    }
}

pub struct AddressStakePartApproxIndexTable;

impl AddressStakePartApproxIndexTable {
    pub const DEF: TableDefinition<'static, BucketedKey<u64>, RoaringValue> =
        TableDefinition::new("bystake");

    pub fn compute_key(address_stake_part: &Vec<u8>) -> u64 {
        xxh3_64(address_stake_part.as_slice())
    }

    pub fn get_by_address_stake_part(
        rx: &ReadTransaction,
        address_stake_part: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        let key = Self::compute_key(&address_stake_part.to_vec());
        let iter = slot_iterator(rx, Self::DEF, key, start_slot, end_slot)?;

        iter.collect::<Result<_, _>>()
    }

    pub fn iter_by_stake(
        rx: &ReadTransaction,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        let key = Self::compute_key(&stake.to_vec());
        slot_iterator(rx, Self::DEF, key, start_slot, end_slot)
    }
}

pub struct AssetApproxIndexTable;

impl AssetApproxIndexTable {
    pub const DEF: TableDefinition<'static, BucketedKey<u64>, RoaringValue> =
        TableDefinition::new("byasset");

    pub fn compute_key(asset: &Vec<u8>) -> u64 {
        xxh3_64(asset.as_slice())
    }

    pub fn get_by_asset(
        rx: &ReadTransaction,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        let key = Self::compute_key(&asset.to_vec());
        let iter = slot_iterator(rx, Self::DEF, key, start_slot, end_slot)?;

        iter.collect::<Result<_, _>>()
    }

    pub fn iter_by_asset(
        rx: &ReadTransaction,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        let key = Self::compute_key(&asset.to_vec());
        slot_iterator(rx, Self::DEF, key, start_slot, end_slot)
    }
}

pub struct BlockHashIndexTable;

impl BlockHashIndexTable {
    pub const DEF: TableDefinition<'static, &'static [u8], u64> =
        TableDefinition::new("byblockhash");

    pub fn get_by_block_hash(
        rx: &ReadTransaction,
        block_hash: &[u8],
    ) -> Result<Option<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        Ok(table.get(block_hash)?.map(|slot| slot.value()))
    }
}

pub struct BlockNumberIndexTable;

impl BlockNumberIndexTable {
    pub const DEF: TableDefinition<'static, u64, u64> = TableDefinition::new("byblocknumber");

    pub fn get_by_block_number(
        rx: &ReadTransaction,
        block_number: &u64,
    ) -> Result<Option<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        Ok(table.get(*block_number)?.map(|slot| slot.value()))
    }
}

pub struct DatumHashApproxIndexTable;

impl DatumHashApproxIndexTable {
    pub const DEF: TableDefinition<'static, BucketedKey<u64>, RoaringValue> =
        TableDefinition::new("bydatum");

    pub fn compute_key(datum_hash: &Vec<u8>) -> u64 {
        xxh3_64(datum_hash.as_slice())
    }

    pub fn get_by_datum_hash(
        rx: &ReadTransaction,
        datum_hash: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        let key = Self::compute_key(&datum_hash.to_vec());
        let iter = slot_iterator(rx, Self::DEF, key, start_slot, end_slot)?;

        iter.collect::<Result<_, _>>()
    }
}

pub struct MetadataApproxIndexTable;

impl MetadataApproxIndexTable {
    pub const DEF: TableDefinition<'static, BucketedKey<u64>, RoaringValue> =
        TableDefinition::new("bymetadata");

    pub fn compute_key(metadata: &u64) -> u64 {
        // Left for readability
        *metadata
    }

    pub fn iter_by_metadata(
        rx: &ReadTransaction,
        metadata: &u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        let key = Self::compute_key(metadata);
        slot_iterator(rx, Self::DEF, key, start_slot, end_slot)
    }
}

pub struct PolicyApproxIndexTable;

impl PolicyApproxIndexTable {
    pub const DEF: TableDefinition<'static, BucketedKey<u64>, RoaringValue> =
        TableDefinition::new("bypolicy");

    pub fn compute_key(policy: &Vec<u8>) -> u64 {
        xxh3_64(policy.as_slice())
    }

    pub fn get_by_policy(
        rx: &ReadTransaction,
        policy: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        let key = Self::compute_key(&policy.to_vec());
        let iter = slot_iterator(rx, Self::DEF, key, start_slot, end_slot)?;

        iter.collect::<Result<_, _>>()
    }
}

pub struct ScriptHashApproxIndexTable;

impl ScriptHashApproxIndexTable {
    pub const DEF: TableDefinition<'static, BucketedKey<u64>, RoaringValue> =
        TableDefinition::new("byscript");

    pub fn compute_key(script_hash: &Vec<u8>) -> u64 {
        xxh3_64(script_hash.as_slice())
    }

    pub fn get_by_script_hash(
        rx: &ReadTransaction,
        script_hash: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        let key = Self::compute_key(&script_hash.to_vec());
        let iter = slot_iterator(rx, Self::DEF, key, start_slot, end_slot)?;

        iter.collect::<Result<_, _>>()
    }
}

pub struct SpentTxoApproxIndexTable;

impl SpentTxoApproxIndexTable {
    pub const DEF: TableDefinition<'static, BucketedKey<u64>, RoaringValue> =
        TableDefinition::new("byspenttxo");

    pub fn compute_key(spent_txo: &Vec<u8>) -> u64 {
        xxh3_64(spent_txo.as_slice())
    }

    pub fn get_by_spent_txo(
        rx: &ReadTransaction,
        spent_txo: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        let key = Self::compute_key(&spent_txo.to_vec());
        let iter = slot_iterator(rx, Self::DEF, key, start_slot, end_slot)?;

        iter.collect::<Result<_, _>>()
    }
}

pub struct AccountCertsApproxIndexTable;

impl AccountCertsApproxIndexTable {
    pub const DEF: TableDefinition<'static, BucketedKey<u64>, RoaringValue> =
        TableDefinition::new("bystakeactions");

    pub fn compute_key(account: &Vec<u8>) -> u64 {
        xxh3_64(account.as_slice())
    }

    pub fn get_by_account(
        rx: &ReadTransaction,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        let key = Self::compute_key(&account.to_vec());
        let iter = slot_iterator(rx, Self::DEF, key, start_slot, end_slot)?;

        iter.collect::<Result<_, _>>()
    }

    pub fn iter_by_account_certs(
        rx: &ReadTransaction,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        let key = Self::compute_key(&account.to_vec());
        slot_iterator(rx, Self::DEF, key, start_slot, end_slot)
    }
}

pub struct TxHashIndexTable;

impl TxHashIndexTable {
    pub const DEF: TableDefinition<'static, &'static [u8], u64> = TableDefinition::new("bytx");

    pub fn get_by_tx_hash(
        rx: &ReadTransaction,
        tx_hash: &[u8],
    ) -> Result<Option<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        Ok(table.get(tx_hash)?.map(|slot| slot.value()))
    }
}

pub struct Indexes;

impl Indexes {
    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(AddressApproxIndexTable::DEF)?;
        wx.open_table(AddressPaymentPartApproxIndexTable::DEF)?;
        wx.open_table(AddressStakePartApproxIndexTable::DEF)?;
        wx.open_table(AssetApproxIndexTable::DEF)?;
        wx.open_table(BlockHashIndexTable::DEF)?;
        wx.open_table(BlockNumberIndexTable::DEF)?;
        wx.open_table(DatumHashApproxIndexTable::DEF)?;
        wx.open_table(PolicyApproxIndexTable::DEF)?;
        wx.open_table(ScriptHashApproxIndexTable::DEF)?;
        wx.open_table(SpentTxoApproxIndexTable::DEF)?;
        wx.open_table(AccountCertsApproxIndexTable::DEF)?;
        wx.open_table(TxHashIndexTable::DEF)?;
        wx.open_table(MetadataApproxIndexTable::DEF)?;

        Ok(())
    }

    pub fn iter_by_address(
        rx: &ReadTransaction,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        AddressApproxIndexTable::iter_by_address(rx, address, start_slot, end_slot)
    }

    pub fn iter_by_asset(
        rx: &ReadTransaction,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        AssetApproxIndexTable::iter_by_asset(rx, asset, start_slot, end_slot)
    }

    pub fn iter_by_payment(
        rx: &ReadTransaction,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        AddressPaymentPartApproxIndexTable::iter_by_payment(rx, payment, start_slot, end_slot)
    }

    pub fn iter_by_stake(
        rx: &ReadTransaction,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        AddressStakePartApproxIndexTable::iter_by_stake(rx, stake, start_slot, end_slot)
    }

    pub fn iter_by_metadata(
        rx: &ReadTransaction,
        metadata: &u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        MetadataApproxIndexTable::iter_by_metadata(rx, metadata, start_slot, end_slot)
    }

    pub fn iter_by_account_certs(
        rx: &ReadTransaction,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        AccountCertsApproxIndexTable::iter_by_account_certs(rx, account, start_slot, end_slot)
    }

    pub fn get_by_address_payment_part(
        rx: &ReadTransaction,
        address_payment_part: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        AddressPaymentPartApproxIndexTable::get_by_address_payment_part(
            rx,
            address_payment_part,
            start_slot,
            end_slot,
        )
    }

    pub fn get_by_address_stake_part(
        rx: &ReadTransaction,
        address_stake_part: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        AddressStakePartApproxIndexTable::get_by_address_stake_part(
            rx,
            address_stake_part,
            start_slot,
            end_slot,
        )
    }

    pub fn get_by_asset(
        rx: &ReadTransaction,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        AssetApproxIndexTable::get_by_asset(rx, asset, start_slot, end_slot)
    }

    pub fn get_by_block_hash(
        rx: &ReadTransaction,
        block_hash: &[u8],
    ) -> Result<Option<BlockSlot>, Error> {
        BlockHashIndexTable::get_by_block_hash(rx, block_hash)
    }

    pub fn get_by_block_number(
        rx: &ReadTransaction,
        block_number: &u64,
    ) -> Result<Option<BlockSlot>, Error> {
        BlockNumberIndexTable::get_by_block_number(rx, block_number)
    }

    pub fn get_by_datum_hash(
        rx: &ReadTransaction,
        datum_hash: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        DatumHashApproxIndexTable::get_by_datum_hash(rx, datum_hash, start_slot, end_slot)
    }

    pub fn get_by_policy(
        rx: &ReadTransaction,
        policy: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        PolicyApproxIndexTable::get_by_policy(rx, policy, start_slot, end_slot)
    }

    pub fn get_by_script_hash(
        rx: &ReadTransaction,
        script_hash: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        ScriptHashApproxIndexTable::get_by_script_hash(rx, script_hash, start_slot, end_slot)
    }

    pub fn get_by_spent_txo(
        rx: &ReadTransaction,
        spent_txo: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        SpentTxoApproxIndexTable::get_by_spent_txo(rx, spent_txo, start_slot, end_slot)
    }

    pub fn get_by_account(
        rx: &ReadTransaction,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, Error> {
        AccountCertsApproxIndexTable::get_by_account(rx, account, start_slot, end_slot)
    }

    pub fn get_by_tx_hash(
        rx: &ReadTransaction,
        tx_hash: &[u8],
    ) -> Result<Option<BlockSlot>, Error> {
        TxHashIndexTable::get_by_tx_hash(rx, tx_hash)
    }

    pub fn copy(rx: &ReadTransaction, wx: &WriteTransaction) -> Result<(), Error> {
        Self::copy_table(AddressApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(AddressPaymentPartApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(AddressStakePartApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(AssetApproxIndexTable::DEF, rx, wx)?;
        Self::copy_value_table(BlockHashIndexTable::DEF, rx, wx)?;
        Self::copy_value_table(BlockNumberIndexTable::DEF, rx, wx)?;
        Self::copy_table(DatumHashApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(PolicyApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(ScriptHashApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(SpentTxoApproxIndexTable::DEF, rx, wx)?;
        Self::copy_table(AccountCertsApproxIndexTable::DEF, rx, wx)?;
        Self::copy_value_table(TxHashIndexTable::DEF, rx, wx)?;

        Ok(())
    }

    pub fn apply(wx: &WriteTransaction, point: &ChainPoint, tags: &SlotTags) -> Result<(), Error> {
        let slot = point.slot();

        if let Some(hash) = point.hash() {
            let mut table = wx.open_table(BlockHashIndexTable::DEF)?;
            table.insert(hash.as_slice(), slot)?;
        }

        if let Some(number) = tags.number {
            let mut table = wx.open_table(BlockNumberIndexTable::DEF)?;
            table.insert(number, slot)?;
        }

        if !tags.tx_hashes.is_empty() {
            let mut table = wx.open_table(TxHashIndexTable::DEF)?;
            for tx_hash in &tags.tx_hashes {
                table.insert(tx_hash.as_slice(), slot)?;
            }
        }

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
            let mut table = wx.open_table(BlockHashIndexTable::DEF)?;
            table.remove(hash.as_slice())?;
        }

        if let Some(number) = tags.number {
            let mut table = wx.open_table(BlockNumberIndexTable::DEF)?;
            table.remove(number)?;
        }

        if !tags.tx_hashes.is_empty() {
            let mut table = wx.open_table(TxHashIndexTable::DEF)?;
            for tx_hash in &tags.tx_hashes {
                table.remove(tx_hash.as_slice())?;
            }
        }

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
        table: TableDefinition<'static, BucketedKey<u64>, RoaringValue>,
        compute_key: fn(&T) -> u64,
        inputs: Vec<T>,
        slot: u64,
    ) -> Result<(), Error> {
        let mut table = wx.open_table(table)?;
        let key_builder = key_builder();
        for x in inputs {
            let key = compute_key(&x);
            let bucketed_key = key_builder.bucketed_key(key, slot);
            Self::insert_member(&mut table, bucketed_key, slot)?;
        }

        Ok(())
    }

    pub fn remove<T>(
        wx: &WriteTransaction,
        table: TableDefinition<'static, BucketedKey<u64>, RoaringValue>,
        compute_key: fn(&T) -> u64,
        inputs: Vec<T>,
        slot: u64,
    ) -> Result<(), Error> {
        let mut table = wx.open_table(table)?;
        let key_builder = key_builder();
        for x in inputs {
            let key = compute_key(&x);
            let bucketed_key = key_builder.bucketed_key(key, slot);
            Self::remove_member(&mut table, bucketed_key, slot)?;
        }

        Ok(())
    }

    fn copy_value_table<K: ::redb::Key, V: ::redb::Value>(
        table: TableDefinition<K, V>,
        rx: &ReadTransaction,
        wx: &WriteTransaction,
    ) -> Result<(), Error> {
        let source = rx.open_table(table)?;
        let mut target = wx.open_table(table)?;

        for entry in source.iter()? {
            let (key, value) = entry?;
            target.insert(key.value(), value.value())?;
        }

        Ok(())
    }

    fn insert_member(
        table: &mut redb::Table<BucketedKey<u64>, RoaringValue>,
        key: BucketedKey<u64>,
        member: u64,
    ) -> Result<(), Error> {
        let mut bitmap = match table.get(&key)? {
            Some(value_guard) => value_guard.value().clone(),
            None => RoaringTreemap::new(),
        };

        bitmap.insert(member);
        table.insert(key, bitmap)?;

        Ok(())
    }

    fn remove_member(
        table: &mut redb::Table<BucketedKey<u64>, RoaringValue>,
        key: BucketedKey<u64>,
        member: u64,
    ) -> Result<(), Error> {
        let bitmap = match table.get(&key)? {
            Some(value_guard) => value_guard.value().clone(),
            None => return Ok(()),
        };

        let mut bitmap = bitmap;
        bitmap.remove(member);

        if bitmap.is_empty() {
            table.remove(&key)?;
        } else {
            table.insert(key, bitmap)?;
        }

        Ok(())
    }

    fn copy_table(
        table: TableDefinition<'static, BucketedKey<u64>, RoaringValue>,
        rx: &ReadTransaction,
        wx: &WriteTransaction,
    ) -> Result<(), Error> {
        let source = rx.open_table(table)?;
        let mut target = wx.open_table(table)?;

        for entry in source.iter()? {
            let (key, value) = entry?;
            target.insert(key.value(), value.value().clone())?;
        }

        Ok(())
    }
}
