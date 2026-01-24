use ::redb::{
    MultimapTableDefinition, ReadTransaction, ReadableTable as _, TableDefinition, WriteTransaction,
};
use redb_extras::buckets::{
    BucketMultimapIterExt, BucketRangeMultimapIterator, BucketedKey, KeyBuilder,
};
use xxhash_rust::xxh3::xxh3_64;

use dolos_core::BlockSlot;

type Error = super::RedbArchiveError;

const BUCKET_SIZE: u64 = 432_000;

pub fn key_builder() -> KeyBuilder {
    KeyBuilder::new(BUCKET_SIZE).expect("bucket size must be positive")
}

pub struct SlotKeyIterator {
    inner: BucketRangeMultimapIterator<u64>,
    start_slot: BlockSlot,
    end_slot: BlockSlot,
}

impl SlotKeyIterator {
    pub fn new(
        inner: BucketRangeMultimapIterator<u64>,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Self {
        Self {
            inner,
            start_slot,
            end_slot,
        }
    }
}

impl Iterator for SlotKeyIterator {
    type Item = Result<u64, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let slot = match self.inner.next()? {
            Ok(slot) => slot,
            Err(err) => return Some(Err(err.into())),
        };

        if slot >= self.start_slot && slot <= self.end_slot {
            Some(Ok(slot))
        } else {
            None
        }
    }
}

impl DoubleEndedIterator for SlotKeyIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        let slot = match self.inner.next_back()? {
            Ok(slot) => slot,
            Err(err) => return Some(Err(err.into())),
        };

        if slot >= self.start_slot && slot <= self.end_slot {
            Some(Ok(slot))
        } else {
            None
        }
    }
}

fn slot_iterator(
    rx: &ReadTransaction,
    table: MultimapTableDefinition<'static, BucketedKey<u64>, u64>,
    base_key: u64,
    start_slot: u64,
    end_slot: u64,
) -> Result<SlotKeyIterator, Error> {
    let table = rx.open_multimap_table(table)?;
    let key_builder = key_builder();
    let iter = table.bucket_range(&key_builder, base_key, start_slot, end_slot)?;

    Ok(SlotKeyIterator::new(iter, start_slot, end_slot))
}

pub struct AddressApproxIndexTable;

impl AddressApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, BucketedKey<u64>, u64> =
        MultimapTableDefinition::new("archive-byaddress");

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
    pub const DEF: MultimapTableDefinition<'static, BucketedKey<u64>, u64> =
        MultimapTableDefinition::new("archive-bypayment");

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
    pub const DEF: MultimapTableDefinition<'static, BucketedKey<u64>, u64> =
        MultimapTableDefinition::new("archive-bystake");

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
    pub const DEF: MultimapTableDefinition<'static, BucketedKey<u64>, u64> =
        MultimapTableDefinition::new("archive-byasset");

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
        TableDefinition::new("archive-byblockhash");

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
    pub const DEF: TableDefinition<'static, u64, u64> =
        TableDefinition::new("archive-byblocknumber");

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
    pub const DEF: MultimapTableDefinition<'static, BucketedKey<u64>, u64> =
        MultimapTableDefinition::new("archive-bydatum");

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

    pub fn iter_by_datum(
        rx: &ReadTransaction,
        datum_hash: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        let key = Self::compute_key(&datum_hash.to_vec());
        slot_iterator(rx, Self::DEF, key, start_slot, end_slot)
    }
}

pub struct MetadataApproxIndexTable;

impl MetadataApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, BucketedKey<u64>, u64> =
        MultimapTableDefinition::new("archive-bymetadata");

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
    pub const DEF: MultimapTableDefinition<'static, BucketedKey<u64>, u64> =
        MultimapTableDefinition::new("archive-bypolicy");

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

    pub fn iter_by_policy(
        rx: &ReadTransaction,
        policy: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        let key = Self::compute_key(&policy.to_vec());
        slot_iterator(rx, Self::DEF, key, start_slot, end_slot)
    }
}

pub struct ScriptHashApproxIndexTable;

impl ScriptHashApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, BucketedKey<u64>, u64> =
        MultimapTableDefinition::new("archive-byscript");

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

    pub fn iter_by_script(
        rx: &ReadTransaction,
        script_hash: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        let key = Self::compute_key(&script_hash.to_vec());
        slot_iterator(rx, Self::DEF, key, start_slot, end_slot)
    }
}

pub struct SpentTxoApproxIndexTable;

impl SpentTxoApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, BucketedKey<u64>, u64> =
        MultimapTableDefinition::new("archive-byspenttxo");

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

    pub fn iter_by_spent_txo(
        rx: &ReadTransaction,
        spent_txo: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        let key = Self::compute_key(&spent_txo.to_vec());
        slot_iterator(rx, Self::DEF, key, start_slot, end_slot)
    }
}

pub struct AccountCertsApproxIndexTable;

impl AccountCertsApproxIndexTable {
    pub const DEF: MultimapTableDefinition<'static, BucketedKey<u64>, u64> =
        MultimapTableDefinition::new("archive-bystakeactions");

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
    pub const DEF: TableDefinition<'static, &'static [u8], u64> =
        TableDefinition::new("archive-bytx");

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
        wx.open_multimap_table(AddressApproxIndexTable::DEF)?;
        wx.open_multimap_table(AddressPaymentPartApproxIndexTable::DEF)?;
        wx.open_multimap_table(AddressStakePartApproxIndexTable::DEF)?;
        wx.open_multimap_table(AssetApproxIndexTable::DEF)?;
        wx.open_table(BlockHashIndexTable::DEF)?;
        wx.open_table(BlockNumberIndexTable::DEF)?;
        wx.open_multimap_table(DatumHashApproxIndexTable::DEF)?;
        wx.open_multimap_table(PolicyApproxIndexTable::DEF)?;
        wx.open_multimap_table(ScriptHashApproxIndexTable::DEF)?;
        wx.open_multimap_table(SpentTxoApproxIndexTable::DEF)?;
        wx.open_multimap_table(AccountCertsApproxIndexTable::DEF)?;
        wx.open_table(TxHashIndexTable::DEF)?;
        wx.open_multimap_table(MetadataApproxIndexTable::DEF)?;

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

    pub fn iter_by_policy(
        rx: &ReadTransaction,
        policy: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        PolicyApproxIndexTable::iter_by_policy(rx, policy, start_slot, end_slot)
    }

    pub fn iter_by_datum(
        rx: &ReadTransaction,
        datum_hash: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        DatumHashApproxIndexTable::iter_by_datum(rx, datum_hash, start_slot, end_slot)
    }

    pub fn iter_by_spent_txo(
        rx: &ReadTransaction,
        spent_txo: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        SpentTxoApproxIndexTable::iter_by_spent_txo(rx, spent_txo, start_slot, end_slot)
    }

    pub fn iter_by_script(
        rx: &ReadTransaction,
        script_hash: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SlotKeyIterator, Error> {
        ScriptHashApproxIndexTable::iter_by_script(rx, script_hash, start_slot, end_slot)
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

    fn copy_table(
        table: MultimapTableDefinition<'static, BucketedKey<u64>, u64>,
        rx: &ReadTransaction,
        wx: &WriteTransaction,
    ) -> Result<(), Error> {
        let source = rx.open_multimap_table(table)?;
        let mut target = wx.open_multimap_table(table)?;

        let all = source.range::<BucketedKey<u64>>(..)?;
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
