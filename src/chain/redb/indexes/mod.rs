use ::redb::{ReadTransaction, ReadableTable as _};
use ::redb::{TableDefinition, WriteTransaction};
use itertools::Itertools;
use pallas::ledger::addresses::Address;
use pallas::ledger::traverse::{ComputeHash, MultiEraBlock, MultiEraOutput};

use crate::ledger::LedgerDelta;
use crate::model::BlockSlot;

mod address;
mod address_payment_part;
mod address_stake_part;
mod asset;
mod block_hash;
mod block_number;
mod datum_hash;
mod policy;
mod script_hash;
mod tx_hash;

use address::AddressApproxIndexTable;
use address_payment_part::AddressPaymentPartApproxIndexTable;
use address_stake_part::AddressStakePartApproxIndexTable;
use asset::AssetApproxIndexTable;
use block_hash::BlockHashApproxIndexTable;
use block_number::BlockNumberApproxIndexTable;
use datum_hash::DatumHashApproxIndexTable;
use policy::PolicyApproxIndexTable;
use script_hash::ScriptHashApproxIndexTable;
use tx_hash::TxHashApproxIndexTable;

type Error = crate::chain::ChainError;

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
        wx.open_table(TxHashApproxIndexTable::DEF)?;

        Ok(())
    }

    pub fn get_by_address(rx: &ReadTransaction, address: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        AddressApproxIndexTable::get_by_address(rx, address)
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

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        if let Some(point) = &delta.new_position {
            let slot = point.0;

            let block =
                MultiEraBlock::decode(&delta.new_block).map_err(Error::BlockDecodingError)?;

            Self::insert(
                wx,
                BlockHashApproxIndexTable::DEF,
                BlockHashApproxIndexTable::compute_key,
                vec![block.hash().to_vec()],
                slot,
            )?;
            Self::insert(
                wx,
                BlockNumberApproxIndexTable::DEF,
                BlockNumberApproxIndexTable::compute_key,
                vec![block.number()],
                slot,
            )?;

            let tx_hashes = block.txs().iter().map(|tx| tx.hash().to_vec()).collect();
            Self::insert(
                wx,
                TxHashApproxIndexTable::DEF,
                TxHashApproxIndexTable::compute_key,
                tx_hashes,
                slot,
            )?;

            let script_hashes = block
                .txs()
                .iter()
                .flat_map(|tx| {
                    tx.aux_native_scripts()
                        .iter()
                        .map(|s| s.compute_hash().to_vec())
                        .collect_vec()
                })
                .collect_vec();
            Self::insert(
                wx,
                ScriptHashApproxIndexTable::DEF,
                ScriptHashApproxIndexTable::compute_key,
                script_hashes,
                slot,
            )?;

            let datum_hashes = block
                .txs()
                .iter()
                .flat_map(|tx| {
                    tx.aux_plutus_v1_scripts()
                        .iter()
                        .map(|d| d.compute_hash().to_vec())
                        .collect_vec()
                })
                .collect_vec();
            Self::insert(
                wx,
                DatumHashApproxIndexTable::DEF,
                DatumHashApproxIndexTable::compute_key,
                datum_hashes,
                slot,
            )?;

            let mut addresses = vec![];
            let mut address_payment_parts = vec![];
            let mut address_stake_parts = vec![];
            let mut policies = vec![];
            let mut assets = vec![];

            for (_, body) in delta.produced_utxo.iter().chain(delta.consumed_utxo.iter()) {
                let utxo = MultiEraOutput::try_from(body).map_err(Error::DecodingError)?;
                match utxo.address()? {
                    Address::Shelley(addr) => {
                        addresses.push(addr.to_vec());
                        address_payment_parts.push(addr.payment().to_vec());
                        address_stake_parts.push(addr.delegation().to_vec());
                    }
                    Address::Stake(addr) => {
                        addresses.push(addr.to_vec());
                        address_stake_parts.push(addr.to_vec());
                    }
                    Address::Byron(addr) => {
                        addresses.push(addr.to_vec());
                    }
                }

                for batch in utxo.value().assets() {
                    policies.push(batch.policy().to_vec());

                    for asset in batch.assets() {
                        let mut subject = asset.policy().to_vec();
                        subject.extend(asset.name());

                        assets.push(subject.to_vec());
                    }
                }
            }

            Self::insert(
                wx,
                AddressApproxIndexTable::DEF,
                AddressApproxIndexTable::compute_key,
                addresses,
                slot,
            )?;
            Self::insert(
                wx,
                AddressPaymentPartApproxIndexTable::DEF,
                AddressPaymentPartApproxIndexTable::compute_key,
                address_payment_parts,
                slot,
            )?;
            Self::insert(
                wx,
                AddressStakePartApproxIndexTable::DEF,
                AddressStakePartApproxIndexTable::compute_key,
                address_stake_parts,
                slot,
            )?;
            Self::insert(
                wx,
                PolicyApproxIndexTable::DEF,
                PolicyApproxIndexTable::compute_key,
                policies,
                slot,
            )?;
            Self::insert(
                wx,
                AssetApproxIndexTable::DEF,
                AssetApproxIndexTable::compute_key,
                assets,
                slot,
            )?;
        }

        if let Some(point) = &delta.undone_position {
            let slot = point.0;

            let block =
                MultiEraBlock::decode(&delta.undone_block).map_err(Error::BlockDecodingError)?;

            Self::insert(
                wx,
                BlockHashApproxIndexTable::DEF,
                BlockHashApproxIndexTable::compute_key,
                vec![block.hash().to_vec()],
                slot,
            )?;
            Self::insert(
                wx,
                BlockNumberApproxIndexTable::DEF,
                BlockNumberApproxIndexTable::compute_key,
                vec![block.number()],
                slot,
            )?;

            let tx_hashes = block.txs().iter().map(|tx| tx.hash().to_vec()).collect();
            Self::remove(
                wx,
                TxHashApproxIndexTable::DEF,
                TxHashApproxIndexTable::compute_key,
                tx_hashes,
                slot,
            )?;

            let script_hashes = block
                .txs()
                .iter()
                .flat_map(|tx| {
                    tx.aux_native_scripts()
                        .iter()
                        .map(|s| s.compute_hash().to_vec())
                        .collect_vec()
                })
                .collect_vec();
            Self::remove(
                wx,
                ScriptHashApproxIndexTable::DEF,
                ScriptHashApproxIndexTable::compute_key,
                script_hashes,
                slot,
            )?;

            let datum_hashes = block
                .txs()
                .iter()
                .flat_map(|tx| {
                    tx.aux_plutus_v1_scripts()
                        .iter()
                        .map(|d| d.compute_hash().to_vec())
                        .collect_vec()
                })
                .collect_vec();
            Self::remove(
                wx,
                DatumHashApproxIndexTable::DEF,
                DatumHashApproxIndexTable::compute_key,
                datum_hashes,
                slot,
            )?;

            let mut addresses = vec![];
            let mut address_payment_parts = vec![];
            let mut address_stake_parts = vec![];
            let mut policies = vec![];
            let mut assets = vec![];

            for (_, body) in delta.recovered_stxi.iter().chain(delta.undone_utxo.iter()) {
                let utxo = MultiEraOutput::try_from(body).map_err(Error::DecodingError)?;
                match utxo.address()? {
                    Address::Shelley(addr) => {
                        addresses.push(addr.to_vec());
                        address_payment_parts.push(addr.payment().to_vec());
                        address_stake_parts.push(addr.delegation().to_vec());
                    }
                    Address::Stake(addr) => {
                        addresses.push(addr.to_vec());
                        address_stake_parts.push(addr.to_vec());
                    }
                    Address::Byron(addr) => {
                        addresses.push(addr.to_vec());
                    }
                }

                for batch in utxo.value().assets() {
                    policies.push(batch.policy().to_vec());

                    for asset in batch.assets() {
                        let mut subject = asset.policy().to_vec();
                        subject.extend(asset.name());

                        assets.push(subject.to_vec());
                    }
                }
            }

            Self::remove(
                wx,
                AddressApproxIndexTable::DEF,
                AddressApproxIndexTable::compute_key,
                addresses,
                slot,
            )?;
            Self::remove(
                wx,
                AddressPaymentPartApproxIndexTable::DEF,
                AddressPaymentPartApproxIndexTable::compute_key,
                address_payment_parts,
                slot,
            )?;
            Self::remove(
                wx,
                AddressStakePartApproxIndexTable::DEF,
                AddressStakePartApproxIndexTable::compute_key,
                address_stake_parts,
                slot,
            )?;
            Self::remove(
                wx,
                PolicyApproxIndexTable::DEF,
                PolicyApproxIndexTable::compute_key,
                policies,
                slot,
            )?;
            Self::remove(
                wx,
                AssetApproxIndexTable::DEF,
                AssetApproxIndexTable::compute_key,
                assets,
                slot,
            )?;
        }

        Ok(())
    }

    pub fn insert<T>(
        wx: &WriteTransaction,
        table: TableDefinition<'static, u64, Vec<u64>>,
        compute_key: fn(&T) -> u64,
        inputs: Vec<T>,
        slot: u64,
    ) -> Result<(), Error> {
        let mut table = wx.open_table(table)?;
        for x in inputs {
            let key = compute_key(&x);

            let maybe_new = match table.get(key)? {
                Some(value) => {
                    let mut previous = value.value().clone();
                    if !previous.contains(&slot) {
                        previous.push(slot);
                        Some(previous)
                    } else {
                        None
                    }
                }
                None => Some(vec![slot]),
            };
            if let Some(new) = maybe_new {
                table.insert(key, new)?;
            }
        }

        Ok(())
    }

    pub fn remove<T>(
        wx: &WriteTransaction,
        table: TableDefinition<'static, u64, Vec<u64>>,
        compute_key: fn(&T) -> u64,
        inputs: Vec<T>,
        slot: u64,
    ) -> Result<(), Error> {
        let mut table = wx.open_table(table)?;

        for x in inputs {
            let key = compute_key(&x);

            let maybe_new = match table.get(key)? {
                Some(value) => {
                    let mut previous = value.value().clone();
                    match previous.iter().position(|x| *x == slot) {
                        Some(index) => {
                            previous.remove(index);
                            Some(previous)
                        }
                        None => None,
                    }
                }
                None => None,
            };
            if let Some(new) = maybe_new {
                table.insert(key, new)?;
            }
        }

        Ok(())
    }

    fn copy_table(
        table: TableDefinition<'static, u64, Vec<u64>>,
        rx: &ReadTransaction,
        wx: &WriteTransaction,
    ) -> Result<(), Error> {
        let source = rx.open_table(table)?;
        let mut target = wx.open_table(table)?;

        for entry in source.iter()? {
            let (k, v) = entry?;
            target.insert(k.value(), v.value())?;
        }

        Ok(())
    }
}
