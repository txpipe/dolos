use ::redb::{ReadTransaction, ReadableTable as _};
use ::redb::{TableDefinition, WriteTransaction};
use pallas::ledger::addresses::Address;
use pallas::ledger::traverse::MultiEraOutput;
use std::hash::{DefaultHasher, Hash as _, Hasher};

use crate::ledger::LedgerDelta;
use crate::model::BlockSlot;

type Error = crate::chain::ChainError;

pub struct AddressApproxIndexTable;
impl AddressApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, Vec<u64>> =
        TableDefinition::new("addressapproxindex");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;

        Ok(())
    }

    pub fn compute_key(address: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        address.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_address(rx: &ReadTransaction, address: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let default = Ok(vec![]);
        let key = Self::compute_key(address);
        match table.get(key)? {
            Some(value) => Ok(value.value().clone()),
            None => default,
        }
    }

    fn insert(wx: &WriteTransaction, addresses: Vec<Vec<u8>>, slot: u64) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;
        for address in addresses {
            let key = Self::compute_key(&address);

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

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        if let Some(point) = &delta.new_position {
            // Produced
            let produced = delta
                .produced_utxo
                .values()
                .map(|body| {
                    let body = MultiEraOutput::try_from(body).map_err(Error::DecodingError)?;
                    match body.address()? {
                        Address::Shelley(add) => Ok(add.to_vec()),
                        Address::Byron(add) => Ok(add.to_vec()),
                        Address::Stake(add) => Ok(add.to_vec()),
                    }
                })
                .collect::<Result<Vec<Vec<u8>>, Error>>()?;

            // Consumed
            let consumed = delta
                .consumed_utxo
                .values()
                .map(|body| {
                    let body = MultiEraOutput::try_from(body).map_err(Error::DecodingError)?;
                    match body.address()? {
                        Address::Shelley(add) => Ok(add.to_vec()),
                        Address::Byron(add) => Ok(add.to_vec()),
                        Address::Stake(add) => Ok(add.to_vec()),
                    }
                })
                .collect::<Result<Vec<Vec<u8>>, Error>>()?;

            Self::insert(
                wx,
                produced.into_iter().chain(consumed.into_iter()).collect(),
                point.0,
            )?;
        }

        if let Some(point) = &delta.undone_position {
            // Produced
            let recovered = delta
                .recovered_stxi
                .values()
                .map(|body| {
                    let body = MultiEraOutput::try_from(body).map_err(Error::DecodingError)?;
                    match body.address()? {
                        Address::Shelley(add) => Ok(add.to_vec()),
                        Address::Byron(add) => Ok(add.to_vec()),
                        Address::Stake(add) => Ok(add.to_vec()),
                    }
                })
                .collect::<Result<Vec<Vec<u8>>, Error>>()?;

            // Consumed
            let undone = delta
                .undone_utxo
                .values()
                .map(|body| {
                    let body = MultiEraOutput::try_from(body).map_err(Error::DecodingError)?;
                    match body.address()? {
                        Address::Shelley(add) => Ok(add.to_vec()),
                        Address::Byron(add) => Ok(add.to_vec()),
                        Address::Stake(add) => Ok(add.to_vec()),
                    }
                })
                .collect::<Result<Vec<Vec<u8>>, Error>>()?;

            Self::remove(
                wx,
                recovered.into_iter().chain(undone.into_iter()).collect(),
                point.0,
            )?;
        }

        Ok(())
    }

    fn remove(wx: &WriteTransaction, addresses: Vec<Vec<u8>>, slot: u64) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

        for address in addresses {
            let key = Self::compute_key(&address);

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

    pub fn copy(rx: &ReadTransaction, wx: &WriteTransaction) -> Result<(), Error> {
        let source = rx.open_table(Self::DEF)?;
        let mut target = wx.open_table(Self::DEF)?;

        for entry in source.iter()? {
            let (k, v) = entry?;
            target.insert(k.value(), v.value())?;
        }

        Ok(())
    }
}

pub struct AddressPaymentPartApproxIndexTable;
impl AddressPaymentPartApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, Vec<u64>> =
        TableDefinition::new("addresspaymentpartapproxindex");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;

        Ok(())
    }

    pub fn compute_key(address: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        address.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_address_payment_part(
        rx: &ReadTransaction,
        address_payment_part: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let default = Ok(vec![]);
        let key = Self::compute_key(address_payment_part);
        match table.get(key)? {
            Some(value) => Ok(value.value().clone()),
            None => default,
        }
    }

    fn insert(
        wx: &WriteTransaction,
        address_payment_parts: Vec<Vec<u8>>,
        slot: u64,
    ) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;
        for address in address_payment_parts {
            let key = Self::compute_key(&address);

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

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let extract = |body| match MultiEraOutput::try_from(body).map_err(Error::DecodingError) {
            Ok(body) => match body.address().map_err(Error::AddressDecoding) {
                Ok(Address::Shelley(add)) => Some(Ok(add.payment().to_vec())),
                Err(e) => Some(Err(e)),
                _ => None,
            },
            Err(e) => Some(Err(e)),
        };

        if let Some(point) = &delta.new_position {
            // Produced
            let produced = delta
                .produced_utxo
                .values()
                .flat_map(extract)
                .collect::<Result<Vec<Vec<u8>>, Error>>()?;

            // Consumed
            let consumed = delta
                .consumed_utxo
                .values()
                .flat_map(extract)
                .collect::<Result<Vec<Vec<u8>>, Error>>()?;

            Self::insert(
                wx,
                produced.into_iter().chain(consumed.into_iter()).collect(),
                point.0,
            )?;
        }

        if let Some(point) = &delta.undone_position {
            // Produced
            let recovered = delta
                .recovered_stxi
                .values()
                .flat_map(extract)
                .collect::<Result<Vec<Vec<u8>>, Error>>()?;

            // Consumed
            let undone = delta
                .undone_utxo
                .values()
                .flat_map(extract)
                .collect::<Result<Vec<Vec<u8>>, Error>>()?;

            Self::remove(
                wx,
                recovered.into_iter().chain(undone.into_iter()).collect(),
                point.0,
            )?;
        }

        Ok(())
    }

    fn remove(
        wx: &WriteTransaction,
        address_payment_parts: Vec<Vec<u8>>,
        slot: u64,
    ) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

        for address in address_payment_parts {
            let key = Self::compute_key(&address);

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

    pub fn copy(rx: &ReadTransaction, wx: &WriteTransaction) -> Result<(), Error> {
        let source = rx.open_table(Self::DEF)?;
        let mut target = wx.open_table(Self::DEF)?;

        for entry in source.iter()? {
            let (k, v) = entry?;
            target.insert(k.value(), v.value())?;
        }

        Ok(())
    }
}

pub struct AddressStakePartApproxIndexTable;
impl AddressStakePartApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, Vec<u64>> =
        TableDefinition::new("addressstakepartapproxindextable");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;

        Ok(())
    }

    pub fn compute_key(address_stake_part: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        address_stake_part.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_address_stake_part(
        rx: &ReadTransaction,
        address_stake_part: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let default = Ok(vec![]);
        let key = Self::compute_key(address_stake_part);
        match table.get(key)? {
            Some(value) => Ok(value.value().clone()),
            None => default,
        }
    }

    fn insert(
        wx: &WriteTransaction,
        address_stake_parts: Vec<Vec<u8>>,
        slot: u64,
    ) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;
        for address in address_stake_parts {
            let key = Self::compute_key(&address);

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

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let extract = |body| match MultiEraOutput::try_from(body).map_err(Error::DecodingError) {
            Ok(body) => match body.address().map_err(Error::AddressDecoding) {
                Ok(Address::Shelley(add)) => Some(Ok(add.delegation().to_vec())),
                Ok(Address::Stake(add)) => Some(Ok(add.to_vec())),
                Err(e) => Some(Err(e)),
                _ => None,
            },
            Err(e) => Some(Err(e)),
        };

        if let Some(point) = &delta.new_position {
            // Produced
            let produced = delta
                .produced_utxo
                .values()
                .flat_map(extract)
                .collect::<Result<Vec<Vec<u8>>, Error>>()?;

            // Consumed
            let consumed = delta
                .consumed_utxo
                .values()
                .flat_map(extract)
                .collect::<Result<Vec<Vec<u8>>, Error>>()?;

            Self::insert(
                wx,
                produced.into_iter().chain(consumed.into_iter()).collect(),
                point.0,
            )?;
        }

        if let Some(point) = &delta.undone_position {
            // Produced
            let recovered = delta
                .recovered_stxi
                .values()
                .flat_map(extract)
                .collect::<Result<Vec<Vec<u8>>, Error>>()?;

            // Consumed
            let undone = delta
                .undone_utxo
                .values()
                .flat_map(extract)
                .collect::<Result<Vec<Vec<u8>>, Error>>()?;

            Self::remove(
                wx,
                recovered.into_iter().chain(undone.into_iter()).collect(),
                point.0,
            )?;
        }

        Ok(())
    }

    fn remove(
        wx: &WriteTransaction,
        address_stake_parts: Vec<Vec<u8>>,
        slot: u64,
    ) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

        for address in address_stake_parts {
            let key = Self::compute_key(&address);

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

    pub fn copy(rx: &ReadTransaction, wx: &WriteTransaction) -> Result<(), Error> {
        let source = rx.open_table(Self::DEF)?;
        let mut target = wx.open_table(Self::DEF)?;

        for entry in source.iter()? {
            let (k, v) = entry?;
            target.insert(k.value(), v.value())?;
        }

        Ok(())
    }
}

pub struct TxsApproxIndexTable;
impl TxsApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, Vec<u64>> = TableDefinition::new("txsapproxindex");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;

        Ok(())
    }

    pub fn compute_key(tx_hash: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        tx_hash.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_tx_hash(rx: &ReadTransaction, tx_hash: &[u8]) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let default = Ok(vec![]);
        let key = Self::compute_key(tx_hash);
        match table.get(key)? {
            Some(value) => Ok(value.value().clone()),
            None => default,
        }
    }

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

        if let Some(point) = &delta.new_position {
            let slot = point.0;
            let tx_hashes = delta
                .new_txs
                .keys()
                .map(|hash| hash.to_vec())
                .collect::<Vec<Vec<u8>>>();

            for tx_hash in tx_hashes {
                let key = Self::compute_key(&tx_hash);

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
        }

        if let Some(point) = &delta.undone_position {
            let slot = point.0;
            let tx_hashes = delta
                .undone_txs
                .keys()
                .map(|hash| hash.to_vec())
                .collect::<Vec<Vec<u8>>>();

            for tx_hash in tx_hashes {
                let key = Self::compute_key(&tx_hash);

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
        }

        Ok(())
    }

    pub fn copy(rx: &ReadTransaction, wx: &WriteTransaction) -> Result<(), Error> {
        let source = rx.open_table(Self::DEF)?;
        let mut target = wx.open_table(Self::DEF)?;

        for entry in source.iter()? {
            let (k, v) = entry?;
            target.insert(k.value(), v.value())?;
        }

        Ok(())
    }
}

pub struct BlockHashApproxIndexTable;
impl BlockHashApproxIndexTable {
    pub const DEF: TableDefinition<'static, u64, Vec<u64>> =
        TableDefinition::new("blockhashapproxindex");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;

        Ok(())
    }

    pub fn compute_key(block_hash: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        block_hash.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_by_block_hash(
        rx: &ReadTransaction,
        block_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, Error> {
        let table = rx.open_table(Self::DEF)?;
        let default = Ok(vec![]);
        let key = Self::compute_key(block_hash);
        match table.get(key)? {
            Some(value) => Ok(value.value().clone()),
            None => default,
        }
    }

    pub fn apply(wx: &WriteTransaction, delta: &LedgerDelta) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

        if let Some(point) = &delta.new_position {
            let key = Self::compute_key(point.1.as_ref());
            let slot = point.0;

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

        if let Some(point) = &delta.undone_position {
            let key = Self::compute_key(point.1.as_ref());
            let slot = point.0;

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

    pub fn copy(rx: &ReadTransaction, wx: &WriteTransaction) -> Result<(), Error> {
        let source = rx.open_table(Self::DEF)?;
        let mut target = wx.open_table(Self::DEF)?;

        for entry in source.iter()? {
            let (k, v) = entry?;
            target.insert(k.value(), v.value())?;
        }

        Ok(())
    }
}
