use std::{collections::HashMap, path::Path, sync::Arc};

use dolos_core::{
    BlockBody, BlockSlot, ChainPoint, EraCbor, IndexError, IndexStore as CoreIndexStore, SlotTags,
    TxHash, TxOrder, UtxoSet, UtxoSetDelta,
};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::conway::PlutusData,
        traverse::{ComputeHash, MultiEraBlock, OriginalHash},
    },
};
use redb::{Database, Durability, ReadTransaction, ReadableDatabase, TableStats};
use tracing::warn;

use crate::{archive, state, Error};

const DEFAULT_CACHE_SIZE_MB: usize = 500;

fn map_db_error(error: impl std::fmt::Display) -> IndexError {
    IndexError::DbError(error.to_string())
}

fn map_codec_error(error: impl std::fmt::Display) -> IndexError {
    IndexError::CodecError(error.to_string())
}

impl From<Error> for IndexError {
    fn from(error: Error) -> Self {
        IndexError::DbError(error.to_string())
    }
}

impl From<archive::RedbArchiveError> for IndexError {
    fn from(error: archive::RedbArchiveError) -> Self {
        IndexError::DbError(error.to_string())
    }
}

#[derive(Clone)]
pub struct IndexStore {
    db: Arc<Database>,
    archive: archive::ArchiveStore,
}

impl IndexStore {
    pub fn open(
        path: impl AsRef<Path>,
        cache_size: Option<usize>,
        archive: archive::ArchiveStore,
    ) -> Result<Self, Error> {
        let db = Database::builder()
            .set_repair_callback(|x| {
                warn!(progress = x.progress() * 100f64, "index db is repairing")
            })
            .set_cache_size(1024 * 1024 * cache_size.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            .create(path)?;

        let store = Self {
            db: db.into(),
            archive,
        };

        store.initialize_schema_internal()?;

        Ok(store)
    }

    pub fn in_memory(archive: archive::ArchiveStore) -> Result<Self, Error> {
        let db =
            Database::builder().create_with_backend(::redb::backends::InMemoryBackend::new())?;

        let store = Self {
            db: db.into(),
            archive,
        };

        store.initialize_schema_internal()?;

        Ok(store)
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub fn count_utxo_by_address(&self, address: &[u8]) -> Result<u64, Error> {
        let rx = self.db.begin_read()?;
        state::utxoset::FilterIndexes::count_within_key(
            &rx,
            state::utxoset::FilterIndexes::BY_ADDRESS,
            address,
        )
    }

    pub fn iter_utxo_by_address(
        &self,
        address: &[u8],
    ) -> Result<state::utxoset::UtxoKeyIterator, Error> {
        let rx = self.db.begin_read()?;
        state::utxoset::FilterIndexes::iter_within_key(
            &rx,
            state::utxoset::FilterIndexes::BY_ADDRESS,
            address,
        )
    }

    pub fn utxo_index_stats(&self) -> Result<HashMap<&'static str, TableStats>, Error> {
        let rx = self.db.begin_read()?;
        state::utxoset::FilterIndexes::stats(&rx)
    }

    fn initialize_schema_internal(&self) -> Result<(), Error> {
        let mut wx = self.db.begin_write()?;
        wx.set_durability(Durability::Immediate)?;

        state::utxoset::FilterIndexes::initialize(&wx)?;
        archive::indexes::Indexes::initialize(&wx)?;

        wx.commit()?;

        Ok(())
    }

    fn archive_blocks(&self) -> Result<ReadTransaction, IndexError> {
        self.archive.db().begin_read().map_err(map_db_error)
    }

    fn index_bounds(&self) -> Result<(BlockSlot, BlockSlot), IndexError> {
        let end_slot = self
            .archive
            .get_tip()
            .map_err(map_db_error)?
            .map(|(slot, _)| slot)
            .unwrap_or_default();
        Ok((0, end_slot))
    }
}

pub struct IndexSparseIter {
    _index_rx: ReadTransaction,
    archive_rx: ReadTransaction,
    range: archive::indexes::SlotKeyIterator,
}

impl Iterator for IndexSparseIter {
    type Item = Result<(BlockSlot, Option<BlockBody>), IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.range.next()?;

        let Ok(slot) = next else {
            return Some(Err(map_db_error(next.err().unwrap())));
        };

        let block =
            archive::tables::BlocksTable::get_by_slot(&self.archive_rx, slot).map_err(map_db_error);

        Some(block.map(|body| (slot, body)))
    }
}

impl DoubleEndedIterator for IndexSparseIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        let next = self.range.next_back()?;

        let Ok(slot) = next else {
            return Some(Err(map_db_error(next.err().unwrap())));
        };

        let block =
            archive::tables::BlocksTable::get_by_slot(&self.archive_rx, slot).map_err(map_db_error);

        Some(block.map(|body| (slot, body)))
    }
}

impl CoreIndexStore for IndexStore {
    type SparseBlockIter = IndexSparseIter;

    fn initialize_schema(&self) -> Result<(), IndexError> {
        self.initialize_schema_internal().map_err(IndexError::from)
    }

    fn copy(&self, target: &Self) -> Result<(), IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let wx = target.db.begin_write().map_err(map_db_error)?;

        state::utxoset::FilterIndexes::copy(&rx, &wx).map_err(IndexError::from)?;
        archive::indexes::Indexes::copy(&rx, &wx).map_err(IndexError::from)?;

        wx.commit().map_err(map_db_error)?;

        Ok(())
    }

    fn apply_utxoset(&self, delta: &UtxoSetDelta) -> Result<(), IndexError> {
        let wx = self.db.begin_write().map_err(map_db_error)?;
        state::utxoset::FilterIndexes::apply(&wx, delta).map_err(IndexError::from)?;
        wx.commit().map_err(map_db_error)?;
        Ok(())
    }

    fn apply_archive_indexes(&self, point: &ChainPoint, tags: &SlotTags) -> Result<(), IndexError> {
        let wx = self.db.begin_write().map_err(map_db_error)?;
        archive::indexes::Indexes::apply(&wx, point, tags).map_err(IndexError::from)?;
        wx.commit().map_err(map_db_error)?;
        Ok(())
    }

    fn undo_archive_indexes(&self, point: &ChainPoint, tags: &SlotTags) -> Result<(), IndexError> {
        let wx = self.db.begin_write().map_err(map_db_error)?;
        archive::indexes::Indexes::undo(&wx, point, tags).map_err(IndexError::from)?;
        wx.commit().map_err(map_db_error)?;
        Ok(())
    }

    fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let out = state::utxoset::FilterIndexes::get_by_address(&rx, address)?;
        Ok(out)
    }

    fn get_utxo_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let out = state::utxoset::FilterIndexes::get_by_payment(&rx, payment)?;
        Ok(out)
    }

    fn get_utxo_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let out = state::utxoset::FilterIndexes::get_by_stake(&rx, stake)?;
        Ok(out)
    }

    fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let out = state::utxoset::FilterIndexes::get_by_policy(&rx, policy)?;
        Ok(out)
    }

    fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let out = state::utxoset::FilterIndexes::get_by_asset(&rx, asset)?;
        Ok(out)
    }

    fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        match archive::indexes::Indexes::get_by_block_hash(&rx, block_hash)? {
            Some(slot) => self.archive.get_block_by_slot(&slot).map_err(map_db_error),
            None => Ok(None),
        }
    }

    fn get_block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        match archive::indexes::Indexes::get_by_block_number(&rx, number)? {
            Some(slot) => self.archive.get_block_by_slot(&slot).map_err(map_db_error),
            None => Ok(None),
        }
    }

    fn get_block_with_tx(
        &self,
        tx_hash: &[u8],
    ) -> Result<Option<(BlockBody, TxOrder)>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let Some(slot) = archive::indexes::Indexes::get_by_tx_hash(&rx, tx_hash)? else {
            return Ok(None);
        };

        let Some(raw) = self
            .archive
            .get_block_by_slot(&slot)
            .map_err(map_db_error)?
        else {
            return Ok(None);
        };

        let block = MultiEraBlock::decode(raw.as_slice()).map_err(map_codec_error)?;
        if let Some((idx, _)) = block
            .txs()
            .iter()
            .enumerate()
            .find(|(_, tx)| tx.hash().to_vec() == tx_hash)
        {
            return Ok(Some((raw, idx)));
        }

        Ok(None)
    }

    fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let Some(slot) = archive::indexes::Indexes::get_by_tx_hash(&rx, tx_hash)? else {
            return Ok(None);
        };

        let Some(raw) = self
            .archive
            .get_block_by_slot(&slot)
            .map_err(map_db_error)?
        else {
            return Ok(None);
        };

        let block = MultiEraBlock::decode(raw.as_slice()).map_err(map_codec_error)?;
        if let Some(tx) = block.txs().iter().find(|x| x.hash().to_vec() == tx_hash) {
            return Ok(Some(EraCbor(block.era().into(), tx.encode())));
        }

        Ok(None)
    }

    fn get_plutus_data(&self, datum_hash: &Hash<32>) -> Result<Option<PlutusData>, IndexError> {
        use pallas::ledger::primitives::conway::DatumOption;

        let (start_slot, end_slot) = self.index_bounds()?;
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let slots = archive::indexes::Indexes::get_by_datum_hash(
            &rx,
            datum_hash.as_slice(),
            start_slot,
            end_slot,
        )?;

        for slot in slots {
            let Some(raw) = self
                .archive
                .get_block_by_slot(&slot)
                .map_err(map_db_error)?
            else {
                continue;
            };

            let block = MultiEraBlock::decode(raw.as_slice()).map_err(map_codec_error)?;
            for tx in block.txs() {
                if let Some(plutus_data) = tx.find_plutus_data(datum_hash) {
                    return Ok(Some(plutus_data.clone().unwrap()));
                }

                for (_, output) in tx.produces() {
                    if let Some(DatumOption::Data(data)) = output.datum() {
                        if &data.original_hash() == datum_hash {
                            return Ok(Some(data.clone().unwrap().unwrap()));
                        }
                    }
                }

                for redeemer in tx.redeemers() {
                    if &redeemer.data().compute_hash() == datum_hash {
                        return Ok(Some(redeemer.data().clone()));
                    }
                }
            }
        }

        Ok(None)
    }

    fn get_slot_for_tx(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        archive::indexes::Indexes::get_by_tx_hash(&rx, tx_hash).map_err(IndexError::from)
    }

    fn get_tx_by_spent_txo(&self, spent_txo: &[u8]) -> Result<Option<TxHash>, IndexError> {
        let (start_slot, end_slot) = self.index_bounds()?;
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let slots =
            archive::indexes::Indexes::get_by_spent_txo(&rx, spent_txo, start_slot, end_slot)?;

        for slot in slots {
            let Some(raw) = self
                .archive
                .get_block_by_slot(&slot)
                .map_err(map_db_error)?
            else {
                continue;
            };

            let block = MultiEraBlock::decode(raw.as_slice()).map_err(map_codec_error)?;
            for tx in block.txs().iter() {
                for input in tx.inputs() {
                    let bytes: Vec<u8> = dolos_core::TxoRef::from(&input).into();
                    if bytes.as_slice() == spent_txo {
                        return Ok(Some(tx.hash()));
                    }
                }
            }
        }

        Ok(None)
    }

    fn iter_blocks_with_address(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, IndexError> {
        let index_rx = self.db.begin_read().map_err(map_db_error)?;
        let archive_rx = self.archive_blocks()?;
        let range =
            archive::indexes::Indexes::iter_by_address(&index_rx, address, start_slot, end_slot)?;
        Ok(IndexSparseIter {
            _index_rx: index_rx,
            archive_rx,
            range,
        })
    }

    fn iter_blocks_with_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, IndexError> {
        let index_rx = self.db.begin_read().map_err(map_db_error)?;
        let archive_rx = self.archive_blocks()?;
        let range =
            archive::indexes::Indexes::iter_by_asset(&index_rx, asset, start_slot, end_slot)?;
        Ok(IndexSparseIter {
            _index_rx: index_rx,
            archive_rx,
            range,
        })
    }

    fn iter_blocks_with_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, IndexError> {
        let index_rx = self.db.begin_read().map_err(map_db_error)?;
        let archive_rx = self.archive_blocks()?;
        let range =
            archive::indexes::Indexes::iter_by_payment(&index_rx, payment, start_slot, end_slot)?;
        Ok(IndexSparseIter {
            _index_rx: index_rx,
            archive_rx,
            range,
        })
    }

    fn iter_blocks_with_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, IndexError> {
        let index_rx = self.db.begin_read().map_err(map_db_error)?;
        let archive_rx = self.archive_blocks()?;
        let range =
            archive::indexes::Indexes::iter_by_stake(&index_rx, stake, start_slot, end_slot)?;
        Ok(IndexSparseIter {
            _index_rx: index_rx,
            archive_rx,
            range,
        })
    }

    fn iter_blocks_with_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, IndexError> {
        let index_rx = self.db.begin_read().map_err(map_db_error)?;
        let archive_rx = self.archive_blocks()?;
        let range = archive::indexes::Indexes::iter_by_account_certs(
            &index_rx, account, start_slot, end_slot,
        )?;
        Ok(IndexSparseIter {
            _index_rx: index_rx,
            archive_rx,
            range,
        })
    }

    fn iter_blocks_with_metadata(
        &self,
        metadata: &u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, IndexError> {
        let index_rx = self.db.begin_read().map_err(map_db_error)?;
        let archive_rx = self.archive_blocks()?;
        let range =
            archive::indexes::Indexes::iter_by_metadata(&index_rx, metadata, start_slot, end_slot)?;
        Ok(IndexSparseIter {
            _index_rx: index_rx,
            archive_rx,
            range,
        })
    }
}
