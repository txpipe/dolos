use std::sync::Arc;

use tokio::sync::Semaphore;

use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    archive::ArchiveStore, indexes::IndexStore, ArchiveError, BlockBody, BlockHash, BlockHeight,
    BlockSlot, ChainError, ChainPoint, Domain, DomainError, EraCbor, IndexError, TagDimension,
    TxHash, TxOrder,
};

/// Lightweight block metadata for a transaction, extracted via a single decode.
///
/// Returned by `block_meta_by_tx_hash`. Callers that need the full block body
/// should use `block_by_tx_hash` instead.
#[derive(Debug, Clone)]
pub struct BlockRefMeta {
    pub slot: BlockSlot,
    pub hash: BlockHash,
    pub height: BlockHeight,
    pub tx_hash: TxHash,
    pub tx_index: TxOrder,
}

#[derive(Debug, Clone)]
pub struct AsyncQueryOptions {
    pub max_blocking: usize,
}

impl Default for AsyncQueryOptions {
    fn default() -> Self {
        Self { max_blocking: 16 }
    }
}

#[derive(Clone)]
pub struct AsyncQueryFacade<D: Domain> {
    inner: D,
    limiter: Arc<Semaphore>,
    options: AsyncQueryOptions,
}

impl<D: Domain> AsyncQueryFacade<D>
where
    D: Clone + Send + Sync + 'static,
{
    pub fn new(inner: D) -> Self {
        Self::with_options(inner, AsyncQueryOptions::default())
    }

    pub fn with_options(inner: D, options: AsyncQueryOptions) -> Self {
        let limiter = Arc::new(Semaphore::new(options.max_blocking));
        Self {
            inner,
            limiter,
            options,
        }
    }

    pub fn options(&self) -> &AsyncQueryOptions {
        &self.options
    }

    pub async fn run_blocking<T, F>(&self, f: F) -> Result<T, DomainError>
    where
        T: Send + 'static,
        F: FnOnce(D) -> Result<T, DomainError> + Send + 'static,
    {
        let permit = self.limiter.clone().acquire_owned().await.map_err(|_| {
            DomainError::ArchiveError(ArchiveError::InternalError(
                "query limiter closed".to_string(),
            ))
        })?;
        let inner = self.inner.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(inner)
        });

        handle
            .await
            .map_err(|e| DomainError::ArchiveError(ArchiveError::InternalError(e.to_string())))?
    }

    pub async fn block_by_slot(&self, slot: BlockSlot) -> Result<Option<BlockBody>, DomainError> {
        self.run_blocking(move |domain| Ok(domain.archive().get_block_by_slot(&slot)?))
            .await
    }

    pub async fn block_by_hash(&self, hash: Vec<u8>) -> Result<Option<BlockBody>, DomainError> {
        self.run_blocking(move |domain| {
            let slot = domain.indexes().slot_by_block_hash(&hash)?;
            match slot {
                Some(slot) => Ok(domain.archive().get_block_by_slot(&slot)?),
                None => Ok(None),
            }
        })
        .await
    }

    pub async fn block_by_number(&self, number: u64) -> Result<Option<BlockBody>, DomainError> {
        self.run_blocking(move |domain| {
            let slot = domain.indexes().slot_by_block_number(number)?;
            match slot {
                Some(slot) => Ok(domain.archive().get_block_by_slot(&slot)?),
                None => Ok(None),
            }
        })
        .await
    }

    pub async fn slot_by_number(&self, number: u64) -> Result<Option<BlockSlot>, DomainError> {
        self.run_blocking(move |domain| Ok(domain.indexes().slot_by_block_number(number)?))
            .await
    }

    pub async fn block_by_tx_hash(
        &self,
        tx_hash: Vec<u8>,
    ) -> Result<Option<(BlockBody, TxOrder)>, DomainError> {
        let tx_hash_lookup = tx_hash.clone();
        let Some(raw) = self
            .run_blocking(move |domain| {
                let slot = domain.indexes().slot_by_tx_hash(&tx_hash_lookup)?;
                let Some(slot) = slot else {
                    return Ok(None);
                };

                Ok(domain.archive().get_block_by_slot(&slot)?)
            })
            .await?
        else {
            return Ok(None);
        };

        let block = MultiEraBlock::decode(raw.as_slice())
            .map_err(|e| DomainError::ChainError(ChainError::DecodingError(e)))?;
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

    /// Look up the block containing a given transaction hash and return only
    /// chain-point metadata, decoding the block once inside the blocking task.
    ///
    /// Prefer this over `block_by_tx_hash` when only the chain point is needed —
    /// it avoids a second `MultiEraBlock::decode` in the caller.
    pub async fn block_meta_by_tx_hash(
        &self,
        tx_hash: Vec<u8>,
    ) -> Result<Option<BlockRefMeta>, DomainError> {
        self.run_blocking(move |domain| {
            let Some(slot) = domain.indexes().slot_by_tx_hash(&tx_hash)? else {
                return Ok(None);
            };
            let Some(raw) = domain.archive().get_block_by_slot(&slot)? else {
                return Ok(None);
            };
            let block = MultiEraBlock::decode(raw.as_slice())
                .map_err(|e| DomainError::ChainError(ChainError::DecodingError(e)))?;
            let Some((tx_index, _)) = block
                .txs()
                .iter()
                .enumerate()
                .find(|(_, tx)| tx.hash().as_slice() == tx_hash.as_slice())
            else {
                return Ok(None);
            };
            Ok(Some(BlockRefMeta {
                slot: block.slot(),
                hash: block.hash(),
                height: block.number(),
                tx_hash: tx_hash.as_slice().into(),
                tx_index,
            }))
        })
        .await
    }

    pub async fn tx_cbor(&self, tx_hash: Vec<u8>) -> Result<Option<EraCbor>, DomainError> {
        let tx_hash_lookup = tx_hash.clone();
        let Some(raw) = self
            .run_blocking(move |domain| {
                let slot = domain.indexes().slot_by_tx_hash(&tx_hash_lookup)?;
                let Some(slot) = slot else {
                    return Ok(None);
                };

                Ok(domain.archive().get_block_by_slot(&slot)?)
            })
            .await?
        else {
            return Ok(None);
        };

        let block = MultiEraBlock::decode(raw.as_slice())
            .map_err(|e| DomainError::ChainError(ChainError::DecodingError(e)))?;
        if let Some(tx) = block.txs().iter().find(|x| x.hash().to_vec() == tx_hash) {
            return Ok(Some(EraCbor(block.era().into(), tx.encode())));
        }

        Ok(None)
    }

    pub async fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: Vec<u8>,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, DomainError> {
        self.run_blocking(move |domain| {
            let slots = domain
                .indexes()
                .slots_by_tag(dimension, &key, start_slot, end_slot)?
                .collect::<Result<Vec<_>, IndexError>>()?;
            Ok(slots)
        })
        .await
    }

    pub async fn find_intersect(
        &self,
        intersect: Vec<ChainPoint>,
    ) -> Result<Option<ChainPoint>, DomainError> {
        self.run_blocking(move |domain| Ok(domain.archive().find_intersect(&intersect)?))
            .await
    }
}
