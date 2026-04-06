use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::{
    archive::ArchiveStore, indexes::IndexStore, ArchiveError, BlockBody, BlockSlot, ChainError,
    ChainLogic, ChainPoint, Domain, DomainError, IndexError, TagDimension, TaggedPayload, TxOrder,
};

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

    pub async fn run_blocking<T, F>(&self, f: F) -> Result<T, DomainError<D::ChainSpecificError>>
    where
        T: Send + 'static,
        F: FnOnce(D) -> Result<T, DomainError<D::ChainSpecificError>> + Send + 'static,
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

    pub async fn block_by_slot(
        &self,
        slot: BlockSlot,
    ) -> Result<Option<BlockBody>, DomainError<D::ChainSpecificError>> {
        self.run_blocking(move |domain| Ok(domain.archive().get_block_by_slot(&slot)?))
            .await
    }

    pub async fn block_by_hash(
        &self,
        hash: Vec<u8>,
    ) -> Result<Option<BlockBody>, DomainError<D::ChainSpecificError>> {
        self.run_blocking(move |domain| {
            let slot = domain.indexes().slot_by_block_hash(&hash)?;
            match slot {
                Some(slot) => Ok(domain.archive().get_block_by_slot(&slot)?),
                None => Ok(None),
            }
        })
        .await
    }

    pub async fn block_by_number(
        &self,
        number: u64,
    ) -> Result<Option<BlockBody>, DomainError<D::ChainSpecificError>> {
        self.run_blocking(move |domain| {
            let slot = domain.indexes().slot_by_block_number(number)?;
            match slot {
                Some(slot) => Ok(domain.archive().get_block_by_slot(&slot)?),
                None => Ok(None),
            }
        })
        .await
    }

    pub async fn slot_by_number(
        &self,
        number: u64,
    ) -> Result<Option<BlockSlot>, DomainError<D::ChainSpecificError>> {
        self.run_blocking(move |domain| Ok(domain.indexes().slot_by_block_number(number)?))
            .await
    }

    pub async fn block_by_tx_hash(
        &self,
        tx_hash: Vec<u8>,
    ) -> Result<Option<(BlockBody, TxOrder)>, DomainError<D::ChainSpecificError>> {
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

        let result = D::Chain::find_tx_in_block(&raw, &tx_hash)
            .map_err(|err| DomainError::ChainError(ChainError::ChainSpecific(err)))?;

        Ok(result.map(|(_, ix)| (raw, ix)))
    }

    pub async fn tx_cbor(
        &self,
        tx_hash: Vec<u8>,
    ) -> Result<Option<TaggedPayload>, DomainError<D::ChainSpecificError>> {
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

        D::Chain::find_tx_in_block(&raw, &tx_hash)
            .map_err(|err| DomainError::ChainError(ChainError::ChainSpecific(err)))
            .map(|maybe_ix| maybe_ix.map(|(era_cbor, _)| era_cbor))
    }

    pub async fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: Vec<u8>,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, DomainError<D::ChainSpecificError>> {
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
    ) -> Result<Option<ChainPoint>, DomainError<D::ChainSpecificError>> {
        self.run_blocking(move |domain| Ok(domain.archive().find_intersect(&intersect)?))
            .await
    }
}
