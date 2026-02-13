//!
//! Cardano-specific async query helpers for `AsyncQueryFacade`.

use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::conway::{DatumOption, PlutusData, ScriptRef},
        traverse::{ComputeHash, MultiEraBlock, OriginalHash},
    },
};

use dolos_core::{
    archive::ArchiveStore, AsyncQueryFacade, BlockBody, BlockSlot, ChainError, Domain, DomainError,
    EntityKey, IndexStore, StateStore as _, TagDimension, TxHash, TxoRef,
};

use crate::indexes::dimensions::archive;
use crate::model::{DatumState, DATUM_NS};

use futures_core::Stream;

/// Number of slots to fetch per chunk when iterating over tagged blocks.
const SLOT_CHUNK_SIZE: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptLanguage {
    Native,
    PlutusV1,
    PlutusV2,
    PlutusV3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptData {
    pub language: ScriptLanguage,
    pub script: Vec<u8>,
}

#[async_trait::async_trait]
pub trait AsyncCardanoQueryExt<D: Domain> {
    fn blocks_by_address_stream(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static;

    fn blocks_by_payment_stream(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static;

    fn blocks_by_stake_stream(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static;

    fn blocks_by_asset_stream(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static;

    fn blocks_by_account_certs_stream(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static;

    fn blocks_by_metadata_stream(
        &self,
        label: u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static;

    async fn blocks_by_address(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError>;

    async fn blocks_by_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError>;

    async fn blocks_by_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError>;

    async fn blocks_by_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError>;

    async fn blocks_by_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError>;

    async fn blocks_by_metadata(
        &self,
        label: u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError>;

    async fn plutus_data(&self, datum_hash: &Hash<32>) -> Result<Option<PlutusData>, DomainError>;

    async fn get_datum(&self, datum_hash: &Hash<32>) -> Result<Option<Vec<u8>>, DomainError>;

    async fn script_by_hash(
        &self,
        script_hash: &Hash<28>,
    ) -> Result<Option<ScriptData>, DomainError>;

    async fn tx_by_spent_txo(&self, spent_txo: &[u8]) -> Result<Option<TxHash>, DomainError>;
}

#[async_trait::async_trait]
impl<D: Domain> AsyncCardanoQueryExt<D> for AsyncQueryFacade<D>
where
    D: Clone + Send + Sync + 'static,
{
    fn blocks_by_address_stream(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static
    {
        blocks_by_tag_stream(
            (*self).clone(),
            archive::ADDRESS,
            address.to_vec(),
            start_slot,
            end_slot,
            order,
        )
    }

    fn blocks_by_payment_stream(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static
    {
        blocks_by_tag_stream(
            (*self).clone(),
            archive::PAYMENT,
            payment.to_vec(),
            start_slot,
            end_slot,
            order,
        )
    }

    fn blocks_by_stake_stream(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static
    {
        blocks_by_tag_stream(
            (*self).clone(),
            archive::STAKE,
            stake.to_vec(),
            start_slot,
            end_slot,
            order,
        )
    }

    fn blocks_by_asset_stream(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static
    {
        blocks_by_tag_stream(
            (*self).clone(),
            archive::ASSET,
            asset.to_vec(),
            start_slot,
            end_slot,
            order,
        )
    }

    fn blocks_by_account_certs_stream(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static
    {
        blocks_by_tag_stream(
            (*self).clone(),
            archive::ACCOUNT_CERTS,
            account.to_vec(),
            start_slot,
            end_slot,
            order,
        )
    }

    fn blocks_by_metadata_stream(
        &self,
        label: u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static
    {
        blocks_by_tag_stream(
            (*self).clone(),
            archive::METADATA,
            label.to_be_bytes().to_vec(),
            start_slot,
            end_slot,
            order,
        )
    }

    async fn blocks_by_address(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError> {
        blocks_by_tag(self, archive::ADDRESS, address, start_slot, end_slot).await
    }

    async fn blocks_by_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError> {
        blocks_by_tag(self, archive::PAYMENT, payment, start_slot, end_slot).await
    }

    async fn blocks_by_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError> {
        blocks_by_tag(self, archive::STAKE, stake, start_slot, end_slot).await
    }

    async fn blocks_by_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError> {
        blocks_by_tag(self, archive::ASSET, asset, start_slot, end_slot).await
    }

    async fn blocks_by_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError> {
        blocks_by_tag(self, archive::ACCOUNT_CERTS, account, start_slot, end_slot).await
    }

    async fn blocks_by_metadata(
        &self,
        label: u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError> {
        blocks_by_tag(
            self,
            archive::METADATA,
            &label.to_be_bytes(),
            start_slot,
            end_slot,
        )
        .await
    }

    async fn plutus_data(&self, datum_hash: &Hash<32>) -> Result<Option<PlutusData>, DomainError> {
        let end_slot = self
            .run_blocking(move |domain| {
                Ok(domain
                    .archive()
                    .get_tip()?
                    .map(|(slot, _)| slot)
                    .unwrap_or_default())
            })
            .await?;

        let datum_hash = *datum_hash;
        find_first_by_tag(
            self,
            archive::DATUM,
            datum_hash.as_slice().to_vec(),
            0,
            end_slot,
            |block| {
                for tx in block.txs() {
                    // Check witnesses
                    if let Some(plutus_data) = tx.find_plutus_data(&datum_hash) {
                        return Some(plutus_data.clone().unwrap());
                    }
                    // Check inline datums
                    for (_, output) in tx.produces() {
                        if let Some(DatumOption::Data(data)) = output.datum() {
                            if data.original_hash() == datum_hash {
                                return Some(data.clone().unwrap().unwrap());
                            }
                        }
                    }
                    // Check redeemer data
                    for redeemer in tx.redeemers() {
                        if redeemer.data().compute_hash() == datum_hash {
                            return Some(redeemer.data().clone());
                        }
                    }
                }
                None
            },
        )
        .await
    }

    async fn get_datum(&self, datum_hash: &Hash<32>) -> Result<Option<Vec<u8>>, DomainError> {
        let key = EntityKey::from(*datum_hash);
        self.run_blocking(move |domain| {
            let datum_state: Option<DatumState> =
                domain.state().read_entity_typed(DATUM_NS, &key)?;
            Ok(datum_state.map(|s| s.bytes))
        })
        .await
    }

    async fn script_by_hash(
        &self,
        script_hash: &Hash<28>,
    ) -> Result<Option<ScriptData>, DomainError> {
        let end_slot = self
            .run_blocking(move |domain| {
                Ok(domain
                    .archive()
                    .get_tip()?
                    .map(|(slot, _)| slot)
                    .unwrap_or_default())
            })
            .await?;

        let script_hash = *script_hash;
        find_first_by_tag(
            self,
            archive::SCRIPT,
            script_hash.as_slice().to_vec(),
            0,
            end_slot,
            |block| {
                for tx in block.txs() {
                    for script in tx.native_scripts() {
                        if script.original_hash() == script_hash {
                            return Some(ScriptData {
                                language: ScriptLanguage::Native,
                                script: script.raw_cbor().to_vec(),
                            });
                        }
                    }

                    for script in tx.plutus_v1_scripts() {
                        if script.compute_hash() == script_hash {
                            return Some(ScriptData {
                                language: ScriptLanguage::PlutusV1,
                                script: script.as_ref().to_vec(),
                            });
                        }
                    }

                    for script in tx.plutus_v2_scripts() {
                        if script.compute_hash() == script_hash {
                            return Some(ScriptData {
                                language: ScriptLanguage::PlutusV2,
                                script: script.as_ref().to_vec(),
                            });
                        }
                    }

                    for script in tx.plutus_v3_scripts() {
                        if script.compute_hash() == script_hash {
                            return Some(ScriptData {
                                language: ScriptLanguage::PlutusV3,
                                script: script.as_ref().to_vec(),
                            });
                        }
                    }

                    for (_, output) in tx.produces() {
                        if let Some(script_ref) = output.script_ref() {
                            match script_ref {
                                ScriptRef::NativeScript(script) => {
                                    if script.original_hash() == script_hash {
                                        return Some(ScriptData {
                                            language: ScriptLanguage::Native,
                                            script: script.raw_cbor().to_vec(),
                                        });
                                    }
                                }
                                ScriptRef::PlutusV1Script(script) => {
                                    if script.compute_hash() == script_hash {
                                        return Some(ScriptData {
                                            language: ScriptLanguage::PlutusV1,
                                            script: script.as_ref().to_vec(),
                                        });
                                    }
                                }
                                ScriptRef::PlutusV2Script(script) => {
                                    if script.compute_hash() == script_hash {
                                        return Some(ScriptData {
                                            language: ScriptLanguage::PlutusV2,
                                            script: script.as_ref().to_vec(),
                                        });
                                    }
                                }
                                ScriptRef::PlutusV3Script(script) => {
                                    if script.compute_hash() == script_hash {
                                        return Some(ScriptData {
                                            language: ScriptLanguage::PlutusV3,
                                            script: script.as_ref().to_vec(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
                None
            },
        )
        .await
    }

    async fn tx_by_spent_txo(&self, spent_txo: &[u8]) -> Result<Option<TxHash>, DomainError> {
        let spent = spent_txo.to_vec();

        let end_slot = self
            .run_blocking(move |domain| {
                Ok(domain
                    .archive()
                    .get_tip()?
                    .map(|(slot, _)| slot)
                    .unwrap_or_default())
            })
            .await?;

        find_first_by_tag(
            self,
            archive::SPENT_TXO,
            spent.clone(),
            0,
            end_slot,
            |block| {
                for tx in block.txs().iter() {
                    for input in tx.inputs() {
                        let bytes: Vec<u8> = TxoRef::from(&input).into();
                        if bytes.as_slice() == spent.as_slice() {
                            return Some(tx.hash());
                        }
                    }
                }
                None
            },
        )
        .await
    }
}

async fn blocks_by_tag<D>(
    facade: &AsyncQueryFacade<D>,
    dimension: TagDimension,
    key: &[u8],
    start_slot: BlockSlot,
    end_slot: BlockSlot,
) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let slots = facade
        .slots_by_tag(dimension, key.to_vec(), start_slot, end_slot)
        .await?;

    let mut out = Vec::with_capacity(slots.len());
    for slot in slots {
        let block = facade.block_by_slot(slot).await?;
        out.push((slot, block));
    }

    Ok(out)
}

/// Find the first match in blocks tagged with a specific dimension/key.
/// Uses chunked slot iteration (512 at a time) to avoid memory spikes.
async fn find_first_by_tag<D, F, T>(
    facade: &AsyncQueryFacade<D>,
    dimension: TagDimension,
    key: Vec<u8>,
    start_slot: BlockSlot,
    end_slot: BlockSlot,
    mut predicate: F,
) -> Result<Option<T>, DomainError>
where
    D: Domain + Clone + Send + Sync + 'static,
    F: FnMut(&MultiEraBlock) -> Option<T>,
{
    let mut current_start = start_slot;

    loop {
        let key_clone = key.clone();
        let chunk_start = current_start;

        let slots: Vec<BlockSlot> = facade
            .run_blocking(move |domain| {
                Ok(domain
                    .indexes()
                    .slots_by_tag(dimension, &key_clone, chunk_start, end_slot)?
                    .take(SLOT_CHUNK_SIZE)
                    .collect::<Result<Vec<_>, _>>()?)
            })
            .await?;

        if slots.is_empty() {
            return Ok(None);
        }

        for slot in &slots {
            let Some(raw) = facade.block_by_slot(*slot).await? else {
                continue;
            };

            let block = MultiEraBlock::decode(raw.as_slice())
                .map_err(|e| DomainError::ChainError(ChainError::DecodingError(e)))?;

            if let Some(result) = predicate(&block) {
                return Ok(Some(result));
            }
        }

        // Update start for next chunk
        if let Some(&last) = slots.last() {
            current_start = last + 1;
            if current_start > end_slot {
                break;
            }
        }
    }

    Ok(None)
}

fn blocks_by_tag_stream<D>(
    facade: AsyncQueryFacade<D>,
    dimension: TagDimension,
    key: Vec<u8>,
    mut start_slot: BlockSlot,
    mut end_slot: BlockSlot,
    order: SlotOrder,
) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static
where
    D: Domain + Clone + Send + Sync + 'static,
{
    async_stream::try_stream! {
        loop {
            // we fetch slots in chunks to avoid holding the index read transaction for too long
            // and to avoid loading all slots into memory at once
            let slots: Vec<BlockSlot> = facade
                .run_blocking({
                    let key = key.clone();
                    move |domain| {
                        let iter = domain
                            .indexes()
                            .slots_by_tag(dimension, &key, start_slot, end_slot)?;

                        let slots = match order {
                            SlotOrder::Asc => iter.take(SLOT_CHUNK_SIZE).collect::<Result<Vec<_>, _>>()?,
                            SlotOrder::Desc => iter.rev().take(SLOT_CHUNK_SIZE).collect::<Result<Vec<_>, _>>()?,
                        };

                        Ok(slots)
                    }
                })
                .await?;

            if slots.is_empty() {
                break;
            }

            for slot in slots {
                let block = facade.block_by_slot(slot).await?;
                yield (slot, block);

                // update bounds to avoid re-fetching same slots in next iteration
                match order {
                    SlotOrder::Asc => start_slot = slot + 1,
                    SlotOrder::Desc => {
                        if slot == 0 {
                            return;
                        }
                        end_slot = slot - 1;
                    }
                }
            }

            match order {
                SlotOrder::Asc if start_slot > end_slot => break,
                SlotOrder::Desc if end_slot < start_slot => break,
                _ => {}
            }
        }
    }
}
