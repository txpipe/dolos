//!
//! Cardano-specific async query helpers for `AsyncQueryFacade`.

use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::conway::{DatumOption, PlutusData, ScriptRef},
        traverse::{ComputeHash, MultiEraBlock, MultiEraTx, OriginalHash},
    },
};

use dolos_core::{
    archive::ArchiveStore, AsyncQueryFacade, BlockBody, BlockSlot, ChainError, Domain, DomainError,
    StateStore as _, TagDimension, TxHash, TxoRef,
};

use crate::indexes::{dimensions::archive, entities};
use crate::model::{DatumKey, DatumState, FirstScriptKey, FirstScriptSet, ScriptKey};
use crate::pallas_extras;
use crate::periods::{GroupKey, Ungrouped};

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

    fn blocks_by_policy_stream(
        &self,
        policy: &[u8],
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

    fn blocks_by_pool_certs_stream(
        &self,
        pool: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static;

    fn blocks_by_drep_certs_stream(
        &self,
        drep: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static;

    fn blocks_by_account_withdrawals_stream(
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

    /// Stream blocks holding redeemers that executed the given script, in
    /// slot order.
    ///
    /// Backed by the SCRIPT_REDEEMERS dimension, which the indexer resolves
    /// at block-application time. The tag is exact but block-level: the
    /// caller still matches each redeemer against the script hash to pick
    /// rows. A store synced before the dimension shipped carries tags only
    /// from its upgrade point; full history needs a store synced from
    /// scratch.
    fn blocks_by_script_redeemers_stream(
        &self,
        script_hash: &Hash<28>,
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

    async fn blocks_by_pool_certs(
        &self,
        pool: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError>;

    async fn blocks_by_account_withdrawals(
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

    /// The script `script_hash` names if `/scripts` lists it, the way
    /// cardano-db-sync's `script` table has it: a script only a
    /// phase-2-invalid tx witnessed is not found.
    ///
    /// The entity index (sealed epochs) or a staged `first-scripts` row
    /// (unsealed epochs) gives the block that counted the script first, and
    /// that block returns it. A script in neither is not found, with no tag
    /// walk; nor is one whose first block the archive no longer holds.
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

    fn blocks_by_policy_stream(
        &self,
        policy: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static
    {
        blocks_by_tag_stream(
            (*self).clone(),
            archive::POLICY,
            policy.to_vec(),
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

    fn blocks_by_pool_certs_stream(
        &self,
        pool: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static
    {
        blocks_by_tag_stream(
            (*self).clone(),
            archive::POOL_CERTS,
            pool.to_vec(),
            start_slot,
            end_slot,
            order,
        )
    }

    fn blocks_by_drep_certs_stream(
        &self,
        drep: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static
    {
        blocks_by_tag_stream(
            (*self).clone(),
            archive::DREP_CERTS,
            drep.to_vec(),
            start_slot,
            end_slot,
            order,
        )
    }

    fn blocks_by_account_withdrawals_stream(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static
    {
        blocks_by_tag_stream(
            (*self).clone(),
            archive::ACCOUNT_WITHDRAWALS,
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

    fn blocks_by_script_redeemers_stream(
        &self,
        script_hash: &Hash<28>,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
        order: SlotOrder,
    ) -> impl Stream<Item = Result<(BlockSlot, Option<BlockBody>), DomainError>> + Send + 'static
    {
        blocks_by_tag_stream(
            (*self).clone(),
            archive::SCRIPT_REDEEMERS,
            script_hash.as_slice().to_vec(),
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

    async fn blocks_by_pool_certs(
        &self,
        pool: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError> {
        blocks_by_tag(self, archive::POOL_CERTS, pool, start_slot, end_slot).await
    }

    async fn blocks_by_account_withdrawals(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<(BlockSlot, Option<BlockBody>)>, DomainError> {
        blocks_by_tag(
            self,
            archive::ACCOUNT_WITHDRAWALS,
            account,
            start_slot,
            end_slot,
        )
        .await
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
        let key = DatumKey(*datum_hash);
        self.run_blocking(move |domain| {
            let datum_state: Option<DatumState> = domain.state().read_entity_typed(&key)?;
            Ok(datum_state.map(|s| s.bytes))
        })
        .await
    }

    async fn script_by_hash(
        &self,
        script_hash: &Hash<28>,
    ) -> Result<Option<ScriptData>, DomainError> {
        let script_hash = *script_hash;

        let blocks = self
            .run_blocking(move |domain| {
                let Some(slot) = first_seen_script_slot(&domain, script_hash)? else {
                    return Ok(vec![]);
                };

                Ok(domain.archive().get_blocks_by_slot(&slot)?)
            })
            .await?;

        for raw in blocks {
            let block = MultiEraBlock::decode(raw.as_slice())
                .map_err(|e| DomainError::ChainError(ChainError::DecodingError(e)))?;

            if let Some(script) = script_in_block(&block, script_hash) {
                return Ok(Some(script));
            }
        }

        Ok(None)
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
                // the spent_txo tag follows `consumes()`, so a failed tx
                // spends its collateral, not its ordinary inputs
                for tx in block.txs().iter() {
                    for input in tx.consumes() {
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

/// The script `script_hash` names, if `block` carries it.
fn script_in_block(block: &MultiEraBlock, script_hash: Hash<28>) -> Option<ScriptData> {
    block
        .txs()
        .iter()
        .find_map(|tx| script_in_tx(tx, script_hash))
}

/// The script `script_hash` names, if `tx` carries it where `/scripts` counts
/// it (`roll::scripts::tx_script_hashes`): a tx that failed phase-2 validation
/// contributes only the reference script of its collateral return, as in
/// cardano-db-sync.
fn script_in_tx(tx: &MultiEraTx, script_hash: Hash<28>) -> Option<ScriptData> {
    let valid = tx.is_valid();

    let outputs = if valid { tx.outputs() } else { vec![] };
    let collateral_return = tx.collateral_return();

    for output in outputs.iter().chain(collateral_return.as_ref()) {
        if let Some(script) = output
            .script_ref()
            .and_then(|x| reference_script(x, script_hash))
        {
            return Some(script);
        }
    }

    if !valid {
        return None;
    }

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

    for script in pallas_extras::aux_native_scripts(tx) {
        if pallas_extras::native_script_hash(&script) == script_hash {
            return Some(ScriptData {
                language: ScriptLanguage::Native,
                script: script.into_owned(),
            });
        }
    }

    for script in tx.aux_plutus_v1_scripts() {
        if script.compute_hash() == script_hash {
            return Some(ScriptData {
                language: ScriptLanguage::PlutusV1,
                script: script.as_ref().to_vec(),
            });
        }
    }

    None
}

/// The reference script `script_ref` holds, if `script_hash` names it.
fn reference_script(script_ref: ScriptRef, script_hash: Hash<28>) -> Option<ScriptData> {
    let (hash, language, script) = match script_ref {
        ScriptRef::NativeScript(x) => (
            x.original_hash(),
            ScriptLanguage::Native,
            x.raw_cbor().to_vec(),
        ),
        ScriptRef::PlutusV1Script(x) => (
            x.compute_hash(),
            ScriptLanguage::PlutusV1,
            x.as_ref().to_vec(),
        ),
        ScriptRef::PlutusV2Script(x) => (
            x.compute_hash(),
            ScriptLanguage::PlutusV2,
            x.as_ref().to_vec(),
        ),
        ScriptRef::PlutusV3Script(x) => (
            x.compute_hash(),
            ScriptLanguage::PlutusV3,
            x.as_ref().to_vec(),
        ),
    };

    (hash == script_hash).then_some(ScriptData { language, script })
}

/// The slot of the block that counted a script first, or `None` for a
/// script never seen.
///
/// The entity index first, then the staged rows, then the index again: a seal
/// running beside the read writes the index entry before it deletes the
/// staged row, so a script the first index read missed is in whichever of the
/// two the read reaches after it.
fn first_seen_script_slot<D: Domain>(
    domain: &D,
    script_hash: Hash<28>,
) -> Result<Option<BlockSlot>, DomainError> {
    let index = || -> Result<Option<BlockSlot>, DomainError> {
        let slots = domain
            .archive()
            .entity_slots(entities::SCRIPT, &[script_hash.as_slice()])?;

        Ok(slots.into_iter().next().flatten())
    };

    if let Some(slot) = index()? {
        return Ok(Some(slot));
    }

    if let Some(slot) = staged_script_slot(domain, script_hash)? {
        return Ok(Some(slot));
    }

    index()
}

/// The slot a staged `first-scripts` row of an unsealed epoch gives a script.
fn staged_script_slot<D: Domain>(
    domain: &D,
    script_hash: Hash<28>,
) -> Result<Option<BlockSlot>, DomainError> {
    let state = domain.state();

    let Some(cursor) = state.read_cursor()? else {
        return Ok(None);
    };

    let chain = crate::eras::load_era_summary::<D>(state)?;
    let (epoch, _) = chain.slot_epoch(cursor.slot());
    let mutable = crate::utils::mutable_slots(&domain.genesis());

    let keys: Vec<FirstScriptKey> = crate::periods::unsealed_epochs(&chain, mutable, epoch)
        .map(|epoch| GroupKey {
            group: Ungrouped,
            epoch,
            member: ScriptKey(script_hash),
        })
        .collect();

    let rows = state.read_entities_typed::<FirstScriptSet>(&keys.iter().collect::<Vec<_>>())?;

    Ok(rows.into_iter().flatten().map(|row| row.slot).min())
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
                    .archive()
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
                            .archive()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roll::scripts::tests::{every_place, tx_cbor};

    /// A tx that failed phase-2 validation lists only the reference scripts
    /// of its collateral return, so its witnesses and auxiliary scripts are
    /// not found.
    #[test]
    fn invalid_tx_yields_its_reference_scripts_only() {
        let (cbor, reference, witness, auxiliary) = tx_cbor(false);
        let tx = MultiEraTx::decode(&cbor).unwrap();
        assert!(!tx.is_valid());

        let found = |hash| script_in_tx(&tx, hash).map(|x| x.language);

        assert_eq!(found(reference), Some(ScriptLanguage::Native));
        assert_eq!(found(witness), None);
        assert_eq!(found(auxiliary), None);
    }

    /// A valid tx lists its collateral return, witnesses and auxiliary
    /// scripts.
    #[test]
    fn valid_tx_yields_witness_and_auxiliary_scripts() {
        let (cbor, reference, witness, auxiliary) = tx_cbor(true);
        let tx = MultiEraTx::decode(&cbor).unwrap();
        assert!(tx.is_valid());

        let found = |hash| script_in_tx(&tx, hash).map(|x| x.language);

        assert_eq!(found(reference), Some(ScriptLanguage::Native));
        assert_eq!(found(witness), Some(ScriptLanguage::PlutusV2));
        assert_eq!(found(auxiliary), Some(ScriptLanguage::Native));
    }

    /// Every script a tx is counted for is found in it, and an auxiliary
    /// native script comes back as written, the bytes its hash covers.
    #[test]
    fn every_listed_script_is_found_as_written() {
        let (tx, listed) = every_place();
        let cbor = tx.cbor();
        let tx = MultiEraTx::decode(&cbor).unwrap();

        for hash in listed {
            let found = script_in_tx(&tx, hash).expect("listed script not found");

            if found.language == ScriptLanguage::Native {
                assert_eq!(pallas_extras::native_script_hash(&found.script), hash);
            }
        }
    }
}
