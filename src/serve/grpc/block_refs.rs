//! Shared helpers for resolving `block_ref` metadata for UTxOs returned by
//! `read_utxos` / `search_utxos` across the v1alpha and v1beta query services.
//!
//! Loads era summary once per request and deduplicates `block_meta_by_tx_hash`
//! lookups by source transaction — UTxOs sharing a producing tx pay a single
//! decode rather than one per item.

use std::collections::HashMap;

use dolos_core::{
    async_query::{AsyncQueryFacade, BlockRefMeta},
    BlockHeight, BlockSlot, Domain, TxHash, TxoRef,
};
use tonic::Status;
use tracing::debug;

/// Chain-agnostic block reference data assembled from a single
/// `block_meta_by_tx_hash` lookup plus the request-scoped era summary.
#[derive(Clone, Debug)]
pub struct BlockRefData {
    pub slot: BlockSlot,
    pub hash: [u8; 32],
    pub height: BlockHeight,
    pub timestamp: u64,
}

/// Fetch a `BlockRefData` for every UTxO in `txo_refs`, deduplicating by
/// source transaction so each unique tx incurs at most one storage round-trip
/// and one block decode. The returned map is keyed by `TxHash`; entries are
/// absent for transactions that are not in the archive (e.g. genesis UTxOs,
/// custom UTxOs, or transactions ahead of the current indexer cursor).
///
/// Storage errors surface as `Status::internal`. Block-decode errors for
/// archive entries that did exist degrade to a `debug!` log and a missing map
/// entry — these indicate node-internal corruption that callers cannot fix.
pub async fn fetch_block_refs<D>(
    domain: &D,
    txo_refs: impl IntoIterator<Item = &TxoRef>,
) -> Result<HashMap<TxHash, BlockRefData>, Status>
where
    D: Domain + pallas::interop::utxorpc::LedgerContext,
{
    let unique_hashes: Vec<TxHash> = txo_refs
        .into_iter()
        .map(|r| r.0)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    if unique_hashes.is_empty() {
        return Ok(HashMap::new());
    }

    let chain_summary = dolos_cardano::eras::load_era_summary::<D>(domain.state())
        .map_err(|e| Status::internal(format!("failed to load era summary: {e}")))?;

    let query = AsyncQueryFacade::new(domain.clone());
    let mut out = HashMap::with_capacity(unique_hashes.len());

    for tx_hash in unique_hashes {
        match query.block_meta_by_tx_hash(tx_hash.to_vec()).await {
            Ok(Some(BlockRefMeta {
                slot, hash, height, ..
            })) => {
                out.insert(
                    tx_hash,
                    BlockRefData {
                        slot,
                        hash: *hash,
                        height,
                        timestamp: chain_summary.slot_time(slot),
                    },
                );
            }
            Ok(None) => {
                // tx not in archive (yet) — leave missing; caller emits None.
            }
            Err(e) => {
                if matches!(e, dolos_core::DomainError::ChainError(_)) {
                    debug!(
                        tx_hash = %hex::encode(tx_hash),
                        error = %e,
                        "failed to decode block while resolving UTxO block_ref",
                    );
                    continue;
                }
                return Err(Status::internal(format!(
                    "block_meta_by_tx_hash lookup failed for tx {}: {}",
                    hex::encode(tx_hash),
                    e,
                )));
            }
        }
    }

    Ok(out)
}
