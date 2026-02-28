//! Transaction validation and mempool submission.
//!
//! This module provides functionality for validating transactions against
//! the current ledger state and submitting them to the mempool.

use tracing::info;

use crate::{
    ChainLogic, Domain, DomainError, MempoolAwareUtxoStore, MempoolStore, MempoolTx, StateStore,
    TxHash,
};

/// Validate a transaction against the current ledger state.
pub async fn validate_tx<D: Domain>(domain: &D, cbor: &[u8]) -> Result<MempoolTx, DomainError> {
    let tip = domain.state().read_cursor()?;

    info!(?tip, "validating tx against tip");

    let utxos =
        MempoolAwareUtxoStore::<'_, D>::new(domain.state(), domain.indexes(), domain.mempool());

    let tx = D::Chain::validate_tx(cbor, &utxos, tip, &domain.genesis()).await?;

    Ok(tx)
}

/// Validate and receive a transaction into the mempool.
pub async fn receive_tx<D: Domain>(
    domain: &D,
    source: &str,
    cbor: &[u8],
) -> Result<TxHash, DomainError> {
    info!(source = source, cbor_len = cbor.len(), "acquiring submit lock");

    let _guard = domain.acquire_submit_lock().await;

    info!(source = source, "submit lock acquired");

    let tx = validate_tx(domain, cbor).await?;
    let hash = tx.hash;

    info!(tx.hash = %hash, source = source, "validation passed");

    domain.mempool().receive(tx).await?;

    info!(tx.hash = %hash, source = source, "tx inserted into mempool");

    Ok(hash)
}

/// Extension trait for transaction submission operations.
///
/// This trait extends any `Domain` implementation with methods for
/// validating and submitting transactions to the mempool.
pub trait SubmitExt: Domain {
    /// Validate a transaction against the current ledger state.
    async fn validate_tx(&self, cbor: &[u8]) -> Result<MempoolTx, DomainError> {
        validate_tx(self, cbor).await
    }

    /// Validate and receive a transaction into the mempool.
    async fn receive_tx(&self, source: &str, cbor: &[u8]) -> Result<TxHash, DomainError> {
        receive_tx(self, source, cbor).await
    }
}

impl<D: Domain> SubmitExt for D {}
