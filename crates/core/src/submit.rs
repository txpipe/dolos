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
    let _guard = domain.acquire_submit_lock().await;
    let tx = validate_tx(domain, cbor).await?;
    let hash = tx.hash;

    info!(tx.hash = %hash, source=source, "tx received");

    domain.mempool().receive(tx).await?;

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
