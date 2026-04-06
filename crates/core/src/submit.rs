//! Transaction validation and mempool submission.
//!
//! This module provides functionality for validating transactions against
//! the current ledger state and submitting them to the mempool.

use tracing::{debug, instrument};

use crate::{
    ChainLogic, Domain, DomainError, MempoolAwareUtxoStore, MempoolStore, MempoolTx, StateStore,
    TxHash,
};

static SUBMIT_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Extension trait for transaction submission operations.
///
/// This trait extends any `Domain` implementation with methods for
/// validating and submitting transactions to the mempool.
pub trait SubmitExt: Domain {
    /// Validate a transaction against the current ledger state.
    ///
    /// Checks that the transaction is valid according to the current
    /// ledger state and mempool contents.
    ///
    /// # Arguments
    ///
    /// * `chain` - Reference to the chain logic for validation
    /// * `cbor` - CBOR-encoded transaction bytes
    ///
    /// # Returns
    ///
    /// The validated mempool transaction if valid.
    #[instrument(skip_all)]
    fn validate_tx(
        &self,
        chain: &Self::Chain,
        cbor: &[u8],
    ) -> Result<MempoolTx, DomainError<Self::ChainSpecificError>> {
        let tip = self.state().read_cursor()?;

        let utxos =
            MempoolAwareUtxoStore::<'_, Self>::new(self.state(), self.indexes(), self.mempool());

        let tx = chain.validate_tx(cbor, &utxos, tip, &self.genesis())?;

        Ok(tx)
    }

    /// Validate and receive a transaction into the mempool.
    ///
    /// Validates the transaction and, if valid, adds it to the mempool
    /// for potential inclusion in a future block.
    ///
    /// # Arguments
    ///
    /// * `source` - A reference to the mechanims from which the tx was received
    /// * `chain` - Reference to the chain logic for validation
    /// * `cbor` - CBOR-encoded transaction bytes
    ///
    /// # Returns
    ///
    /// The transaction hash if successfully submitted.
    /// Evaluate a transaction's scripts against the current ledger state.
    ///
    /// Returns execution unit reports without submitting to the mempool.
    #[instrument(skip_all)]
    fn eval_tx(
        &self,
        cbor: &[u8],
    ) -> Result<<Self::Chain as ChainLogic>::EvalReport, DomainError<Self::ChainSpecificError>>
    {
        let utxos =
            MempoolAwareUtxoStore::<'_, Self>::new(self.state(), self.indexes(), self.mempool());

        Ok(Self::Chain::eval_tx(cbor, &utxos)?)
    }

    #[instrument(skip_all)]
    fn receive_tx(
        &self,
        source: &str,
        chain: &Self::Chain,
        cbor: &[u8],
    ) -> Result<TxHash, DomainError<Self::ChainSpecificError>> {
        let _guard = match SUBMIT_LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        let tx = self.validate_tx(chain, cbor)?;
        let hash = tx.hash;

        debug!(tx.hash = %hash, source=source, "tx received");

        self.mempool().receive(tx)?;

        Ok(hash)
    }
}

impl<D: Domain> SubmitExt for D {}
