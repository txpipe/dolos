use dolos_core::mempool::{MempoolEvent, MempoolTx, MempoolTxStage};
use dolos_core::{ChainPoint, EraCbor, MempoolError, MempoolStore, TxHash, TxStatus};

use pallas::codec::minicbor;
use pallas::codec::utils::{KeepRaw, NonEmptySet, Nullable, Set};
use pallas::ledger::primitives::conway::{TransactionBody, TransactionInput, Tx};
use pallas::ledger::traverse::MultiEraTx;

use crate::streams::ScriptedStream;

/// Build a minimal `MempoolTx` for testing.
pub fn make_test_mempool_tx(hash: TxHash) -> MempoolTx {
    MempoolTx::new(hash, EraCbor(7, vec![0x80]), vec![])
}

/// Build a `MempoolTx` holding a real Conway transaction that spends the first
/// output of each of `parents`. `salt` tells apart transactions that spend the
/// same.
pub fn make_test_mempool_tx_spending(parents: &[TxHash], salt: u64) -> MempoolTx {
    make_conway_tx(parents, &[], &[], salt)
}

/// Build a `MempoolTx` holding a real Conway transaction that uses the first
/// output of each of `reference_inputs` and `collateral` that way, and spends
/// nothing.
pub fn make_test_mempool_tx_requiring(
    reference_inputs: &[TxHash],
    collateral: &[TxHash],
    salt: u64,
) -> MempoolTx {
    make_conway_tx(&[], reference_inputs, collateral, salt)
}

fn first_outputs(parents: &[TxHash]) -> Vec<TransactionInput> {
    parents
        .iter()
        .map(|parent| TransactionInput {
            transaction_id: *parent,
            index: 0,
        })
        .collect()
}

fn make_conway_tx(
    inputs: &[TxHash],
    reference_inputs: &[TxHash],
    collateral: &[TxHash],
    salt: u64,
) -> MempoolTx {
    let body = TransactionBody {
        inputs: Set::from(first_outputs(inputs)),
        outputs: vec![],
        fee: 200_000 + salt,
        ttl: None,
        validity_interval_start: None,
        certificates: None,
        withdrawals: None,
        auxiliary_data_hash: None,
        mint: None,
        script_data_hash: None,
        collateral: NonEmptySet::from_vec(first_outputs(collateral)),
        required_signers: None,
        network_id: None,
        collateral_return: None,
        total_collateral: None,
        reference_inputs: NonEmptySet::from_vec(first_outputs(reference_inputs)),
        voting_procedures: None,
        proposal_procedures: None,
        treasury_value: None,
        donation: None,
    };

    let tx = Tx {
        transaction_body: KeepRaw::from(body),
        transaction_witness_set: KeepRaw::from(crate::blocks::empty_witness_set()),
        success: true,
        auxiliary_data: Nullable::Null,
    };

    let bytes = minicbor::to_vec(&tx).unwrap();
    let payload = EraCbor(7, bytes);
    let hash = MultiEraTx::try_from(&payload).unwrap().hash();

    MempoolTx::new(hash, payload, vec![])
}

/// Build a minimal `MempoolEvent` at the `Pending` stage for testing.
pub fn make_test_mempool_event(hash: TxHash) -> MempoolEvent {
    MempoolEvent {
        tx: make_test_mempool_tx(hash),
    }
}

/// A no-op `MempoolStore` implementation for tests.
#[derive(Clone)]
pub struct MockMempoolStore;

impl MempoolStore for MockMempoolStore {
    type Stream = ScriptedStream<Result<MempoolEvent, MempoolError>>;

    fn receive(&self, _tx: MempoolTx) -> Result<(), MempoolError> {
        Ok(())
    }

    fn has_pending(&self) -> bool {
        false
    }

    fn peek_pending(&self) -> Vec<MempoolTx> {
        vec![]
    }

    fn mark_inflight(&self, _hashes: &[TxHash]) -> Result<(), MempoolError> {
        Ok(())
    }

    fn mark_acknowledged(&self, _hashes: &[TxHash]) -> Result<(), MempoolError> {
        Ok(())
    }

    fn requeue_propagated(&self, _except: &[TxHash]) -> Result<(), MempoolError> {
        Ok(())
    }

    fn find_inflight(&self, _tx_hash: &TxHash) -> Option<MempoolTx> {
        None
    }

    fn peek_inflight(&self) -> Vec<MempoolTx> {
        vec![]
    }

    fn confirm(
        &self,
        _point: &ChainPoint,
        _seen: &[TxHash],
        _unseen: &[TxHash],
        _finalize_threshold: u32,
        _drop_threshold: u32,
    ) -> Result<(), MempoolError> {
        Ok(())
    }

    fn check_status(&self, _hash: &TxHash) -> TxStatus {
        TxStatus {
            stage: MempoolTxStage::Unknown,
            confirmations: 0,
            non_confirmations: 0,
            confirmed_at: None,
        }
    }

    fn dump_finalized(&self, _cursor: u64, _limit: usize) -> dolos_core::MempoolPage {
        dolos_core::MempoolPage {
            items: vec![],
            next_cursor: None,
        }
    }

    fn subscribe(&self) -> Self::Stream {
        ScriptedStream::empty()
    }
}
