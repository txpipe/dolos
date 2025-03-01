use crate::{
    ledger::pparams::Genesis,
    state::LedgerStore,
    uplc::{script_context::SlotConfig, tx, EvalReport},
};
use futures_util::StreamExt;
use itertools::Itertools;
use pallas::{
    applying::{utils::AccountState, validate_tx, CertState, Environment, UTxOs},
    crypto::hash::Hash,
    ledger::{
        primitives::{NetworkId, TransactionInput},
        traverse::{MultiEraBlock, MultiEraInput, MultiEraOutput, MultiEraTx},
    },
};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::debug;

type TxHash = Hash<32>;

#[derive(Debug, Error)]
pub enum MempoolError {
    #[error("traverse error: {0}")]
    TraverseError(#[from] pallas::ledger::traverse::Error),

    #[error("decode error: {0}")]
    DecodeError(#[from] pallas::codec::minicbor::decode::Error),

    #[error("tx validation failed: {0}")]
    ValidationError(#[from] pallas::applying::utils::ValidationError),

    #[cfg(feature = "phase2")]
    #[error("tx evaluation failed")]
    EvaluationError(#[from] crate::uplc::error::Error),

    #[error("state error: {0}")]
    StateError(#[from] crate::state::LedgerError),

    #[error("plutus not supported")]
    PlutusNotSupported,

    #[error("invalid tx: {0}")]
    InvalidTx(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Tx {
    pub hash: TxHash,
    pub era: u16,
    pub bytes: Vec<u8>,
    // TODO: we'll improve this to track number of confirmations in further iterations.
    pub confirmed: bool,
}

#[derive(Clone)]
pub enum TxStage {
    Pending,
    Inflight,
    Acknowledged,
    Confirmed,
    Unknown,
}

#[derive(Clone)]
pub struct Event {
    pub new_stage: TxStage,
    pub tx: Tx,
}

#[derive(Default)]
struct MempoolState {
    pending: Vec<Tx>,
    inflight: Vec<Tx>,
    acknowledged: HashMap<TxHash, Tx>,
}

/// A very basic, FIFO, single consumer mempool
#[derive(Clone)]
pub struct Mempool {
    mempool: Arc<RwLock<MempoolState>>,
    updates: broadcast::Sender<Event>,
    genesis: Arc<Genesis>,
    ledger: LedgerStore,
}

impl Mempool {
    pub fn new(genesis: Arc<Genesis>, ledger: LedgerStore) -> Self {
        let mempool = Arc::new(RwLock::new(MempoolState::default()));
        let (updates, _) = broadcast::channel(16);

        Self {
            mempool,
            updates,
            genesis,
            ledger,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.updates.subscribe()
    }

    pub fn notify(&self, new_stage: TxStage, tx: Tx) {
        if self.updates.send(Event { new_stage, tx }).is_err() {
            debug!("no mempool update receivers");
        }
    }

    fn receive(&self, tx: Tx) {
        let mut state = self.mempool.write().unwrap();

        state.pending.push(tx.clone());
        self.notify(TxStage::Pending, tx);

        debug!(
            pending = state.pending.len(),
            inflight = state.inflight.len(),
            acknowledged = state.acknowledged.len(),
            "mempool state changed"
        );
    }

    pub fn validate(&self, tx: &MultiEraTx) -> Result<(), MempoolError> {
        let tip = self.ledger.cursor()?;

        let updates: Vec<_> = self
            .ledger
            .get_pparams(tip.as_ref().map(|p| p.0).unwrap_or_default())?;

        let updates: Vec<_> = updates.into_iter().map(TryInto::try_into).try_collect()?;

        let eras = crate::ledger::pparams::fold_with_hacks(
            &self.genesis,
            &updates,
            tip.as_ref().unwrap().0,
        );

        let era = eras.era_for_slot(tip.as_ref().unwrap().0);

        let network_id = match self.genesis.shelley.network_id.as_ref() {
            Some(network) => match network.as_str() {
                "Mainnet" => Some(NetworkId::Mainnet.into()),
                "Testnet" => Some(NetworkId::Testnet.into()),
                _ => None,
            },
            None => None,
        }
        .unwrap();

        let env = Environment {
            prot_params: era.pparams.clone(),
            prot_magic: self.genesis.shelley.network_magic.unwrap(),
            block_slot: tip.unwrap().0,
            network_id,
            acnt: Some(AccountState::default()),
        };

        let input_refs = tx.requires().iter().map(From::from).collect();

        let utxos = self.ledger.get_utxos(input_refs)?;

        let mut pallas_utxos = UTxOs::new();

        for (txoref, eracbor) in utxos.iter() {
            let tx_in = TransactionInput {
                transaction_id: txoref.0,
                index: txoref.1.into(),
            };
            let input = MultiEraInput::AlonzoCompatible(<Box<Cow<'_, TransactionInput>>>::from(
                Cow::Owned(tx_in),
            ));
            let output = MultiEraOutput::try_from(eracbor)?;
            pallas_utxos.insert(input, output);
        }

        validate_tx(tx, 0, &env, &pallas_utxos, &mut CertState::default())?;

        Ok(())
    }

    #[cfg(feature = "phase2")]
    pub fn evaluate(&self, tx: &MultiEraTx) -> Result<EvalReport, MempoolError> {
        let tip = self.ledger.cursor()?;

        let updates: Vec<_> = self
            .ledger
            .get_pparams(tip.as_ref().map(|p| p.0).unwrap_or_default())?;

        let updates: Vec<_> = updates.into_iter().map(TryInto::try_into).try_collect()?;

        let eras = crate::ledger::pparams::fold_with_hacks(
            &self.genesis,
            &updates,
            tip.as_ref().unwrap().0,
        );

        let slot_config = SlotConfig {
            slot_length: eras.edge().pparams.slot_length(),
            zero_slot: eras.edge().start.slot,
            zero_time: eras.edge().start.timestamp.timestamp().try_into().unwrap(),
        };

        let input_refs = tx.requires().iter().map(From::from).collect();

        let utxos = self.ledger.get_utxos(input_refs)?;

        let report = tx::eval_tx(tx, &eras.edge().pparams, &utxos, &slot_config)?;

        Ok(report)
    }

    #[cfg(feature = "phase2")]
    pub fn evaluate_raw(&self, cbor: &[u8]) -> Result<EvalReport, MempoolError> {
        let tx = MultiEraTx::decode(cbor)?;
        self.evaluate(&tx)
    }

    pub fn receive_raw(&self, cbor: &[u8]) -> Result<TxHash, MempoolError> {
        let tx = MultiEraTx::decode(cbor)?;

        self.validate(&tx)?;

        #[cfg(feature = "phase2")]
        self.evaluate(&tx)?;

        // if we don't have phase-2 enabled, we reject txs before propagating something
        // that could result in collateral loss
        #[cfg(not(feature = "phase2"))]
        if !decoded.redeemers().is_empty() {
            return Err(MempoolError::PlutusNotSupported);
        }

        let hash = tx.hash();

        let tx = Tx {
            hash,
            // TODO: this is a hack to make the era compatible with the ledger
            era: u16::from(tx.era()) - 1,
            bytes: cbor.into(),
            confirmed: false,
        };

        self.receive(tx);

        Ok(hash)
    }

    pub fn request(&self, desired: usize) -> Vec<Tx> {
        let available = self.pending_total();
        self.request_exact(std::cmp::min(desired, available))
    }

    pub fn request_exact(&self, count: usize) -> Vec<Tx> {
        let mut state = self.mempool.write().unwrap();

        let selected = state.pending.drain(..count).collect_vec();

        for tx in selected.iter() {
            state.inflight.push(tx.clone());
            self.notify(TxStage::Inflight, tx.clone());
        }

        debug!(
            pending = state.pending.len(),
            inflight = state.inflight.len(),
            acknowledged = state.acknowledged.len(),
            "mempool state changed"
        );

        selected
    }

    pub fn acknowledge(&self, count: usize) {
        debug!(n = count, "acknowledging txs");

        let mut state = self.mempool.write().unwrap();

        let selected = state.inflight.drain(..count).collect_vec();

        for tx in selected {
            state.acknowledged.insert(tx.hash, tx.clone());
            self.notify(TxStage::Acknowledged, tx.clone());
        }

        debug!(
            pending = state.pending.len(),
            inflight = state.inflight.len(),
            acknowledged = state.acknowledged.len(),
            "mempool state changed"
        );
    }

    pub fn find_inflight(&self, tx_hash: &TxHash) -> Option<Tx> {
        let state = self.mempool.read().unwrap();
        state.inflight.iter().find(|x| x.hash.eq(tx_hash)).cloned()
    }

    pub fn find_pending(&self, tx_hash: &TxHash) -> Option<Tx> {
        let state = self.mempool.read().unwrap();
        state.pending.iter().find(|x| x.hash.eq(tx_hash)).cloned()
    }

    pub fn pending_total(&self) -> usize {
        let state = self.mempool.read().unwrap();
        state.pending.len()
    }

    pub fn check_stage(&self, tx_hash: &TxHash) -> TxStage {
        let state = self.mempool.read().unwrap();

        if let Some(tx) = state.acknowledged.get(tx_hash) {
            if tx.confirmed {
                TxStage::Confirmed
            } else {
                TxStage::Acknowledged
            }
        } else if self.find_inflight(tx_hash).is_some() {
            TxStage::Inflight
        } else if self.find_pending(tx_hash).is_some() {
            TxStage::Pending
        } else {
            TxStage::Unknown
        }
    }

    pub fn apply_block(&self, block: &MultiEraBlock) {
        let mut state = self.mempool.write().unwrap();

        if state.acknowledged.is_empty() {
            return;
        }

        for tx in block.txs() {
            let tx_hash = tx.hash();

            if let Some(acknowledged_tx) = state.acknowledged.get_mut(&tx_hash) {
                acknowledged_tx.confirmed = true;
                self.notify(TxStage::Confirmed, acknowledged_tx.clone());
                debug!(%tx_hash, "confirming tx");
            }
        }
    }

    pub fn undo_block(&self, block: &MultiEraBlock) {
        let mut state = self.mempool.write().unwrap();

        if state.acknowledged.is_empty() {
            return;
        }

        for tx in block.txs() {
            let tx_hash = tx.hash();

            if let Some(acknowledged_tx) = state.acknowledged.get_mut(&tx_hash) {
                acknowledged_tx.confirmed = false;
                debug!(%tx_hash, "un-confirming tx");
            }
        }
    }
}

pub struct UpdateFilter {
    inner: BroadcastStream<Event>,
    subjects: HashSet<TxHash>,
}

impl UpdateFilter {
    pub fn new(updates: broadcast::Receiver<Event>, subjects: HashSet<TxHash>) -> Self {
        Self {
            inner: BroadcastStream::new(updates),
            subjects,
        }
    }
}

impl futures_core::Stream for UpdateFilter {
    type Item = Event;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let x = self.inner.poll_next_unpin(cx);

        match x {
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Ready(Some(x)) => match x {
                Ok(x) => {
                    if self.subjects.contains(&x.tx.hash) {
                        std::task::Poll::Ready(Some(x))
                    } else {
                        std::task::Poll::Pending
                    }
                }
                Err(_) => std::task::Poll::Ready(None),
            },
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}
