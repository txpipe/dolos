use std::sync::Arc;

use gasket::framework::*;
use tokio::sync::RwLock;
use tracing::info;

use super::{monitor::BlockMonitorMessage, BlockSlot, Transaction};

pub type SubmitEndpointReceiver = gasket::messaging::tokio::InputPort<Vec<Transaction>>;
pub type BlockMonitorReceiver = gasket::messaging::tokio::InputPort<BlockMonitorMessage>;

pub type PropagatorSender = gasket::messaging::tokio::OutputPort<Vec<Transaction>>;

#[derive(Clone, Debug)]
pub struct InclusionPoint {
    slot: u64,
    // height: u64,
}

#[derive(Debug)]
pub enum MempoolEvent {
    AddTxs(Vec<Transaction>),
    ChainUpdate(BlockMonitorMessage),
}

#[derive(Stage)]
#[stage(name = "mempool", unit = "MempoolEvent", worker = "Worker")]
pub struct Stage {
    pub tip_slot: BlockSlot,

    pub txs: Arc<RwLock<Vec<(Transaction, Option<InclusionPoint>)>>>,
    pub change_notifier: Arc<tokio::sync::Notify>,

    pub prune_after_slots: u64,

    pub upstream_submit_endpoint: SubmitEndpointReceiver,
    pub upstream_block_monitor: BlockMonitorReceiver,
    pub downstream_propagator: PropagatorSender,
    // #[metric]
    // received_txs: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(
        txs: Arc<RwLock<Vec<(Transaction, Option<InclusionPoint>)>>>,
        change_notifier: Arc<tokio::sync::Notify>,
        prune_after_slots: u64,
    ) -> Self {
        Self {
            tip_slot: 0,
            txs,
            change_notifier,
            prune_after_slots,
            upstream_submit_endpoint: Default::default(),
            upstream_block_monitor: Default::default(),
            downstream_propagator: Default::default(),
        }
    }
}

pub struct Worker {}

impl Worker {}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Self {})
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<MempoolEvent>, WorkerError> {
        tokio::select! {
            txs_msg = stage.upstream_submit_endpoint.recv() => {
                let txs_msg = txs_msg.or_panic()?;

                info!("received txs message: {:?}", txs_msg);

                Ok(WorkSchedule::Unit(MempoolEvent::AddTxs(txs_msg.payload)))
            }
            monitor_msg = stage.upstream_block_monitor.recv() => {
                let monitor_msg = monitor_msg.or_panic()?;

                info!("received monitor message: {:?}", monitor_msg);

                Ok(WorkSchedule::Unit(MempoolEvent::ChainUpdate(monitor_msg.payload)))
            }
        }
    }

    async fn execute(&mut self, unit: &MempoolEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        match unit {
            MempoolEvent::AddTxs(txs) => {
                // pass new txs to downstream/propagate txs
                stage
                    .downstream_propagator
                    .send(txs.clone().into())
                    .await
                    .or_panic()?;

                // make note of txs for monitoring
                stage
                    .txs
                    .write()
                    .await
                    .extend(txs.clone().into_iter().map(|x| (x, None)))
            }
            MempoolEvent::ChainUpdate(monitor_msg) => {
                match monitor_msg {
                    BlockMonitorMessage::NewBlock(slot, _hash, block_txs) => {
                        // set inclusion point for txs found in new block
                        for (tx, inclusion) in stage.txs.write().await.iter_mut() {
                            if block_txs.contains(&tx.hash) {
                                info!("setting inclusion point for {}: {slot}", tx.hash);
                                *inclusion = Some(InclusionPoint { slot: *slot })
                            }
                        }

                        // prune txs which have sufficient slot-confirmations
                        // TODO: make height based instead of slots
                        stage.txs.write().await.retain(|(_, inclusion)| {
                            if let Some(point) = inclusion {
                                slot - point.slot > stage.prune_after_slots
                            } else {
                                true
                            }
                        });

                        stage.tip_slot = *slot;
                    }
                    BlockMonitorMessage::Rollback(rb_slot) => {
                        // remove inclusion points later than rollback slot
                        for (tx, inclusion) in stage.txs.write().await.iter_mut() {
                            if let Some(point) = inclusion {
                                if point.slot > *rb_slot {
                                    info!(
                                        "removing inclusion point for {} due to rollback ({} > {})",
                                        tx.hash, point.slot, rb_slot
                                    );

                                    *inclusion = None
                                }
                            }
                        }

                        stage.tip_slot = *rb_slot;
                    }
                }

                stage.change_notifier.notify_waiters()
            }
        }

        Ok(())
    }
}
