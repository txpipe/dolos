use super::*;

pub use pallas::ledger::validate::phase2::EvalReport;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MempoolTx {
    pub hash: TxHash,
    pub era: u16,
    pub bytes: Vec<u8>,
    // TODO: we'll improve this to track number of confirmations in further iterations.
    pub confirmed: bool,
}

#[derive(Clone)]
pub enum MempoolTxStage {
    Pending,
    Inflight,
    Acknowledged,
    Confirmed,
    Unknown,
}

#[derive(Clone)]
pub struct MempoolEvent {
    pub new_stage: MempoolTxStage,
    pub tx: MempoolTx,
}
