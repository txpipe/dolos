use std::sync::Arc;
use std::time::SystemTime;

use arc_swap::ArcSwap;

use crate::{BlockHash, BlockSlot, ChainTip};

#[derive(Debug, Clone)]
pub struct LatestBlock {
    pub hash: BlockHash,
    pub slot: BlockSlot,
    pub received_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct HealthSnapshot {
    pub connected: bool,
    pub node_tip: Option<ChainTip>,
    pub latest_block: Option<LatestBlock>,
}

#[derive(Debug, Clone)]
pub struct Health {
    inner: Arc<ArcSwap<HealthState>>,
}

#[derive(Debug, Clone, Default)]
struct HealthState {
    connected: bool,
    node_tip: Option<ChainTip>,
    latest_block: Option<LatestBlock>,
}

impl Default for Health {
    fn default() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(HealthState::default())),
        }
    }
}

impl Health {
    pub fn snapshot(&self) -> HealthSnapshot {
        let state = self.inner.load();

        HealthSnapshot {
            connected: state.connected,
            node_tip: state.node_tip.clone(),
            latest_block: state.latest_block.clone(),
        }
    }

    pub fn synced(&self) -> bool {
        self.sync_percentage().map(|x| x == 1.0f64).unwrap_or(false)
    }

    pub fn sync_percentage(&self) -> Option<f64> {
        match (self.latest_block(), self.node_tip()) {
            (Some(latest_block), Some(tip)) => {
                Some((latest_block.slot as f64) / (tip.0.slot_or_default() as f64))
            }
            _ => None,
        }
    }

    pub fn connected(&self) -> bool {
        self.inner.load().connected
    }

    pub fn node_tip(&self) -> Option<ChainTip> {
        self.inner.load().node_tip.clone()
    }

    pub fn latest_block(&self) -> Option<LatestBlock> {
        self.inner.load().latest_block.clone()
    }

    pub fn set_connected(&self, connected: bool) {
        self.inner.rcu(|state| {
            let mut next = (**state).clone();
            next.connected = connected;
            Arc::new(next)
        });
    }

    pub fn set_node_tip(&self, node_tip: ChainTip) {
        self.inner.rcu(|state| {
            let mut next = (**state).clone();
            next.node_tip = Some(node_tip.clone());
            Arc::new(next)
        });
    }

    pub fn set_latest_block(&self, hash: BlockHash, slot: BlockSlot, received_at: SystemTime) {
        let latest_block = LatestBlock {
            hash,
            slot,
            received_at,
        };
        self.inner.rcu(|state| {
            let mut next = (**state).clone();
            next.latest_block = Some(latest_block.clone());
            Arc::new(next)
        });
    }
}
