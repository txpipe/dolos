use pallas::{
    ledger::traverse::MultiEraUpdate, ledger::validate::utils::MultiEraProtocolParameters,
};
use tracing::debug;

use super::Genesis;

#[derive(Clone, Debug)]
pub struct EraBoundary {
    pub epoch: u64,
    pub slot: u64,
    pub timestamp: chrono::DateTime<chrono::FixedOffset>,
}

#[derive(Clone, Debug)]
pub struct EraSummary {
    pub start: EraBoundary,
    pub end: Option<EraBoundary>,
    pub pparams: MultiEraProtocolParameters,
}

#[derive(Debug)]
pub struct ChainSummary {
    pub past: Vec<EraSummary>,
    edge: Option<EraSummary>,
}

impl ChainSummary {
    pub fn start(genesis: &Genesis) -> Self {
        let mut pparams =
            MultiEraProtocolParameters::Byron(super::bootstrap_byron_pparams(&genesis.byron));

        if let Some(force_protocol) = genesis.force_protocol {
            for next_protocol in 1..=force_protocol {
                pparams = super::migrate_pparams(pparams, genesis, next_protocol);

                debug!(protocol = next_protocol, "forced hardfork");
            }
        }

        Self {
            past: vec![],
            edge: Some(EraSummary {
                start: EraBoundary {
                    epoch: 0,
                    slot: 0,
                    timestamp: pparams.system_start(),
                },
                end: None,
                pparams,
            }),
        }
    }

    pub fn apply_update(&mut self, update: &MultiEraUpdate, genesis: &Genesis) {
        let apply_epoch = update.epoch() + 1;

        assert!(
            apply_epoch >= self.edge().start.epoch,
            "can't apply update for past era"
        );

        let mut pparams = super::apply_param_update(self.edge().pparams.clone(), update);

        let next_version = pparams.protocol_version();

        if next_version > self.edge().pparams.protocol_version() {
            pparams = super::migrate_pparams(pparams, genesis, next_version);
            debug!(protocol = next_version, "hardfork executed");
        }

        self.advance(apply_epoch, pparams);
    }

    fn advance(&mut self, at_epoch: u64, pparams: MultiEraProtocolParameters) {
        let mut edge = self.edge.take().unwrap();
        let epoch_delta = at_epoch - edge.start.epoch;

        let slot_delta = epoch_delta * edge.pparams.epoch_length();
        let end_slot = edge.start.slot + slot_delta;
        let second_delta = slot_delta * edge.pparams.slot_length();
        let end_timestamp = edge.start.timestamp + chrono::Duration::seconds(second_delta as i64);

        let boundary = EraBoundary {
            epoch: at_epoch,
            slot: end_slot,
            timestamp: end_timestamp,
        };

        edge.end = Some(boundary.clone());
        self.past.push(edge);

        self.edge = Some(EraSummary {
            start: boundary.clone(),
            end: None,
            pparams,
        });
    }

    /// Return the edge era
    ///
    /// The edge era represent the last era in chronological order that we know
    /// about. This generally represents the current era except when the
    /// chain has already received a hardfork update that is going to be applied
    /// in the next epoch.
    pub fn edge(&self) -> &EraSummary {
        // safe to unwrap since it's a business invariant
        self.edge.as_ref().unwrap()
    }

    /// Return the era for a given epoch
    ///
    /// This method will scan the different eras looking for one that includes
    /// the given epoch.
    pub fn era_for_epoch(&self, epoch: u64) -> &EraSummary {
        if epoch >= self.edge().start.epoch {
            return self.edge();
        }

        self.past
            .iter()
            .find(|e| epoch >= e.start.epoch && e.end.as_ref().unwrap().epoch > epoch)
            .unwrap()
    }

    /// Return the era for a given slot
    ///
    /// This method will scan the different eras looking for one that includes
    /// the given slot.
    pub fn era_for_slot(&self, slot: u64) -> &EraSummary {
        if slot >= self.edge().start.slot {
            return self.edge();
        }

        self.past
            .iter()
            .find(|e| slot >= e.start.slot && e.end.as_ref().unwrap().slot > slot)
            .unwrap()
    }

    pub(crate) fn apply_hacks<F>(&mut self, epoch: u64, change: F)
    where
        F: Fn(&mut EraSummary),
    {
        if epoch >= self.edge().start.epoch {
            change(self.edge.as_mut().unwrap());
        }

        let era = self
            .past
            .iter_mut()
            .find(|e| epoch >= e.start.epoch && e.end.as_ref().unwrap().epoch > epoch);

        if let Some(era) = era {
            change(era);
        }
    }
}
