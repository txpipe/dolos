use dolos_core::{BlockSlot, ChainError, Domain, LogKey, StateStore, TemporalKey};
use pallas::ledger::primitives::Epoch;

use crate::{model::EraSummary, EraBoundary, EraProtocol, FixedNamespace as _};

pub type EpochSlot = u32;

impl EraSummary {
    /// Resolve epoch and sub-epoch slot from a slot number and a chain summary.
    pub fn slot_epoch(&self, slot: u64) -> (Epoch, EpochSlot) {
        if slot < self.start.slot {
            panic!("can't compute epoch for slot {slot} since it's prior to this era")
        }

        let era_slot = slot - self.start.slot;
        let era_epoch = era_slot / self.epoch_length;
        let epoch = self.start.epoch + era_epoch;
        let epoch_slot = era_slot - era_epoch * self.epoch_length;

        (epoch as Epoch, epoch_slot as EpochSlot)
    }

    pub fn slot_time(&self, slot: u64) -> Timestamp {
        let time = self.start.timestamp + (slot - self.start.slot) * self.slot_length;

        time as Timestamp
    }

    pub fn epoch_start(&self, epoch: u64) -> BlockSlot {
        self.start.slot + (epoch - self.start.epoch) * self.epoch_length
    }

    pub fn define_end(&mut self, at_epoch: u64) {
        let epoch_delta = at_epoch - self.start.epoch;

        let slot_delta = epoch_delta * self.epoch_length;
        let end_slot = self.start.slot + slot_delta;
        let second_delta = slot_delta * self.slot_length;
        let end_timestamp = self.start.timestamp + second_delta;

        let boundary = EraBoundary {
            epoch: at_epoch,
            slot: end_slot,
            timestamp: end_timestamp,
        };

        self.end = Some(boundary.clone());
    }
}

pub type Timestamp = u64;

/// Milliseconds per second.
///
/// [`ChainSummary`] stores time in seconds; Plutus `POSIXTime` is in
/// milliseconds. Used when crossing that boundary.
const MS_PER_SECOND: u64 = 1000;

#[derive(Debug, Default, Clone)]
pub struct ChainSummary {
    past: Vec<EraSummary>,
    protocols: Vec<u16>,
    edge: Option<EraSummary>,
}

impl ChainSummary {
    pub fn slot_epoch(&self, slot: u64) -> (Epoch, EpochSlot) {
        let era = self.era_for_slot(slot);
        era.slot_epoch(slot)
    }

    pub fn epoch_start(&self, epoch: u64) -> BlockSlot {
        let era = self.era_for_epoch(epoch);
        era.epoch_start(epoch)
    }

    /// Resolve wall-clock time from a slot number and a chain summary.
    pub fn slot_time(&self, slot: u64) -> Timestamp {
        let era = self.era_for_slot(slot);
        era.slot_time(slot)
    }

    /// Build a Plutus [`SlotConfig`] from the edge era.
    ///
    /// [`ChainSummary`] keeps `slot_length` and `timestamp` in **seconds**
    /// (the Ouroboros era-summary convention), but a Plutus `ScriptContext`
    /// expects `POSIXTime` in **milliseconds**. Pallas' `SlotConfig` does no
    /// scaling of its own (`zero_time + (slot - zero_slot) * slot_length`), so
    /// both fields must already be in milliseconds. Convert here, at the seam
    /// between dolos' seconds-world and pallas' ms-world, so no call site has
    /// to remember the unit mismatch.
    pub fn to_pallas_slot_config(
        &self,
    ) -> pallas::ledger::validate::phase2::script_context::SlotConfig {
        let edge = self.edge();

        pallas::ledger::validate::phase2::script_context::SlotConfig {
            slot_length: edge.slot_length * MS_PER_SECOND,
            zero_slot: edge.start.slot,
            zero_time: edge.start.timestamp * MS_PER_SECOND,
        }
    }

    pub fn append_era(&mut self, protocol: u16, era: EraSummary) {
        if let Some(edge) = self.edge.take() {
            self.past.push(edge);
        }

        self.protocols.push(protocol);
        self.edge = Some(era);
    }

    pub fn first(&self) -> &EraSummary {
        if let Some(era) = self.past.first() {
            era
        } else {
            self.edge()
        }
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
        self.protocol_and_era_for_epoch(epoch).1
    }

    /// Return the protocol and era for a given epoch
    ///
    /// This method will scan the different eras looking for one that includes
    /// the given epoch.
    pub fn protocol_and_era_for_epoch(&self, epoch: u64) -> (&u16, &EraSummary) {
        if epoch >= self.edge().start.epoch {
            return (self.protocols.last().unwrap(), self.edge());
        }

        self.protocols
            .iter()
            .zip(self.past.iter())
            .find(|(_, e)| epoch >= e.start.epoch && e.end.as_ref().unwrap().epoch > epoch)
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

    #[allow(unused)]
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

    pub fn iter_all(&self) -> impl Iterator<Item = &EraSummary> {
        self.past.iter().chain(std::iter::once(self.edge()))
    }

    pub fn iter_past(&self) -> impl Iterator<Item = &EraSummary> {
        self.past.iter()
    }

    pub fn iter_past_with_protocol(&self) -> impl Iterator<Item = (&u16, &EraSummary)> {
        self.protocols.iter().zip(self.past.iter())
    }

    pub fn first_shelley_epoch(&self) -> u64 {
        for (protocol, era) in self.iter_past_with_protocol() {
            if *protocol == 2 {
                return era.start.epoch;
            }
        }
        if let Some(last) = self.protocols.last() {
            if *last == 2 {
                return self.edge().start.epoch;
            }
        }
        0
    }

    /// Epoch at which the chain entered Conway — the first era with
    /// protocol >= 9 (a hard fork can jump over 9, mirroring
    /// `EraTransition::entering_conway`). `None` when no known era has
    /// reached Conway.
    pub fn first_conway_epoch(&self) -> Option<u64> {
        self.first_epoch_with_protocol(9)
    }

    /// Epoch at which the chain entered Mary — the first era with
    /// protocol >= 4, which is where native assets appear. Nothing before
    /// it can mint, so listings of assets can start their scan here. `None`
    /// when no known era has reached Mary.
    pub fn first_mary_epoch(&self) -> Option<u64> {
        self.first_epoch_with_protocol(4)
    }

    /// Start epoch of the first era whose protocol is at least `protocol`.
    /// A hard fork can jump over a version, so this is a threshold rather
    /// than an equality check.
    fn first_epoch_with_protocol(&self, protocol: u16) -> Option<u64> {
        for (era_protocol, era) in self.iter_past_with_protocol() {
            if *era_protocol >= protocol {
                return Some(era.start.epoch);
            }
        }
        match self.protocols.last() {
            Some(last) if *last >= protocol => Some(self.edge().start.epoch),
            _ => None,
        }
    }
}

pub fn load_era_summary<D: Domain>(state: &D::State) -> Result<ChainSummary, ChainError> {
    let eras = state.iter_entities_typed(EraSummary::NS, None)?;

    let mut chain = ChainSummary::default();

    for result in eras {
        let (key, era) = result?;
        let protocol = EraProtocol::from(key);
        chain.append_era(protocol.into(), era);
    }

    Ok(chain)
}

pub fn load_chain_summary_from_state(state: &impl StateStore) -> Result<ChainSummary, ChainError> {
    let eras = state.iter_entities_typed(EraSummary::NS, None)?;

    let mut chain = ChainSummary::default();

    for result in eras {
        let (key, era) = result?;
        let protocol = EraProtocol::from(key);
        chain.append_era(protocol.into(), era);
    }

    Ok(chain)
}

pub fn log_epoch_range_to_key_range(
    summary: &ChainSummary,
    start_epoch: Option<u64>,
    end_epoch: Option<u64>,
) -> (Option<u64>, Option<u64>, Option<std::ops::Range<LogKey>>) {
    let start_slot = start_epoch.map(|epoch| summary.epoch_start(epoch));
    let end_slot = end_epoch.map(|epoch| summary.epoch_start(epoch));
    let range = match (start_slot, end_slot) {
        (Some(start), Some(end)) => {
            let start_key = LogKey::from(TemporalKey::from(start));
            let end_key = LogKey::from(TemporalKey::from(end));
            Some(std::ops::Range {
                start: start_key,
                end: end_key,
            })
        }
        _ => None,
    };

    (start_slot, end_slot, range)
}

pub fn load_active_era<D: Domain>(
    state: &D::State,
) -> Result<(EraProtocol, EraSummary), ChainError> {
    let eras = state.iter_entities_typed::<EraSummary>(EraSummary::NS, None)?;

    match eras.last() {
        Some(x) => match x {
            Ok((key, summary)) => {
                let protocol = EraProtocol::from(key);
                Ok((protocol, summary))
            }
            Err(_) => Err(ChainError::EraNotFound),
        },
        None => Err(ChainError::EraNotFound),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_config_is_in_milliseconds() {
        // Mainnet Shelley edge era: time in seconds, slot length of 1 second.
        let mut summary = ChainSummary::default();
        summary.append_era(
            2,
            EraSummary {
                start: EraBoundary {
                    epoch: 208,
                    slot: 4_492_800,
                    timestamp: 1_596_059_091,
                },
                end: None,
                epoch_length: 432_000,
                slot_length: 1,
                protocol: 2,
            },
        );

        let sc = summary.to_pallas_slot_config();

        // POSIXTime must be milliseconds; this matches pallas' mainnet default.
        assert_eq!(sc.slot_length, 1_000);
        assert_eq!(sc.zero_slot, 4_492_800);
        assert_eq!(sc.zero_time, 1_596_059_091_000);
    }

    fn era(protocol: u16, start_epoch: u64) -> EraSummary {
        EraSummary {
            start: EraBoundary {
                epoch: start_epoch,
                slot: start_epoch * 100,
                timestamp: 0,
            },
            end: None,
            epoch_length: 100,
            slot_length: 1,
            protocol,
        }
    }

    #[test]
    fn first_conway_epoch_finds_conway_entry() {
        // empty summary
        assert_eq!(ChainSummary::default().first_conway_epoch(), None);

        // no era reached Conway
        let mut summary = ChainSummary::default();
        summary.append_era(2, era(2, 0));
        summary.append_era(8, era(8, 300));
        assert_eq!(summary.first_conway_epoch(), None);

        // Conway at the edge
        summary.append_era(9, era(9, 507));
        assert_eq!(summary.first_conway_epoch(), Some(507));

        // Conway in the past with a later intra-Conway edge
        summary.append_era(10, era(10, 600));
        assert_eq!(summary.first_conway_epoch(), Some(507));

        // a fork that jumps over protocol 9 still enters Conway
        let mut jumped = ChainSummary::default();
        jumped.append_era(8, era(8, 300));
        jumped.append_era(10, era(10, 480));
        assert_eq!(jumped.first_conway_epoch(), Some(480));
    }

    #[test]
    fn first_mary_epoch_finds_first_multi_asset_era() {
        // empty summary
        assert_eq!(ChainSummary::default().first_mary_epoch(), None);

        // Byron, Shelley and Allegra have no native assets
        let mut summary = ChainSummary::default();
        summary.append_era(1, era(1, 0));
        summary.append_era(2, era(2, 208));
        summary.append_era(3, era(3, 236));
        assert_eq!(summary.first_mary_epoch(), None);

        // Mary at the edge
        summary.append_era(4, era(4, 251));
        assert_eq!(summary.first_mary_epoch(), Some(251));

        // later eras keep the original Mary entry
        summary.append_era(6, era(6, 290));
        summary.append_era(9, era(9, 507));
        assert_eq!(summary.first_mary_epoch(), Some(251));

        // a chain that starts past Mary (testnets) has assets from epoch 0
        let mut recent = ChainSummary::default();
        recent.append_era(6, era(6, 0));
        recent.append_era(9, era(9, 100));
        assert_eq!(recent.first_mary_epoch(), Some(0));
    }
}
