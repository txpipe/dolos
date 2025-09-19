use dolos_core::{ChainError, Domain, StateStore as _};

use crate::{model::EraSummary, EraBoundary, EraProtocol, FixedNamespace as _};

pub type Epoch = u32;
pub type EpochSlot = u32;

impl EraSummary {
    /// Resolve epoch and sub-epoch slot from a slot number and a chain summary.
    pub fn slot_epoch(&self, slot: u64) -> (Epoch, EpochSlot) {
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

#[derive(Debug, Default)]
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

    /// Resolve wall-clock time from a slot number and a chain summary.
    pub fn slot_time(&self, slot: u64) -> Timestamp {
        let era = self.era_for_slot(slot);
        era.slot_time(slot)
    }

    pub(crate) fn append_era(&mut self, protocol: u16, era: EraSummary) {
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
        0
    }
}

pub fn load_era_summary<D: Domain>(domain: &D) -> Result<ChainSummary, ChainError> {
    let eras = domain.state().iter_entities_typed(EraSummary::NS, None)?;

    let mut chain = ChainSummary::default();

    for result in eras {
        let (key, era) = result?;
        let protocol = EraProtocol::from(key);
        chain.append_era(protocol.into(), era);
    }

    Ok(chain)
}

pub fn load_active_era<D: Domain>(domain: &D) -> Result<(EraProtocol, EraSummary), ChainError> {
    let eras = domain
        .state()
        .iter_entities_typed::<EraSummary>(EraSummary::NS, None)?;

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
