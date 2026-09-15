use dolos_core::{BlockSlot, ChainError, Domain, Genesis, LogKey, StateStore, TemporalKey};
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

/// Same as [`load_era_summary`], but keeps each era tagged with its raw
/// protocol number instead of folding them into a [`ChainSummary`] — the
/// shape [`pad_era_history`] takes as input.
pub fn load_era_summary_with_protocols<D: Domain>(
    state: &D::State,
) -> Result<Vec<(u16, EraSummary)>, ChainError> {
    let eras = state.iter_entities_typed::<EraSummary>(EraSummary::NS, None)?;

    eras.map(|result| {
        let (key, era) = result?;
        let protocol = EraProtocol::from(key);
        Ok((protocol.into(), era))
    })
    .collect()
}

/// Eras where a hard fork actually changes the reported era name, as
/// opposed to an intra-era protocol version bump (e.g. 5→6 stay
/// "alonzo", 7→8 stay "babbage", 9→10 stay "conway"). Mirrors
/// `protocol_to_era_name`'s grouping in the gRPC/Ogmios mapping code.
const KNOWN_HARDFORKS: [u16; 6] = [2, 3, 4, 5, 7, 9];

fn parse_system_start(genesis: &Genesis) -> Result<u64, ChainError> {
    genesis
        .shelley
        .system_start
        .as_ref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp() as u64)
        .ok_or_else(|| ChainError::GenesisFieldMissing("shelley.system_start".into()))
}

/// A single hardcoded era row spanning `[start_epoch, end_epoch)`, with
/// absolute timestamps derived from `system_start` — used for Byron and,
/// on Preview, the zero-width placeholders for the eras its genesis
/// skips entirely.
fn hardcoded_era(
    protocol: u16,
    system_start: u64,
    epoch_length: u64,
    slot_length: u64,
    start_epoch: u64,
    end_epoch: u64,
) -> EraSummary {
    EraSummary {
        start: EraBoundary {
            epoch: start_epoch,
            slot: start_epoch * epoch_length,
            timestamp: system_start + start_epoch * epoch_length * slot_length,
        },
        end: Some(EraBoundary {
            epoch: end_epoch,
            slot: end_epoch * epoch_length,
            timestamp: system_start + end_epoch * epoch_length * slot_length,
        }),
        epoch_length,
        slot_length,
        protocol,
    }
}

/// Rebuilds the full historical era table (Byron through the current tip)
/// from whatever era-boundary records the backing node has itself
/// locally processed since it started tracking chain state.
///
/// Mainnet/preprod nodes never observe Byron (protocol 0/1) as tracked
/// on-chain state, and Preview's genesis jumps straight past
/// Shelley/Allegra/Mary into Alonzo — so `eras` alone is a partial table.
/// This pads in the missing early eras from well-known per-network
/// constants, the same hardcode minibf's Blockfrost-compatible
/// `/network/eras` route already applies for its own clients, so gRPC
/// and Ogmios consumers see the same complete table
/// (`#1331`).
pub fn pad_era_history(
    eras: &[(u16, EraSummary)],
    tip: BlockSlot,
    genesis: &Genesis,
) -> Result<Vec<EraSummary>, ChainError> {
    let system_start = parse_system_start(genesis)?;

    let mut out = Vec::new();

    let mut previous =
        match genesis.shelley.network_magic {
            Some(764824073) => hardcoded_era(0, system_start, 21600, 20, 0, 208),
            Some(1) => hardcoded_era(0, system_start, 21600, 20, 0, 4),
            Some(2) => {
                let placeholder = hardcoded_era(0, system_start, 4320, 20, 0, 0);
                out.push(placeholder.clone());

                let epoch_length =
                    genesis.shelley.epoch_length.ok_or_else(|| {
                        ChainError::GenesisFieldMissing("shelley.epoch_length".into())
                    })? as u64;
                let slot_length =
                    genesis.shelley.slot_length.ok_or_else(|| {
                        ChainError::GenesisFieldMissing("shelley.slot_length".into())
                    })? as u64;

                // Preview's genesis skips Shelley, Allegra and Mary entirely —
                // pad in one zero-width placeholder row per skipped era so the
                // table still names them before the first real recorded entry.
                // Shelley and Allegra are pushed directly here; Mary is left as
                // `previous` so the main loop below chains its `end` to the
                // first real recorded era instead of leaving it zero-width.
                let mut skipped = placeholder;
                skipped.epoch_length = epoch_length;
                skipped.slot_length = slot_length;

                for protocol in [2u16, 3] {
                    let mut row = skipped.clone();
                    row.protocol = protocol;
                    out.push(row);
                }

                skipped.protocol = 4;
                skipped
            }
            Some(magic) => {
                return Err(ChainError::InvalidConfig(format!(
                    "unsupported network magic for era history padding: {magic}"
                )));
            }
            None => {
                return Err(ChainError::GenesisFieldMissing(
                    "shelley.network_magic".into(),
                ))
            }
        };

    for (protocol, era) in eras
        .iter()
        .filter(|(protocol, _)| KNOWN_HARDFORKS.contains(protocol))
    {
        let start_time = era.slot_time(era.start.slot);
        let end_epoch = era.slot_epoch(tip).0;
        let end_time = era.slot_time(tip);

        previous.end = Some(EraBoundary {
            epoch: era.start.epoch,
            slot: era.start.slot,
            timestamp: start_time,
        });

        let current = EraSummary {
            start: EraBoundary {
                epoch: era.start.epoch,
                slot: era.start.slot,
                timestamp: start_time,
            },
            end: Some(EraBoundary {
                epoch: end_epoch,
                slot: tip,
                timestamp: end_time,
            }),
            epoch_length: era.epoch_length,
            slot_length: era.slot_length,
            protocol: *protocol,
        };

        out.push(previous);
        previous = current;
    }

    out.push(previous);

    Ok(out)
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

#[cfg(test)]
mod pad_era_history_tests {
    use super::*;

    fn era(
        protocol: u16,
        start_epoch: u64,
        start_slot: u64,
        start_timestamp: u64,
        epoch_length: u64,
        slot_length: u64,
    ) -> (u16, EraSummary) {
        (
            protocol,
            EraSummary {
                start: EraBoundary {
                    epoch: start_epoch,
                    slot: start_slot,
                    timestamp: start_timestamp,
                },
                end: None,
                epoch_length,
                slot_length,
                protocol,
            },
        )
    }

    #[test]
    fn pads_byron_for_mainnet_and_chains_to_the_first_recorded_era() {
        let genesis = crate::include::mainnet::load();
        let system_start = parse_system_start(&genesis).unwrap();
        let tip = 200_000_000;

        // Byron -> Shelley on mainnet: epoch 208, slot 4_492_800. The
        // node only ever recorded Shelley onward as on-chain state.
        let shelley_start_timestamp = system_start + 89_856_000;
        let eras = vec![era(2, 208, 4_492_800, shelley_start_timestamp, 432_000, 1)];

        let padded = pad_era_history(&eras, tip, &genesis).unwrap();

        assert_eq!(padded.len(), 2, "byron placeholder + the one recorded era");

        let byron = &padded[0];
        assert_eq!(byron.protocol, 0);
        assert_eq!(byron.start.slot, 0);
        assert_eq!(byron.start.epoch, 0);
        assert_eq!(byron.start.timestamp, system_start);

        let byron_end = byron.end.as_ref().unwrap();
        let shelley = &padded[1];

        // chained: byron's end is overwritten by shelley's own recorded start
        assert_eq!(byron_end.slot, shelley.start.slot);
        assert_eq!(byron_end.epoch, shelley.start.epoch);
        assert_eq!(byron_end.timestamp, shelley.start.timestamp);
        assert_eq!(byron_end.timestamp, shelley_start_timestamp);

        // the open (last) row's end is derived from tip, not hardcoded
        let shelley_end = shelley.end.as_ref().unwrap();
        assert_eq!(shelley_end.slot, tip);
    }

    #[test]
    fn dedupes_intra_era_protocol_bumps() {
        let genesis = crate::include::preprod::load();
        let tip = 100_000_000;

        // 5->6 (alonzo), 7->8 (babbage) and 9->10 (conway) are intra-era
        // bumps that must not produce a duplicate named row.
        let eras = vec![
            era(2, 4, 86_400, 0, 432_000, 1),
            era(3, 5, 518_400, 0, 432_000, 1),
            era(4, 6, 950_400, 0, 432_000, 1),
            era(5, 7, 1_382_400, 0, 432_000, 1),
            era(6, 9, 2_246_400, 0, 432_000, 1),
            era(7, 12, 3_542_400, 0, 432_000, 1),
            era(8, 20, 5_000_000, 0, 432_000, 1),
            era(9, 163, 68_774_400, 0, 432_000, 1),
            era(10, 250, 90_000_000, 0, 432_000, 1),
        ];

        let padded = pad_era_history(&eras, tip, &genesis).unwrap();

        let protocols: Vec<u16> = padded.iter().map(|e| e.protocol).collect();
        assert_eq!(protocols, vec![0, 2, 3, 4, 5, 7, 9]);
    }

    #[test]
    fn pads_skipped_eras_for_preview() {
        let genesis = crate::include::preview::load();
        let system_start = parse_system_start(&genesis).unwrap();
        let tip = 50_000_000;

        // preview's real recorded history starts at Alonzo — Shelley,
        // Allegra and Mary never happened as tracked on-chain state. A
        // real era starting at slot 0 carries an absolute timestamp of
        // `system_start` (see `genesis::bootstrap_eras`), not zero.
        let eras = vec![era(5, 0, 0, system_start, 432_000, 1)];

        let padded = pad_era_history(&eras, tip, &genesis).unwrap();

        let protocols: Vec<u16> = padded.iter().map(|e| e.protocol).collect();
        assert_eq!(protocols, vec![0, 2, 3, 4, 5]);

        // the skipped-era placeholders (shelley, allegra, mary) are
        // zero-width, sitting at system_start
        for row in &padded[1..4] {
            let end = row.end.as_ref().unwrap();
            assert_eq!(row.start.slot, end.slot);
            assert_eq!(row.start.epoch, end.epoch);
            assert_eq!(row.start.timestamp, end.timestamp);
        }
    }

    #[test]
    fn rejects_unsupported_network_magic() {
        let mut genesis = crate::include::preview::load();
        genesis.shelley.network_magic = Some(42);

        let err = pad_era_history(&[], 0, &genesis).unwrap_err();
        assert!(matches!(err, ChainError::InvalidConfig(_)));
    }

    #[test]
    fn pads_with_no_recorded_eras_at_all() {
        let genesis = crate::include::mainnet::load();
        let system_start = parse_system_start(&genesis).unwrap();

        let padded = pad_era_history(&[], 999, &genesis).unwrap();

        assert_eq!(padded.len(), 1);
        assert_eq!(padded[0].protocol, 0);
        let end = padded[0].end.as_ref().unwrap();
        assert_eq!(end.epoch, 208);
        assert_eq!(end.timestamp, system_start + 89_856_000);
    }
}
