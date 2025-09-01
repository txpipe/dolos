use dolos_core::*;

use crate::pparams::{ChainSummary, EraSummary};

/// Computes the amount of mutable slots in chain.
///
/// Reads the relevant genesis config values and uses the security window
/// guarantee formula from consensus to calculate the latest slot that can be
/// considered immutable.
pub fn mutable_slots(genesis: &Genesis) -> u64 {
    ((3.0 * genesis.byron.protocol_consts.k as f32) / (genesis.shelley.active_slots_coeff.unwrap()))
        as u64
}

/// Computes the latest immutable slot
///
/// Takes the latest known tip, reads the relevant genesis config values and
/// uses the security window guarantee formula from consensus to calculate the
/// latest slot that can be considered immutable. This is used mainly to define
/// which slots can be finalized in the ledger store (aka: compaction).
pub fn lastest_immutable_slot(tip: BlockSlot, genesis: &Genesis) -> BlockSlot {
    tip.saturating_sub(mutable_slots(genesis))
}

pub type Timestamp = u64;

pub fn slot_time_within_era(slot: u64, era: &EraSummary) -> Timestamp {
    let time = era.start.timestamp.timestamp() as u64
        + (slot - era.start.slot) * era.pparams.slot_length();

    time as Timestamp
}

/// Resolve wall-clock time from a slot number and a chain summary.
pub fn slot_time(slot: u64, summary: &ChainSummary) -> Timestamp {
    let era = summary.era_for_slot(slot);

    slot_time_within_era(slot, era)
}

pub type Epoch = u32;
pub type EpochSlot = u32;

/// Resolve epoch and sub-epoch slot from a slot number and a chain summary.
pub fn slot_epoch(slot: u64, summary: &ChainSummary) -> (Epoch, EpochSlot) {
    let era = summary.era_for_slot(slot);
    let era_slot = slot - era.start.slot;
    let era_epoch = era_slot / era.pparams.epoch_length();
    let epoch = era.start.epoch + era_epoch;
    let epoch_slot = era_slot - era_epoch * era.pparams.epoch_length();

    (epoch as Epoch, epoch_slot as EpochSlot)
}

pub fn load_genesis(path: &std::path::Path) -> Genesis {
    let byron = pallas::ledger::configs::byron::from_file(&path.join("byron.json")).unwrap();
    let shelley = pallas::ledger::configs::shelley::from_file(&path.join("shelley.json")).unwrap();
    let alonzo = pallas::ledger::configs::alonzo::from_file(&path.join("alonzo.json")).unwrap();
    let conway = pallas::ledger::configs::conway::from_file(&path.join("conway.json")).unwrap();

    Genesis {
        byron,
        shelley,
        alonzo,
        conway,
        force_protocol: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lastest_immutable_slot() {
        let path = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("test_data")
            .join("mainnet")
            .join("genesis");

        let genesis = load_genesis(&path);

        let tip: BlockSlot = 1_000_000;

        let result = lastest_immutable_slot(tip, &genesis);

        // slot delta in hours
        let delta_in_hours = tip.saturating_sub(result) / (60 * 60);

        // the well-known volatility window for mainnet is 36 hours.
        assert_eq!(delta_in_hours, 36);
    }
}
