use super::ChainSummary;

mod mainnet_epoch526;
mod preview_epoch736;

pub fn mainnet(eras: &mut ChainSummary, current_slot: u64) {
    if current_slot >= mainnet_epoch526::SLOT {
        eras.apply_hacks(mainnet_epoch526::SLOT, mainnet_epoch526::change);
    }
}

pub fn preprod(eras: &mut ChainSummary, current_slot: u64) {
    // TODO
}

pub fn preview(eras: &mut ChainSummary, current_slot: u64) {
    if current_slot >= preview_epoch736::SLOT {
        eras.apply_hacks(preview_epoch736::SLOT, preview_epoch736::change);
    }
}
