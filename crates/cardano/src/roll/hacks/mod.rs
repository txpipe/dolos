use pallas::ledger::validate::utils::MultiEraProtocolParameters;

mod mainnet_epoch526;
mod preprod_epoch191;
mod preview_epoch736;

pub fn mainnet(eras: &mut MultiEraProtocolParameters, current_slot: u64) {
    if current_slot >= mainnet_epoch526::SLOT {
        eras.apply_hacks(mainnet_epoch526::SLOT, mainnet_epoch526::change);
    }
}

pub fn preprod(eras: &mut MultiEraProtocolParameters, current_slot: u64) {
    if current_slot >= preprod_epoch191::SLOT {
        eras.apply_hacks(preprod_epoch191::SLOT, preprod_epoch191::change);
    }
}

pub fn preview(eras: &mut MultiEraProtocolParameters, current_slot: u64) {
    if current_slot >= preview_epoch736::SLOT {
        eras.apply_hacks(preview_epoch736::SLOT, preview_epoch736::change);
    }
}
