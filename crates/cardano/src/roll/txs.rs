use dolos_core::{batch::WorkDeltas, ChainError, SlotTags};
use pallas::{
    codec::utils::KeepRaw,
    ledger::{
        addresses::{Address, ShelleyDelegationPart},
        primitives::{conway::DatumOption, PlutusData},
        traverse::{MultiEraBlock, MultiEraTx, MultiEraValue, OriginalHash as _},
    },
};

use crate::{roll::BlockVisitor, CardanoLogic};

#[derive(Default, Clone)]
pub struct TxLogVisitor;

fn unpack_address(tags: &mut SlotTags, address: &Address) {
    match address {
        Address::Shelley(x) => {
            tags.full_addresses.push(x.to_vec());
            tags.payment_addresses.push(x.payment().to_vec());

            match x.delegation() {
                ShelleyDelegationPart::Key(..) => {
                    tags.stake_addresses.push(x.delegation().to_vec());
                }
                ShelleyDelegationPart::Script(..) => {
                    tags.stake_addresses.push(x.delegation().to_vec());
                }
                _ => (),
            };
        }
        Address::Stake(x) => {
            tags.full_addresses.push(x.to_vec());
            tags.stake_addresses.push(x.to_vec());
        }
        Address::Byron(x) => {
            tags.full_addresses.push(x.to_vec());
        }
    }
}

fn unpack_assets(tags: &mut SlotTags, assets: &MultiEraValue) {
    let assets = assets.assets();

    for ma in assets {
        tags.policies.push(ma.policy().to_vec());

        for asset in ma.assets() {
            let mut subject = asset.policy().to_vec();
            subject.extend(asset.name());

            tags.assets.push(subject);
        }
    }
}

fn unpack_datum(tags: &mut SlotTags, datum: &DatumOption) {
    match datum {
        DatumOption::Hash(hash) => {
            tags.datums.push(hash.to_vec());
        }
        DatumOption::Data(datum) => {
            tags.datums.push(datum.original_hash().to_vec());
        }
    }
}

impl BlockVisitor for TxLogVisitor {
    fn visit_tx(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        tx: &MultiEraTx,
    ) -> Result<(), ChainError> {
        deltas.slot.tx_hashes.push(tx.hash().to_vec());

        Ok(())
    }

    fn visit_input(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: &pallas::ledger::traverse::MultiEraInput,
        resolved: &pallas::ledger::traverse::MultiEraOutput,
    ) -> Result<(), ChainError> {
        if let Ok(address) = resolved.address() {
            unpack_address(&mut deltas.slot, &address);
        }

        unpack_assets(&mut deltas.slot, &resolved.value());

        if let Some(datum) = resolved.datum() {
            unpack_datum(&mut deltas.slot, &datum);
        }

        Ok(())
    }

    fn visit_output(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: u32,
        output: &pallas::ledger::traverse::MultiEraOutput,
    ) -> Result<(), ChainError> {
        if let Ok(address) = output.address() {
            unpack_address(&mut deltas.slot, &address);
        }

        unpack_assets(&mut deltas.slot, &output.value());

        if let Some(datum) = output.datum() {
            unpack_datum(&mut deltas.slot, &datum);
        }

        Ok(())
    }

    fn visit_datums(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        datum: &KeepRaw<'_, PlutusData>,
    ) -> Result<(), ChainError> {
        deltas.slot.datums.push(datum.original_hash().to_vec());

        Ok(())
    }
}
