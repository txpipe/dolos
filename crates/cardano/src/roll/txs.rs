use dolos_core::{batch::WorkDeltas, ChainError, SlotTags};
use pallas::ledger::{
    addresses::{Address, ShelleyDelegationPart},
    traverse::{MultiEraBlock, MultiEraTx},
};

use crate::{roll::BlockVisitor, CardanoLogic};

#[derive(Default)]
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
        let address = resolved.address().ok();

        if let Some(address) = address {
            unpack_address(&mut deltas.slot, &address);
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
        let address = output.address().ok();

        if let Some(address) = address {
            unpack_address(&mut deltas.slot, &address);
        }

        Ok(())
    }
}
