//! Stake addresses (decision 0049): the first output of each epoch that pays
//! an address of an account, staged until the epoch seals.

use std::collections::HashSet;

use dolos_core::{ChainError, Genesis, TxOrder};
use pallas::ledger::{
    primitives::{Epoch, StakeCredential},
    traverse::{MultiEraBlock, MultiEraOutput, MultiEraTx},
};

use super::{BlockVisitor, WorkDeltas};
use crate::{pallas_extras, PParamsSet, StageStakeAddress, StagedStakeAddress};

#[derive(Default)]
pub struct StakeAddressVisitor {
    epoch: Option<Epoch>,

    /// Account addresses already staged by this block: a repeat changes no
    /// staged row but would still cost a WAL delta.
    staged: HashSet<(StakeCredential, Vec<u8>)>,
}

impl BlockVisitor for StakeAddressVisitor {
    fn visit_root(
        &mut self,
        _: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &Genesis,
        _: &PParamsSet,
        epoch: Epoch,
        _: u64,
        _: u16,
    ) -> Result<(), ChainError> {
        self.epoch = Some(epoch);
        Ok(())
    }

    fn visit_output(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        order: &TxOrder,
        index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), ChainError> {
        let address = output.address().expect("valid address");

        let Some(account) = pallas_extras::address_account(&address) else {
            return Ok(());
        };

        let bytes = address.to_vec();

        if self.staged.insert((account.clone(), bytes.clone())) {
            deltas.add_for_entity(StageStakeAddress::new(StagedStakeAddress {
                account,
                address: bytes,
                epoch: self.epoch.expect("value set in root"),
                slot: block.slot(),
                tx_index: *order as u32,
                output_index: index,
            }));
        }

        Ok(())
    }
}
