use dolos_core::{batch::WorkDeltas, BlockSlot, ChainError, NsKey};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::conway::ProposalProcedure,
        traverse::{MultiEraBlock, MultiEraTx},
    },
};
use serde::{Deserialize, Serialize};

use crate::{model::FixedNamespace as _, roll::BlockVisitor, CardanoLogic, Proposal};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewProposal {
    slot: BlockSlot,
    transaction: Hash<32>,
    idx: u32,
    procedure: ProposalProcedure,
}

impl dolos_core::EntityDelta for NewProposal {
    type Entity = Proposal;

    fn key(&self) -> NsKey {
        NsKey::from((
            Proposal::NS,
            Proposal::build_entity_key(self.transaction, self.idx),
        ))
    }

    fn apply(&mut self, entity: &mut Option<Proposal>) {
        let _ = entity.insert(Proposal::new(
            self.slot,
            self.transaction,
            self.idx,
            self.procedure.clone(),
        ));
    }

    fn undo(&self, entity: &mut Option<Proposal>) {
        entity.take();
    }
}

#[derive(Clone, Default)]
pub struct ProposalVisitor;

impl BlockVisitor for ProposalVisitor {
    fn visit_proposal(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        proposal: &pallas::ledger::traverse::MultiEraProposal,
        idx: usize,
    ) -> Result<(), ChainError> {
        if let Some(procedure) = proposal.as_conway() {
            deltas.add_for_entity(NewProposal {
                slot: block.slot(),
                transaction: tx.hash(),
                idx: idx as u32,
                procedure: procedure.to_owned(),
            });
        }

        Ok(())
    }
}
