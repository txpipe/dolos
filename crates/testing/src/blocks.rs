use std::sync::Arc;

use dolos_core::{BlockHash, BlockSlot, ChainPoint, RawBlock};
use pallas::{
    codec::utils::{Bytes, KeepRaw},
    ledger::{
        primitives::{
            conway::{Header, OperationalCert},
            VrfCert,
        },
        traverse::{ComputeHash, Era},
    },
};

pub fn slot_to_hash(slot: u64) -> BlockHash {
    let mut hasher = pallas::crypto::hash::Hasher::<256>::new();
    hasher.input(&(slot as i32).to_le_bytes());
    hasher.finalize()
}

pub fn make_conway_block(slot: BlockSlot) -> (ChainPoint, RawBlock) {
    let block_body_hash = slot_to_hash(slot);

    let block = pallas::ledger::primitives::conway::Block {
        header: KeepRaw::from(Header {
            header_body: pallas::ledger::primitives::conway::HeaderBody {
                slot,
                block_body_hash,
                block_number: 0,
                prev_hash: None,
                issuer_vkey: vec![].into(),
                vrf_vkey: vec![].into(),
                vrf_result: VrfCert(vec![].into(), vec![].into()),
                block_body_size: 0,
                protocol_version: (1, 0),
                operational_cert: OperationalCert {
                    operational_cert_hot_vkey: vec![].into(),
                    operational_cert_sequence_number: 0,
                    operational_cert_kes_period: 0,
                    operational_cert_sigma: vec![].into(),
                },
            },
            body_signature: Bytes::from(vec![]),
        }),
        transaction_bodies: Default::default(),
        transaction_witness_sets: Default::default(),
        auxiliary_data_set: Default::default(),
        invalid_transactions: Default::default(),
    };

    let hash = block.header.compute_hash();

    let wrapper = (Era::Conway as u16, block);

    let raw_bytes = pallas::codec::minicbor::to_vec(&wrapper).unwrap();
    let chain_point = ChainPoint::Specific(slot, hash);

    (chain_point, Arc::new(raw_bytes))
}

#[cfg(test)]
mod tests {
    use pallas::ledger::traverse::MultiEraBlock;

    use super::*;

    #[test]
    fn test_fake_block_can_be_decoded() {
        let (_, body) = make_conway_block(1);
        let _ = MultiEraBlock::decode(&body).unwrap();
    }
}
