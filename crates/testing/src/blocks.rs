use dolos_core::{BlockBody, BlockHash, BlockSlot, RawBlock};
use pallas::{
    codec::utils::{Bytes, KeepRaw},
    ledger::{
        primitives::{
            VrfCert,
            conway::{Header, OperationalCert},
        },
        traverse::Era,
    },
};

pub fn slot_to_hash(slot: u64) -> BlockHash {
    let mut hasher = pallas::crypto::hash::Hasher::<256>::new();
    hasher.input(&(slot as i32).to_le_bytes());
    hasher.finalize()
}

pub fn make_conway_block(slot: BlockSlot) -> RawBlock {
    let hash = slot_to_hash(slot);

    let block = pallas::ledger::primitives::conway::Block {
        header: KeepRaw::from(Header {
            header_body: pallas::ledger::primitives::conway::HeaderBody {
                slot,
                block_number: 0,
                block_body_hash: slot_to_hash(slot),
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

    let wrapper = (Era::Conway as u16, block);

    RawBlock {
        slot,
        hash,
        era: Era::Conway,
        body: pallas::codec::minicbor::to_vec(&wrapper).unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use pallas::ledger::traverse::MultiEraBlock;

    use super::*;

    #[test]
    fn test_fake_block_can_be_decoded() {
        let block = make_conway_block(1);
        let _ = MultiEraBlock::decode(&block.body).unwrap();
    }
}
