use std::sync::Arc;

use dolos_core::{BlockHash, BlockSlot, ChainPoint, RawBlock};
use pallas::{
    codec::utils::{Bytes, KeepRaw},
    crypto::hash::Hash,
    ledger::{
        primitives::{
            alonzo,
            conway::{Block, Header, HeaderBody, OperationalCert, WitnessSet},
            VrfCert,
        },
        traverse::{ComputeHash, Era},
    },
};
use std::collections::BTreeMap;

pub fn slot_to_hash(slot: u64) -> BlockHash {
    let mut hasher = pallas::crypto::hash::Hasher::<256>::new();
    hasher.input(&(slot as i32).to_le_bytes());
    hasher.finalize()
}

pub fn make_conway_block(slot: BlockSlot) -> (ChainPoint, RawBlock) {
    let block_body_hash = slot_to_hash(slot);

    let header = KeepRaw::from(Header {
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
    });

    let block = pallas::ledger::primitives::conway::Block {
        header,
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

pub fn make_conway_block_with_tx(
    _slot: BlockSlot,
    _tx_body: pallas::ledger::primitives::conway::TransactionBody<'static>,
    auxiliary_data: Option<alonzo::AuxiliaryData>,
) -> (ChainPoint, RawBlock) {
    let header_body = HeaderBody {
        block_number: 1,
        slot: 10,
        prev_hash: Some(Hash::from([9u8; 32])),
        issuer_vkey: Bytes::from(vec![0x10, 0x11]),
        vrf_vkey: Bytes::from(vec![0x12, 0x13]),
        vrf_result: VrfCert(Bytes::from(vec![0x14]), Bytes::from(vec![0x15])),
        block_body_size: 0,
        block_body_hash: Hash::from([0u8; 32]),
        operational_cert: OperationalCert {
            operational_cert_hot_vkey: Bytes::from(vec![0x16]),
            operational_cert_sequence_number: 1,
            operational_cert_kes_period: 0,
            operational_cert_sigma: Bytes::from(vec![0x17]),
        },
        protocol_version: (1, 0),
    };

    let header = Header {
        header_body,
        body_signature: Bytes::from(vec![0x18]),
    };

    let body = _tx_body;
    let witness_set = WitnessSet {
        vkeywitness: None,
        native_script: None,
        bootstrap_witness: None,
        plutus_v1_script: None,
        plutus_data: None,
        redeemer: None,
        plutus_v2_script: None,
        plutus_v3_script: None,
    };

    let block = Block {
        header: KeepRaw::from(header),
        transaction_bodies: vec![KeepRaw::from(body)],
        transaction_witness_sets: vec![KeepRaw::from(witness_set)],
        auxiliary_data_set: match auxiliary_data {
            Some(aux) => {
                let mut map = BTreeMap::new();
                map.insert(0u32, KeepRaw::from(aux));
                map
            }
            None => BTreeMap::new(),
        },
        invalid_transactions: None,
    };

    let hash = block.header.compute_hash();

    let wrapper = (Era::Conway as u16, block);

    let raw_bytes = pallas::codec::minicbor::to_vec(&wrapper).unwrap();
    let chain_point = ChainPoint::Specific(10, hash);

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
