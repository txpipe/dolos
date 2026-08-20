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
    make_conway_block_with_prev(slot, None, 0)
}

pub fn make_conway_block_with_prev(
    slot: BlockSlot,
    prev_hash: Option<Hash<32>>,
    block_number: u64,
) -> (ChainPoint, RawBlock) {
    let block_body_hash = slot_to_hash(slot);

    let header = KeepRaw::from(Header {
        header_body: pallas::ledger::primitives::conway::HeaderBody {
            slot,
            block_body_hash,
            block_number,
            prev_hash,
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
    slot: BlockSlot,
    tx_body: pallas::ledger::primitives::conway::TransactionBody<'static>,
    auxiliary_data: Option<alonzo::AuxiliaryData>,
) -> (ChainPoint, RawBlock) {
    let header_body = HeaderBody {
        block_number: 1,
        slot,
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

    let body = tx_body;
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
    let chain_point = ChainPoint::Specific(slot, hash);

    (chain_point, Arc::new(raw_bytes))
}

/// Number of slots in a Byron epoch on the default genesis values pallas uses
/// to resolve an epoch-boundary block's absolute slot.
pub const BYRON_EPOCH_LENGTH: u64 = 21_600;

/// The absolute slot a Byron epoch-boundary block for `epoch` lands on — the
/// same slot the epoch's first main block carries, which is the collision the
/// archive has to keep both sides of.
pub fn byron_ebb_slot(epoch: u64) -> BlockSlot {
    epoch * BYRON_EPOCH_LENGTH
}

/// Build a Byron epoch-boundary block opening `epoch`, chained onto
/// `prev_hash`.
///
/// The body is empty: nothing downstream of the archive reads an EBB's
/// stakeholder list, and what the tests need from it is the shape the era
/// probe and the header hash see.
pub fn make_byron_ebb(epoch: u64, prev_hash: Hash<32>) -> (ChainPoint, RawBlock) {
    use pallas::codec::utils::{EmptyMap, MaybeIndefArray};
    use pallas::ledger::primitives::byron::{EbBlock, EbbCons, EbbHead};

    let header = KeepRaw::from(EbbHead {
        protocol_magic: 764_824_073,
        prev_block: prev_hash,
        body_proof: Hash::new([0u8; 32]),
        consensus_data: EbbCons {
            epoch_id: epoch,
            difficulty: MaybeIndefArray::Def(vec![epoch]),
        },
        extra_data: (EmptyMap,),
    });

    let block = EbBlock {
        header,
        body: MaybeIndefArray::Def(vec![]),
        extra: MaybeIndefArray::Def(vec![]),
    };

    let wrapper = (0u16, block);
    let raw_bytes = pallas::codec::minicbor::to_vec(&wrapper).unwrap();

    // The header hash is taken off the encoded bytes rather than computed from
    // the value, so it is the hash every reader of the archived block will
    // arrive at.
    let decoded = pallas::ledger::traverse::MultiEraBlock::decode(&raw_bytes).unwrap();
    let point = ChainPoint::Specific(decoded.slot(), decoded.hash());

    debug_assert_eq!(decoded.slot(), byron_ebb_slot(epoch));

    (point, Arc::new(raw_bytes))
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
