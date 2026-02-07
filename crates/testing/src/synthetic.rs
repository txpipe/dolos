use std::{collections::BTreeMap, sync::Arc};

use dolos_core::{
    config::{CardanoConfig, CustomUtxo},
    ChainPoint, RawBlock, TxoRef,
};

use crate::{tx_sequence_to_hash, utxo_with_value};

use pallas::codec::{minicbor, utils::KeepRaw};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        addresses::Address,
        primitives::{
            alonzo,
            conway::{
                Certificate, PostAlonzoTransactionOutput, TransactionBody, TransactionOutput, Value,
            },
            AddrKeyhash, Bytes, NonEmptySet, NonZeroInt, PositiveCoin, Set, StakeCredential,
            TransactionInput, VrfKeyhash,
        },
        traverse::ComputeHash,
    },
};
use bech32::{FromBase32, ToBase32};

#[derive(Clone, Debug)]
pub struct SyntheticBlockConfig {
    pub address: String,
    pub seed_address: String,
    pub slot: u64,
    pub metadata_label: u64,
    pub metadata_value: String,
    pub policy_id: [u8; 28],
    pub asset_name: String,
    pub lovelace: u64,
    pub asset_amount: u64,
    pub mint_amount: i64,
    pub seed_amount: u64,
    pub pool_id: String,
}

impl Default for SyntheticBlockConfig {
    fn default() -> Self {
        let address = crate::TestAddress::Alice.as_str().to_string();
        let seed_address = crate::TestAddress::Bob.as_str().to_string();
        Self {
            seed_address,
            address,
            slot: 1,
            metadata_label: 1990,
            metadata_value: "synthetic".to_string(),
            policy_id: [1u8; 28],
            asset_name: "SYNTH".to_string(),
            lovelace: crate::MIN_UTXO_AMOUNT,
            asset_amount: 1,
            mint_amount: 1,
            seed_amount: crate::MIN_UTXO_AMOUNT,
            pool_id: "pool1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
                .to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SyntheticVectors {
    pub address: String,
    pub stake_address: String,
    pub asset_unit: String,
    pub metadata_label: String,
    pub block_hash: String,
    pub tx_hash: String,
    pub pool_id: String,
}

pub fn build_synthetic_blocks(
    cfg: SyntheticBlockConfig,
) -> (Vec<RawBlock>, SyntheticVectors, CardanoConfig) {
    let seed_tx_hash = tx_sequence_to_hash(1);
    let seed_ref = TxoRef(seed_tx_hash, 0);

    let seed_utxo = utxo_with_value(cfg.seed_address.clone(), Value::Coin(cfg.seed_amount));
    let crate::EraCbor(_, seed_cbor) = seed_utxo;

    let mut chain_config = CardanoConfig::default();
    chain_config.custom_utxos.push(CustomUtxo {
        ref_: seed_ref,
        era: Some(pallas::ledger::traverse::Era::Conway.into()),
        cbor: seed_cbor,
    });

    let address_bytes = Address::from_bech32(&cfg.address)
        .expect("invalid synthetic address")
        .to_vec();
    let metadata: alonzo::Metadata = vec![(
        cfg.metadata_label,
        alonzo::Metadatum::Text(cfg.metadata_value.clone()),
    )]
    .into_iter()
    .collect();

    let aux_data = alonzo::AuxiliaryData::PostAlonzo(alonzo::PostAlonzoAuxiliaryData {
        metadata: Some(metadata),
        native_scripts: None,
        plutus_scripts: None,
    });

    let policy_id = Hash::from(cfg.policy_id);
    let asset_name = Bytes::from(cfg.asset_name.as_bytes().to_vec());

    let stake_cred = Address::from_bech32(&cfg.address)
        .ok()
        .and_then(|addr| dolos_cardano::pallas_extras::address_as_stake_cred(&addr))
        .map(|(cred, _)| cred)
        .unwrap_or_else(|| StakeCredential::AddrKeyhash(AddrKeyhash::from([0u8; 28])));

    let (pool_keyhash, pool_id) = match pool_keyhash_from_bech32(&cfg.pool_id) {
        Ok(hash) => (hash, cfg.pool_id.clone()),
        Err(_) => {
            let fallback = Hash::from([9u8; 28]);
            (fallback, pool_id_from_keyhash(fallback))
        }
    };

    let (block, tx_hash) = sample_block(
        cfg.slot,
        Bytes::from(address_bytes),
        cfg.lovelace,
        seed_tx_hash,
        policy_id,
        asset_name,
        cfg.asset_amount,
        cfg.mint_amount,
        stake_cred,
        pool_keyhash,
        Some(aux_data),
    );

    let block_hash = block.header.compute_hash();
    let wrapper = (7, block);

    let raw_block = Arc::new(minicbor::to_vec(wrapper).unwrap());

    let asset_unit = format!(
        "{}{}",
        hex::encode(cfg.policy_id),
        hex::encode(cfg.asset_name.as_bytes())
    );

    let stake_address = match Address::from_bech32(&cfg.address).expect("invalid synthetic address")
    {
        Address::Shelley(shelley) => {
            dolos_cardano::pallas_extras::shelley_address_to_stake_address(&shelley)
                .and_then(|stake| stake.to_bech32().ok())
                .unwrap_or_default()
        }
        _ => String::new(),
    };

    let vectors = SyntheticVectors {
        address: cfg.address,
        stake_address,
        asset_unit,
        metadata_label: cfg.metadata_label.to_string(),
        block_hash: block_hash.to_string(),
        tx_hash: tx_hash.to_string(),
        pool_id,
    };

    (vec![raw_block], vectors, chain_config)
}

fn sample_transaction_body(
    address: Bytes,
    lovelace: u64,
    tx_hash: Hash<32>,
    policy_id: Hash<28>,
    asset_name: Bytes,
    asset_amount: u64,
    mint_amount: i64,
    stake_cred: StakeCredential,
    pool_keyhash: Hash<28>,
) -> TransactionBody<'static> {
    let input = TransactionInput {
        transaction_id: tx_hash,
        index: 0,
    };

    let mint_amount =
        NonZeroInt::try_from(mint_amount).expect("mint amount must be non-zero");
    let mut mint_assets = BTreeMap::new();
    mint_assets.insert(asset_name.clone(), mint_amount);
    let mut mint = BTreeMap::new();
    mint.insert(policy_id, mint_assets);

    let asset_amount =
        PositiveCoin::try_from(asset_amount).expect("asset amount must be non-zero");
    let mut output_assets = BTreeMap::new();
    output_assets.insert(asset_name.clone(), asset_amount);
    let mut output_multiasset = BTreeMap::new();
    output_multiasset.insert(policy_id, output_assets);

    let output = PostAlonzoTransactionOutput {
        address,
        value: Value::Multiasset(lovelace, output_multiasset),
        datum_option: None,
        script_ref: None,
    };

    let vrf_keyhash = VrfKeyhash::from([1u8; 32]);
    let pool_owner = AddrKeyhash::from([2u8; 28]);
    let pool_metadata = None;
    let pool_cert = Certificate::PoolRegistration {
        operator: pool_keyhash,
        vrf_keyhash,
        pledge: 0,
        cost: 0,
        margin: pallas::ledger::primitives::RationalNumber {
            numerator: 0,
            denominator: 1,
        },
        reward_account: Bytes::from(vec![0u8]),
        pool_owners: Set::from(vec![pool_owner]),
        relays: vec![],
        pool_metadata,
    };

    let delegation = Certificate::StakeDelegation(stake_cred.clone(), pool_keyhash);
    let registration = Certificate::StakeRegistration(stake_cred);

    let certificates = NonEmptySet::try_from(vec![registration, delegation, pool_cert])
        .expect("non-empty certificates");

    TransactionBody {
        inputs: Set::from(vec![input]),
        outputs: vec![TransactionOutput::PostAlonzo(KeepRaw::from(output))],
        fee: 7,
        ttl: Some(10),
        certificates: Some(certificates),
        withdrawals: None,
        auxiliary_data_hash: None,
        validity_interval_start: Some(5),
        mint: Some(mint),
        script_data_hash: None,
        collateral: None,
        required_signers: None,
        network_id: None,
        collateral_return: None,
        total_collateral: None,
        reference_inputs: None,
        voting_procedures: None,
        proposal_procedures: None,
        treasury_value: None,
        donation: None,
    }
}

fn sample_block(
    slot: u64,
    address: Bytes,
    lovelace: u64,
    tx_hash: Hash<32>,
    policy_id: Hash<28>,
    asset_name: Bytes,
    asset_amount: u64,
    mint_amount: i64,
    stake_cred: StakeCredential,
    pool_keyhash: Hash<28>,
    aux_data: Option<alonzo::AuxiliaryData>,
) -> (pallas::ledger::primitives::conway::Block<'static>, Hash<32>) {
    let header_body = pallas::ledger::primitives::conway::HeaderBody {
        block_number: 1,
        slot,
        prev_hash: Some(Hash::from([9u8; 32])),
        issuer_vkey: Bytes::from(vec![0x10, 0x11]),
        vrf_vkey: Bytes::from(vec![0x12, 0x13]),
        vrf_result: pallas::ledger::primitives::VrfCert(
            Bytes::from(vec![0x14]),
            Bytes::from(vec![0x15]),
        ),
        block_body_size: 0,
        block_body_hash: Hash::from([0u8; 32]),
        operational_cert: pallas::ledger::primitives::conway::OperationalCert {
            operational_cert_hot_vkey: Bytes::from(vec![0x16]),
            operational_cert_sequence_number: 1,
            operational_cert_kes_period: 0,
            operational_cert_sigma: Bytes::from(vec![0x17]),
        },
        protocol_version: (1, 0),
    };

    let header = pallas::ledger::primitives::conway::Header {
        header_body,
        body_signature: Bytes::from(vec![0x18]),
    };

    let body = sample_transaction_body(
        address,
        lovelace,
        tx_hash,
        policy_id,
        asset_name,
        asset_amount,
        mint_amount,
        stake_cred,
        pool_keyhash,
    );
    let body_hash = body.compute_hash();
    let witness_set = pallas::ledger::primitives::conway::WitnessSet {
        vkeywitness: None,
        native_script: None,
        bootstrap_witness: None,
        plutus_v1_script: None,
        plutus_data: None,
        redeemer: None,
        plutus_v2_script: None,
        plutus_v3_script: None,
    };

    let block = pallas::ledger::primitives::conway::Block {
        header: KeepRaw::from(header),
        transaction_bodies: vec![KeepRaw::from(body)],
        transaction_witness_sets: vec![KeepRaw::from(witness_set)],
        auxiliary_data_set: match aux_data {
            Some(aux) => {
                let mut map = BTreeMap::new();
                map.insert(0u32, KeepRaw::from(aux));
                map
            }
            None => BTreeMap::new(),
        },
        invalid_transactions: None,
    };

    (block, body_hash)
}

fn pool_keyhash_from_bech32(pool_id: &str) -> Result<Hash<28>, bech32::Error> {
    let (_hrp, data, _variant) = bech32::decode(pool_id)?;
    let raw = Vec::<u8>::from_base32(&data).map_err(|_| bech32::Error::InvalidData(0))?;
    let bytes: [u8; 28] = raw
        .as_slice()
        .try_into()
        .map_err(|_| bech32::Error::InvalidLength)?;
    Ok(Hash::from(bytes))
}

fn pool_id_from_keyhash(hash: Hash<28>) -> String {
    bech32::encode("pool", hash.as_ref().to_base32(), bech32::Variant::Bech32)
        .expect("failed to encode pool id")
}

#[cfg(test)]
mod tests {

    use pallas::ledger::traverse::MultiEraBlock;

    use super::*;

    #[test]
    fn synthetic_block_decodes() {
        let (blocks, _vectors, _cfg) = build_synthetic_blocks(SyntheticBlockConfig::default());
        let block = blocks.first().expect("missing synthetic block");
        let raw = block.as_ref();
        if let Err(err) = MultiEraBlock::decode(raw) {
            panic!("failed to decode synthetic block: {err:?}");
        }
    }
}
