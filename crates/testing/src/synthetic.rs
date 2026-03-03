use std::{collections::BTreeMap, sync::Arc};

use dolos_core::{
    config::{CardanoConfig, CustomUtxo},
    ArchiveStore, ArchiveWriter, ChainError, Domain, LogKey, RawBlock, TemporalKey, TxoRef,
};

use crate::{tx_sequence_to_hash, utxo_with_value};

use bech32::{FromBase32, ToBase32, Variant};
use dolos_cardano::model::MemberRewardLog;
use dolos_cardano::rupd::credential_to_key;
use pallas::codec::utils::Nullable;
use pallas::codec::{minicbor, utils::KeepRaw};
use pallas::crypto::{hash::Hasher, key::ed25519::SecretKeyExtended};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        addresses::{
            Address, Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
            StakeAddress, StakePayload,
        },
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

#[derive(Clone, Debug)]
pub struct SyntheticBlockConfig {
    pub address: String,
    pub seed_address: String,
    pub block_count: usize,
    pub txs_per_block: usize,
    pub start_block: u64,
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
    pub drep_keyhash: [u8; 28],
    pub drep_deposit: u64,
}

impl Default for SyntheticBlockConfig {
    fn default() -> Self {
        let address = crate::TestAddress::Alice.as_str().to_string();
        let seed_address = crate::TestAddress::Bob.as_str().to_string();
        Self {
            seed_address,
            address,
            block_count: 3,
            txs_per_block: 2,
            start_block: 1,
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
            drep_keyhash: [7u8; 28],
            drep_deposit: 1000,
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
    pub blocks: Vec<BlockVectors>,
    pub account_addresses: Vec<String>,
    pub account_address_blocks: Vec<(String, u64)>,
    pub account_address_bounds: Vec<(String, u64, u64)>,
    pub pool_id: String,
    pub drep_id: String,
    pub tx_cbor: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct BlockVectors {
    pub block_number: u64,
    pub slot: u64,
    pub block_hash: String,
    pub tx_hashes: Vec<String>,
}

impl SyntheticVectors {
    pub fn tx_position(&self, hash: &str) -> (u64, usize) {
        self.blocks
            .iter()
            .find_map(|block| {
                block
                    .tx_hashes
                    .iter()
                    .position(|x| x == hash)
                    .map(|idx| (block.block_number, idx))
            })
            .expect("missing tx hash in vectors")
    }
}

pub fn build_synthetic_blocks(
    cfg: SyntheticBlockConfig,
) -> (Vec<RawBlock>, SyntheticVectors, CardanoConfig) {
    let submit_sk = unsafe { SecretKeyExtended::from_bytes_unchecked([3u8; 64]) };
    let submit_pk = submit_sk.public_key();
    let submit_keyhash = keyhash_from_pubkey(submit_pk.as_ref());
    let submit_address = ShelleyAddress::new(
        Network::Testnet,
        ShelleyPaymentPart::key_hash(submit_keyhash),
        ShelleyDelegationPart::Null,
    )
    .to_bech32()
    .expect("failed to encode submit address");
    let submit_amount = cfg.lovelace * 10;
    let submit_output = cfg.lovelace * 5;

    let mut chain_config = CardanoConfig::default();

    let address_bytes = Address::from_bech32(&cfg.address)
        .expect("invalid synthetic address")
        .to_vec();
    let network = Address::from_bech32(&cfg.address)
        .ok()
        .and_then(|addr| match addr {
            Address::Shelley(shelley) => Some(shelley.network()),
            Address::Stake(_) | Address::Byron(_) => None,
        })
        .unwrap_or(Network::Testnet);
    let metadata_label = cfg.metadata_label;

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

    let mut raw_blocks = Vec::with_capacity(cfg.block_count.max(1));
    let mut block_vectors = Vec::with_capacity(cfg.block_count.max(1));
    let mut first_block_hash = None;
    let mut first_tx_hash = None;
    let mut account_addresses = Vec::new();
    let mut account_address_blocks = Vec::new();
    let mut account_addresses_seen = std::collections::HashSet::new();
    let mut account_address_bounds: std::collections::HashMap<String, (u64, u64)> =
        std::collections::HashMap::new();

    let block_count = cfg.block_count.max(1);
    let txs_per_block = cfg.txs_per_block.max(1);
    let submit_tx_hash = tx_sequence_to_hash(block_count as u64 * txs_per_block as u64 + 1);
    let submit_ref = TxoRef(submit_tx_hash, 0);
    let submit_utxo = utxo_with_value(submit_address.clone(), Value::Coin(submit_amount));
    let crate::EraCbor(_, submit_cbor) = submit_utxo;
    chain_config.custom_utxos.push(CustomUtxo {
        ref_: submit_ref.clone(),
        era: Some(pallas::ledger::traverse::Era::Conway.into()),
        cbor: submit_cbor,
    });
    for offset in 0..block_count {
        let slot = cfg.slot + offset as u64;
        let block_number = cfg.start_block + offset as u64;
        let mut tx_bodies = Vec::with_capacity(txs_per_block);
        let mut tx_hashes = Vec::with_capacity(txs_per_block);

        let metadata: alonzo::Metadata = vec![(
            metadata_label,
            alonzo::Metadatum::Text(cfg.metadata_value.clone()),
        )]
        .into_iter()
        .collect();

        let aux_data = alonzo::AuxiliaryData::ShelleyMa(alonzo::ShelleyMaAuxiliaryData {
            transaction_metadata: metadata,
            auxiliary_scripts: None,
        });
        let aux_hash = aux_data.compute_hash();

        for tx_offset in 0..txs_per_block {
            let seed_tx_hash = tx_sequence_to_hash(1 + (offset * txs_per_block + tx_offset) as u64);
            let seed_ref = TxoRef(seed_tx_hash, 0);
            let seed_utxo = utxo_with_value(cfg.seed_address.clone(), Value::Coin(cfg.seed_amount));
            let crate::EraCbor(_, seed_cbor) = seed_utxo;

            chain_config.custom_utxos.push(CustomUtxo {
                ref_: seed_ref,
                era: Some(pallas::ledger::traverse::Era::Conway.into()),
                cbor: seed_cbor,
            });

            let output_address = if tx_offset == 0 {
                address_bytes.clone()
            } else {
                let byte = 0x20u8
                    .wrapping_add(offset as u8)
                    .wrapping_add(tx_offset as u8);
                address_with_stake_cred(&stake_cred, network, Hash::from([byte; 28]))
            };

            if let Ok(addr) = Address::from_bytes(&output_address) {
                if let Ok(bech32) = addr.to_bech32() {
                    if account_addresses_seen.insert(bech32.clone()) {
                        account_addresses.push(bech32.clone());
                        account_address_blocks.push((bech32.clone(), block_number));
                    }
                    account_address_bounds
                        .entry(bech32)
                        .and_modify(|bounds| {
                            bounds.0 = bounds.0.min(block_number);
                            bounds.1 = bounds.1.max(block_number);
                        })
                        .or_insert((block_number, block_number));
                }
            }

            tx_bodies.push(sample_transaction_body(
                Bytes::from(output_address),
                cfg.lovelace,
                seed_tx_hash,
                policy_id,
                asset_name.clone(),
                cfg.asset_amount,
                cfg.mint_amount,
                stake_cred.clone(),
                pool_keyhash,
                cfg.drep_keyhash,
                cfg.drep_deposit,
                if tx_offset == 0 { Some(aux_hash) } else { None },
            ));
        }

        let (block, hashes) = sample_block(block_number, slot, tx_bodies, Some(aux_data));

        for hash in &hashes {
            tx_hashes.push(hex::encode(hash.as_ref()));
        }

        let block_hash = block.header.compute_hash();
        let wrapper = (7, block);
        let raw_block = Arc::new(minicbor::to_vec(wrapper).unwrap());

        if first_block_hash.is_none() {
            first_block_hash = Some(block_hash);
            first_tx_hash = hashes.first().cloned();
        }

        block_vectors.push(BlockVectors {
            block_number,
            slot,
            block_hash: hex::encode(block_hash.as_ref()),
            tx_hashes,
        });

        raw_blocks.push(raw_block);
    }

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

    let drep_id = drep_id_from_keyhash(Hash::from(cfg.drep_keyhash));
    let tx_cbor = build_submit_tx_cbor(
        submit_ref,
        submit_amount,
        submit_output,
        Address::from_bech32(&submit_address)
            .expect("invalid submit address")
            .to_vec(),
        &submit_sk,
    );

    let mut account_address_bounds = account_address_bounds
        .into_iter()
        .map(|(address, (min, max))| (address, min, max))
        .collect::<Vec<_>>();
    account_address_bounds.sort_by(|a, b| a.0.cmp(&b.0));

    let vectors = SyntheticVectors {
        address: cfg.address,
        stake_address,
        asset_unit,
        metadata_label: metadata_label.to_string(),
        block_hash: hex::encode(first_block_hash.expect("missing block hash").as_ref()),
        tx_hash: hex::encode(first_tx_hash.expect("missing tx hash").as_ref()),
        blocks: block_vectors,
        account_addresses,
        account_address_blocks,
        account_address_bounds,
        pool_id,
        drep_id,
        tx_cbor,
    };

    (raw_blocks, vectors, chain_config)
}

pub fn seed_reward_logs<D: Domain>(
    domain: &D,
    stake_address: &str,
    pool_id: &str,
    epochs: &[u64],
) -> Result<(), ChainError> {
    let address = Address::from_bech32(stake_address)?;
    let (stake_cred, _) = dolos_cardano::pallas_extras::address_as_stake_cred(&address)
        .ok_or(ChainError::InvalidPoolParams)?;
    let entity_key = credential_to_key(&stake_cred);
    let pool_keyhash =
        pool_keyhash_from_bech32(pool_id).map_err(|_| ChainError::InvalidPoolParams)?;

    let summary = dolos_cardano::eras::load_era_summary::<D>(domain.state())?;
    let writer = domain.archive().start_writer()?;

    for epoch in epochs {
        let slot = summary.epoch_start(*epoch);
        let log_key: LogKey = (TemporalKey::from(slot), entity_key.clone()).into();
        let log = MemberRewardLog {
            amount: 42,
            pool_id: pool_keyhash.as_ref().to_vec(),
        };
        writer
            .write_log_typed(&log_key, &log)
            .map_err(ChainError::from)?;
    }

    writer.commit().map_err(ChainError::from)?;
    Ok(())
}

pub fn seed_epoch_logs<D: Domain>(domain: &D, epochs: &[u64]) -> Result<(), ChainError> {
    let summary = dolos_cardano::eras::load_era_summary::<D>(domain.state())?;
    let base = dolos_cardano::load_epoch::<D>(domain.state())?;

    let writer = domain.archive().start_writer()?;

    for epoch in epochs {
        let slot = summary.epoch_start(*epoch);
        let log_key = LogKey::from(TemporalKey::from(slot));
        let mut state = base.clone();
        state.number = *epoch;
        state.largest_stable_slot = slot;
        writer.write_log_typed(&log_key, &state)?;
    }

    writer.commit()?;
    Ok(())
}

fn address_with_stake_cred(
    stake_cred: &StakeCredential,
    network: Network,
    payment_hash: Hash<28>,
) -> Vec<u8> {
    let payment = ShelleyPaymentPart::Key(payment_hash);
    let delegation = match stake_cred {
        StakeCredential::AddrKeyhash(hash) => ShelleyDelegationPart::Key(*hash),
        StakeCredential::ScriptHash(hash) => ShelleyDelegationPart::Script(*hash),
    };

    ShelleyAddress::new(network, payment, delegation).to_vec()
}

#[allow(clippy::too_many_arguments)]
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
    drep_keyhash: [u8; 28],
    drep_deposit: u64,
    auxiliary_data_hash: Option<Hash<32>>,
) -> TransactionBody<'static> {
    let input = TransactionInput {
        transaction_id: tx_hash,
        index: 0,
    };

    let mint_amount = NonZeroInt::try_from(mint_amount).expect("mint amount must be non-zero");
    let mut mint_assets = BTreeMap::new();
    mint_assets.insert(asset_name.clone(), mint_amount);
    let mut mint = BTreeMap::new();
    mint.insert(policy_id, mint_assets);

    let asset_amount = PositiveCoin::try_from(asset_amount).expect("asset amount must be non-zero");
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
    let reward_payload = match stake_cred {
        StakeCredential::AddrKeyhash(hash) => StakePayload::Stake(hash),
        StakeCredential::ScriptHash(hash) => StakePayload::Script(hash),
    };
    let reward_account = StakeAddress::new(Network::Testnet, reward_payload);
    let pool_cert = Certificate::PoolRegistration {
        operator: pool_keyhash,
        vrf_keyhash,
        pledge: 0,
        cost: 0,
        margin: pallas::ledger::primitives::RationalNumber {
            numerator: 0,
            denominator: 1,
        },
        reward_account: Bytes::from(reward_account.to_vec()),
        pool_owners: Set::from(vec![pool_owner]),
        relays: vec![],
        pool_metadata,
    };

    let delegation = Certificate::StakeDelegation(stake_cred.clone(), pool_keyhash);
    let registration = Certificate::StakeRegistration(stake_cred);

    let drep_cred = StakeCredential::AddrKeyhash(AddrKeyhash::from(drep_keyhash));
    let drep_cert = Certificate::RegDRepCert(drep_cred, drep_deposit, None);

    let certificates = NonEmptySet::try_from(vec![registration, delegation, pool_cert, drep_cert])
        .expect("non-empty certificates");

    TransactionBody {
        inputs: Set::from(vec![input]),
        outputs: vec![TransactionOutput::PostAlonzo(KeepRaw::from(output))],
        fee: 7,
        ttl: Some(10),
        certificates: Some(certificates),
        withdrawals: None,
        auxiliary_data_hash,
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

#[allow(clippy::too_many_arguments)]
fn sample_block(
    block_number: u64,
    slot: u64,
    tx_bodies: Vec<TransactionBody<'static>>,
    aux_data: Option<alonzo::AuxiliaryData>,
) -> (
    pallas::ledger::primitives::conway::Block<'static>,
    Vec<Hash<32>>,
) {
    let header_body = pallas::ledger::primitives::conway::HeaderBody {
        block_number,
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

    let body_hashes: Vec<_> = tx_bodies.iter().map(|body| body.compute_hash()).collect();
    let block = pallas::ledger::primitives::conway::Block {
        header: KeepRaw::from(header),
        transaction_bodies: tx_bodies.into_iter().map(KeepRaw::from).collect(),
        transaction_witness_sets: std::iter::repeat_with(|| {
            KeepRaw::from(pallas::ledger::primitives::conway::WitnessSet {
                vkeywitness: None,
                native_script: None,
                bootstrap_witness: None,
                plutus_v1_script: None,
                plutus_data: None,
                redeemer: None,
                plutus_v2_script: None,
                plutus_v3_script: None,
            })
        })
        .take(body_hashes.len())
        .collect(),
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

    (block, body_hashes)
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

fn drep_id_from_keyhash(hash: Hash<28>) -> String {
    let mut payload = Vec::with_capacity(29);
    payload.push(0b00100010);
    payload.extend_from_slice(hash.as_ref());
    bech32::encode("drep", payload.to_base32(), Variant::Bech32).expect("failed to encode drep id")
}

fn keyhash_from_pubkey(pubkey: &[u8]) -> Hash<28> {
    let mut hasher = Hasher::<224>::new();
    hasher.input(pubkey);
    hasher.finalize()
}

fn build_submit_tx_cbor(
    input: TxoRef,
    input_amount: u64,
    output_amount: u64,
    output_address: Vec<u8>,
    signing_key: &SecretKeyExtended,
) -> Vec<u8> {
    let input = TransactionInput {
        transaction_id: input.0,
        index: input.1.into(),
    };

    let output = PostAlonzoTransactionOutput {
        address: Bytes::from(output_address),
        value: Value::Coin(output_amount),
        datum_option: None,
        script_ref: None,
    };

    let fee = input_amount
        .checked_sub(output_amount)
        .expect("submit output exceeds input");
    let fee = fee.max(200_000);

    let body = TransactionBody {
        inputs: Set::from(vec![input]),
        outputs: vec![TransactionOutput::PostAlonzo(KeepRaw::from(output))],
        fee,
        ttl: None,
        certificates: None,
        withdrawals: None,
        auxiliary_data_hash: None,
        validity_interval_start: None,
        mint: None,
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
    };

    let body_cbor = minicbor::to_vec(&body).expect("failed to encode submit body");
    let body_hash = Hasher::<256>::hash(&body_cbor);
    let body_keep = minicbor::decode::<KeepRaw<'_, TransactionBody<'_>>>(&body_cbor)
        .expect("failed to decode submit body")
        .to_owned();
    let signature = signing_key.sign(body_hash.as_ref());
    let vkey_witness = pallas::ledger::primitives::alonzo::VKeyWitness {
        vkey: Bytes::from(signing_key.public_key().as_ref().to_vec()),
        signature: Bytes::from(signature.as_ref().to_vec()),
    };
    let witness_set = pallas::ledger::primitives::conway::WitnessSet {
        vkeywitness: Some(
            NonEmptySet::try_from(vec![vkey_witness]).expect("non-empty vkeywitness"),
        ),
        native_script: None,
        bootstrap_witness: None,
        plutus_v1_script: None,
        plutus_data: None,
        redeemer: None,
        plutus_v2_script: None,
        plutus_v3_script: None,
    };

    let tx = pallas::ledger::primitives::conway::Tx {
        transaction_body: body_keep,
        transaction_witness_set: KeepRaw::from(witness_set),
        success: true,
        auxiliary_data: Nullable::Null,
    };

    minicbor::to_vec(tx).expect("failed to encode submit tx")
}

#[cfg(test)]
mod tests {

    use pallas::ledger::traverse::MultiEraBlock;

    use super::*;

    #[test]
    fn synthetic_block_decodes() {
        let (blocks, vectors, _cfg) = build_synthetic_blocks(SyntheticBlockConfig::default());
        let block = blocks.first().expect("missing synthetic block");
        let raw = block.as_ref();
        let block = MultiEraBlock::decode(raw).expect("failed to decode synthetic block");
        let label: u64 = vectors
            .metadata_label
            .parse()
            .expect("invalid metadata label");
        let txs = block.txs();
        let found = txs.iter().any(|tx| tx.metadata().find(label).is_some());
        assert!(found, "synthetic metadata label was not found in block");
    }
}
