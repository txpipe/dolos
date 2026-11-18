//! `/pools/{pool_id}`: one pool with its live metrics and parameters.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{pool::Pool, pool_calidus_key::PoolCalidusKey};
use dolos_cardano::{
    cip151,
    indexes::{AsyncCardanoQueryExt, SlotOrder},
    model::{AccountState, CalidusRegistration, PoolState},
    pallas_extras, pool_history, PoolHash,
};
use dolos_core::{ArchiveStore as _, Domain};
use futures::StreamExt;
use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    error::Error,
    log_and_500,
    mapping::{
        bech32_calidus, bech32_pool, rational_to_f64, stake_cred_to_address, vkey_to_stake_address,
    },
    scan::ENTITY_CHUNK,
    Facade,
};

use super::{compute_pool_metrics, load_active_pools, parse_pool_id_unbounded, read_pool_row};

pub async fn by_id<D>(
    Path(id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Pool>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
    Option<AccountState>: From<D::Entity>,
{
    let Some(hash) = parse_pool_id_unbounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    let pool = read_pool_row(&domain, hash)
        .await?
        .ok_or(StatusCode::NOT_FOUND)?;

    let snapshot = pool
        .snapshot
        .live()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    let params = &snapshot.params;

    let network = domain.get_network_id()?;
    let epoch = dolos_cardano::load_epoch::<D>(domain.state())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let circulating_supply = epoch.initial_pots.circulating();
    let optimal = domain
        .get_current_effective_pparams()?
        .ensure_k()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // The active stake is the distribution ESTART froze two epochs back,
    // which `set` holds, against the epoch's total of it.
    let active_stake = pool
        .snapshot
        .set()
        .and_then(|x| x.stake)
        .unwrap_or_default();
    let total_active_stake = epoch.active_stake.unwrap_or_default();

    let active_pools = load_active_pools(&domain, ENTITY_CHUNK).await?;
    let owners = params.pool_owners.clone();
    let metrics =
        compute_pool_metrics(&domain, pool.operator, active_pools, owners, ENTITY_CHUNK).await?;

    let (registration, retirement) = load_pool_cert_hashes(&domain, pool.operator).await?;

    let chain = domain.get_chain_summary()?;
    let blocks_minted = pool_history::blocks_minted(&domain.inner, &chain, vec![pool.clone()])
        .await
        .map_err(log_and_500("failed to derive a pool's lifetime blocks"))?
        .pop()
        .unwrap_or_default();
    let calidus_key = match &pool.calidus {
        Some(registration) => pool_calidus_key(&domain, registration)?,
        None => None,
    };

    let reward_account = pallas_extras::parse_reward_account(&params.reward_account)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)
        .and_then(|cred| {
            stake_cred_to_address(&cred, network)
                .to_bech32()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        })?;

    let owners = params
        .pool_owners
        .iter()
        .map(|owner| {
            vkey_to_stake_address(*owner, network)
                .to_bech32()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let response = Pool {
        pool_id: bech32_pool(pool.operator)?,
        hex: hex::encode(pool.operator),
        vrf_key: params.vrf_keyhash.to_string(),
        blocks_minted: blocks_minted as i32,
        blocks_epoch: snapshot.blocks_minted as i32,
        live_stake: metrics.live_stake.to_string(),
        live_size: safe_ratio(metrics.live_stake, metrics.total_live_stake),
        live_saturation: if circulating_supply == 0 {
            0.0
        } else {
            metrics.live_stake as f64 * optimal as f64 / circulating_supply as f64
        },
        live_delegators: metrics.live_delegators as f64,
        active_stake: active_stake.to_string(),
        active_size: safe_ratio(active_stake, total_active_stake),
        declared_pledge: params.pledge.to_string(),
        live_pledge: metrics.live_pledge.to_string(),
        margin_cost: rational_to_f64::<6>(&params.margin),
        fixed_cost: params.cost.to_string(),
        reward_account,
        owners,
        registration,
        retirement,
        calidus_key: calidus_key.map(Box::new),
    };

    Ok(Json(response))
}

fn safe_ratio(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 / total as f64
    }
}

fn scan_pool_cert_hashes_in_block(
    block: &MultiEraBlock,
    target_pool: &PoolHash,
) -> (Vec<String>, Vec<String>) {
    let mut registrations = Vec::new();
    let mut retirements = Vec::new();

    for tx in block.txs() {
        let mut has_registration = false;
        let mut has_retirement = false;

        for cert in tx.certs() {
            if pallas_extras::cert_as_pool_registration(&cert)
                .is_some_and(|cert| &cert.operator == target_pool)
            {
                has_registration = true;
            }

            if pallas_extras::cert_as_pool_retirement(&cert)
                .is_some_and(|cert| &cert.operator == target_pool)
            {
                has_retirement = true;
            }
        }

        if has_registration || has_retirement {
            let tx_hash = tx.hash().to_string();

            if has_registration {
                registrations.push(tx_hash.clone());
            }

            if has_retirement {
                retirements.push(tx_hash);
            }
        }
    }

    (registrations, retirements)
}

async fn load_pool_cert_hashes<D>(
    domain: &Facade<D>,
    pool: PoolHash,
) -> Result<(Vec<String>, Vec<String>), StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let tip = domain.get_tip_slot()?;
    let stream =
        domain
            .query()
            .blocks_by_pool_certs_stream(pool.as_slice(), 0, tip, SlotOrder::Asc);

    let mut stream = Box::pin(stream);
    let mut registrations = Vec::new();
    let mut retirements = Vec::new();

    while let Some(item) = stream.next().await {
        let (_, block) = item.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let Some(block) = block else {
            continue;
        };

        let block = MultiEraBlock::decode(block.as_slice())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let (mut block_registrations, mut block_retirements) =
            scan_pool_cert_hashes_in_block(&block, &pool);

        registrations.append(&mut block_registrations);
        retirements.append(&mut block_retirements);
    }

    Ok((registrations, retirements))
}

/// The `calidus_key` answer for the registration the pool's state carries.
/// Height, time and epoch come from the registering block, found by slot. A
/// revocation, a nonce the answer's `i32` cannot hold, or a block the archive
/// no longer holds, answers `None`.
fn pool_calidus_key<D>(
    domain: &Facade<D>,
    registration: &CalidusRegistration,
) -> Result<Option<PoolCalidusKey>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let Some(pub_key) = registration.pub_key else {
        return Ok(None);
    };

    let Ok(nonce) = registration.nonce.try_into() else {
        return Ok(None);
    };

    let Some(body) = domain
        .archive()
        .get_block_by_slot(&registration.slot)
        .map_err(log_and_500("failed to read a calidus registration block"))?
    else {
        return Ok(None);
    };

    let block = MultiEraBlock::decode(&body)
        .map_err(log_and_500("failed to decode a calidus registration block"))?;
    let chain = domain.get_chain_summary()?;

    Ok(Some(PoolCalidusKey {
        id: bech32_calidus(cip151::calidus_key_id_bytes(&pub_key))?,
        pub_key: hex::encode(pub_key),
        nonce,
        tx_hash: registration.tx_hash.to_string(),
        block_height: block
            .number()
            .try_into()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        block_time: chain
            .slot_time(registration.slot)
            .try_into()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        epoch: chain
            .slot_epoch(registration.slot)
            .0
            .try_into()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::pools::{
            reduce_account_chunk,
            testing::{assert_error_message, assert_status, invalid_pool_id, missing_pool_id},
            ActivePools,
        },
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::{
        block_content::BlockContent, pool::Pool, pool_list_extended_inner::PoolListExtendedInner,
    };
    use dolos_cardano::{
        model::{DRepDelegation, EpochValue, PoolDelegation, Stake},
        PoolKey,
    };
    use dolos_core::{StateStore as _, StateWriter as _};
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use pallas::{
        crypto::hash::Hash,
        ledger::primitives::{alonzo, StakeCredential},
    };

    /// The cold key of the pool the calidus tests register.
    fn calidus_cold_key() -> pallas::crypto::key::ed25519::SecretKey {
        dolos_testing::cip151::cold_key(9)
    }

    fn calidus_pool_id() -> String {
        dolos_testing::cip151::pool_bech32(&calidus_cold_key())
    }

    /// A CIP-151 registration of `calidus_key` for the calidus tests' pool,
    /// signed by its cold key.
    fn cip151_registration(nonce: u64, calidus_key: [u8; 32]) -> Vec<(u64, alonzo::Metadatum)> {
        dolos_testing::cip151::signed_registration(&calidus_cold_key(), nonce, calidus_key)
    }

    #[tokio::test]
    async fn pool_by_id_happy_path() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let pool: Pool = serde_json::from_slice(&bytes).expect("failed to parse pool response");
        assert_eq!(pool.pool_id, pool_id);
        assert!(!pool.registration.is_empty());
        assert!(pool.retirement.is_empty());
        assert!(pool.calidus_key.is_none());
    }

    /// Every synthetic block carries the same registration in its first
    /// transaction; the first one holds, since a later one with the same
    /// nonce does not exceed it. The chain starts at slot 500,000, so the
    /// registering block's slot and height differ.
    #[tokio::test]
    async fn pool_by_id_returns_calidus_key() {
        let default_cfg = SyntheticBlockConfig {
            pool_id: calidus_pool_id(),
            slot: 500_000,
            ..Default::default()
        };
        let pool_id = default_cfg.pool_id.clone();
        let calidus_pub_key = [0x57; 32];
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            metadata_entries: cip151_registration(12345, calidus_pub_key),
            ..default_cfg
        });

        let path = format!("/pools/{pool_id}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let pool: Pool = serde_json::from_slice(&bytes).expect("failed to parse pool response");
        let calidus = pool.calidus_key.expect("missing calidus key");
        assert_eq!(calidus.pub_key, hex::encode(calidus_pub_key));
        assert_eq!(calidus.nonce, 12345);
        assert!(calidus.id.starts_with("calidus1"));

        let registering = &app.vectors().blocks[0];
        assert_ne!(registering.slot, registering.block_number);
        assert_eq!(calidus.tx_hash, registering.tx_hashes[0]);

        let (status, bytes) = app
            .get_bytes(&format!("/blocks/{}", registering.block_hash))
            .await;
        assert_eq!(status, StatusCode::OK);
        let block: BlockContent = serde_json::from_slice(&bytes).expect("block response");
        assert_eq!(Some(calidus.block_height), block.height);
        assert_eq!(calidus.block_time, block.time);
        assert_eq!(Some(calidus.epoch), block.epoch);
    }

    /// A registration whose block the archive no longer holds is served as
    /// no key, not as an error.
    #[tokio::test]
    async fn pool_by_id_serves_no_calidus_key_without_its_block() {
        let app = TestApp::new_with_cfg_and_setup(
            SyntheticBlockConfig {
                pool_id: calidus_pool_id(),
                ..Default::default()
            },
            |domain, vectors| {
                let operator = dolos_testing::cip151::pool_id(&calidus_cold_key());
                let mut pool = domain
                    .state()
                    .read_entity_typed::<PoolState>(&PoolKey(operator))
                    .expect("pool read")
                    .expect("pool state");
                let tip = vectors.blocks.last().expect("synthetic blocks");
                pool.calidus = Some(CalidusRegistration {
                    pub_key: Some([0x57; 32]),
                    nonce: 1,
                    tx_hash: [0xaa; 32].into(),
                    slot: tip.slot + 1_000,
                });

                let writer = domain.state().start_writer().expect("state writer");
                writer
                    .write_entity_typed(&PoolKey(operator), &pool)
                    .expect("pool write");
                writer.commit().expect("state commit");
            },
        );

        let pool_id = app.vectors().pool_id.clone();
        let (status, bytes) = app.get_bytes(&format!("/pools/{pool_id}")).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let pool: Pool = serde_json::from_slice(&bytes).expect("failed to parse pool response");
        assert!(pool.calidus_key.is_none());
    }

    #[tokio::test]
    async fn pool_by_id_treats_zero_calidus_key_as_revocation() {
        let pool_id = calidus_pool_id();
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            pool_id: pool_id.clone(),
            metadata_entries: cip151_registration(12345, [0u8; 32]),
            ..Default::default()
        });

        let path = format!("/pools/{pool_id}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let pool: Pool = serde_json::from_slice(&bytes).expect("failed to parse pool response");
        assert!(pool.calidus_key.is_none());
    }

    /// The answer's nonce is an `i32`: a registration with a larger one is
    /// served as no key, not as an error.
    #[tokio::test]
    async fn pool_by_id_serves_no_calidus_key_for_a_nonce_above_i32() {
        let pool_id = calidus_pool_id();
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            pool_id: pool_id.clone(),
            metadata_entries: cip151_registration(i32::MAX as u64 + 1, [0x57; 32]),
            ..Default::default()
        });

        let path = format!("/pools/{pool_id}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let pool: Pool = serde_json::from_slice(&bytes).expect("failed to parse pool response");
        assert!(pool.calidus_key.is_none());
    }

    /// A registration without the pool's cold-key signature is not served.
    #[tokio::test]
    async fn pool_by_id_ignores_an_unsigned_calidus_key() {
        let pool_id = calidus_pool_id();
        let pool = dolos_testing::cip151::pool_id(&calidus_cold_key());
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            pool_id: pool_id.clone(),
            metadata_entries: dolos_testing::cip151::unsigned_registration(pool, 12345, [0x57; 32]),
            ..Default::default()
        });

        let path = format!("/pools/{pool_id}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let pool: Pool = serde_json::from_slice(&bytes).expect("failed to parse pool response");
        assert!(pool.calidus_key.is_none());
    }

    /// A registration in a phase-2-invalid transaction registers nothing, so
    /// the key a valid one set before it is still the one served, though the
    /// invalid one carries a higher nonce.
    #[tokio::test]
    async fn pool_by_id_skips_an_invalid_transactions_calidus_key() {
        use dolos_core::import::ImportExt as _;

        let pool_id = calidus_pool_id();
        let registration = cip151_registration;
        let app = TestApp::new_with_cfg_and_setup(
            SyntheticBlockConfig {
                pool_id: pool_id.clone(),
                metadata_entries: registration(12345, [0x57; 32]),
                ..Default::default()
            },
            |domain, vectors| {
                let tip = vectors.blocks.last().expect("synthetic blocks");
                let (_, raw) = dolos_testing::blocks::make_conway_block_with_metadata_at(
                    tip.slot + 1,
                    tip.block_number + 1,
                    Some(tip.block_hash.parse().expect("block hash")),
                    registration(12346, [0x58; 32]),
                    false,
                );
                domain
                    .import_blocks(vec![raw])
                    .expect("import the invalid registration");
            },
        );

        let path = format!("/pools/{pool_id}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let pool: Pool = serde_json::from_slice(&bytes).expect("failed to parse pool response");
        let calidus = pool.calidus_key.expect("missing calidus key");
        assert_eq!(calidus.pub_key, hex::encode([0x57; 32]));
    }

    #[tokio::test]
    async fn pool_by_id_bad_request() {
        let app = TestApp::new();
        let path = format!("/pools/{}", invalid_pool_id());
        assert_error_message(&app, &path, "Invalid or malformed pool id format.").await;
    }

    #[tokio::test]
    async fn pool_by_id_not_found() {
        let app = TestApp::new();
        let path = format!("/pools/{}", missing_pool_id());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn pool_by_id_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[test]
    fn pool_metrics_exclude_unregistered_retired_pool_stake() {
        let target_pool = Hash::from([1u8; 28]);
        let account = AccountState {
            registered_at: None,
            stake: EpochValue::with_live(
                3,
                Stake {
                    utxo_sum: 42,
                    ..Default::default()
                },
            ),
            pool: EpochValue::with_live(3, PoolDelegation::NotDelegated),
            drep: EpochValue::with_live(3, DRepDelegation::NotDelegated),
            vote_delegated_at: None,
            deregistered_at: None,
            credential: StakeCredential::AddrKeyhash(Hash::from([2u8; 28])),
            retired_pool: Some(target_pool),
        };

        let metrics = reduce_account_chunk(&[account], target_pool, &ActivePools::default());

        assert_eq!(metrics.live_stake, 0);
        assert_eq!(metrics.live_pledge, 0);
        assert_eq!(metrics.live_delegators, 0);
        assert_eq!(metrics.total_live_stake, 0);
    }

    #[test]
    fn pool_metrics_only_count_registered_accounts_with_pool_relation() {
        let target_pool = Hash::from([1u8; 28]);
        let other_active_pool = Hash::from([2u8; 28]);
        let retired_pool = Hash::from([3u8; 28]);

        let accounts = [
            AccountState {
                registered_at: Some(1),
                stake: EpochValue::with_live(
                    3,
                    Stake {
                        utxo_sum: 10,
                        ..Default::default()
                    },
                ),
                pool: EpochValue::with_live(3, PoolDelegation::Pool(target_pool)),
                drep: EpochValue::with_live(3, DRepDelegation::NotDelegated),
                vote_delegated_at: None,
                deregistered_at: None,
                credential: StakeCredential::AddrKeyhash(Hash::from([2u8; 28])),
                retired_pool: None,
            },
            AccountState {
                registered_at: Some(1),
                stake: EpochValue::with_live(
                    3,
                    Stake {
                        utxo_sum: 20,
                        ..Default::default()
                    },
                ),
                pool: EpochValue::with_live(3, PoolDelegation::Pool(other_active_pool)),
                drep: EpochValue::with_live(3, DRepDelegation::NotDelegated),
                vote_delegated_at: None,
                deregistered_at: None,
                credential: StakeCredential::AddrKeyhash(Hash::from([3u8; 28])),
                retired_pool: None,
            },
            AccountState {
                registered_at: Some(1),
                stake: EpochValue::with_live(
                    3,
                    Stake {
                        utxo_sum: 30,
                        ..Default::default()
                    },
                ),
                pool: EpochValue::with_live(3, PoolDelegation::Pool(retired_pool)),
                drep: EpochValue::with_live(3, DRepDelegation::NotDelegated),
                vote_delegated_at: None,
                deregistered_at: None,
                credential: StakeCredential::AddrKeyhash(Hash::from([4u8; 28])),
                retired_pool: Some(retired_pool),
            },
            AccountState {
                registered_at: Some(1),
                stake: EpochValue::with_live(
                    3,
                    Stake {
                        utxo_sum: 40,
                        ..Default::default()
                    },
                ),
                pool: EpochValue::with_live(3, PoolDelegation::NotDelegated),
                drep: EpochValue::with_live(3, DRepDelegation::NotDelegated),
                vote_delegated_at: None,
                deregistered_at: None,
                credential: StakeCredential::AddrKeyhash(Hash::from([5u8; 28])),
                retired_pool: None,
            },
            AccountState {
                registered_at: Some(1),
                stake: EpochValue::with_live(
                    3,
                    Stake {
                        utxo_sum: 50,
                        ..Default::default()
                    },
                ),
                pool: EpochValue::with_live(3, PoolDelegation::Pool(target_pool)),
                drep: EpochValue::with_live(3, DRepDelegation::NotDelegated),
                vote_delegated_at: None,
                deregistered_at: Some(2),
                credential: StakeCredential::AddrKeyhash(Hash::from([6u8; 28])),
                retired_pool: None,
            },
        ];

        let active_pools = ActivePools::from([target_pool, other_active_pool]);
        let metrics = reduce_account_chunk(&accounts, target_pool, &active_pools);

        assert_eq!(metrics.live_stake, 10);
        assert_eq!(metrics.live_pledge, 0);
        assert_eq!(metrics.live_delegators, 1);
        assert_eq!(metrics.total_live_stake, 30);
    }

    #[test]
    fn pool_metrics_include_retired_pool_stake_for_registered_accounts() {
        let target_pool = Hash::from([1u8; 28]);
        let account = AccountState {
            registered_at: Some(1),
            stake: EpochValue::with_live(
                3,
                Stake {
                    utxo_sum: 42,
                    ..Default::default()
                },
            ),
            pool: EpochValue::with_live(3, PoolDelegation::NotDelegated),
            drep: EpochValue::with_live(3, DRepDelegation::NotDelegated),
            vote_delegated_at: None,
            deregistered_at: None,
            credential: StakeCredential::AddrKeyhash(Hash::from([2u8; 28])),
            retired_pool: Some(target_pool),
        };

        let metrics = reduce_account_chunk(&[account], target_pool, &ActivePools::default());

        assert_eq!(metrics.live_stake, 42);
        assert_eq!(metrics.total_live_stake, 0);
        assert_eq!(metrics.live_delegators, 1);
    }

    #[tokio::test]
    async fn pool_by_id_matches_extended_live_metrics() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let pool_path = format!("/pools/{pool_id}");
        let (pool_status, pool_bytes) = app.get_bytes(&pool_path).await;
        let (extended_status, extended_bytes) =
            app.get_bytes("/pools/extended?page=1&count=100").await;

        assert_eq!(pool_status, StatusCode::OK);
        assert_eq!(extended_status, StatusCode::OK);

        let pool: Pool =
            serde_json::from_slice(&pool_bytes).expect("failed to parse pool response");
        let extended: Vec<PoolListExtendedInner> =
            serde_json::from_slice(&extended_bytes).expect("failed to parse pool list extended");

        let extended_pool = extended
            .into_iter()
            .find(|candidate| candidate.pool_id == pool_id)
            .expect("pool missing from extended response");

        assert_eq!(extended_pool.live_stake, pool.live_stake);
        assert_eq!(extended_pool.live_saturation, pool.live_saturation);
    }
}
