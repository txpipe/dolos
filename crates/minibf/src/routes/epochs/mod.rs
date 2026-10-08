use axum::{http::StatusCode, Json};
use blockfrost_openapi::models::epoch_content::EpochContent;
use dolos_cardano::{model::EpochState, rupd::StakeSnapshot, ChainSummary, EraProtocol};
use dolos_core::Domain;
use pallas::{
    codec::minicbor,
    ledger::{primitives::Epoch, traverse::MultiEraHeader},
};

use crate::{
    error::Error,
    log_and_500,
    mapping::{epochs::EpochContentModelBuilder, IntoModel as _},
    Facade,
};

mod blocks;
mod blocks_pool;
mod by_number;
mod latest;
mod latest_parameters;
mod next;
mod parameters;
mod previous;
mod stakes;
mod stakes_pool;

pub use blocks::by_number_blocks;
pub use blocks_pool::by_number_blocks_pool;
pub use by_number::by_number;
pub use latest::latest;
pub use latest_parameters::latest_parameters;
pub use next::by_number_next;
pub use parameters::by_number_parameters;
pub use previous::by_number_previous;
pub use stakes::by_number_stakes;
pub use stakes_pool::by_number_stakes_pool;

const MAX_EPOCH_NUMBER: Epoch = i32::MAX as Epoch;

fn ensure_epoch_in_range(epoch: Epoch) -> Result<(), Error> {
    if epoch > MAX_EPOCH_NUMBER {
        return Err(Error::InvalidEpochNumber);
    }

    Ok(())
}

fn build_epoch_content<D: Domain>(
    domain: &Facade<D>,
    chain: &ChainSummary,
    epoch: Epoch,
    mut state: EpochState,
    active_stake: Option<u64>,
) -> Result<EpochContentModelBuilder, StatusCode> {
    // Use the epoch from the caller, not `state.number`. The live `EpochState`
    // of the current epoch can hold a number that differs from the number that
    // the tip resolves.
    state.number = epoch;

    let start_time = chain.slot_time(chain.epoch_start(epoch));
    let end_time = chain.slot_time(chain.epoch_start(epoch + 1));

    // The roll pipeline precomputes the block aggregates on `RollingStats`, so
    // this request needs no block scan. The first and last block times are
    // slots, and this function converts them here. A zero slot means the epoch
    // had no block.
    //
    // A Byron epoch boundary block (EBB) does not pass through the roll
    // pipeline. So `first_block_slot` is the first *regular* block of the epoch.
    // Every Byron epoch opens with an EBB. For these epochs, Blockfrost reports
    // the time of the EBB, so `first_block_time` differs. See the systemic EBB
    // omission tracked for `/epochs/{n}/blocks` and `/blocks/{block}`.
    let rolling = state.rolling.live().cloned().unwrap_or_default();
    let first_block_time = if rolling.first_block_slot == 0 {
        0
    } else {
        chain.slot_time(rolling.first_block_slot)
    };
    let last_block_time = if rolling.last_block_slot == 0 {
        0
    } else {
        chain.slot_time(rolling.last_block_slot)
    };

    // The early history of preprod has a gap in the stake snapshot. The
    // reference reports `null` active stake for epochs 13-28. The value can
    // come from the current-epoch snapshot or the StakeLogs. This override
    // resets those epochs to `null` in both cases (see `null_active_stake`).
    let active_stake =
        if crate::hacks::null_active_stake::contains(domain.genesis().network_magic(), epoch) {
            None
        } else {
            match active_stake {
                Some(active_stake) => Some(active_stake),
                None => domain.sum_active_stake_for_epoch(epoch, chain)?,
            }
        };

    Ok(EpochContentModelBuilder {
        state,
        start_time,
        end_time,
        first_block_time,
        last_block_time,
        tx_count: rolling.tx_count,
        output: rolling.output,
        active_stake,
    })
}

async fn derive_current_active_stake<D: Domain>(
    domain: &Facade<D>,
    chain: &ChainSummary,
    current: Epoch,
) -> Result<u64, StatusCode> {
    // A stake distribution becomes active three epoch boundaries after it is
    // live (live -> mark -> set -> go). So the active stake for epoch E is the
    // stake that was live at E-2. RUPD applies this same offset one epoch back
    // (it scores E-1 from the snapshot at E-3); here we target the current
    // epoch, so we read the snapshot at `current - 2`.
    let stake_epoch = current.saturating_sub(2);
    let protocol = EraProtocol::from(chain.era_for_epoch(stake_epoch.saturating_add(1)).protocol);
    let domain = domain.clone();

    tokio::task::spawn_blocking(move || {
        StakeSnapshot::load_globals::<D>(domain.state(), current, stake_epoch, protocol)
            .map(|snapshot| snapshot.active_stake_sum)
    })
    .await
    .map_err(log_and_500("failed to join current active stake scan"))?
    .map_err(log_and_500("failed to derive current active stake"))
}

fn load_epoch_state<D: Domain>(
    domain: &Facade<D>,
    chain: &ChainSummary,
    current: Epoch,
    epoch: Epoch,
) -> Result<EpochState, StatusCode>
where
    Option<EpochState>: From<D::Entity>,
{
    if epoch == current {
        dolos_cardano::load_epoch::<D>(domain.state())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    } else {
        domain
            .get_epoch_log(epoch, chain)?
            .ok_or(StatusCode::NOT_FOUND)
    }
}

async fn collect_epoch_contents<D: Domain>(
    domain: &Facade<D>,
    chain: &ChainSummary,
    current: Epoch,
    epochs: Vec<Epoch>,
) -> Result<Json<Vec<EpochContent>>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    let current_active_stake = if epochs.contains(&current) {
        Some(derive_current_active_stake(domain, chain, current).await?)
    } else {
        None
    };

    let mut out = Vec::with_capacity(epochs.len());
    for epoch in epochs {
        let state = if epoch == current {
            dolos_cardano::load_epoch::<D>(domain.state())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        } else {
            match domain.get_epoch_log(epoch, chain)? {
                Some(state) => state,
                None => continue,
            }
        };

        let active_stake = if epoch == current {
            current_active_stake
        } else {
            None
        };
        let model = build_epoch_content(domain, chain, epoch, state, active_stake)?;
        out.push(model.into_model()?);
    }

    Ok(Json(out))
}

/// Parses the header of a stored block without decoding the transactions.
/// Returns `None` for Byron blocks (no issuer).
pub(crate) fn decode_block_header(body: &[u8]) -> Result<Option<MultiEraHeader<'_>>, StatusCode> {
    use std::borrow::Cow;

    use pallas::codec::utils::KeepRaw;
    use pallas::ledger::primitives::{alonzo, babbage};
    use pallas::ledger::traverse::{probe, Era};

    let era = match probe::block_era(body) {
        probe::Outcome::Matched(era) => era,
        probe::Outcome::EpochBoundary => return Ok(None),
        probe::Outcome::Inconclusive => {
            return Err(log_and_500("failed to probe block era")("inconclusive"))
        }
    };

    if era == Era::Byron {
        return Ok(None);
    }

    // A stored block is `[era_tag, [header, tx_bodies, ...]]`. Open the
    // wrapper array, skip the era tag, open the block array. The next item
    // is the header.
    let mut d = minicbor::Decoder::new(body);
    let header = (|| -> Result<_, minicbor::decode::Error> {
        d.array()?;
        d.u8()?;
        d.array()?;

        match era {
            Era::Shelley | Era::Allegra | Era::Mary | Era::Alonzo => {
                let header: KeepRaw<alonzo::Header> = d.decode()?;
                Ok(MultiEraHeader::ShelleyCompatible(Cow::Owned(header)))
            }
            _ => {
                let header: KeepRaw<babbage::Header> = d.decode()?;
                Ok(MultiEraHeader::BabbageCompatible(Cow::Owned(header)))
            }
        }
    })()
    .map_err(log_and_500("failed to decode block header"))?;

    Ok(Some(header))
}

#[cfg(test)]
mod testing {
    use axum::http::StatusCode;
    use pallas::crypto::hash::Hasher;

    use crate::{mapping::bech32_pool, test_support::TestApp};

    pub async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    /// Every synthetic block is minted by the same fixed issuer key, so the
    /// pool derived from it owns the whole epoch (see `issuer_vkey` in
    /// dolos-testing's synthetic builder).
    pub fn toy_issuer_pool() -> String {
        bech32_pool(Hasher::<224>::hash(&[0x10, 0x11])).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{pool_id_cases, PoolIdCase, TestApp, TestFault, REG_POOL_ID};
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use pallas::{crypto::hash::Hasher, ledger::traverse::MultiEraBlock};

    /// A caller can pass a computed active stake for an epoch. The builder
    /// resets that value to null inside the preprod gap and keeps it elsewhere.
    /// This test calls the real builder for epochs inside the gap, on the
    /// bounds, and on each side. It also uses a preview epoch and a mainnet
    /// epoch, so the reset depends on the network magic. The `next` and
    /// `previous` handlers build each array item through this same builder, so
    /// this test covers them too.
    #[test]
    fn build_epoch_content_nulls_active_stake_across_the_preprod_gap() {
        use std::sync::Arc;

        use dolos_core::config::{CardanoConfig, MinibfConfig};
        use dolos_testing::toy_domain::ToyDomain;

        use crate::mapping::IntoModel as _;

        // This helper builds a minibf facade over a fresh domain for one
        // network. The genesis work unit runs during construction, so the era
        // summary and the base epoch load without an imported block.
        fn facade_for(genesis: dolos_core::Genesis) -> Facade<ToyDomain> {
            let domain = ToyDomain::new_with_genesis_and_config(
                Arc::new(genesis),
                CardanoConfig::default(),
                None,
                None,
            );
            Facade {
                inner: domain,
                config: MinibfConfig::new("[::]:0".parse().expect("valid listen address")),
                cache: crate::cache::CacheService::default(),
            }
        }

        // This value is a non-null figure. The builder keeps it outside the gap
        // and resets it to null inside the gap. The value matches the genesis
        // stake sum of preprod.
        const ACTIVE_STAKE: u64 = 300_000_000_000_000;

        // This helper resolves one epoch through the real builder. It returns
        // the mapped `active_stake`, exactly as a handler serializes it.
        fn active_stake_for(facade: &Facade<ToyDomain>, epoch: Epoch) -> Option<String> {
            let chain = facade.get_chain_summary().expect("era summary");
            let state =
                dolos_cardano::load_epoch::<ToyDomain>(facade.state()).expect("base epoch state");
            build_epoch_content(facade, &chain, epoch, state, Some(ACTIVE_STAKE))
                .expect("build epoch content")
                .into_model()
                .expect("map epoch content")
                .active_stake
        }

        let with_value = || Some(ACTIVE_STAKE.to_string());

        let preprod = facade_for(dolos_cardano::include::preprod::load());

        // The gap runs from epoch 13 to epoch 28. Epochs 5 and 12 are before
        // the gap. Epochs 29 and 100 are after it. All of these epochs keep the
        // value. Epochs 13, 20, and 28 are inside the gap, so they reset to
        // null.
        assert_eq!(active_stake_for(&preprod, 5), with_value());
        assert_eq!(active_stake_for(&preprod, 12), with_value());
        assert_eq!(active_stake_for(&preprod, 13), None);
        assert_eq!(active_stake_for(&preprod, 20), None);
        assert_eq!(active_stake_for(&preprod, 28), None);
        assert_eq!(active_stake_for(&preprod, 29), with_value());
        assert_eq!(active_stake_for(&preprod, 100), with_value());

        // Preview shares the endpoint but has no gap, so the same epoch keeps
        // its value.
        let preview = facade_for(dolos_cardano::include::preview::load());
        assert_eq!(active_stake_for(&preview, 20), with_value());

        // Mainnet also has no gap, so the same epoch keeps its value.
        let mainnet = facade_for(dolos_cardano::include::mainnet::load());
        assert_eq!(active_stake_for(&mainnet, 20), with_value());

        // The archive fault proves that gap epochs do not read the StakeLogs.
        // The builder still returns null when the caller supplies no value.
        let faulty_preprod = Facade {
            inner: dolos_testing::faults::FaultyToyDomain::new(
                preprod.inner.clone(),
                TestFault::ArchiveStoreError,
            ),
            config: preprod.config.clone(),
            cache: preprod.cache.clone(),
        };
        let chain = faulty_preprod.get_chain_summary().expect("era summary");
        let state = dolos_cardano::load_epoch::<dolos_testing::faults::FaultyToyDomain>(
            faulty_preprod.state(),
        )
        .expect("base epoch state");
        let content = build_epoch_content(&faulty_preprod, &chain, 20, state, None)
            .expect("build epoch content")
            .into_model()
            .expect("map epoch content");
        assert_eq!(content.active_stake, None);
    }

    #[test]
    fn decode_block_header_matches_full_decode() {
        let (_, raw) = dolos_testing::blocks::make_conway_block(1234);

        let header = decode_block_header(&raw).unwrap().expect("conway header");
        let block = MultiEraBlock::decode(&raw).unwrap();

        assert_eq!(header.hash(), block.hash());
        assert_eq!(header.issuer_vkey(), block.header().issuer_vkey());
    }

    #[test]
    fn decode_block_header_reads_the_shelley_header_shape() {
        use pallas::ledger::primitives::alonzo;

        let issuer_vkey = vec![0xAA; 32];
        let header = alonzo::Header {
            header_body: alonzo::HeaderBody {
                block_number: 7,
                slot: 42,
                prev_hash: None,
                issuer_vkey: issuer_vkey.clone().into(),
                vrf_vkey: vec![].into(),
                nonce_vrf: alonzo::VrfCert(vec![].into(), vec![].into()),
                leader_vrf: alonzo::VrfCert(vec![].into(), vec![].into()),
                block_body_size: 0,
                block_body_hash: pallas::crypto::hash::Hash::from([0u8; 32]),
                operational_cert_hot_vkey: vec![].into(),
                operational_cert_sequence_number: 0,
                operational_cert_kes_period: 0,
                operational_cert_sigma: vec![].into(),
                protocol_major: 6,
                protocol_minor: 0,
            },
            body_signature: vec![].into(),
        };
        let header_cbor = minicbor::to_vec(&header).unwrap();

        // Wrap as a stored alonzo block: `[5, [header]]`. The helper stops at
        // the header, so the block needs no transaction sections.
        let mut body = vec![0x82, 0x05, 0x81];
        body.extend(&header_cbor);

        let decoded = decode_block_header(&body).unwrap().expect("alonzo header");

        assert!(matches!(decoded, MultiEraHeader::ShelleyCompatible(_)));
        assert_eq!(decoded.issuer_vkey().unwrap(), issuer_vkey.as_slice());
        assert_eq!(decoded.hash(), Hasher::<256>::hash(&header_cbor));
    }

    #[test]
    fn decode_block_header_skips_byron_blocks() {
        // `[0, []]` = epoch boundary block, `[1, []]` = byron main block.
        assert!(decode_block_header(&[0x82, 0x00, 0x80]).unwrap().is_none());
        assert!(decode_block_header(&[0x82, 0x01, 0x80]).unwrap().is_none());
    }

    #[test]
    fn decode_block_header_rejects_malformed_bytes() {
        // Not a block wrapper at all.
        assert!(decode_block_header(&[0xff, 0x00]).is_err());

        // A real block truncated inside the header.
        let (_, raw) = dolos_testing::blocks::make_conway_block(1234);
        assert!(decode_block_header(&raw[..raw.len() / 4]).is_err());
    }

    /// Gives a text for each difference from the Blockfrost error body.
    async fn error_mismatch(
        app: &TestApp,
        path: &str,
        status: StatusCode,
        message: &str,
    ) -> Option<String> {
        let (actual, bytes) = app.get_bytes(path).await;
        let expected = serde_json::json!({
            "status_code": status.as_u16(),
            "error": status.canonical_reason(),
            "message": message,
        });
        let body = serde_json::from_slice::<serde_json::Value>(&bytes).ok();

        (actual != status || body.as_ref() != Some(&expected)).then(|| {
            format!(
                "{path}: expected {status} {message:?}, got {actual} {}",
                String::from_utf8_lossy(&bytes)
            )
        })
    }

    fn pool_path_ids(labels: &[&str]) -> Vec<String> {
        labels
            .iter()
            .map(|label| {
                pool_id_cases()
                    .into_iter()
                    .find(|case| case.label == *label)
                    .expect("Cannot find the pool ID case.")
                    .path_id()
            })
            .collect()
    }

    #[tokio::test]
    async fn epochs_pool_routes_check_in_blockfrost_order() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            pool_id: REG_POOL_ID.to_string(),
            ..Default::default()
        });
        let tip = app.tip_epoch();
        let mut mismatches = Vec::new();

        for route in ["blocks", "stakes"] {
            for id in pool_path_ids(&["invalid", "bmissing", "b29", "h58"]) {
                for epoch in [tip, MAX_EPOCH_NUMBER + 1] {
                    let path = format!("/epochs/{epoch}/{route}/{id}?count=0");
                    mismatches.extend(
                        error_mismatch(
                            &app,
                            &path,
                            StatusCode::BAD_REQUEST,
                            "querystring/count must be >= 1",
                        )
                        .await,
                    );
                }
            }

            for id in pool_path_ids(&["invalid", "b28", "b29", "h58"]) {
                for epoch in [999_999, MAX_EPOCH_NUMBER] {
                    let path = format!("/epochs/{epoch}/{route}/{id}");
                    mismatches.extend(
                        error_mismatch(
                            &app,
                            &path,
                            StatusCode::NOT_FOUND,
                            "The requested component has not been found.",
                        )
                        .await,
                    );
                }

                let path = format!("/epochs/{}/{route}/{id}", MAX_EPOCH_NUMBER + 1);
                mismatches.extend(
                    error_mismatch(
                        &app,
                        &path,
                        StatusCode::BAD_REQUEST,
                        "Missing, out of range or malformed epoch_number.",
                    )
                    .await,
                );
            }
        }

        assert!(mismatches.is_empty(), "{mismatches:#?}");
    }

    /// Gives a text for a difference from the Blockfrost response.
    /// The text contains the case label, not the path, because some IDs have
    /// thousands of characters.
    async fn pool_id_case_mismatch(
        app: &TestApp,
        case: &PoolIdCase,
        route: &str,
        expected: u16,
    ) -> Option<String> {
        let (status, bytes) = app.get_bytes(&route.replace("{id}", &case.path_id())).await;
        let expected_body = match expected {
            400 => Some(("Bad Request", "Invalid or malformed pool id format.")),
            404 => Some(("Not Found", "The requested component has not been found.")),
            _ => None,
        }
        .map(|(error, message)| {
            serde_json::json!({ "status_code": expected, "error": error, "message": message })
        });
        let body = serde_json::from_slice::<serde_json::Value>(&bytes).ok();
        let matched =
            status.as_u16() == expected && (expected_body.is_none() || body == expected_body);

        (!matched).then(|| {
            format!(
                "{} {route}: expected {expected}, got {status} {}",
                case.label,
                String::from_utf8_lossy(&bytes)
            )
        })
    }

    #[tokio::test]
    async fn epoch_pool_routes_match_blockfrost_for_each_pool_id() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            pool_id: REG_POOL_ID.to_string(),
            ..Default::default()
        });
        let tip = app.tip_epoch();
        let mut mismatches = Vec::new();

        for case in pool_id_cases() {
            for route in ["blocks", "stakes"] {
                let route = format!("/epochs/{tip}/{route}/{{id}}");
                mismatches
                    .extend(pool_id_case_mismatch(&app, &case, &route, case.bounded_status).await);
            }
        }

        assert!(mismatches.is_empty(), "{mismatches:#?}");
    }
}
