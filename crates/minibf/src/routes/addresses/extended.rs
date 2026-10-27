use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    address_content_extended::AddressContentExtended,
    address_content_extended_amount_inner::AddressContentExtendedAmountInner,
};
use dolos_cardano::periods::assets::WalkBudget;
use dolos_core::Domain;
use futures_util::{StreamExt, TryStreamExt};

use crate::{
    error::Error,
    mapping::{
        assets::{walks_reference_token, AssetAmountExtras, OnchainExtras},
        AddressExtendedModelBuilder, IntoModel,
    },
    Facade,
};

use super::{amount_for_refs, is_address_in_chain, parse_address, refs_for_parsed_address};

/// This constant sets how many units of an address ask the off-chain registry
/// for their metadata at the same time. Each unit needs one request to the
/// registry. One request at a time is slow for an address that holds many
/// tokens. Their on-chain reads run on the query limiter, as many at once as
/// one request may run there.
const EXTENDED_ASSET_CONCURRENCY: usize = 16;

pub async fn extended<D>(
    Path(address): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<AddressContentExtended>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let parsed = parse_address(&address)?;
    let refs = refs_for_parsed_address(&domain, &parsed)?;

    if refs.is_empty() && !is_address_in_chain(&domain, &address).await? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let entries = amount_for_refs(&domain, refs)?;

    // one max_scan_items for the request's label-100 walks, whatever the
    // number of units it holds, split among the units that walk in the order
    // the address lists them, so that what each answers does not depend on
    // how their walks interleave
    let walking = entries
        .iter()
        .filter(|entry| walks_reference_token(&entry.unit))
        .count();

    let mut shares = WalkBudget::split(domain.config.max_scan_items(), walking).into_iter();

    let units: Vec<_> = entries
        .into_iter()
        .map(|entry| {
            let share = if walks_reference_token(&entry.unit) {
                shares.next()
            } else {
                None
            };

            (entry, share.unwrap_or_else(|| WalkBudget::new(0)))
        })
        .collect();

    let amount = futures_util::stream::iter(units)
        .map(|(entry, budget)| {
            let domain = &domain;

            async move {
                let extras = if entry.unit == "lovelace" {
                    OnchainExtras::known(AssetAmountExtras::lovelace())
                } else {
                    AssetAmountExtras::onchain(domain, &entry.unit, &budget).await?
                };

                Ok::<_, Error>((entry, extras))
            }
        })
        .buffered(domain.inner.query_limiter().fan_out())
        .map_ok(|(entry, extras)| async move {
            let extras = extras.with_registry().await?;

            Ok::<_, Error>(AddressContentExtendedAmountInner {
                unit: entry.unit,
                quantity: entry.quantity,
                decimals: extras.decimals,
                has_nft_onchain_metadata: extras.has_nft_onchain_metadata,
            })
        })
        .try_buffered(EXTENDED_ASSET_CONCURRENCY)
        .try_collect()
        .await?;

    let model = AddressExtendedModelBuilder {
        address,
        amount,
        kind: parsed.into_model_kind(),
    }
    .into_model()
    .map_err(Error::Code)?;

    Ok(Json(model))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::addresses::testing::*,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::{
        address_content::AddressContent, address_content_extended::Type as AddressExtendedType,
    };
    use pallas::ledger::addresses::Address;

    #[tokio::test]
    async fn addresses_extended_happy_path() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/extended");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: AddressContentExtended =
            serde_json::from_slice(&bytes).expect("failed to parse extended address content");

        assert_eq!(item.address, address);
        assert_eq!(item.r#type, AddressExtendedType::Shelley);
        assert!(item.stake_address.is_some());
        assert!(!item.script);

        let lovelace = item.amount.first().expect("expected a lovelace entry");
        assert_eq!(lovelace.unit, "lovelace");
        assert_eq!(lovelace.decimals, Some(6));
        assert!(!lovelace.has_nft_onchain_metadata);

        // the test mints the synthetic asset with no metadata of any standard
        let asset = item
            .amount
            .iter()
            .find(|x| x.unit == app.vectors().asset_unit)
            .expect("expected the synthetic asset");
        assert_eq!(asset.decimals, None);
        assert!(!asset.has_nft_onchain_metadata);
    }

    #[tokio::test]
    async fn addresses_extended_matches_plain_amounts() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();

        let (_, plain) = app.get_bytes(&format!("/addresses/{address}")).await;
        let (_, extended) = app
            .get_bytes(&format!("/addresses/{address}/extended"))
            .await;

        let plain: AddressContent =
            serde_json::from_slice(&plain).expect("failed to parse address");
        let extended: AddressContentExtended =
            serde_json::from_slice(&extended).expect("failed to parse extended address");

        let plain: Vec<_> = plain
            .amount
            .iter()
            .map(|x| (&x.unit, &x.quantity))
            .collect();
        let extended: Vec<_> = extended
            .amount
            .iter()
            .map(|x| (&x.unit, &x.quantity))
            .collect();

        assert_eq!(plain, extended);
    }

    #[tokio::test]
    async fn addresses_extended_payment_credential_happy_path() {
        let app = TestApp::new();
        let address = Address::from_bech32(app.vectors().address.as_str())
            .expect("invalid synthetic test address");

        let Address::Shelley(shelley) = address else {
            panic!("expected shelley test address")
        };

        let payment_cred = bech32::encode::<bech32::Bech32>(
            bech32::Hrp::parse("addr_vkh").expect("invalid hrp"),
            &shelley.payment().to_vec(),
        )
        .expect("failed to encode payment credential");

        let path = format!("/addresses/{payment_cred}/extended");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: AddressContentExtended =
            serde_json::from_slice(&bytes).expect("failed to parse extended address content");

        assert_eq!(item.address, payment_cred);
        assert_eq!(item.r#type, AddressExtendedType::Shelley);
        assert_eq!(item.stake_address, None);
        assert!(!item.script);
        assert!(!item.amount.is_empty());
    }

    #[tokio::test]
    async fn addresses_extended_bad_request() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/extended", invalid_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn addresses_extended_not_found() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/extended", missing_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn addresses_extended_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/extended");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    /// Finding W3 of the third QA review of dolos#1524: the units' on-chain
    /// reads run as many at once as one request may run on the query limiter,
    /// here 4 of 8 permits, whatever the number of units.
    #[tokio::test]
    async fn an_extended_request_reads_as_many_units_at_once_as_the_limiter_allows() {
        let app = crate::test_support::many_assets_app(12, 8);
        let fan_out = app.facade().inner.query_limiter().fan_out();
        assert_eq!(fan_out, 4);

        let path = format!("/addresses/{}/extended", app.vectors().address);
        let (peak, (status, bytes)) =
            crate::test_support::readers_at_once(&app, app.get_bytes(&path)).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&bytes)
        );

        let item: AddressContentExtended = serde_json::from_slice(&bytes).unwrap();
        assert!(item.amount.len() > fan_out, "{} units", item.amount.len());
        assert!((1..=fan_out).contains(&peak), "{peak} unit reads at once");
    }

    /// The label-100 walks of one request: two CIP-68 user tokens held at the
    /// address, each reference token minted with a datum and then moved
    /// [`MOVES`] times without one, so that each walk reads `MOVES + 1`
    /// blocks to find it.
    mod walks {
        use std::collections::BTreeMap;

        use dolos_core::{config::MinibfConfig, import::ImportExt as _, RawBlock, StateStore as _};
        use dolos_testing::synthetic::SyntheticBlockConfig;
        use pallas::{
            codec::utils::{
                Bytes, CborWrap, KeepRaw, KeyValuePairs, MaybeIndefArray, NonZeroInt, PositiveCoin,
                Set,
            },
            crypto::hash::Hash,
            ledger::{
                addresses::{Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart},
                primitives::{
                    conway::{
                        DatumOption, PostAlonzoTransactionOutput, TransactionBody,
                        TransactionOutput, Value,
                    },
                    BigInt, Constr, PlutusData, TransactionInput,
                },
                traverse::MultiEraBlock,
            },
        };

        use super::*;
        use crate::test_support::TestDomainBuilder;

        const MOVES: u64 = 3;
        const WALK: u64 = MOVES + 1;
        const POLICIES: [[u8; 28]; 2] = [[0xa1; 28], [0xb2; 28]];
        const REFERENCE: &[u8] = b"\x00\x06\x43\xb0QA";
        const USER: &[u8] = b"\x00\x0d\xe1\x40QA";

        fn holding(policy: [u8; 28], name: &[u8]) -> Value {
            Value::Multiasset(
                0,
                BTreeMap::from([(
                    policy.into(),
                    BTreeMap::from([(
                        Bytes::from(name.to_vec()),
                        PositiveCoin::try_from(1).unwrap(),
                    )]),
                )]),
            )
        }

        fn output(
            address: &[u8],
            value: Value,
            datum: Option<PlutusData>,
        ) -> TransactionOutput<'static> {
            TransactionOutput::PostAlonzo(KeepRaw::from(PostAlonzoTransactionOutput {
                address: Bytes::from(address.to_vec()),
                value,
                datum_option: datum
                    .map(|datum| KeepRaw::from(DatumOption::Data(CborWrap(KeepRaw::from(datum))))),
                script_ref: None,
            }))
        }

        /// A CIP-68 v1 datum naming the token.
        fn metadata_datum() -> PlutusData {
            let text = |x: &str| PlutusData::BoundedBytes(x.as_bytes().to_vec().into());

            PlutusData::Constr(Constr {
                tag: 121,
                any_constructor: None,
                fields: MaybeIndefArray::Def(vec![
                    PlutusData::Map(KeyValuePairs::Def(vec![
                        (text("name"), text("QA")),
                        (text("image"), text("ipfs://qa")),
                    ])),
                    PlutusData::BigInt(BigInt::Int(1.into())),
                    PlutusData::Constr(Constr {
                        tag: 121,
                        any_constructor: None,
                        fields: MaybeIndefArray::Def(vec![]),
                    }),
                ]),
            })
        }

        /// A block at `slot` whose one tx spends output 0 of `input`, if any,
        /// mints `policy`'s pair, if any, and pays `outputs`.
        fn block(
            slot: u64,
            input: Option<Hash<32>>,
            policy: Option<[u8; 28]>,
            outputs: Vec<TransactionOutput<'static>>,
        ) -> (RawBlock, Hash<32>) {
            let mint = policy.map(|policy| {
                BTreeMap::from([(
                    policy.into(),
                    BTreeMap::from([
                        (
                            Bytes::from(REFERENCE.to_vec()),
                            NonZeroInt::try_from(1).unwrap(),
                        ),
                        (Bytes::from(USER.to_vec()), NonZeroInt::try_from(1).unwrap()),
                    ]),
                )])
            });

            let inputs: Vec<TransactionInput> = input
                .map(|transaction_id| TransactionInput {
                    transaction_id,
                    index: 0,
                })
                .into_iter()
                .collect();

            let body = TransactionBody {
                inputs: Set::from(inputs),
                outputs,
                fee: 0,
                ttl: None,
                certificates: None,
                withdrawals: None,
                auxiliary_data_hash: None,
                validity_interval_start: None,
                mint,
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

            let (_, raw) = dolos_testing::blocks::make_conway_block_with_txs_at(
                slot,
                slot,
                None,
                vec![(body, true)],
                None,
            );

            let tx = MultiEraBlock::decode(&raw).unwrap().txs()[0].hash();

            (raw, tx)
        }

        /// Each policy's pair, the user token paid to `address` and the
        /// reference token elsewhere with its datum, then the reference token
        /// moved [`MOVES`] times without one.
        fn pairs(after: u64, address: &[u8]) -> Vec<RawBlock> {
            let elsewhere = ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Key([0x99; 28].into()),
                ShelleyDelegationPart::Null,
            )
            .to_vec();

            let mut out = vec![];
            let mut slot = after;

            for policy in POLICIES {
                slot += 1;
                let (minted, mut input) = block(
                    slot,
                    None,
                    Some(policy),
                    vec![
                        output(
                            &elsewhere,
                            holding(policy, REFERENCE),
                            Some(metadata_datum()),
                        ),
                        output(address, holding(policy, USER), None),
                    ],
                );
                out.push(minted);

                for _ in 0..MOVES {
                    slot += 1;
                    let (moved, tx) = block(
                        slot,
                        Some(input),
                        None,
                        vec![output(&elsewhere, holding(policy, REFERENCE), None)],
                    );
                    out.push(moved);
                    input = tx;
                }
            }

            out
        }

        fn app(max_scan_items: u64) -> TestApp {
            let cfg = SyntheticBlockConfig {
                block_count: 5,
                txs_per_block: 3,
                ..Default::default()
            };

            let (domain, vectors) = TestDomainBuilder::new_with_synthetic(cfg).finish();
            let tip = domain.state().read_cursor().unwrap().unwrap().slot();
            let address = Address::from_bech32(&vectors.address).unwrap().to_vec();
            domain.import_blocks(pairs(tip, &address)).unwrap();

            let minibf =
                MinibfConfig::new("[::]:0".parse().unwrap()).with_max_scan_items(max_scan_items);

            TestApp::from_domain(domain, vectors, None, minibf)
        }

        /// Whether each policy's user token reports on-chain metadata, which
        /// it takes from its reference token's datum, in unit order.
        async fn flags(app: &TestApp) -> Vec<bool> {
            let path = format!("/addresses/{}/extended", app.vectors().address);
            let (status, bytes) = app.get_bytes(&path).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "{}",
                String::from_utf8_lossy(&bytes)
            );

            let item: AddressContentExtended = serde_json::from_slice(&bytes).unwrap();

            POLICIES
                .iter()
                .map(|policy| {
                    let unit = format!("{}{}", hex::encode(policy), hex::encode(USER));

                    item.amount
                        .iter()
                        .find(|x| x.unit == unit)
                        .expect("the user token")
                        .has_nft_onchain_metadata
                })
                .collect()
        }

        /// Findings TA1 and W1 of the third QA review of dolos#1524: one
        /// request splits one `max_scan_items` among the units that walk, in
        /// unit order, so each answers by its share alone.
        #[tokio::test]
        async fn an_extended_request_splits_one_walk_budget_among_its_units() {
            // a share of WALK each
            assert_eq!(flags(&app(2 * WALK)).await, [true, true]);

            // WALK for the first unit, a block short for the second
            assert_eq!(flags(&app(2 * WALK - 1)).await, [true, false]);

            // half a walk each
            assert_eq!(flags(&app(WALK)).await, [false, false]);
        }
    }
}
