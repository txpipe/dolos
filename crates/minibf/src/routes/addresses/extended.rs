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
/// tokens. Their on-chain reads run before, on the query limiter, as many at
/// once as one request may run there.
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

    // every on-chain read ends before the first registry request starts. a
    // stage waiting on a slow registry stops polling the reads queued behind
    // it, and the limiter hands a read queued for a permit its permit whether
    // it is polled or not, so the permit would sit idle until the registry
    // answers
    let onchain: Vec<_> = futures_util::stream::iter(units)
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
        .try_collect()
        .await?;

    let amount = futures_util::stream::iter(onchain)
        .map(|(entry, extras)| async move {
            let extras = extras.with_registry().await?;

            Ok::<_, Error>(AddressContentExtendedAmountInner {
                unit: entry.unit,
                quantity: entry.quantity,
                decimals: extras.decimals,
                has_nft_onchain_metadata: extras.has_nft_onchain_metadata,
            })
        })
        .buffered(EXTENDED_ASSET_CONCURRENCY)
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

    /// Finding YT4 of the fourth QA review of dolos#1524: a held asset with no
    /// first mint, on a node whose history window pruned it or a store an
    /// earlier build synced, answers bare, and the address still answers.
    #[tokio::test]
    async fn a_held_asset_with_no_first_mint_answers_bare() {
        use chain::*;

        let ghost: Token = ([0x6f; 28], b"GHOST".to_vec());

        let app = app_with(minibf(), |tip, address| {
            vec![
                block(
                    tip + 1,
                    None,
                    &[],
                    vec![output(address, holding(std::slice::from_ref(&ghost)), None)],
                )
                .0,
            ]
        });

        let (status, _) = app.get_bytes(&format!("/assets/{}", unit_of(&ghost))).await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let item = extended_of(&app).await;
        let entry = item
            .amount
            .iter()
            .find(|x| x.unit == unit_of(&ghost))
            .expect("the held asset");

        assert_eq!(entry.decimals, None);
        assert!(!entry.has_nft_onchain_metadata);
    }

    /// Blocks built past the tip of a five-block synthetic chain.
    mod chain {
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

        pub const LABEL_100: [u8; 4] = [0x00, 0x06, 0x43, 0xb0];
        pub const LABEL_222: [u8; 4] = [0x00, 0x0d, 0xe1, 0x40];
        pub const LABEL_333: [u8; 4] = [0x00, 0x14, 0xdf, 0x10];

        /// A policy and an asset name.
        pub type Token = ([u8; 28], Vec<u8>);

        pub fn labelled(label: [u8; 4], name: &[u8]) -> Vec<u8> {
            [label.as_slice(), name].concat()
        }

        pub fn unit_of((policy, name): &Token) -> String {
            format!("{}{}", hex::encode(policy), hex::encode(name))
        }

        /// One of each of `tokens`.
        pub fn holding(tokens: &[Token]) -> Value {
            let mut map: BTreeMap<Hash<28>, BTreeMap<Bytes, PositiveCoin>> = BTreeMap::new();

            for (policy, name) in tokens {
                map.entry(Hash::from(*policy)).or_default().insert(
                    Bytes::from(name.clone()),
                    PositiveCoin::try_from(1).unwrap(),
                );
            }

            Value::Multiasset(0, map)
        }

        pub fn output(
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

        pub fn text(x: &str) -> PlutusData {
            PlutusData::BoundedBytes(x.as_bytes().to_vec().into())
        }

        pub fn int(x: i64) -> PlutusData {
            PlutusData::BigInt(BigInt::Int(x.into()))
        }

        /// A CIP-68 v1 datum with `fields` as its metadata.
        pub fn cip68_datum(fields: Vec<(PlutusData, PlutusData)>) -> PlutusData {
            PlutusData::Constr(Constr {
                tag: 121,
                any_constructor: None,
                fields: MaybeIndefArray::Def(vec![
                    PlutusData::Map(KeyValuePairs::Def(fields)),
                    int(1),
                    PlutusData::Constr(Constr {
                        tag: 121,
                        any_constructor: None,
                        fields: MaybeIndefArray::Def(vec![]),
                    }),
                ]),
            })
        }

        /// A block at `slot` whose one tx spends output 0 of `input`, if any,
        /// mints one of each of `mints` and pays `outputs`.
        pub fn block(
            slot: u64,
            input: Option<Hash<32>>,
            mints: &[Token],
            outputs: Vec<TransactionOutput<'static>>,
        ) -> (RawBlock, Hash<32>) {
            let mint = (!mints.is_empty()).then(|| {
                let mut map: BTreeMap<Hash<28>, BTreeMap<Bytes, NonZeroInt>> = BTreeMap::new();

                for (policy, name) in mints {
                    map.entry(Hash::from(*policy))
                        .or_default()
                        .insert(Bytes::from(name.clone()), NonZeroInt::try_from(1).unwrap());
                }

                map
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

        /// An address the tests never read.
        pub fn elsewhere() -> Vec<u8> {
            ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Key([0x99; 28].into()),
                ShelleyDelegationPart::Null,
            )
            .to_vec()
        }

        pub fn minibf() -> MinibfConfig {
            MinibfConfig::new("[::]:0".parse().unwrap())
        }

        /// The chain, then what `blocks` builds from its tip slot and the
        /// address of its vectors, served with `minibf`.
        pub fn app_with(
            minibf: MinibfConfig,
            blocks: impl FnOnce(u64, &[u8]) -> Vec<RawBlock>,
        ) -> TestApp {
            let cfg = SyntheticBlockConfig {
                block_count: 5,
                txs_per_block: 3,
                ..Default::default()
            };

            let (domain, vectors) = TestDomainBuilder::new_with_synthetic(cfg).finish();
            let tip = domain.state().read_cursor().unwrap().unwrap().slot();
            let address = Address::from_bech32(&vectors.address).unwrap().to_vec();
            domain.import_blocks(blocks(tip, &address)).unwrap();

            TestApp::from_domain(domain, vectors, None, minibf)
        }

        pub async fn extended_of(app: &TestApp) -> AddressContentExtended {
            let path = format!("/addresses/{}/extended", app.vectors().address);
            let (status, bytes) = app.get_bytes(&path).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "{}",
                String::from_utf8_lossy(&bytes)
            );

            serde_json::from_slice(&bytes).unwrap()
        }
    }

    /// The label-100 walks of one request: CIP-68 user tokens held at the
    /// address, each reference token minted with a datum and then moved
    /// [`MOVES`] times without one, so that each walk reads `MOVES + 1`
    /// blocks to find it.
    mod walks {
        use dolos_core::RawBlock;

        use super::{chain::*, *};

        const MOVES: u64 = 3;
        const WALK: u64 = MOVES + 1;
        const POLICIES: [[u8; 28]; 2] = [[0xa1; 28], [0xb2; 28]];

        fn reference(policy: [u8; 28]) -> Token {
            (policy, labelled(LABEL_100, b"QA"))
        }

        fn user(policy: [u8; 28]) -> Token {
            (policy, labelled(LABEL_222, b"QA"))
        }

        /// `policy`'s pair minted at `slot`, the user token paid to `address`
        /// and the reference token to `holder` with a datum naming the token,
        /// then the reference token moved [`MOVES`] times to `holder` without
        /// one.
        fn pair(slot: u64, policy: [u8; 28], address: &[u8], holder: &[u8]) -> Vec<RawBlock> {
            let datum = cip68_datum(vec![
                (text("name"), text("QA")),
                (text("image"), text("ipfs://qa")),
            ]);

            let (minted, mut input) = block(
                slot,
                None,
                &[reference(policy), user(policy)],
                vec![
                    output(holder, holding(&[reference(policy)]), Some(datum)),
                    output(address, holding(&[user(policy)]), None),
                ],
            );

            let mut out = vec![minted];

            for slot in slot + 1..=slot + MOVES {
                let (moved, tx) = block(
                    slot,
                    Some(input),
                    &[],
                    vec![output(holder, holding(&[reference(policy)]), None)],
                );
                out.push(moved);
                input = tx;
            }

            out
        }

        /// Every policy's pair, each reference token held elsewhere.
        fn app(max_scan_items: u64) -> TestApp {
            app_with(
                minibf().with_max_scan_items(max_scan_items),
                |tip, address| {
                    POLICIES
                        .iter()
                        .zip(0..)
                        .flat_map(|(policy, i)| {
                            pair(tip + 1 + i * WALK, *policy, address, &elsewhere())
                        })
                        .collect()
                },
            )
        }

        /// Whether `token` reports on-chain metadata.
        fn flag(item: &AddressContentExtended, token: &Token) -> bool {
            item.amount
                .iter()
                .find(|x| x.unit == unit_of(token))
                .expect("the token is held")
                .has_nft_onchain_metadata
        }

        /// Whether each policy's user token reports on-chain metadata, which
        /// it takes from its reference token's datum, in unit order.
        async fn flags(app: &TestApp) -> Vec<bool> {
            let item = extended_of(app).await;

            POLICIES
                .iter()
                .map(|policy| flag(&item, &user(*policy)))
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

        /// Whether the user token reports on-chain metadata with both tokens
        /// of its pair held at the address.
        async fn held_pair_flag(max_scan_items: u64) -> bool {
            let [policy, _] = POLICIES;

            let app = app_with(
                minibf().with_max_scan_items(max_scan_items),
                |tip, address| pair(tip + 1, policy, address, address),
            );

            let item = extended_of(&app).await;
            let held = |token: &Token| item.amount.iter().any(|x| x.unit == unit_of(token));
            assert!(held(&reference(policy)) && held(&user(policy)));

            flag(&item, &user(policy))
        }

        /// Finding YT3 of the fourth QA review of dolos#1524: a reference
        /// token held at the address walks on a share of its own, which comes
        /// before its user token's, since it sorts first.
        #[tokio::test]
        async fn a_held_reference_token_takes_a_share_of_its_own() {
            // a share of WALK each
            assert!(held_pair_flag(2 * WALK).await);

            // WALK for the reference token, a block short for the user token
            assert!(!held_pair_flag(2 * WALK - 1).await);
        }
    }

    /// The decimal places of units: the chain's where it declares them, the
    /// token registry's where it does not.
    mod decimals {
        use std::{
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc, Mutex,
            },
            time::{Duration, Instant},
        };

        use dolos_core::{QueryLimiter, RawBlock};
        use dolos_testing::{faults::FaultyToyDomain, synthetic::SyntheticBlockConfig};

        use super::{chain::*, *};
        use crate::test_support::TestDomainBuilder;

        fn fungible() -> (Token, Token) {
            let policy = [0xf7; 28];

            (
                (policy, labelled(LABEL_100, b"FT")),
                (policy, labelled(LABEL_333, b"FT")),
            )
        }

        /// A CIP-68 fungible pair whose reference datum declares 6 decimal
        /// places, the reference token paid elsewhere and the user token
        /// (label 333) to `address`.
        fn fungible_pair(slot: u64, address: &[u8]) -> RawBlock {
            let (reference, user) = fungible();

            let datum = cip68_datum(vec![
                (text("name"), text("QA FT")),
                (text("description"), text("a fungible QA token")),
                (text("decimals"), int(6)),
            ]);

            block(
                slot,
                None,
                &[reference.clone(), user.clone()],
                vec![
                    output(&elsewhere(), holding(&[reference]), Some(datum)),
                    output(address, holding(&[user]), None),
                ],
            )
            .0
        }

        /// Finding YT1 of the fourth QA review of dolos#1524: with no token
        /// registry, which is the default, a CIP-68 fungible user token
        /// answers the decimals its reference datum declares.
        #[tokio::test]
        async fn a_fungible_user_token_answers_its_datum_decimals() {
            let app = app_with(minibf(), |tip, address| {
                vec![fungible_pair(tip + 1, address)]
            });

            let item = extended_of(&app).await;
            let (_, user) = fungible();
            let entry = item
                .amount
                .iter()
                .find(|x| x.unit == unit_of(&user))
                .expect("the user token");

            assert_eq!(entry.decimals, Some(6));
            assert!(entry.has_nft_onchain_metadata);
        }

        /// Finding YT2 of the fourth QA review of dolos#1524: the registry is
        /// asked only for the units the chain declares no decimals for, and
        /// each answers the registry's. The units keep `/addresses/{a}`'s
        /// order, although the registry answers the first one last.
        #[tokio::test]
        async fn the_registry_answers_the_decimals_the_chain_does_not_declare() {
            let order: Arc<Mutex<Vec<String>>> = Default::default();
            let asked: Arc<Mutex<Vec<String>>> = Default::default();

            let registry = axum::Router::new().fallback({
                let (order, asked) = (order.clone(), asked.clone());

                move |uri: axum::http::Uri| {
                    let (order, asked) = (order.clone(), asked.clone());

                    async move {
                        let unit = uri.path().rsplit('/').next().unwrap_or_default();
                        asked.lock().unwrap().push(unit.to_string());

                        let (position, len) = {
                            let order = order.lock().unwrap();
                            (order.iter().position(|x| x == unit), order.len())
                        };

                        let Some(position) = position else {
                            return (StatusCode::NOT_FOUND, String::new());
                        };

                        // the earlier a unit is listed, the later it is answered
                        let delay = 80 * (len - position) as u64;
                        tokio::time::sleep(Duration::from_millis(delay)).await;

                        let body = serde_json::json!({
                            "name": { "value": format!("unit {position}") },
                            "description": { "value": "registered" },
                            "decimals": { "value": 10 + position },
                        });

                        (StatusCode::OK, body.to_string())
                    }
                }
            });

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut config = minibf();
            config.token_registry_url = Some(format!("http://{}", listener.local_addr().unwrap()));
            let server =
                tokio::spawn(async move { axum::serve(listener, registry).await.unwrap() });

            let app = app_with(config, |tip, address| {
                let natives: Vec<Token> = (0..5)
                    .map(|i| ([0x7e; 28], format!("N{i}").into_bytes()))
                    .collect();

                vec![
                    fungible_pair(tip + 1, address),
                    block(
                        tip + 2,
                        None,
                        &natives,
                        vec![output(address, holding(&natives), None)],
                    )
                    .0,
                ]
            });

            let path = format!("/addresses/{}", app.vectors().address);
            let (status, bytes) = app.get_bytes(&path).await;
            assert_eq!(status, StatusCode::OK);

            let plain: AddressContent = serde_json::from_slice(&bytes).unwrap();
            let units: Vec<String> = plain.amount.iter().map(|x| x.unit.clone()).collect();
            *order.lock().unwrap() = units.clone();

            let (_, user) = fungible();
            let declared = ["lovelace".to_string(), unit_of(&user)];
            assert!(
                units.len() >= 8 && units.contains(&declared[1]),
                "{units:?}"
            );

            let item = extended_of(&app).await;
            server.abort();

            let answered: Vec<_> = item.amount.iter().map(|x| x.unit.clone()).collect();
            assert_eq!(answered, units);

            for (position, entry) in item.amount.iter().enumerate() {
                let expected = if declared.contains(&entry.unit) {
                    6
                } else {
                    10 + position as i32
                };

                assert_eq!(entry.decimals, Some(expected), "{}", entry.unit);
            }

            let mut asked = asked.lock().unwrap().clone();
            asked.sort();
            let mut undeclared: Vec<_> = units
                .into_iter()
                .filter(|x| !declared.contains(x))
                .collect();
            undeclared.sort();
            assert_eq!(asked, undeclared);
        }

        /// Finding Y1 of the fourth QA review of dolos#1524: every unit's
        /// on-chain read ends before the first registry request starts. A
        /// read still queued for a query permit while the registry stalls
        /// would be handed one it cannot use until the registry answers. The
        /// address holds more units than the registry's 16 and the reads'
        /// fan-out together, and the registry never answers.
        #[tokio::test]
        async fn every_onchain_read_ends_before_the_first_registry_request() {
            const HELD: usize = 40;

            // entity reads run when the registry accepts each request
            let reads = Arc::new(AtomicUsize::new(0));
            let seen: Arc<Mutex<Vec<usize>>> = Default::default();

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut config = minibf();
            config.token_registry_url = Some(format!("http://{}", listener.local_addr().unwrap()));

            let registry = tokio::spawn({
                let (reads, seen) = (reads.clone(), seen.clone());

                async move {
                    let mut held = vec![];

                    while let Ok((stream, _)) = listener.accept().await {
                        seen.lock().unwrap().push(reads.load(Ordering::SeqCst));
                        held.push(stream);
                    }
                }
            });

            let mints = (0..HELD)
                .map(|i| ([0x5a; 28], format!("M{i:02}"), 1))
                .collect();

            let cfg = SyntheticBlockConfig {
                block_count: 5,
                txs_per_block: 1,
                mints_by_block: vec![vec![mints]],
                ..Default::default()
            };

            let (domain, vectors) = TestDomainBuilder::new_with_synthetic(cfg).finish();
            let domain = domain.with_query_limiter(QueryLimiter::new(8));
            let app = TestApp::from_domain(domain, vectors, None, config);
            let facade = app.facade();

            facade.inner.during_entity_slots({
                let reads = reads.clone();

                move || {
                    reads.fetch_add(1, Ordering::SeqCst);
                }
            });

            let request = tokio::spawn(extended::<FaultyToyDomain>(
                Path(app.vectors().address.clone()),
                State(facade.clone()),
            ));

            let deadline = Instant::now() + Duration::from_secs(10);
            while seen.lock().unwrap().len() < EXTENDED_ASSET_CONCURRENCY {
                assert!(Instant::now() < deadline, "{:?}", seen.lock().unwrap());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }

            let in_use = facade.inner.query_limiter().in_use();
            request.abort();
            registry.abort();

            let seen = seen.lock().unwrap().clone();
            assert!(seen.iter().all(|x| *x >= HELD), "{seen:?}");
            assert_eq!(in_use, 0);
        }

        /// A token registry that answers only at `/metadata/{unit}`, with the
        /// body `answers` gives the unit, and records the units it is asked
        /// for.
        async fn registry_at_its_metadata_path(
            answers: Vec<(String, serde_json::Value)>,
        ) -> (String, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
            let asked: Arc<Mutex<Vec<String>>> = Default::default();

            let registry = axum::Router::new().route(
                "/metadata/{unit}",
                axum::routing::get({
                    let asked = asked.clone();

                    move |Path(unit): Path<String>| {
                        let asked = asked.clone();
                        let answer = answers
                            .iter()
                            .find(|(x, _)| *x == unit)
                            .map(|(_, body)| body.to_string());

                        async move {
                            asked.lock().unwrap().push(unit);

                            match answer {
                                Some(body) => (StatusCode::OK, body),
                                None => (StatusCode::NOT_FOUND, String::new()),
                            }
                        }
                    }
                }),
            );

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server =
                tokio::spawn(async move { axum::serve(listener, registry).await.unwrap() });

            (url, asked, server)
        }

        /// The chain, then a block that mints one of each of `tokens` to the
        /// address, served with the token registry at `url`.
        fn app_holding(url: String, tokens: &[Token]) -> TestApp {
            let mut config = minibf();
            config.token_registry_url = Some(url);

            app_with(config, |tip, address| {
                vec![
                    block(
                        tip + 1,
                        None,
                        tokens,
                        vec![output(address, holding(tokens), None)],
                    )
                    .0,
                ]
            })
        }

        /// Findings T2 and T3 of the fifth QA review of dolos#1524: the
        /// registry is asked at `/metadata/{unit}`, by `/assets/{asset}` for
        /// its metadata and by `/addresses/{a}/extended` for its decimals.
        #[tokio::test]
        async fn the_registry_is_asked_at_its_metadata_path() {
            let native: Token = ([0x7c; 28], b"REG".to_vec());

            let (url, _, server) = registry_at_its_metadata_path(vec![(
                unit_of(&native),
                serde_json::json!({
                    "name": { "value": "registered" },
                    "description": { "value": "a registered token" },
                    "ticker": { "value": "REG" },
                    "decimals": { "value": 4 },
                }),
            )])
            .await;

            let app = app_holding(url, std::slice::from_ref(&native));

            let path = format!("/assets/{}", unit_of(&native));
            let (status, bytes) = app.get_bytes(&path).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "{}",
                String::from_utf8_lossy(&bytes)
            );

            let asset: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(asset["metadata"]["name"], "registered");
            assert_eq!(asset["metadata"]["ticker"], "REG");
            assert_eq!(asset["metadata"]["decimals"], 4);

            let item = extended_of(&app).await;
            server.abort();

            let entry = item
                .amount
                .iter()
                .find(|x| x.unit == unit_of(&native))
                .expect("the native token");
            assert_eq!(entry.decimals, Some(4));
        }

        /// Finding T2 of the fifth QA review of dolos#1524: a registry entry
        /// without a name or a description is no metadata. Its unit answers
        /// no decimals, its asset no metadata, and neither request fails.
        #[tokio::test]
        async fn a_registry_entry_without_a_name_or_a_description_is_no_metadata() {
            let nameless: Token = ([0x7d; 28], b"NONAME".to_vec());
            let undescribed: Token = ([0x7d; 28], b"NODESC".to_vec());

            let (url, asked, server) = registry_at_its_metadata_path(vec![
                (
                    unit_of(&nameless),
                    serde_json::json!({
                        "description": { "value": "a token with no name" },
                        "decimals": { "value": 4 },
                    }),
                ),
                (
                    unit_of(&undescribed),
                    serde_json::json!({
                        "name": { "value": "undescribed" },
                        "decimals": { "value": 5 },
                    }),
                ),
            ])
            .await;

            let app = app_holding(url, &[nameless.clone(), undescribed.clone()]);

            for token in [&nameless, &undescribed] {
                let path = format!("/assets/{}", unit_of(token));
                let (status, bytes) = app.get_bytes(&path).await;
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "{}",
                    String::from_utf8_lossy(&bytes)
                );

                let asset: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert!(asset["metadata"].is_null(), "{asset}");
            }

            let item = extended_of(&app).await;
            server.abort();

            for token in [&nameless, &undescribed] {
                let entry = item
                    .amount
                    .iter()
                    .find(|x| x.unit == unit_of(token))
                    .expect("the native token");
                assert_eq!(entry.decimals, None, "{}", entry.unit);
            }

            // the registry answered each, whichever request asked it
            let asked = asked.lock().unwrap().clone();
            for token in [&nameless, &undescribed] {
                assert!(asked.contains(&unit_of(token)), "{asked:?}");
            }
        }

        /// Finding T1 of the fifth QA review of dolos#1524: a request asks the
        /// registry for at most [`EXTENDED_ASSET_CONCURRENCY`] units at once.
        /// The registry never answers, and the address holds twice as many
        /// units the chain declares no decimals for.
        #[tokio::test]
        async fn the_registry_is_asked_for_a_bounded_number_of_units_at_once() {
            let accepted = Arc::new(AtomicUsize::new(0));

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());

            let registry = tokio::spawn({
                let accepted = accepted.clone();

                async move {
                    let mut held = vec![];

                    while let Ok((stream, _)) = listener.accept().await {
                        accepted.fetch_add(1, Ordering::SeqCst);
                        held.push(stream);
                    }
                }
            });

            let natives: Vec<Token> = (0..2 * EXTENDED_ASSET_CONCURRENCY)
                .map(|i| ([0x7b; 28], format!("U{i:02}").into_bytes()))
                .collect();

            let app = app_holding(url, &natives);

            let request = tokio::spawn(extended::<FaultyToyDomain>(
                Path(app.vectors().address.clone()),
                State(app.facade()),
            ));

            let deadline = Instant::now() + Duration::from_secs(10);
            while accepted.load(Ordering::SeqCst) < EXTENDED_ASSET_CONCURRENCY {
                assert!(Instant::now() < deadline, "{accepted:?}");
                tokio::time::sleep(Duration::from_millis(1)).await;
            }

            // the registry stage asks for no more units while its requests
            // are pending
            tokio::time::sleep(Duration::from_millis(300)).await;
            let accepted = accepted.load(Ordering::SeqCst);
            request.abort();
            registry.abort();

            assert_eq!(accepted, EXTENDED_ASSET_CONCURRENCY);
        }
    }
}
