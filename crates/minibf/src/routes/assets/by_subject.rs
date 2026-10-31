use axum::{
    extract::{Path, State},
    Json,
};
use blockfrost_openapi::models::asset::Asset;
use dolos_cardano::periods::assets::WalkBudget;
use dolos_core::Domain;

use crate::{error::Error, mapping::assets::load_asset_builder, Facade};

pub async fn by_subject<D>(
    Path(unit): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Asset>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let budget = WalkBudget::new(domain.config.max_scan_items());
    let model = load_asset_builder(&domain, unit, &budget).await?;

    Ok(Json(model.into_model(&domain).await?))
}

#[cfg(test)]
mod tests {

    use crate::{
        routes::assets::testing::*,
        test_support::{TestApp, TestFault},
    };
    use axum::http::StatusCode;
    use blockfrost_openapi::models::asset::Asset;

    #[tokio::test]
    async fn assets_by_subject_happy_path() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Asset = serde_json::from_slice(&bytes).expect("failed to parse asset");
    }

    /// The facts the read folds land in their Blockfrost fields: the default
    /// chain mints its asset in every tx, so its first tx is the first mint.
    #[tokio::test]
    async fn assets_by_subject_reports_the_first_mint_tx() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let (status, bytes) = app.get_bytes(&format!("/assets/{asset}")).await;
        assert_eq!(status, StatusCode::OK);

        let asset: Asset = serde_json::from_slice(&bytes).expect("failed to parse asset");

        assert_eq!(asset.initial_mint_tx_hash, app.vectors().tx_hash);
        assert_ne!(asset.quantity, "0");
        assert!(asset.mint_or_burn_count > 0);
    }

    /// A Conway block at `slot` whose one tx mints a CIP-68 pair under
    /// `policy`: the reference token (label 100), held with an inline datum
    /// that names it, and the user NFT (label 222).
    fn cip68_pair_block(slot: u64, policy: [u8; 28]) -> dolos_core::RawBlock {
        use pallas::{
            codec::utils::{Bytes, CborWrap, KeepRaw, KeyValuePairs, MaybeIndefArray, NonZeroInt},
            codec::utils::{PositiveCoin, Set},
            ledger::{
                addresses::Address,
                primitives::{
                    conway::{
                        DatumOption, PostAlonzoTransactionOutput, TransactionBody,
                        TransactionOutput, Value,
                    },
                    BigInt, Constr, PlutusData,
                },
            },
        };

        let reference = Bytes::from([[0x00, 0x06, 0x43, 0xb0].as_slice(), b"PAIR"].concat());
        let user = Bytes::from([[0x00, 0x0d, 0xe1, 0x40].as_slice(), b"PAIR"].concat());

        let text = |x: &str| PlutusData::BoundedBytes(x.as_bytes().to_vec().into());

        let datum = PlutusData::Constr(Constr {
            tag: 121,
            any_constructor: None,
            fields: MaybeIndefArray::Def(vec![
                PlutusData::Map(KeyValuePairs::Def(vec![
                    (text("name"), text("Pair")),
                    (text("image"), text("ipfs://pair")),
                ])),
                PlutusData::BigInt(BigInt::Int(1.into())),
                PlutusData::Constr(Constr {
                    tag: 121,
                    any_constructor: None,
                    fields: MaybeIndefArray::Def(vec![]),
                }),
            ]),
        });

        let address = Address::from_bech32(
            &dolos_testing::synthetic::SyntheticBlockConfig::default().address,
        )
        .unwrap();

        let holding = |name: &Bytes| {
            Value::Multiasset(
                0,
                std::collections::BTreeMap::from([(
                    policy.into(),
                    std::collections::BTreeMap::from([(
                        name.clone(),
                        PositiveCoin::try_from(1).unwrap(),
                    )]),
                )]),
            )
        };

        let output = |value, datum_option| {
            TransactionOutput::PostAlonzo(KeepRaw::from(PostAlonzoTransactionOutput {
                address: Bytes::from(address.to_vec()),
                value,
                datum_option,
                script_ref: None,
            }))
        };

        let mint = std::collections::BTreeMap::from([(
            policy.into(),
            std::collections::BTreeMap::from([
                (reference.clone(), NonZeroInt::try_from(1).unwrap()),
                (user.clone(), NonZeroInt::try_from(1).unwrap()),
            ]),
        )]);

        let body = TransactionBody {
            inputs: Set::from(vec![]),
            outputs: vec![
                output(
                    holding(&reference),
                    Some(KeepRaw::from(DatumOption::Data(CborWrap(KeepRaw::from(
                        datum,
                    ))))),
                ),
                output(holding(&user), None),
            ],
            fee: 0,
            ttl: None,
            certificates: None,
            withdrawals: None,
            auxiliary_data_hash: None,
            validity_interval_start: None,
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
        };

        let (_, raw) = dolos_testing::blocks::make_conway_block_with_txs_at(
            slot,
            slot,
            None,
            vec![(body, true)],
            None,
        );

        raw
    }

    /// A CIP-68 user token reports the on-chain metadata its reference
    /// token's datum carries: the lookup reads the reference token's metadata
    /// tx, not the user token's.
    #[tokio::test]
    async fn assets_by_subject_reads_a_user_token_from_its_reference() {
        use dolos_core::{import::ImportExt as _, Domain as _, StateStore as _};

        let policy = [0x5a; 28];

        let app = TestApp::new_with_cfg_and_setup(Default::default(), |domain, _| {
            let tip = domain.state().read_cursor().unwrap().unwrap().slot();
            domain
                .import_blocks(vec![cip68_pair_block(tip + 1, policy)])
                .unwrap();
        });

        let unit = format!("{}000de14050414952", hex::encode(policy));
        let (status, bytes) = app.get_bytes(&format!("/assets/{unit}")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let asset: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(asset["onchain_metadata_standard"], "CIP68v1");
        assert_eq!(asset["onchain_metadata"]["name"], "Pair");
        assert_eq!(asset["onchain_metadata"]["image"], "ipfs://pair");
    }

    #[tokio::test]
    async fn assets_by_subject_bad_request() {
        let app = TestApp::new();
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn assets_by_subject_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}", missing_asset());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
