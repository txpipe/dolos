mod dreps;
mod metadata;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::DrepsInner;
use dolos_cardano::{model::DRepState, ChainSummary, PParamsSet};
use dolos_core::{BlockSlot, Domain};
use dreps::{drep_is_expired, drep_is_retired, drep_list_item, parse_drep_id, DrepModelBuilder};
use futures::future::join_all;
use metadata::fetch_drep_metadata;
use serde::Deserialize;

use crate::{
    error::Error,
    mapping::IntoModel as _,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

fn chain_context<D: Domain>(
    domain: &Facade<D>,
) -> Result<(ChainSummary, BlockSlot, PParamsSet), StatusCode> {
    let chain = domain.get_chain_summary()?;
    let tip = domain.get_tip_slot()?;
    let pparams = domain.get_current_effective_pparams()?;

    Ok((chain, tip, pparams))
}

/// Query parameters of `/governance/dreps`: the shared pagination set plus
/// the endpoint's own `order_by`, `retired` and `expired`. Blockfrost does
/// not define `from`/`to` here.
#[derive(Debug, Deserialize)]
pub struct DrepsListParameters {
    pub count: Option<String>,
    pub page: Option<String>,
    pub order: Option<String>,
    pub order_by: Option<String>,
    pub retired: Option<String>,
    pub expired: Option<String>,
}

impl DrepsListParameters {
    fn pagination(&self) -> PaginationParameters {
        PaginationParameters {
            count: self.count.clone(),
            page: self.page.clone(),
            order: self.order.clone(),
            from: None,
            to: None,
        }
    }

    /// `order_by` accepts only `amount`, mirroring the openapi enum.
    fn order_by_amount(&self) -> Result<bool, Error> {
        match self.order_by.as_deref() {
            None => Ok(false),
            Some("amount") => Ok(true),
            Some(_) => Err(StatusCode::BAD_REQUEST.into()),
        }
    }
}

/// Blockfrost validates these as booleans and rejects anything else.
fn parse_bool_filter(value: Option<&str>) -> Result<Option<bool>, Error> {
    match value {
        None => Ok(None),
        Some("true") => Ok(Some(true)),
        Some("false") => Ok(Some(false)),
        Some(_) => Err(StatusCode::BAD_REQUEST.into()),
    }
}

pub async fn all_dreps<D: Domain>(
    Query(params): Query<DrepsListParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<DrepsInner>>, Error>
where
    Option<DRepState>: From<D::Entity>,
{
    let order_by_amount = params.order_by_amount()?;
    let retired = parse_bool_filter(params.retired.as_deref())?;
    let expired = parse_bool_filter(params.expired.as_deref())?;

    let pagination = Pagination::try_from(params.pagination())?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let (chain, tip, pparams) = chain_context(&domain)?;

    let mut dreps = vec![];

    for item in domain.iter_cardano_entities::<DRepState>(None)? {
        let (key, state) = item.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // Blockfrost applies the filters before pagination, so every page
        // holds up to `count` matching rows.
        if retired.is_some_and(|wanted| drep_is_retired(&state) != wanted) {
            continue;
        }

        if expired.is_some_and(|wanted| drep_is_expired(&state, &chain, tip, &pparams) != wanted) {
            continue;
        }

        let appeared_at = state.first_seen_at.unwrap_or((u64::MAX, usize::MAX));

        dreps.push((appeared_at, key, state));
    }

    if order_by_amount {
        // `order` flips only the amount; the appearance order stays the
        // ascending tie-breaker, like Blockfrost's `ORDER BY amount, id ASC`.
        dreps.sort_by(|(a_order, a_key, a_state), (b_order, b_key, b_state)| {
            let amounts = match pagination.order {
                Order::Desc => b_state.voting_power.cmp(&a_state.voting_power),
                Order::Asc => a_state.voting_power.cmp(&b_state.voting_power),
            };

            amounts.then_with(|| (a_order, a_key).cmp(&(b_order, b_key)))
        });
    } else {
        dreps.sort_by(|(a_order, a_key, _), (b_order, b_key, _)| {
            (a_order, a_key).cmp(&(b_order, b_key))
        });

        if matches!(pagination.order, Order::Desc) {
            dreps.reverse();
        }
    }

    let items = dreps
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .map(|(_, _, state)| async {
            let metadata = fetch_drep_metadata(state.anchor.clone()).await;
            let mut model = drep_list_item(state, &pparams, &chain, tip)?;
            model.metadata = metadata.map(Box::new);
            Ok::<_, StatusCode>(model)
        });

    let page = join_all(items)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(page))
}

pub async fn drep_by_id<D: Domain>(
    Path(drep): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<blockfrost_openapi::models::drep::Drep>, StatusCode>
where
    Option<DRepState>: From<D::Entity>,
{
    let parsed = parse_drep_id(&drep)?;

    let drep_state = if parsed.is_special {
        domain.read_cardano_entity::<DRepState>(parsed.encoded.clone())?
    } else {
        Some(
            domain
                .read_cardano_entity::<DRepState>(parsed.encoded.clone())?
                .ok_or(StatusCode::NOT_FOUND)?,
        )
    };

    let (chain, tip, pparams) = chain_context(&domain)?;

    let model = DrepModelBuilder {
        drep_id: parsed.drep_id,
        drep_id_encoded: parsed.encoded,
        is_legacy: parsed.is_legacy,
        is_special: parsed.is_special,
        state: drep_state,
        pparams: &pparams,
        chain: &chain,
        tip,
    };

    model.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use bech32::{Bech32, Hrp};
    use blockfrost_openapi::models::drep::Drep as DrepModel;
    use dolos_cardano::pallas_extras;
    use dolos_testing::synthetic::SyntheticBlockConfig;

    fn invalid_drep() -> &'static str {
        "not-a-drep"
    }

    fn encode_id(hrp: &str, payload: &[u8]) -> String {
        let hrp = Hrp::parse_unchecked(hrp);
        bech32::encode::<Bech32>(hrp, payload).expect("failed to encode bech32 id")
    }

    fn missing_drep() -> String {
        let payload = [vec![pallas_extras::DREP_KEY_PREFIX], vec![8u8; 28]].concat();
        encode_id("drep", &payload)
    }

    fn vector_drep_hash(app: &TestApp) -> Vec<u8> {
        let (_, payload) = bech32::decode(&app.vectors().drep_id).expect("invalid vector drep id");

        payload[1..].to_vec()
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, _body) = app.get_bytes(path).await;
        assert_eq!(status, expected);
    }

    async fn get_drep(app: &TestApp, drep_id: &str) -> DrepModel {
        let path = format!("/governance/dreps/{drep_id}");
        let (status, body) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&body)
        );

        serde_json::from_slice(&body).expect("failed to parse drep model")
    }

    #[tokio::test]
    async fn governance_drep_bad_request() {
        let app = TestApp::new();
        let path = format!("/governance/dreps/{}", invalid_drep());

        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn governance_drep_not_found() {
        let app = TestApp::new();
        let missing = missing_drep();
        let path = format!("/governance/dreps/{missing}");

        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_drep_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let drep = &app.vectors().drep_id;
        let path = format!("/governance/dreps/{drep}");

        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn governance_drep_happy_path() {
        let app = TestApp::builder()
            .with_cfg(SyntheticBlockConfig {
                drep_deposit: 7777,
                ..Default::default()
            })
            .with_protocol(9)
            .build();

        let drep_id = app.vectors().drep_id.clone();
        let model = get_drep(&app, &drep_id).await;

        let (_, payload) = bech32::decode(&drep_id).expect("invalid vector drep id");

        let expected = DrepModel {
            drep_id,
            hex: hex::encode(&payload),
            // the ledger's drep_distr counts the DRep's own deposit
            amount: "7777".to_string(),
            active: true,
            active_epoch: Some(2),
            has_script: false,
            retired: false,
            expired: false,
            last_active_epoch: Some(2),
        };

        assert_eq!(model, expected);
    }

    #[tokio::test]
    async fn governance_drep_special_ids() {
        let app = TestApp::new();

        for id in ["drep_always_abstain", "drep_always_no_confidence"] {
            let model = get_drep(&app, id).await;

            let expected = DrepModel {
                drep_id: id.to_string(),
                hex: "".to_string(),
                amount: "0".to_string(),
                active: true,
                active_epoch: None,
                has_script: false,
                retired: false,
                expired: false,
                last_active_epoch: None,
            };

            assert_eq!(model, expected);
        }
    }

    #[tokio::test]
    async fn governance_drep_by_id_accepts_legacy_encodings() {
        let app = TestApp::new();
        let hash = vector_drep_hash(&app);
        let cip105 = encode_id("drep", &hash);
        let cip129 = get_drep(&app, &app.vectors().drep_id.clone()).await;

        let expected = DrepModel {
            drep_id: cip105.clone(),
            hex: hex::encode(&hash),
            ..cip129
        };

        assert_eq!(get_drep(&app, &cip105).await, expected);

        // Blockfrost rejects the drep_vkh prefix
        let path = format!("/governance/dreps/{}", encode_id("drep_vkh", &hash));
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn governance_drep_by_id_script_variant_not_found() {
        let app = TestApp::new();
        let hash = vector_drep_hash(&app);

        let path = format!("/governance/dreps/{}", encode_id("drep_script", &hash));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        let cip129_script = [vec![pallas_extras::DREP_SCRIPT_PREFIX], hash].concat();
        let path = format!("/governance/dreps/{}", encode_id("drep", &cip129_script));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    async fn get_dreps_list(app: &TestApp, path: &str) -> Vec<DrepsInner> {
        let (status, body) = app.get_bytes(path).await;
        assert_eq!(status, StatusCode::OK);

        serde_json::from_slice(&body).expect("failed to parse dreps list")
    }

    #[tokio::test]
    async fn governance_dreps_list_happy_path() {
        let app = TestApp::builder()
            .with_cfg(SyntheticBlockConfig {
                drep_deposit: 7777,
                ..Default::default()
            })
            .with_protocol(9)
            .build();

        let models = get_dreps_list(&app, "/governance/dreps").await;

        let drep_id = app.vectors().drep_id.clone();
        let (_, payload) = bech32::decode(&drep_id).expect("invalid vector drep id");

        assert_eq!(
            models,
            vec![DrepsInner {
                drep_id,
                hex: hex::encode(&payload),
                // the ledger's drep_distr counts the DRep's own deposit
                amount: "7777".to_string(),
                has_script: false,
                retired: false,
                expired: false,
                last_active_epoch: Some(2),
                metadata: None,
            }]
        );
    }

    #[tokio::test]
    async fn governance_dreps_list_pagination() {
        let app = TestApp::new();

        let models = get_dreps_list(&app, "/governance/dreps?page=2").await;
        assert!(models.is_empty());

        let models = get_dreps_list(&app, "/governance/dreps?order=desc&count=1").await;
        assert_eq!(models.len(), 1);
    }

    #[tokio::test]
    async fn governance_dreps_list_bad_request() {
        let app = TestApp::new();

        assert_status(&app, "/governance/dreps?count=0", StatusCode::BAD_REQUEST).await;
        assert_status(
            &app,
            "/governance/dreps?order=sideways",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            "/governance/dreps?order_by=alphabet",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            "/governance/dreps?retired=banana",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            "/governance/dreps?expired=banana",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn governance_dreps_list_filters_apply_before_pagination() {
        let app = TestApp::new();

        // the synthetic drep is registered and active: it survives the
        // negative filters and disappears behind the positive ones
        let models = get_dreps_list(&app, "/governance/dreps?retired=false&expired=false").await;
        assert_eq!(models.len(), 1);

        let models = get_dreps_list(&app, "/governance/dreps?retired=true").await;
        assert!(models.is_empty());

        let models = get_dreps_list(&app, "/governance/dreps?expired=true").await;
        assert!(models.is_empty());
    }

    #[tokio::test]
    async fn governance_dreps_list_order_by_amount() {
        let app = TestApp::new();

        let models = get_dreps_list(&app, "/governance/dreps?order_by=amount").await;
        assert_eq!(models.len(), 1);

        let models = get_dreps_list(&app, "/governance/dreps?order_by=amount&order=desc").await;
        assert_eq!(models.len(), 1);
    }

    #[tokio::test]
    async fn governance_dreps_list_scan_limit() {
        let app = TestApp::new();

        // page * count above `max_scan_items` (default 3000)
        assert_status(
            &app,
            "/governance/dreps?page=1000&count=100",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn governance_dreps_list_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));

        assert_status(&app, "/governance/dreps", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
