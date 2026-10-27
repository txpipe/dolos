use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    time::Duration,
};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{DrepsInner, DrepsInnerMetadata};
use dolos_cardano::{
    model::{CertPosition, DRepKey, DRepSightingState, DRepState},
    ChainSummary, PParamsSet,
};
use dolos_core::{BlockSlot, Domain};
use futures::{stream, FutureExt as _, StreamExt as _};
use pallas::ledger::primitives::conway::{Anchor, DRep};
use serde::Deserialize;

use crate::{
    error::Error,
    mapping::governance::dreps::{drep_amount, drep_is_expired, drep_is_retired, drep_list_item},
    offchain::{cached_drep_metadata, fetch_drep_metadata, unfetched, OnOverload},
    pagination::{Order, Pagination, PaginationParameters},
    scan::{EntityScan, ENTITY_CHUNK},
    Facade,
};

use super::{chain_context, drep_distribution, drep_final_rows, read_drep_row};

/// How many anchors one page loads at a time. A page asks for up to 100 rows
/// and every miss costs a round trip capped at the fetch timeout, so the bound
/// keeps a slow page from queueing all of them at once while a cached row
/// still costs nothing. Outbound requests have their own process-wide bound
/// (`offchain::MAX_CONCURRENT_FETCHES`), shared by every page.
const MAX_CONCURRENT_METADATA_FETCHES: usize = 8;

/// How long a page waits for the anchors it misses. A cold page of 100 rows
/// behind dead hosts would otherwise wait out one fetch timeout per wave of
/// [`MAX_CONCURRENT_METADATA_FETCHES`]. A load the page started before the
/// deadline keeps going in the background and fills the caches for the next
/// request; one it had not started yet is dropped. Either way its row goes out
/// unfetched, the way Blockfrost serves a DRep whose metadata db-sync has not
/// fetched yet.
const PAGE_METADATA_DEADLINE: Duration = Duration::from_secs(3);

/// Runs `loads`, at most `concurrency` at a time, and returns, in order, what
/// each one settled to within `deadline`: `None` for a load still running or
/// never started. Every load is dropped at the deadline, so one that should
/// outlive it has to detach its own work, the way `fetch_drep_metadata` does.
async fn settle_within<F: Future>(
    loads: Vec<F>,
    concurrency: usize,
    deadline: Duration,
) -> Vec<Option<F::Output>> {
    let mut settled: Vec<_> = loads.iter().map(|_| None).collect();

    let mut running = stream::iter(loads.into_iter().enumerate())
        .map(|(index, load)| load.map(move |value| (index, value)))
        .buffer_unordered(concurrency);

    let _ = tokio::time::timeout(deadline, async {
        while let Some((index, value)) = running.next().await {
            settled[index] = Some(value);
        }
    })
    .await;

    settled
}

/// The metadata of each anchor of a page, in page order. What `cached` holds
/// is answered up front, outside the fetch slots: only the misses go to
/// `load`, through [`settle_within`], so slow hosts early in the page never
/// keep a later row that is already at hand from going out.
async fn page_metadata<'a, C, L>(
    anchors: &'a [Anchor],
    cached: impl Fn(&'a Anchor) -> C,
    load: impl Fn(&'a Anchor) -> L,
    deadline: Duration,
) -> Vec<DrepsInnerMetadata>
where
    C: Future<Output = Option<DrepsInnerMetadata>>,
    L: Future<Output = DrepsInnerMetadata>,
{
    let cached = futures::future::join_all(anchors.iter().map(cached)).await;

    let loads = anchors
        .iter()
        .zip(&cached)
        .filter(|(_, cached)| cached.is_none())
        .map(|(anchor, _)| load(anchor))
        .collect();

    let mut settled = settle_within(loads, MAX_CONCURRENT_METADATA_FETCHES, deadline)
        .await
        .into_iter();

    anchors
        .iter()
        .zip(cached)
        .map(|(anchor, cached)| {
            // `settled` holds the misses alone, in page order
            cached
                .or_else(|| settled.next().flatten())
                .unwrap_or_else(|| unfetched(anchor))
        })
        .collect()
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

/// One listed DRep: its sighting, and its ledger row where the ledger has
/// one, in state or as its final row in the log. A DRep only a vote
/// delegation named has none.
struct DrepRow {
    key: DRepKey,
    sighting: DRepSightingState,
    state: Option<DRepState>,
}

impl DrepRow {
    /// The listing's default order, db-sync's `drep_hash` order: first
    /// sighting, down to the certificate, the key breaking ties no chain
    /// produces.
    fn appearance(&self) -> (CertPosition, &DRepKey) {
        (self.sighting.first_seen, &self.key)
    }

    fn amount(&self, distribution: &BTreeMap<DRep, u64>) -> u64 {
        drep_amount(&self.key.0, self.state.as_ref(), distribution)
    }
}

/// One page of `/governance/dreps`, with the chain context its rows render
/// against.
struct DrepsPage {
    rows: Vec<DrepRow>,
    chain: ChainSummary,
    tip: BlockSlot,
    pparams: PParamsSet,
    distribution: BTreeMap<DRep, u64>,
}

/// Every DRep a certificate has named, in key order.
async fn load_drep_sightings<D>(
    domain: &Facade<D>,
    chunk: usize,
) -> Result<Vec<(DRepKey, DRepSightingState)>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    EntityScan::<DRepSightingState>::all(chunk)
        .filter_map(domain, |key, sighting| Some((key, sighting)))
        .await
}

/// The ledger row of every sighted DRep, read by one scan of `dreps` and one
/// of its log rather than a point read per DRep: the filters and
/// `order_by=amount` need every row before they can cut a page. The state
/// row wins over a final row.
async fn join_drep_states<D>(
    domain: &Facade<D>,
    sightings: Vec<(DRepKey, DRepSightingState)>,
    chunk: usize,
) -> Result<Vec<DrepRow>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let mut states: HashMap<DRepKey, DRepState> = EntityScan::<DRepState>::all(chunk)
        .filter_map(domain, |key, state| Some((key, state)))
        .await?
        .into_iter()
        .collect();

    let mut finals = domain
        .blocking(|facade| drep_final_rows(&facade.inner))
        .await?;

    let rows = sightings
        .into_iter()
        .map(|(key, sighting)| {
            let state = states.remove(&key).or_else(|| finals.remove(&key));

            DrepRow {
                key,
                sighting,
                state,
            }
        })
        .collect();

    Ok(rows)
}

/// The ledger rows of one page's DReps, by point reads in one blocking task.
async fn read_drep_states<D>(
    domain: &Facade<D>,
    sightings: Vec<(DRepKey, DRepSightingState)>,
) -> Result<Vec<DrepRow>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    domain
        .blocking(move |facade| {
            sightings
                .into_iter()
                .map(|(key, sighting)| {
                    let state = read_drep_row(&facade.inner, &key)?;

                    Ok(DrepRow {
                        key,
                        sighting,
                        state,
                    })
                })
                .collect()
        })
        .await
}

/// The listing sorts every sighted DRep before it cuts the page. Without a
/// filter or `order_by=amount`, only the page's ledger rows are read.
///
/// Blockfrost applies the filters before pagination, so every page holds up
/// to `count` matching rows.
async fn drep_page<D>(
    domain: &Facade<D>,
    retired: Option<bool>,
    expired: Option<bool>,
    order_by_amount: bool,
    pagination: &Pagination,
) -> Result<DrepsPage, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let (chain, tip, pparams) = chain_context(domain)?;
    let distribution = drep_distribution(domain)?;

    let mut sightings = load_drep_sightings(domain, ENTITY_CHUNK).await?;

    if retired.is_none() && expired.is_none() && !order_by_amount {
        sightings
            .sort_by(|(a_key, a), (b_key, b)| (a.first_seen, a_key).cmp(&(b.first_seen, b_key)));

        if matches!(pagination.order, Order::Desc) {
            sightings.reverse();
        }

        let page = sightings
            .into_iter()
            .skip(pagination.from())
            .take(pagination.count)
            .collect();

        let rows = read_drep_states(domain, page).await?;

        return Ok(DrepsPage {
            rows,
            chain,
            tip,
            pparams,
            distribution,
        });
    }

    let mut rows = join_drep_states(domain, sightings, ENTITY_CHUNK).await?;

    // a DRep with no ledger row is listed, neither retired nor expired
    rows.retain(|row| {
        let is_retired = row.state.as_ref().is_some_and(drep_is_retired);

        let is_expired = row
            .state
            .as_ref()
            .is_some_and(|state| drep_is_expired(state, &chain, tip, &pparams));

        retired.is_none_or(|wanted| is_retired == wanted)
            && expired.is_none_or(|wanted| is_expired == wanted)
    });

    if order_by_amount {
        // `order` flips only the amount; the appearance order stays the
        // ascending tie-breaker, like Blockfrost's `ORDER BY amount, id ASC`.
        rows.sort_by(|a, b| {
            let (a_amount, b_amount) = (a.amount(&distribution), b.amount(&distribution));

            let amounts = match pagination.order {
                Order::Desc => b_amount.cmp(&a_amount),
                Order::Asc => a_amount.cmp(&b_amount),
            };

            amounts.then_with(|| a.appearance().cmp(&b.appearance()))
        });
    } else {
        rows.sort_by(|a, b| a.appearance().cmp(&b.appearance()));

        if matches!(pagination.order, Order::Desc) {
            rows.reverse();
        }
    }

    let rows = rows
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .collect();

    Ok(DrepsPage {
        rows,
        chain,
        tip,
        pparams,
        distribution,
    })
}

pub async fn all_dreps<D>(
    Query(params): Query<DrepsListParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<DrepsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let order_by_amount = params.order_by_amount()?;
    let retired = parse_bool_filter(params.retired.as_deref())?;
    let expired = parse_bool_filter(params.expired.as_deref())?;

    let pagination = Pagination::try_from(params.pagination())?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let DrepsPage {
        rows,
        chain,
        tip,
        pparams,
        distribution,
    } = drep_page(&domain, retired, expired, order_by_amount, &pagination).await?;

    let cache = domain.inner.cache();
    let gateways = domain.config.ipfs_gateways();

    // the latest anchor a registration or update carried, as db-sync serves
    // it; the ledger's own anchor is cleared by an update that carries none
    let anchors: Vec<_> = rows
        .iter()
        .flat_map(|row| row.sighting.anchor.clone())
        .collect();

    // a page shows what it has rather than wait on a backlog of loads
    let on_overload = OnOverload::Unfetched;

    let metadata = page_metadata(
        &anchors,
        |anchor| cached_drep_metadata(cache, anchor),
        |anchor| fetch_drep_metadata(cache, &gateways, anchor, on_overload),
        PAGE_METADATA_DEADLINE,
    )
    .await;

    let mut metadata = metadata.into_iter();

    let page = rows
        .into_iter()
        .map(|row| {
            let has_anchor = row.sighting.anchor.is_some();
            let amount = row.amount(&distribution);
            let mut model =
                drep_list_item(row.key.into(), row.state, amount, &pparams, &chain, tip)?;

            // `anchors` keeps the anchored rows in page order
            model.metadata = has_anchor.then(|| metadata.next()).flatten().map(Box::new);

            Ok(model)
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(page))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mapping::{bech32_drep, governance::dreps::parse_drep_id},
        routes::governance::testing::*,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::drep_metadata::DrepMetadata;
    use dolos_cardano::model::DRepExpiry;
    use dolos_core::{StateStore as _, StateWriter as _};
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use pallas::{
        crypto::hash::{Hash, Hasher},
        ledger::primitives::{
            conway::{Anchor, Certificate, DRep},
            StakeCredential,
        },
    };

    /// A page answers by its deadline: a slow load leaves its row unsettled,
    /// and the deadline drops it along with every load not started yet, so a
    /// page leaves nothing of its own running once it has answered.
    #[tokio::test]
    async fn settle_within_answers_by_the_deadline_and_drops_the_rest() {
        let (_release, released) = tokio::sync::oneshot::channel::<()>();
        let (dropped, was_dropped) = tokio::sync::oneshot::channel::<()>();

        let slow = async move {
            // dropped with the load, which closes the channel
            let _dropped = dropped;
            let _ = released.await;
            2
        }
        .boxed();

        let started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let queued = {
            let started = started.clone();

            async move {
                started.store(true, std::sync::atomic::Ordering::SeqCst);
                3
            }
            .boxed()
        };

        // one at a time: the slow load holds the only slot past the deadline
        let loads = vec![async { 1 }.boxed(), slow, queued];

        let settled = settle_within(loads, 1, Duration::from_millis(100)).await;

        assert_eq!(settled, vec![Some(1), None, None]);

        // the deadline dropped the slow load rather than leaving it running,
        // and the queued one never started
        assert!(was_dropped.await.is_err());
        assert!(!started.load(std::sync::atomic::Ordering::SeqCst));
    }

    /// A row already at hand goes out with its metadata however many slow
    /// misses come before it on the page: only the misses wait for a fetch
    /// slot, and only they can run out the deadline.
    #[tokio::test]
    async fn page_metadata_answers_held_rows_past_slow_misses() {
        let anchor = |name: &str| Anchor {
            url: format!("https://example.invalid/{name}"),
            content_hash: Hasher::<256>::hash(name.as_bytes()),
        };

        // every fetch slot taken by a miss that outlasts the deadline, then
        // a row the caches hold
        let mut anchors: Vec<_> = (0..MAX_CONCURRENT_METADATA_FETCHES)
            .map(|index| anchor(&format!("slow-{index}")))
            .collect();
        anchors.push(anchor("held"));

        let held = |anchor: &Anchor| {
            let mut out = unfetched(anchor);
            out.bytes = Some("\\x00".to_string());
            out
        };

        let metadata = page_metadata(
            &anchors,
            |anchor| {
                let hit = anchor.url.ends_with("held").then(|| held(anchor));
                async move { hit }
            },
            |_| futures::future::pending(),
            Duration::from_millis(100),
        )
        .await;

        let fetched: Vec<_> = metadata.iter().map(|x| x.bytes.is_some()).collect();
        let mut expected = vec![false; MAX_CONCURRENT_METADATA_FETCHES];
        expected.push(true);

        assert_eq!(fetched, expected);
        assert_eq!(metadata.last(), Some(&held(&anchors[anchors.len() - 1])));
    }

    /// The list and `/governance/dreps/{drep_id}/metadata` fetch through the
    /// same client, so one anchor reads the same in both.
    #[tokio::test]
    async fn governance_dreps_list_metadata_matches_the_drep_metadata_endpoint() {
        let app =
            TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, vectors| {
                let parsed = parse_drep_id(&vectors.drep_id).expect("failed to parse drep id");
                seed_drep_anchor(domain, parsed.encoded);
            });

        let drep = app.vectors().drep_id.clone();

        let listed = get_dreps_list(&app, "/governance/dreps?count=100")
            .await
            .into_iter()
            .find(|x| x.drep_id == drep)
            .expect("the drep is not listed")
            .metadata
            .expect("the listed drep has no metadata");

        let path = format!("/governance/dreps/{drep}/metadata");
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let single: DrepMetadata =
            serde_json::from_slice(&body).expect("failed to parse drep metadata");

        assert_eq!(listed.url, single.url);
        assert_eq!(listed.hash, single.hash);
        assert_eq!(listed.error, single.error);
        assert!(listed.error.is_some());
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
                // registered after the last completed distribution, so in
                // none yet: Blockfrost serves 0, not the deposit
                amount: "0".to_string(),
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

    /// Writes a DRep's sighting, as the roll would have.
    fn write_sighting(
        writer: &impl dolos_core::StateWriter,
        identifier: &DRep,
        first_seen: CertPosition,
        anchor: Option<Anchor>,
    ) {
        writer
            .write_entity_typed(
                &DRepKey::from(identifier),
                &DRepSightingState { first_seen, anchor },
            )
            .expect("failed to write drep sighting");
    }

    /// Three DReps whose voting powers and first sightings disagree, so
    /// ordering by amount cannot be mistaken for the default ordering and
    /// each direction names a different row first.
    fn amount_app() -> TestApp {
        TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _| {
            seed_amounts(domain)
        })
    }

    fn seed_amounts(domain: &ToyDomain) {
        let mut distribution = BTreeMap::new();

        let writer = domain
            .state()
            .start_writer()
            .expect("failed to start writer");

        // seen in this order, so appearance order is a, b, c while the
        // amounts run the other way
        for (byte, power, seen) in [(0xa1u8, 300u64, 10u64), (0xb2, 100, 20), (0xc3, 200, 30)] {
            let identifier = DRep::Key([byte; 28].into());

            let mut state = DRepState::new(identifier.clone(), DRepExpiry::new(u64::MAX, 0));
            state.registered_at = Some((seen, 0));

            writer
                .write_entity_typed(&DRepKey::from(&identifier), &state)
                .expect("failed to write drep");

            write_sighting(&writer, &identifier, (seen, 0, 0), None);

            distribution.insert(identifier, power);
        }

        writer.commit().expect("failed to commit dreps");

        seed_distribution(domain, distribution);
    }

    /// The seeded DReps in the order they first appeared, which is the
    /// listing's default order.
    fn seeded_ids() -> Vec<String> {
        [0xa1u8, 0xb2, 0xc3]
            .iter()
            .map(|byte| bech32_drep(&DRep::Key([*byte; 28].into())).expect("failed to encode"))
            .collect()
    }

    fn listed_ids(models: &[DrepsInner], seeded: &[String]) -> Vec<String> {
        models
            .iter()
            .map(|x| x.drep_id.clone())
            .filter(|id| seeded.contains(id))
            .collect()
    }

    #[tokio::test]
    async fn governance_dreps_list_order_by_amount() {
        let app = amount_app();
        let seeded = seeded_ids();
        let (a, b, c) = (seeded[0].clone(), seeded[1].clone(), seeded[2].clone());

        // default: the order they were first seen, amounts ignored
        let models = get_dreps_list(&app, "/governance/dreps?count=100").await;
        assert_eq!(
            listed_ids(&models, &seeded),
            vec![a.clone(), b.clone(), c.clone()]
        );

        // by amount ascending: 100, 200, 300
        let models = get_dreps_list(&app, "/governance/dreps?count=100&order_by=amount").await;
        assert_eq!(
            listed_ids(&models, &seeded),
            vec![b.clone(), c.clone(), a.clone()]
        );

        // and descending is that read backwards
        let models = get_dreps_list(
            &app,
            "/governance/dreps?count=100&order_by=amount&order=desc",
        )
        .await;
        assert_eq!(listed_ids(&models, &seeded), vec![a, c, b]);
    }

    /// `amount` and `order_by=amount` follow the last completed distribution
    /// while the next boundary's shards build one that orders them otherwise.
    #[tokio::test]
    async fn governance_dreps_list_amount_ignores_the_partial_distribution() {
        let app = TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _| {
            seed_amounts(domain);

            let partial = [(0xa1u8, 100u64), (0xb2, 300), (0xc3, 200)]
                .map(|(byte, power)| (DRep::Key([byte; 28].into()), power));
            seed_partial_distribution(domain, partial.into());
        });

        let seeded = seeded_ids();
        let models = get_dreps_list(&app, "/governance/dreps?count=100&order_by=amount").await;

        assert_eq!(
            listed_ids(&models, &seeded),
            vec![seeded[1].clone(), seeded[2].clone(), seeded[0].clone()]
        );

        let amounts: Vec<_> = models
            .iter()
            .filter(|x| seeded.contains(&x.drep_id))
            .map(|x| x.amount.as_str())
            .collect();
        assert_eq!(amounts, ["100", "200", "300"]);
    }

    /// DReps the synthetic chain's first tx names after its own DRep, whose
    /// keys sort in neither their appearance order nor its reverse.
    mod sighted {
        use super::*;

        /// Delegated to in certificate 5, registered in certificate 8.
        pub const DELEGATED_FIRST: [u8; 28] = [0xf1; 28];
        /// Registered in certificate 6, delegated to in certificate 7.
        pub const REGISTERED_FIRST: [u8; 28] = [0xe2; 28];
        /// Delegated to in certificate 9, never registered.
        pub const NEVER_REGISTERED: [u8; 28] = [0xd3; 28];

        pub fn drep(hash: [u8; 28]) -> DRep {
            DRep::Key(hash.into())
        }

        pub fn id(drep: &DRep) -> String {
            bech32_drep(drep).expect("failed to encode")
        }

        fn delegation(target: DRep) -> Certificate {
            Certificate::VoteDeleg(
                crate::test_support::stake_address_accounts::primary(),
                target,
            )
        }

        fn registration(hash: [u8; 28], anchor: Option<Anchor>) -> Certificate {
            Certificate::RegDRepCert(StakeCredential::AddrKeyhash(hash.into()), 1000, anchor)
        }

        pub fn anchor(byte: u8) -> Anchor {
            Anchor {
                url: format!("https://example.invalid/drep/{byte}"),
                content_hash: Hash::from([byte; 32]),
            }
        }

        /// Two blocks. The first tx of block 0 names, after the synthetic
        /// DRep's certificates 3 and 4, the DReps above and then the two
        /// special DReps. The first tx of block 1 updates `REGISTERED_FIRST`
        /// without an anchor.
        pub fn config() -> SyntheticBlockConfig {
            SyntheticBlockConfig {
                block_count: 2,
                extra_certs_by_block: vec![
                    vec![vec![
                        delegation(drep(DELEGATED_FIRST)),
                        registration(REGISTERED_FIRST, Some(anchor(0x51))),
                        delegation(drep(REGISTERED_FIRST)),
                        registration(DELEGATED_FIRST, None),
                        delegation(drep(NEVER_REGISTERED)),
                        delegation(DRep::Abstain),
                        delegation(DRep::NoConfidence),
                    ]],
                    vec![vec![Certificate::UpdateDRepCert(
                        StakeCredential::AddrKeyhash(REGISTERED_FIRST.into()),
                        None,
                    )]],
                ],
                ..Default::default()
            }
        }

        /// Every DRep the chain names, in the order its certificates first
        /// name them.
        pub fn appearance(app: &TestApp) -> Vec<String> {
            vec![
                app.vectors().drep_id.clone(),
                id(&drep(DELEGATED_FIRST)),
                id(&drep(REGISTERED_FIRST)),
                id(&drep(NEVER_REGISTERED)),
                id(&DRep::Abstain),
                id(&DRep::NoConfidence),
            ]
        }
    }

    /// The list follows the chain's certificate order, in both directions,
    /// whatever order the keys sort in: a DRep a tx delegates to before it
    /// registers sorts by the delegation, one it registers first by the
    /// registration.
    #[tokio::test]
    async fn governance_dreps_list_follows_certificate_order() {
        let app = TestApp::new_with_cfg(sighted::config());
        let expected = sighted::appearance(&app);

        let ids = |models: Vec<DrepsInner>| -> Vec<String> {
            models.into_iter().map(|x| x.drep_id).collect()
        };

        let models = get_dreps_list(&app, "/governance/dreps?count=100").await;
        assert_eq!(ids(models), expected);

        let models = get_dreps_list(&app, "/governance/dreps?count=100&order=desc").await;
        let mut reversed = expected.clone();
        reversed.reverse();
        assert_eq!(ids(models), reversed);

        // pages cut the same order
        let mut paged = vec![];

        for page in 1..=3 {
            let path = format!("/governance/dreps?count=2&page={page}");
            paged.extend(ids(get_dreps_list(&app, &path).await));
        }

        assert_eq!(paged, expected);
    }

    /// A DRep only a vote delegation names has no ledger row. It is listed
    /// all the same, neither retired nor expired, with no amount, no
    /// activity and no metadata, and the filters agree.
    #[tokio::test]
    async fn governance_dreps_list_includes_dreps_only_a_delegation_names() {
        let app = TestApp::new_with_cfg(sighted::config());
        let drep = sighted::drep(sighted::NEVER_REGISTERED);
        let drep_id = sighted::id(&drep);

        let ledger_row = app
            .facade()
            .read_cardano_entity::<DRepState>(&DRepKey::from(&drep))
            .expect("drep read");
        assert!(ledger_row.is_none(), "the ledger holds no row for it");

        let models = get_dreps_list(&app, "/governance/dreps?count=100").await;
        let listed = models
            .into_iter()
            .find(|x| x.drep_id == drep_id)
            .expect("the drep is not listed");

        assert_eq!(
            listed,
            DrepsInner {
                drep_id: drep_id.clone(),
                hex: hex::encode(dolos_core::keys::encode(&DRepKey::from(&drep))),
                amount: "0".to_string(),
                has_script: false,
                retired: false,
                expired: false,
                last_active_epoch: None,
                metadata: None,
            }
        );

        for (path, kept) in [
            ("/governance/dreps?count=100&retired=false", true),
            ("/governance/dreps?count=100&expired=false", true),
            ("/governance/dreps?count=100&retired=true", false),
            ("/governance/dreps?count=100&expired=true", false),
        ] {
            let models = get_dreps_list(&app, path).await;
            assert_eq!(models.iter().any(|x| x.drep_id == drep_id), kept, "{path}");
        }
    }

    /// The special DReps are listed where a delegation first names them,
    /// with an empty `hex` and their distribution entries, as Blockfrost lists
    /// them.
    #[tokio::test]
    async fn governance_dreps_list_includes_the_special_dreps() {
        let app = TestApp::new_with_cfg_and_setup(sighted::config(), |domain, _| {
            seed_distribution(
                domain,
                [(DRep::Abstain, 40), (DRep::NoConfidence, 50)].into(),
            );
        });

        let models = get_dreps_list(&app, "/governance/dreps?count=100").await;

        for (drep, amount) in [(DRep::Abstain, 40), (DRep::NoConfidence, 50)] {
            let drep_id = sighted::id(&drep);

            let listed = models
                .iter()
                .find(|x| x.drep_id == drep_id)
                .unwrap_or_else(|| panic!("{drep_id} is not listed"));

            assert_eq!(listed.hex, "");
            assert_eq!(listed.amount, amount.to_string());
            assert!(!listed.has_script);
            assert!(!listed.retired);
            assert!(!listed.expired);
            assert_eq!(listed.last_active_epoch, None);
            assert_eq!(listed.metadata, None);
        }
    }

    /// An update without an anchor leaves the list on the one the
    /// registration carried, as Blockfrost does.
    #[tokio::test]
    async fn governance_dreps_list_keeps_the_anchor_an_update_omits() {
        let app = TestApp::new_with_cfg(sighted::config());
        let drep = sighted::drep(sighted::REGISTERED_FIRST);
        let registered = sighted::anchor(0x51);

        let models = get_dreps_list(&app, "/governance/dreps?count=100").await;
        let metadata = models
            .into_iter()
            .find(|x| x.drep_id == sighted::id(&drep))
            .expect("the drep is not listed")
            .metadata
            .expect("the drep has no metadata");

        assert_eq!(metadata.url, registered.url);
        assert_eq!(metadata.hash, hex::encode(registered.content_hash));
    }

    /// A DRep that never registered and never voted — a vote-delegation
    /// target the chain only ever mentioned — has no last-active epoch, and
    /// Blockfrost's SQL sends that row to the `ELSE FALSE` arm however old
    /// it is. Verified against live Blockfrost on preview, where 13 such
    /// DReps sit in the first 3000 rows.
    #[tokio::test]
    async fn governance_dreps_list_never_active_dreps_never_expire() {
        let identifier = DRep::Key([0xd4u8; 28].into());

        let app = {
            let identifier = identifier.clone();

            TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), move |domain, _| {
                let writer = domain
                    .state()
                    .start_writer()
                    .expect("failed to start writer");

                // seen at the very first slot and silent ever since, with the
                // ledger expiry the boundary would have long since tripped
                let mut state = DRepState::new(identifier.clone(), DRepExpiry::new(0, 0));
                state.expired = true;

                writer
                    .write_entity_typed(&DRepKey::from(&identifier), &state)
                    .expect("failed to write drep");

                write_sighting(&writer, &identifier, (0, 0, 0), None);

                writer.commit().expect("failed to commit drep");
            })
        };

        let drep_id = bech32_drep(&identifier).expect("failed to encode");
        let model = get_drep(&app, &drep_id).await;

        assert_eq!(model.last_active_epoch, None);
        assert!(!model.retired);
        assert!(!model.expired, "Blockfrost reports these as not expired");

        // and the filter agrees with the field
        let listed = get_dreps_list(&app, "/governance/dreps?count=100&expired=true").await;
        assert!(listed.iter().all(|x| x.drep_id != drep_id));

        let listed = get_dreps_list(&app, "/governance/dreps?count=100&expired=false").await;
        assert!(listed.iter().any(|x| x.drep_id == drep_id));
    }

    /// Ordering by amount has to hold across a page boundary, not just
    /// inside one page, which is what the Blockfrost suite checks too.
    #[tokio::test]
    async fn governance_dreps_list_order_by_amount_spans_pages() {
        let app = amount_app();
        let seeded = seeded_ids();

        let mut paged = vec![];

        for page in 1..=4 {
            let path = format!("/governance/dreps?count=1&order_by=amount&page={page}");
            paged.extend(listed_ids(&get_dreps_list(&app, &path).await, &seeded));
        }

        let whole = listed_ids(
            &get_dreps_list(&app, "/governance/dreps?count=100&order_by=amount").await,
            &seeded,
        );

        assert_eq!(paged, whole);
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

    /// Writes `copies` copies of the synthetic DRep and its sighting under
    /// new keys, so that a scan of either reads several chunks.
    fn replicate_dreps(domain: &ToyDomain, copies: u8) {
        use dolos_core::StateWriter as _;

        let key = DRepKey::from(&synthetic_drep());

        let state = domain
            .state()
            .read_entity_typed::<DRepState>(&key)
            .expect("drep read")
            .expect("drep state");

        let sighting = domain
            .state()
            .read_entity_typed::<DRepSightingState>(&key)
            .expect("sighting read")
            .expect("drep sighting");

        let writer = domain.state().start_writer().expect("state writer");

        for copy in 1..=copies {
            let drep = DRep::Key(Hash::from([copy; 28]));
            let mut state = state.clone();
            state.identifier = drep.clone();

            let mut sighting = sighting.clone();
            sighting.first_seen.2 += usize::from(copy);

            writer
                .write_entity_typed(&DRepKey(drep.clone()), &state)
                .expect("drep write");
            writer
                .write_entity_typed(&DRepKey(drep), &sighting)
                .expect("sighting write");
        }

        writer.commit().expect("state commit");
    }

    #[tokio::test]
    async fn chunked_drep_rows_match_unchunked() {
        let app = TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _| {
            replicate_dreps(domain, 4)
        });
        let facade = app.facade();

        let rows = |chunk| {
            let facade = facade.clone();
            async move {
                let sightings = load_drep_sightings(&facade, chunk)
                    .await
                    .expect("drep sightings");

                join_drep_states(&facade, sightings, chunk)
                    .await
                    .expect("drep rows")
                    .into_iter()
                    .map(|row| {
                        let registered = row.state.map(|state| state.registered_at);
                        (row.sighting.first_seen, row.key, registered)
                    })
                    .collect::<Vec<_>>()
            }
        };

        let whole = rows(usize::MAX).await;
        assert!(whole.len() > 2, "too few dreps to split into chunks");

        for chunk in [1, 2] {
            assert_eq!(rows(chunk).await, whole);
        }
    }
}
