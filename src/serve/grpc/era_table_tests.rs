//! `ReadEraSummary` in both schemas, `GetInterpreter` and `/network/eras` agree
//! on the era table.

use std::sync::Arc;

use axum::{body::Body, http::Request as HttpRequest};
use dolos_cardano::EraState;
use dolos_core::config::MinibfConfig;
use dolos_core::{
    Domain, EntityKey, FixedNamespace as _, Genesis, StateStore as _, StateWriter as _,
};
use dolos_testing::{era_fixtures::PublicNetwork, toy_domain::ToyDomain};
use pallas::codec::minicbor;
use tonic::Request;
use tower::ServiceExt as _;

use crate::serve::o7s_unix::utils::load_era_history_response;

/// One row: start and end as (slot, epoch), the end `None` on an open era.
type Row = ((u64, u64), Option<(u64, u64)>);

/// A row by name, with its params flag, as `ReadEraSummary` answers it.
type Named = (String, Row, bool);

macro_rules! read_era_summary {
    ($schema:ident, $domain:expr) => {{
        use pallas::interop::utxorpc::$schema::spec as u5c;
        use u5c::query::query_service_server::QueryService;

        let service = super::$schema::query::QueryServiceImpl::new($domain.clone());
        let request = Request::new(u5c::query::ReadEraSummaryRequest { field_mask: None });

        QueryService::read_era_summary(&service, request)
            .await
            .map(|response| match response.into_inner().summary {
                Some(u5c::query::read_era_summary_response::Summary::Cardano(cardano)) => cardano
                    .summaries
                    .into_iter()
                    .map(|era| {
                        let start = era.start.unwrap();
                        let row = (
                            (start.slot, start.epoch),
                            era.end.map(|end| (end.slot, end.epoch)),
                        );

                        (era.name, row, era.protocol_params.is_some())
                    })
                    .collect::<Vec<Named>>(),
                _ => panic!("missing cardano era summaries"),
            })
    }};
}

async fn network_eras(domain: &ToyDomain) -> Vec<Row> {
    let config = MinibfConfig::new("[::]:0".parse().unwrap());
    let response = dolos_minibf::build_router(config, domain.clone())
        .oneshot(
            HttpRequest::get("/network/eras")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status().is_success());

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let eras: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let pair = |bound: &serde_json::Value| {
        (
            bound["slot"].as_u64().unwrap(),
            bound["epoch"].as_u64().unwrap(),
        )
    };

    eras.as_array()
        .unwrap()
        .iter()
        .map(|era| (pair(&era["start"]), Some(pair(&era["end"]))))
        .collect()
}

fn interpreter_rows(domain: &ToyDomain) -> Vec<Row> {
    let answer = load_era_history_response(domain).unwrap();
    let mut decoder = minicbor::Decoder::new(answer.raw_bytes());
    let count = decoder.array().unwrap().unwrap();

    (0..count)
        .map(|_| {
            decoder.array().unwrap();

            decoder.array().unwrap();
            read_picos(&mut decoder);
            let start = (decoder.u64().unwrap(), decoder.u64().unwrap());

            let end = match decoder.datatype().unwrap() {
                minicbor::data::Type::Null => {
                    decoder.null().unwrap();
                    None
                }
                _ => {
                    decoder.array().unwrap();
                    read_picos(&mut decoder);
                    Some((decoder.u64().unwrap(), decoder.u64().unwrap()))
                }
            };

            decoder.skip().unwrap();

            (start, end)
        })
        .collect()
}

/// Reads a relative time, as a plain integer or a bignum.
fn read_picos(decoder: &mut minicbor::Decoder) -> u128 {
    match decoder.datatype().unwrap() {
        minicbor::data::Type::Tag => {
            decoder.tag().unwrap();
            decoder
                .bytes()
                .unwrap()
                .iter()
                .fold(0, |value, byte| (value << 8) | u128::from(*byte))
        }
        _ => u128::from(decoder.u64().unwrap()),
    }
}

fn with_network(network: &PublicNetwork) -> ToyDomain {
    let domain = ToyDomain::new_with_genesis(network.genesis.clone(), None, None);
    network.seed(&domain);
    domain
}

/// A domain whose state is at protocol 10, as the live public networks are.
fn at_protocol_ten(network: &PublicNetwork) -> ToyDomain {
    let mut genesis = (*network.genesis).clone();
    genesis.force_protocol = Some(10);

    let domain = ToyDomain::new_with_genesis(Arc::new(genesis), None, None);
    network.seed(&domain);
    domain
}

fn without_open_end(rows: &[Row]) -> Vec<Row> {
    let mut rows = rows.to_vec();
    rows.last_mut().unwrap().1 = None;
    rows
}

const ERA_NAMES: [&str; 7] = [
    "byron", "shelley", "allegra", "mary", "alonzo", "babbage", "conway",
];

#[tokio::test]
async fn all_interfaces_serve_the_same_era_table_on_public_networks() {
    for network in PublicNetwork::all() {
        let domain = with_network(&network);

        let minibf = network_eras(&domain).await;
        let alpha = read_era_summary!(v1alpha, domain).unwrap();
        let beta = read_era_summary!(v1beta, domain).unwrap();
        let interpreter = interpreter_rows(&domain);

        assert_eq!(alpha, beta);
        assert_eq!(
            alpha.iter().map(|era| era.0.as_str()).collect::<Vec<_>>(),
            ERA_NAMES
        );

        let grpc: Vec<Row> = alpha.iter().map(|era| era.1).collect();
        assert_eq!(grpc, without_open_end(&minibf));
        assert_eq!(interpreter, grpc);

        for pair in grpc.windows(2) {
            assert_eq!(pair[0].1, Some(pair[1].0), "contiguous boundaries");
        }
    }
}

#[test]
fn the_interpreter_writes_times_past_u64_picoseconds_as_bignums() {
    let domain = with_network(&PublicNetwork::mainnet());
    let answer = load_era_history_response(&domain).unwrap();
    let mut decoder = minicbor::Decoder::new(answer.raw_bytes());

    decoder.array().unwrap();
    decoder.array().unwrap();

    decoder.array().unwrap();
    assert_eq!(read_picos(&mut decoder), 0);
    decoder.u64().unwrap();
    decoder.u64().unwrap();

    decoder.array().unwrap();
    assert_eq!(decoder.datatype().unwrap(), minicbor::data::Type::Tag);
    assert_eq!(read_picos(&mut decoder), 89_856_000_000_000_000_000);
}

#[tokio::test]
async fn only_the_active_era_carries_protocol_params_inside_a_bump() {
    for network in PublicNetwork::all() {
        let domain = at_protocol_ten(&network);

        let beta = read_era_summary!(v1beta, domain).unwrap();
        let alpha = read_era_summary!(v1alpha, domain).unwrap();

        for summaries in [alpha, beta] {
            let with_params: Vec<_> = summaries
                .iter()
                .filter(|era| era.2)
                .map(|era| era.0.as_str())
                .collect();

            assert_eq!(with_params, ["conway"]);
        }
    }
}

fn custom_genesis() -> Arc<Genesis> {
    let mut genesis = dolos_cardano::include::preview::load();
    genesis.shelley.network_magic = Some(42);
    Arc::new(genesis)
}

#[tokio::test]
async fn a_custom_network_answers_with_what_state_holds() {
    let domain = ToyDomain::new_with_genesis(custom_genesis(), None, None);

    let expected = vec![((0, 0), None)];

    let alpha = read_era_summary!(v1alpha, domain).unwrap();
    let beta = read_era_summary!(v1beta, domain).unwrap();

    assert_eq!(alpha, vec![("alonzo".to_string(), expected[0], true)]);
    assert_eq!(beta, alpha);
    assert_eq!(interpreter_rows(&domain), expected);
}

fn clear_eras(domain: &ToyDomain) {
    let keys: Vec<_> = domain
        .state()
        .iter_entities_typed::<EraState>(None)
        .unwrap()
        .map(|row| row.unwrap().0)
        .collect();

    let writer = domain.state().start_writer().unwrap();
    for key in keys {
        writer
            .delete_entity(EraState::NS, &EntityKey::of(&key))
            .unwrap();
    }
    writer.commit().unwrap();
}

#[tokio::test]
async fn a_store_without_eras_answers_without_a_panic() {
    let domain = ToyDomain::new_with_genesis(
        Arc::new(dolos_cardano::include::mainnet::load()),
        None,
        None,
    );
    clear_eras(&domain);

    let alpha = read_era_summary!(v1alpha, domain).unwrap();
    let beta = read_era_summary!(v1beta, domain).unwrap();
    assert!(alpha.is_empty());
    assert!(beta.is_empty());

    assert!(load_era_history_response(&domain).is_err());
}

#[tokio::test]
async fn a_chain_with_no_tip_yet_answers_without_a_panic() {
    // bootstrapped at Origin: state holds the genesis era and no block yet
    for network in PublicNetwork::all() {
        let domain = ToyDomain::new_with_genesis(network.genesis.clone(), None, None);

        assert!(!read_era_summary!(v1alpha, domain).unwrap().is_empty());
        assert!(!read_era_summary!(v1beta, domain).unwrap().is_empty());
        assert!(!interpreter_rows(&domain).is_empty());
    }
}
