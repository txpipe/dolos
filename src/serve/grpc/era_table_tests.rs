//! `ReadEraSummary` in both schemas, `GetInterpreter` and `/network/eras` agree
//! on the era table.

use std::sync::Arc;

use axum::{body::Body, http::Request as HttpRequest};
use dolos_cardano::{model::EpochState, networks::KnownNetwork, PParamValue};
use dolos_core::config::MinibfConfig;
use dolos_core::{
    Domain, EntityKey, FixedNamespace as _, Genesis, StateStore as _, StateWriter as _,
};
use dolos_testing::{
    era_fixtures::{clear_eras, PublicNetwork},
    toy_domain::ToyDomain,
};
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

/// One `/network/eras` row, in the units that endpoint answers.
#[derive(Clone, Copy)]
struct MinibfRow {
    row: Row,
    /// Seconds from the system start.
    times: (u64, u64),
    epoch_length: u64,
    slot_length: u64,
    safe_zone: u64,
}

/// One `GetInterpreter` row, in the units the node writes.
#[derive(Clone, Copy)]
struct InterpreterRow {
    row: Row,
    /// Picoseconds from the system start, the end `None` on an open era.
    times: (u128, Option<u128>),
    epoch_length: u64,
    slot_length_ms: u64,
    /// `None` on the open era's indefinite safe zone.
    safe_zone: Option<u64>,
    genesis_window: u64,
}

async fn network_eras(domain: &ToyDomain) -> Vec<MinibfRow> {
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
        .map(|era| MinibfRow {
            row: (pair(&era["start"]), Some(pair(&era["end"]))),
            times: (
                era["start"]["time"].as_u64().unwrap(),
                era["end"]["time"].as_u64().unwrap(),
            ),
            epoch_length: era["parameters"]["epoch_length"].as_u64().unwrap(),
            slot_length: era["parameters"]["slot_length"].as_u64().unwrap(),
            safe_zone: era["parameters"]["safe_zone"].as_u64().unwrap(),
        })
        .collect()
}

fn interpreter_rows(domain: &ToyDomain) -> Vec<InterpreterRow> {
    let answer = load_era_history_response(domain).unwrap();
    let mut decoder = minicbor::Decoder::new(answer.raw_bytes());
    let count = decoder.array().unwrap().unwrap();

    (0..count)
        .map(|_| {
            decoder.array().unwrap();

            decoder.array().unwrap();
            let start_time = read_picos(&mut decoder);
            let start = (decoder.u64().unwrap(), decoder.u64().unwrap());

            let (end_time, end) = match decoder.datatype().unwrap() {
                minicbor::data::Type::Null => {
                    decoder.null().unwrap();
                    (None, None)
                }
                _ => {
                    decoder.array().unwrap();
                    let time = read_picos(&mut decoder);
                    (
                        Some(time),
                        Some((decoder.u64().unwrap(), decoder.u64().unwrap())),
                    )
                }
            };

            assert_eq!(decoder.array().unwrap(), Some(4));
            let epoch_length = decoder.u64().unwrap();
            let slot_length_ms = decoder.u64().unwrap();
            let safe_zone = read_safe_zone(&mut decoder);
            let genesis_window = decoder.u64().unwrap();

            InterpreterRow {
                row: (start, end),
                times: (start_time, end_time),
                epoch_length,
                slot_length_ms,
                safe_zone,
                genesis_window,
            }
        })
        .collect()
}

/// `[0, slots, [0]]` is a standard safe zone, `[1]` an indefinite one.
fn read_safe_zone(decoder: &mut minicbor::Decoder) -> Option<u64> {
    match decoder.array().unwrap() {
        Some(1) => {
            assert_eq!(decoder.u8().unwrap(), 1);
            None
        }
        Some(3) => {
            assert_eq!(decoder.u8().unwrap(), 0);
            let slots = decoder.u64().unwrap();
            assert_eq!(decoder.array().unwrap(), Some(1));
            assert_eq!(decoder.u8().unwrap(), 0);
            Some(slots)
        }
        other => panic!("unexpected safe zone shape: {other:?}"),
    }
}

fn plain_rows(rows: &[InterpreterRow]) -> Vec<Row> {
    rows.iter().map(|row| row.row).collect()
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
    let domain = with_network(network);

    let mut epoch = dolos_cardano::load_epoch::<ToyDomain>(domain.state()).unwrap();
    epoch
        .pparams
        .unwrap_live_mut()
        .set(PParamValue::ProtocolVersion((10, 0)));

    let writer = domain.state().start_writer().unwrap();
    writer.write_entity_typed(&(), &epoch).unwrap();
    writer.commit().unwrap();

    domain
}

fn without_open_end(rows: &[Row]) -> Vec<Row> {
    let mut rows = rows.to_vec();
    rows.last_mut().unwrap().1 = None;
    rows
}

const PICOS: u128 = 1_000_000_000_000;

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
        let interpreter = plain_rows(&interpreter_rows(&domain));

        assert_eq!(alpha, beta);
        assert_eq!(
            alpha.iter().map(|era| era.0.as_str()).collect::<Vec<_>>(),
            ERA_NAMES
        );

        let grpc: Vec<Row> = alpha.iter().map(|era| era.1).collect();
        let minibf: Vec<Row> = minibf.iter().map(|era| era.row).collect();
        assert_eq!(grpc, without_open_end(&minibf));
        assert_eq!(interpreter, grpc);

        for pair in grpc.windows(2) {
            assert_eq!(pair[0].1, Some(pair[1].0), "contiguous boundaries");
        }
    }
}

#[tokio::test]
async fn the_interpreter_writes_the_same_times_and_parameters_as_network_eras() {
    for network in PublicNetwork::all() {
        let domain = with_network(&network);

        let minibf = network_eras(&domain).await;
        let interpreter = interpreter_rows(&domain);
        assert_eq!(interpreter.len(), minibf.len());

        for (ours, theirs) in interpreter.iter().zip(&minibf) {
            assert_eq!(ours.times.0, u128::from(theirs.times.0) * PICOS);
            assert_eq!(ours.epoch_length, theirs.epoch_length);
            assert_eq!(ours.slot_length_ms, theirs.slot_length * 1000);
            assert_eq!(ours.genesis_window, theirs.safe_zone);
        }

        let (open, closed) = interpreter.split_last().unwrap();

        for (ours, theirs) in closed.iter().zip(&minibf) {
            assert_eq!(ours.times.1, Some(u128::from(theirs.times.1) * PICOS));
            assert_eq!(ours.safe_zone, Some(theirs.safe_zone));
        }

        assert_eq!(open.times.1, None);
        assert_eq!(open.safe_zone, None);
    }
}

#[test]
fn the_interpreter_writes_the_node_units_for_each_era() {
    // Byron: 20 s slots and 2k; Shelley on: 1 s slots and 3k/f
    let cases = [
        (PublicNetwork::mainnet(), 21600, 4320, 432000, 129600),
        (PublicNetwork::preprod(), 21600, 4320, 432000, 129600),
        (PublicNetwork::preview(), 4320, 864, 86400, 25920),
    ];

    for (network, byron_epoch, byron_zone, shelley_epoch, shelley_zone) in cases {
        let rows = interpreter_rows(&with_network(&network));
        let (open, rest) = rows.split_last().unwrap();
        let (byron, closed) = rest.split_first().unwrap();

        assert_eq!(byron.epoch_length, byron_epoch);
        assert_eq!(byron.slot_length_ms, 20_000);
        assert_eq!(byron.safe_zone, Some(byron_zone));
        assert_eq!(byron.genesis_window, byron_zone);

        for era in closed {
            assert_eq!(era.epoch_length, shelley_epoch);
            assert_eq!(era.slot_length_ms, 1000);
            assert_eq!(era.safe_zone, Some(shelley_zone));
            assert_eq!(era.genesis_window, shelley_zone);
        }

        assert_eq!(open.epoch_length, shelley_epoch);
        assert_eq!(open.slot_length_ms, 1000);
        assert_eq!(open.safe_zone, None);
        assert_eq!(open.genesis_window, shelley_zone);
    }
}

#[test]
fn the_interpreter_pins_the_bytes_of_a_bignum_time() {
    let domain = with_network(&PublicNetwork::mainnet());
    let answer = load_era_history_response(&domain).unwrap();

    // seven rows; Byron from zero to 89_856_000 s, as an unsigned bignum of
    // picoseconds with no leading zeros
    let prefix = [
        0x87, 0x83, 0x83, 0x00, 0x00, 0x00, 0x83, 0xc2, 0x49, 0x04, 0xdf, 0x00, 0xa3, 0xec, 0x29,
        0x80, 0x00, 0x00,
    ];

    assert_eq!(&answer.raw_bytes()[..prefix.len()], prefix);
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
    assert_eq!(plain_rows(&interpreter_rows(&domain)), expected);
}

/// A custom chain that reuses a public magic with its own genesis files.
fn on_a_public_magic(magic: u32, starting_protocol: usize) -> ToyDomain {
    let mut genesis = custom_genesis();
    let genesis_mut = Arc::make_mut(&mut genesis);
    genesis_mut.shelley.network_magic = Some(magic);
    genesis_mut.shelley_hash = pallas::crypto::hash::Hash::new([7; 32]);
    genesis_mut.force_protocol = Some(starting_protocol);

    ToyDomain::new_with_genesis(genesis, None, None)
}

#[tokio::test]
async fn a_custom_chain_on_a_public_magic_answers_with_what_state_holds() {
    for magic in [1, 2, 764824073] {
        for starting_protocol in [9, 10] {
            let domain = on_a_public_magic(magic, starting_protocol);
            let expected = ((0, 0), None);

            let alpha = read_era_summary!(v1alpha, domain).unwrap();
            let beta = read_era_summary!(v1beta, domain).unwrap();

            assert_eq!(alpha, vec![("conway".to_string(), expected, true)]);
            assert_eq!(beta, alpha);

            let interpreter = interpreter_rows(&domain);
            assert_eq!(plain_rows(&interpreter), vec![expected]);
            assert_eq!(interpreter[0].slot_length_ms, 1000);
        }
    }
}

/// A custom chain on a public network's own files, with its own start.
fn on_public_files(magic: u32, force_protocol: usize) -> ToyDomain {
    let mut genesis = KnownNetwork::by_magic(magic.into()).unwrap().genesis();
    genesis.force_protocol = Some(force_protocol);

    ToyDomain::new_with_genesis(Arc::new(genesis), None, None)
}

#[tokio::test]
async fn a_custom_start_on_a_public_networks_files_answers_with_what_state_holds() {
    for (magic, force_protocol) in [(764824073, 10), (1, 10), (1, 9), (2, 10)] {
        let domain = on_public_files(magic, force_protocol);
        let expected = ((0, 0), None);

        let alpha = read_era_summary!(v1alpha, domain).unwrap();
        let beta = read_era_summary!(v1beta, domain).unwrap();

        assert_eq!(alpha, vec![("conway".to_string(), expected, true)]);
        assert_eq!(beta, alpha);

        let interpreter = interpreter_rows(&domain);
        assert_eq!(plain_rows(&interpreter), vec![expected]);
        assert_eq!(interpreter[0].slot_length_ms, 1000);
    }
}

/// A custom chain on a public magic whose own Shelley file opens it at 10.
#[tokio::test]
async fn a_custom_chain_on_a_public_magic_starting_where_its_files_say_answers_with_what_state_holds(
) {
    for magic in [1, 764824073] {
        let mut genesis = (*custom_genesis()).clone();
        genesis.shelley.network_magic = Some(magic);
        genesis.shelley_hash = pallas::crypto::hash::Hash::new([7; 32]);
        genesis.shelley.protocol_params.protocol_version.major = 10;
        genesis.force_protocol = None;
        let domain = ToyDomain::new_with_genesis(Arc::new(genesis), None, None);
        let expected = ((0, 0), None);

        let alpha = read_era_summary!(v1alpha, domain).unwrap();
        let beta = read_era_summary!(v1beta, domain).unwrap();

        assert_eq!(alpha, vec![("conway".to_string(), expected, true)]);
        assert_eq!(beta, alpha);

        let interpreter = interpreter_rows(&domain);
        assert_eq!(plain_rows(&interpreter), vec![expected]);
        assert_eq!(interpreter[0].slot_length_ms, 1000);
    }
}

/// `force_protocol` is read only when genesis is applied, so removing it later
/// must not hand the store a public history it never had.
#[tokio::test]
async fn a_forced_start_restarted_without_the_force_answers_with_what_state_holds() {
    let domain =
        on_public_files(1, 10).with_genesis(Arc::new(KnownNetwork::by_magic(1).unwrap().genesis()));
    let expected = ((0, 0), None);

    let alpha = read_era_summary!(v1alpha, domain).unwrap();
    let beta = read_era_summary!(v1beta, domain).unwrap();

    assert_eq!(alpha, vec![("conway".to_string(), expected, true)]);
    assert_eq!(beta, alpha);

    let interpreter = interpreter_rows(&domain);
    assert_eq!(plain_rows(&interpreter), vec![expected]);
    assert_eq!(interpreter[0].slot_length_ms, 1000);
}

/// Preview's files give protocol 6, so forcing it changes nothing.
#[tokio::test]
async fn preview_forcing_its_own_start_keeps_its_history() {
    for force_protocol in [None, Some(6)] {
        let network = PublicNetwork::preview();
        let mut genesis = (*network.genesis).clone();
        genesis.force_protocol = force_protocol;

        let domain = ToyDomain::new_with_genesis(Arc::new(genesis), None, None);
        network.seed(&domain);

        let alpha = read_era_summary!(v1alpha, domain).unwrap();
        let beta = read_era_summary!(v1beta, domain).unwrap();

        assert_eq!(
            alpha.iter().map(|era| era.0.as_str()).collect::<Vec<_>>(),
            ERA_NAMES
        );
        assert_eq!(beta, alpha);
        assert_eq!(interpreter_rows(&domain).len(), ERA_NAMES.len());
    }
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
async fn a_store_with_nothing_in_it_answers_with_an_error() {
    let domain = ToyDomain::new_with_genesis(
        Arc::new(dolos_cardano::include::mainnet::load()),
        None,
        None,
    );
    clear_eras(&domain);

    let writer = domain.state().start_writer().unwrap();
    writer
        .delete_entity(EpochState::NS, &EntityKey::of(&()))
        .unwrap();
    writer.commit().unwrap();

    let alpha = read_era_summary!(v1alpha, domain).unwrap_err();
    let beta = read_era_summary!(v1beta, domain).unwrap_err();
    assert_eq!(alpha.code(), tonic::Code::Internal);
    assert_eq!(beta.code(), tonic::Code::Internal);

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
