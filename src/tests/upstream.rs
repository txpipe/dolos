//! The pull stage against an upstream peer: a live mainnet session (ignored),
//! and the session and delivery series against a local one.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use gasket::{
    framework::*,
    runtime::{Policy, Tether, TetherState},
};
use tracing::{debug, info};

use dolos_core::{
    config::{RetryConfig, SyncConfig},
    PullEvent,
};
use dolos_testing::{
    metrics::MetricsRecorder,
    peer::{PeerFixture, PeerOptions},
};

use crate::adapters::storage::WalStoreBackend;
use crate::sync::{metrics::SyncGauges, SyncLimit, SyncProgress};

struct WitnessStage {
    input: gasket::messaging::InputPort<PullEvent>,
}

impl gasket::framework::Stage for WitnessStage {
    type Unit = PullEvent;
    type Worker = WitnessWorker;

    fn name(&self) -> &str {
        "witness"
    }
}

struct WitnessWorker;

#[async_trait::async_trait(?Send)]
impl Worker<WitnessStage> for WitnessWorker {
    async fn bootstrap(_: &WitnessStage) -> Result<Self, WorkerError> {
        Ok(Self)
    }

    async fn schedule(
        &mut self,
        stage: &mut WitnessStage,
    ) -> Result<WorkSchedule<PullEvent>, WorkerError> {
        debug!("dequeing form witness");
        let msg = stage.input.recv().await.or_panic()?;
        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, _: &PullEvent, _: &mut WitnessStage) -> Result<(), WorkerError> {
        info!("witnessing block event");

        Ok(())
    }
}

#[test]
#[ignore]
fn test_mainnet_upstream() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::TRACE)
            .finish(),
    )
    .unwrap();

    let wal = WalStoreBackend::in_memory().unwrap();

    let (send, receive) = gasket::messaging::tokio::mpsc_channel(200);

    let mut upstream = crate::sync::pull::Stage::new(
        &SyncConfig {
            pull_batch_size: 20,
            ..Default::default()
        },
        "relays-new.cardano-mainnet.iohk.io:3001".into(),
        764824073,
        crate::sync::SyncLimit::default(),
        Default::default(),
        wal,
    );

    upstream.downstream.connect(send);

    let mut witness = WitnessStage {
        input: Default::default(),
    };

    witness.input.connect(receive);

    let upstream = gasket::runtime::spawn_stage(upstream, Policy::default());
    let witness = gasket::runtime::spawn_stage(witness, Policy::default());

    let daemon = gasket::daemon::Daemon::new(vec![upstream, witness]);

    daemon.block();
}

const PREVIEW_MAGIC: u64 = 2;

/// A pull stage toward `address` catching up on `progress`, counting its
/// sessions on `recorder`, with a witness downstream.
fn pull_alone(
    address: String,
    max_retries: usize,
    progress: &Arc<SyncProgress>,
    recorder: &MetricsRecorder,
) -> Vec<Tether> {
    let config = SyncConfig {
        pull_batch_size: 2,
        retry: RetryConfig {
            max_retries,
            backoff_unit_seconds: 0,
            ..Default::default()
        },
        ..Default::default()
    };

    let mut pull = crate::sync::pull::Stage::new(
        &config,
        address,
        PREVIEW_MAGIC,
        SyncLimit::CatchUp,
        progress.clone(),
        WalStoreBackend::in_memory().unwrap(),
    );
    pull.count_sessions_on(&recorder.meter("test"));

    let (send, receive) = gasket::messaging::tokio::mpsc_channel(200);
    pull.downstream.connect(send);

    let mut witness = WitnessStage {
        input: Default::default(),
    };
    witness.input.connect(receive);

    vec![
        gasket::runtime::spawn_stage(pull, Policy::default()),
        gasket::runtime::spawn_stage(witness, Policy::default()),
    ]
}

fn teardown(tethers: Vec<Tether>) {
    for tether in &tethers {
        let _ = tether.dismiss_stage();
    }

    for tether in tethers {
        tether.join_stage();
    }
}

fn is_ended(tether: &Tether) -> bool {
    !matches!(tether.check_state(), TetherState::Alive(phase) if phase.end_cause().is_none())
}

/// Wait until `done` holds, or panic naming `what` after `limit`.
fn wait_until(what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + limit;

    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn sessions(recorder: &MetricsRecorder, outcome: &str) -> f64 {
    recorder
        .collect()
        .value("dolos.sync.upstream.sessions", &[("outcome", outcome)])
        .unwrap_or(0.0)
}

#[test]
fn a_refused_connection_counts_a_failed_session() {
    let address = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };

    let recorder = MetricsRecorder::default();
    let progress = Arc::new(SyncProgress::default());
    let tethers = pull_alone(address.to_string(), 1, &progress, &recorder);

    wait_until("the stage gives up", Duration::from_secs(30), || {
        is_ended(&tethers[0])
    });
    teardown(tethers);

    // The first attempt and its one retry.
    assert_eq!(sessions(&recorder, "failed"), 2.0);
    assert_eq!(sessions(&recorder, "connected"), 0.0);
    assert_eq!(sessions(&recorder, "lost"), 0.0);
}

#[test]
fn a_session_counts_connected_and_dropping_it_counts_lost() {
    let peer = PeerFixture::start(
        "127.0.0.1:0",
        PREVIEW_MAGIC,
        PeerOptions {
            initial_blocks: 50,
            close_first_session_after_headers: Some(5),
            ..Default::default()
        },
    )
    .unwrap();

    let recorder = MetricsRecorder::default();
    let progress = Arc::new(SyncProgress::default());
    let tethers = pull_alone(peer.address().to_string(), 1, &progress, &recorder);

    wait_until("the target is fixed", Duration::from_secs(30), || {
        progress.target().is_some()
    });
    teardown(tethers);

    // The peer dropped the first session; the second reached the tip.
    assert_eq!(sessions(&recorder, "connected"), 2.0);
    assert_eq!(sessions(&recorder, "lost"), 1.0);
    assert_eq!(sessions(&recorder, "failed"), 0.0);
}

#[test]
fn the_delivery_age_is_small_after_a_block_and_grows_without_one() {
    let peer = PeerFixture::start(
        "127.0.0.1:0",
        PREVIEW_MAGIC,
        PeerOptions {
            initial_blocks: 20,
            ..Default::default()
        },
    )
    .unwrap();

    let recorder = MetricsRecorder::default();
    let progress = Arc::new(SyncProgress::default());
    let _gauges = SyncGauges::register(
        &recorder.meter("test"),
        progress.clone(),
        || None,
        SystemTime::now,
    );

    // Nothing delivered yet: no age.
    let age = recorder
        .collect()
        .value("dolos.sync.upstream.last_delivery.age", &[]);
    assert_eq!(age, None);

    let tethers = pull_alone(peer.address().to_string(), 1, &progress, &recorder);

    // A catch-up pulls nothing once the target is fixed.
    wait_until("the target is fixed", Duration::from_secs(30), || {
        progress.target().is_some()
    });

    let age = || {
        recorder
            .collect()
            .value("dolos.sync.upstream.last_delivery.age", &[])
            .expect("an age once the peer delivered")
    };

    let first = age();
    assert!(first < 5.0, "{first}");

    std::thread::sleep(Duration::from_millis(1500));
    let later = age();
    assert!(later >= first + 1.0, "{first} then {later}");

    teardown(tethers);
}
