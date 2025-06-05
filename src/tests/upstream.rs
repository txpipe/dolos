use gasket::{framework::*, runtime::Policy};
use tracing::{error, info};

use dolos_core::PullEvent;
use dolos_redb::wal::RedbWalStore;

use crate::adapters::WalAdapter;

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
        error!("dequeing form witness");
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

    let mut wal = RedbWalStore::memory().unwrap();

    wal.initialize_from_origin().unwrap();

    let (send, receive) = gasket::messaging::tokio::mpsc_channel(200);

    let mut upstream = crate::sync::pull::Stage::new(
        "relays-new.cardano-mainnet.iohk.io:3001".into(),
        764824073,
        20,
        WalAdapter::Redb(wal),
        false,
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
