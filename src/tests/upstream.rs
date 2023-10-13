use gasket::{framework::*, messaging::*, runtime::Policy};
use tracing::{error, info};

use crate::model::PullEvent;

struct WitnessStage {
    input: gasket::messaging::tokio::InputPort<PullEvent>,
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

    let rolldb = pallas::storage::rolldb::wal::Store::open("tmp", 10).unwrap();

    let intersection = rolldb.intersect_options(5).unwrap().into_iter().collect();

    let (send, receive) = gasket::messaging::tokio::mpsc_channel(200);

    let mut upstream = crate::sync::pull::Stage::new(
        "relays-new.cardano-mainnet.iohk.io:3001".into(),
        764824073,
        intersection,
    );

    upstream.downstream.connect(send);

    let mut witness = WitnessStage {
        input: Default::default(),
    };

    witness.input.connect(receive);

    let upstream = gasket::runtime::spawn_stage(upstream, Policy::default());
    let witness = gasket::runtime::spawn_stage(witness, Policy::default());

    let daemon = gasket::daemon::Daemon(vec![upstream, witness]);

    daemon.block();
}
