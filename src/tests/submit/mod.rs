mod tests {
    use std::time::Duration;

    use log::info;
    use pallas::ledger::traverse::MultiEraBlock;
    use utxorpc::proto::submit::v1::{
        any_chain_tx::Type, submit_service_client::SubmitServiceClient, AnyChainTx, SubmitTxRequest,
    };

    fn load_test_block(name: &str) -> Vec<u8> {
        let path = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("test_data")
            .join(name);

        let content = std::fs::read_to_string(path).unwrap();
        hex::decode(content).unwrap()
    }

    #[tokio::test]
    #[ignore]
    async fn run_submit_service() {
        tracing::subscriber::set_global_default(
            tracing_subscriber::FmtSubscriber::builder()
                .with_max_level(tracing::Level::DEBUG)
                .finish(),
        )
        .unwrap();

        let mut s = config::Config::builder();

        let path = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("src/tests/submit/dolos.toml");

        s = s.add_source(config::File::with_name(path.to_str().unwrap()).required(false));

        let config: crate::submit::Config = s.build().unwrap().try_deserialize().unwrap();

        let daemon = crate::submit::grpc::pipeline(config.grpc).unwrap();

        let cbor = load_test_block("alonzo27.block");

        let block = MultiEraBlock::decode(&cbor).unwrap();
        let txs = block.txs();
        let tx = txs[0].encode();

        let request = SubmitTxRequest {
            tx: vec![AnyChainTx {
                r#type: Some(Type::Raw(tx.into())),
            }],
        };

        info!("trying to connect to grpc...");

        let mut submit_client = SubmitServiceClient::connect("http://localhost:50052")
            .await
            .unwrap();

        info!("submitting tx...");

        tokio::time::sleep(Duration::from_secs(2)).await;

        submit_client.submit_tx(request.clone()).await.unwrap();

        tokio::time::sleep(Duration::from_secs(30)).await;

        submit_client.submit_tx(request.clone()).await.unwrap();

        daemon.block()
    }
}
