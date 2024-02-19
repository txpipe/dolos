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

        let wal = pallas::storage::rolldb::wal::Store::open("tmp", 10).unwrap();

        let daemon = crate::submit::grpc::pipeline(config.grpc, wal, true).unwrap();

        // let cbor = load_test_block("alonzo27.block");

        // let block = MultiEraBlock::decode(&cbor).unwrap();
        // let txs = block.txs();
        // let tx = txs[0].encode();

        let tx = "84a30081825820af9f7a12bcc0825957a4fd909c8275866755e8a9178b51582d4ee938fb51cdee000181a200581d60b6a03720b0c3dae80b0e38b08f904eadb5372486f56a8e7a04af7c10011b0000000241fc7a40021a000f4240a100818258207dc72470db3c452fafdce8910a5da38fa763c2893c524f4a3b3610049fc34e1458406299bf7fd991d02af9822fdd72d71d7eede3f8d88545961a4ff714e4bdc8802fc94a8076c8407b7c8a6b9c49a785a29553f8e045ca3096394f7ba1d2090ee801f5f6";

        let request = SubmitTxRequest {
            tx: vec![AnyChainTx {
                r#type: Some(Type::Raw(hex::decode(tx).unwrap().into())),
            }],
        };

        info!("trying to connect to grpc...");

        let mut submit_client = SubmitServiceClient::connect("http://localhost:50052")
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;

        info!("submitting tx...");

        submit_client.submit_tx(request.clone()).await.unwrap();

        daemon.block()
    }
}
