mod tests {
    use std::time::Duration;

    use futures_util::StreamExt;
    use pallas::ledger::traverse::MultiEraTx;
    use utxorpc::proto::submit::v1::{
        any_chain_tx::Type, submit_service_client::SubmitServiceClient, AnyChainTx,
        SubmitTxRequest, WaitForTxRequest,
    };

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

        let wal = pallas::storage::rolldb::wal::Store::open("tmp", 10000).unwrap();

        let daemon = crate::submit::grpc::pipeline(config.grpc, wal, true).unwrap();

        let tx = "84a30081825820243c538cbe09a954ac5bd94a21200e9aefc52defd10862bcb3b6cf8dba01f99b000181a200581d60b6a03720b0c3dae80b0e38b08f904eadb5372486f56a8e7a04af7c10011b0000000241826840021a000f4240a100818258207dc72470db3c452fafdce8910a5da38fa763c2893c524f4a3b3610049fc34e1458403f6b431765e1af64d853dda412d8422ed0dde2220223739ba2c1768f3890b85e3c40295d2b340b6e1433327a2e8cf51add89d80335c33cddb7a3c197b4c9800bf5f6";

        let tx_bytes = hex::decode(&tx).unwrap();

        let tx_obj = MultiEraTx::decode(&tx_bytes).unwrap();

        let request = SubmitTxRequest {
            tx: vec![AnyChainTx {
                r#type: Some(Type::Raw(hex::decode(tx).unwrap().into())),
            }],
        };

        tokio::time::sleep(Duration::from_secs(2)).await;

        println!("trying to connect to grpc...");

        let mut submit_client = SubmitServiceClient::connect("http://localhost:50052")
            .await
            .unwrap();

        println!("submitting tx...");

        submit_client.submit_tx(request.clone()).await.unwrap();

        let wait_request = WaitForTxRequest {
            r#ref: vec![tx_obj.hash().to_vec().into()],
        };

        println!("waiting for tx...");

        let mut stream = submit_client
            .wait_for_tx(wait_request.clone())
            .await
            .unwrap()
            .into_inner();

        println!("streaming...");

        while let Some(next) = stream.next().await {
            let next = next.unwrap();

            println!("received wait for tx response: {next:?}")
        }

        println!("finished waiting for tx...");

        daemon.block()
    }
}
