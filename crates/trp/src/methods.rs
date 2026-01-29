use jsonrpsee::types::Params;
use pallas::{codec::utils::NonEmptySet, ledger::primitives::conway::VKeyWitness};
use std::sync::Arc;

use tx3_resolver::trp::{ResolveParams, SubmitParams, SubmitResponse, SubmitWitness, TxEnvelope};

use dolos_core::{Domain, MempoolAwareUtxoStore, StateStore as _, SubmitExt};

use crate::{compiler::load_compiler, utxos::UtxoStoreAdapter};

use super::{Context, Error};

pub async fn trp_resolve<D: Domain>(
    params: Params<'_>,
    context: Arc<Context<D>>,
) -> Result<TxEnvelope, Error> {
    let params: ResolveParams = params.parse()?;

    let (tx, args) = tx3_resolver::trp::parse_resolve_request(params)?;

    let mut compiler = load_compiler::<D>(&context.domain, &context.config)?;

    let store = MempoolAwareUtxoStore::<D>::new(
        context.domain.state(),
        context.domain.indexes(),
        context.domain.mempool(),
    );

    let utxos = UtxoStoreAdapter::<D>::new(store);

    let resolved = tx3_resolver::resolve_tx(
        tx,
        &args,
        &mut compiler,
        &utxos,
        context.config.max_optimize_rounds.into(),
    )
    .await?;

    Ok(TxEnvelope {
        tx: hex::encode(resolved.payload),
        hash: hex::encode(resolved.hash),
    })
}

fn apply_witnesses(original: &[u8], witnesses: &[SubmitWitness]) -> Result<Vec<u8>, Error> {
    let tx = pallas::ledger::traverse::MultiEraTx::decode(original)?;

    let mut tx = tx.as_conway().ok_or(Error::UnsupportedTxEra)?.to_owned();

    let map_witness = |witness: &SubmitWitness| VKeyWitness {
        vkey: Vec::<u8>::from(witness.key.clone()).into(),
        signature: Vec::<u8>::from(witness.signature.clone()).into(),
    };

    let mut witness_set = tx.transaction_witness_set.unwrap();

    let old = witness_set
        .vkeywitness
        .iter()
        .flat_map(|x| x.iter())
        .cloned();

    let new = witnesses.iter().map(map_witness);

    let all: Vec<_> = old.chain(new).collect();

    witness_set.vkeywitness = NonEmptySet::from_vec(all);

    tx.transaction_witness_set = pallas::codec::utils::KeepRaw::from(witness_set);

    Ok(pallas::codec::minicbor::to_vec(&tx).unwrap())
}

pub async fn trp_submit<D: Domain>(
    params: Params<'_>,
    context: Arc<Context<D>>,
) -> Result<SubmitResponse, Error> {
    let params: SubmitParams = params.parse()?;

    let mut bytes = Vec::<u8>::from(params.tx);

    if !params.witnesses.is_empty() {
        bytes = apply_witnesses(&bytes, &params.witnesses)?;
    }

    let chain = context.domain.read_chain();

    let hash = context.domain.receive_tx(&chain, &bytes)?;

    Ok(SubmitResponse {
        hash: hash.to_string(),
    })
}

pub fn health<D: Domain>(context: &Context<D>) -> bool {
    context.domain.state().read_cursor().is_ok()
}

#[cfg(test)]
mod tests {
    use dolos_core::config::TrpConfig;
    use dolos_testing::toy_domain::ToyDomain;
    use dolos_testing::TestAddress::{Alice, Bob};
    use jsonrpsee::types::ErrorObjectOwned;
    use serde_json::json;

    use crate::metrics::Metrics;

    use super::*;

    async fn setup_test_context() -> Arc<Context<ToyDomain>> {
        let delta = dolos_testing::make_custom_utxo_delta(
            dolos_testing::TestAddress::everyone(),
            2..4,
            |x: &dolos_testing::TestAddress| {
                dolos_testing::utxo_with_random_amount(x, 4_000_000..5_000_000)
            },
        );

        let domain = ToyDomain::new(Some(delta), None);

        Arc::new(Context {
            domain,
            config: Arc::new(TrpConfig {
                max_optimize_rounds: 3,
                extra_fees: None,

                // next are dummy, not used
                listen_address: "[::]:1234".parse().unwrap(),
                permissive_cors: None,
            }),
            metrics: Metrics::default(),
        })
    }

    const SUBJECT_PROTOCOL: &str = r#"ab6466656573a1694576616c506172616d6a457870656374466565736a7265666572656e6365738066696e7075747381a3646e616d6566736f75726365657574786f73a1694576616c506172616da16b457870656374496e7075748266736f75726365a56761646472657373a1694576616c506172616da16b45787065637456616c7565826673656e64657267416464726573736a6d696e5f616d6f756e74a16b4576616c4275696c74496ea16341646482a16641737365747381a366706f6c696379644e6f6e656a61737365745f6e616d65644e6f6e6566616d6f756e74a1694576616c506172616da16b45787065637456616c756582687175616e7469747963496e74a1694576616c506172616d6a4578706563744665657363726566644e6f6e65646d616e79f46a636f6c6c61746572616cf46872656465656d6572644e6f6e65676f75747075747382a46761646472657373a1694576616c506172616da16b45787065637456616c756582687265636569766572674164647265737365646174756d644e6f6e6566616d6f756e74a16641737365747381a366706f6c696379644e6f6e656a61737365745f6e616d65644e6f6e6566616d6f756e74a1694576616c506172616da16b45787065637456616c756582687175616e7469747963496e74686f7074696f6e616cf4a46761646472657373a1694576616c506172616da16b45787065637456616c7565826673656e646572674164647265737365646174756d644e6f6e6566616d6f756e74a16b4576616c4275696c74496ea16353756282a16b4576616c4275696c74496ea16353756282a16a4576616c436f65726365a16a496e746f417373657473a1694576616c506172616da16b457870656374496e7075748266736f75726365a56761646472657373a1694576616c506172616da16b45787065637456616c7565826673656e64657267416464726573736a6d696e5f616d6f756e74a16b4576616c4275696c74496ea16341646482a16641737365747381a366706f6c696379644e6f6e656a61737365745f6e616d65644e6f6e6566616d6f756e74a1694576616c506172616da16b45787065637456616c756582687175616e7469747963496e74a1694576616c506172616d6a4578706563744665657363726566644e6f6e65646d616e79f46a636f6c6c61746572616cf4a16641737365747381a366706f6c696379644e6f6e656a61737365745f6e616d65644e6f6e6566616d6f756e74a1694576616c506172616da16b45787065637456616c756582687175616e7469747963496e74a1694576616c506172616d6a45787065637446656573686f7074696f6e616cf46876616c6964697479f6656d696e747380656275726e7380656164686f63806a636f6c6c61746572616c80677369676e657273f6686d6574616461746180"#;

    async fn attempt_resolve(args: &serde_json::Value) -> Result<TxEnvelope, ErrorObjectOwned> {
        let req = json!({
            "tir": {
                "version": "v1beta0",
                "content": SUBJECT_PROTOCOL,
                "encoding": "hex"
            },
            "args": args,
            "env": {},
        })
        .to_string();

        let params = Params::new(Some(req.as_str()));

        let context = setup_test_context().await;

        let resolved = trp_resolve(params, context.clone()).await?;

        Ok(resolved)
    }

    #[tokio::test]
    async fn test_resolve_happy_path() {
        let args = json!({
            "quantity": 100,
            "sender": Alice.as_str(),
            "receiver": Bob.as_str(),
        });

        let resolved = attempt_resolve(&args).await.unwrap();

        let tx = hex::decode(resolved.tx).unwrap();

        let _ = pallas::ledger::traverse::MultiEraTx::decode(&tx).unwrap();
    }

    #[tokio::test]
    async fn test_resolve_missing_args() {
        let args = json!({});

        let resolved = attempt_resolve(&args).await;

        let err = resolved.unwrap_err();

        dbg!(&err);

        assert_eq!(err.code(), tx3_resolver::trp::errors::CODE_MISSING_TX_ARG);
    }

    #[tokio::test]
    async fn test_resolve_invalid_args() {
        let args = json!({
            "quantity": "abc",
            "sender": "Alice",
            "receiver": "Bob",
        });

        let resolved = attempt_resolve(&args).await;

        let err = resolved.unwrap_err();

        dbg!(&err);

        assert_eq!(err.code(), tx3_resolver::trp::errors::CODE_INTEROP_ERROR);
    }
}
