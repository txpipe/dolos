use base64::{engine::general_purpose::STANDARD, Engine};
use jsonrpsee::types::Params;
use pallas::{codec::utils::NonEmptySet, ledger::primitives::conway::VKeyWitness};
use std::{collections::BTreeMap, sync::Arc};
use tx3_tir::{interop, model::v1beta0 as tir};

use dolos_core::{facade::receive_tx, Domain, MempoolAwareUtxoStore, StateStore as _};

use crate::{
    compiler::load_compiler,
    specs::{ResolveParams, SubmitParams, SubmitResponse, SubmitWitness, TirInfo, TxEnvelope},
    utxos::UtxoStoreAdapter,
};

use super::{Context, Error};

fn unwrap_tir(envelope: TirInfo) -> Result<Vec<u8>, Error> {
    match envelope.encoding.as_str() {
        "base64" => STANDARD
            .decode(envelope.bytecode)
            .map_err(|_| Error::InvalidTirEnvelope),
        "hex" => hex::decode(envelope.bytecode).map_err(|_| Error::InvalidTirEnvelope),
        _ => return Err(Error::InvalidTirEnvelope),
    }
}

fn load_tx(params: ResolveParams) -> Result<tir::Tx, Error> {
    if params.tir.version != tir::IR_VERSION {
        return Err(Error::UnsupportedTir {
            expected: tir::IR_VERSION.to_string(),
            provided: params.tir.version,
        });
    }

    let tir = unwrap_tir(params.tir)?;

    let tir = interop::from_bytes(&tir)?;

    let tx_params = tx3_tir::reduce::find_params(&tir);
    let mut tx_args = BTreeMap::new();

    for (key, ty) in tx_params {
        let Some(arg) = params.args.get(&key) else {
            return Err(Error::MissingTxArg { key, ty });
        };

        let arg = interop::json::from_json(arg.clone(), &ty)?;
        tx_args.insert(key, arg);
    }

    let tir = tx3_tir::reduce::apply_args(tir, &tx_args)?;

    Ok(tir)
}

pub async fn trp_resolve<D: Domain>(
    params: Params<'_>,
    context: Arc<Context<D>>,
) -> Result<TxEnvelope, Error> {
    let params: ResolveParams = params.parse()?;

    let tx = load_tx(params)?;

    let mut compiler = load_compiler::<D>(&context.domain, &context.config)?;

    let store = MempoolAwareUtxoStore::<D>::new(context.domain.state(), context.domain.mempool());

    let utxos = UtxoStoreAdapter::<D>::new(store);

    let resolved = tx3_resolver::resolve_tx(
        tx,
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

    let chain = context.domain.read_chain().await;

    let hash = receive_tx(&context.domain, &chain, &bytes)?;

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
    use jsonrpsee::types::{ErrorCode, ErrorObjectOwned};
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

        let domain = ToyDomain::new(Some(delta), None).await;

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

    const SUBJECT_PROTOCOL: &str = r#"
        party Sender;
        party Receiver;
    
        tx swap(quantity: Int) {
            input source {
                from: Sender,
                min_amount: Ada(quantity) + fees,
            }

            output {
                to: Receiver,
                amount: Ada(quantity),
            }

            output {
                to: Sender,
                amount: source - Ada(quantity) - fees,
            }
        }
    "#;

    async fn attempt_resolve(args: &serde_json::Value) -> Result<TxEnvelope, ErrorObjectOwned> {
        let protocol = tx3_lang::Protocol::from_string(SUBJECT_PROTOCOL.to_owned())
            .load()
            .unwrap();

        let tx = protocol.new_tx("swap").unwrap();

        let ir = tx.apply().unwrap().ir_bytes();

        let req = json!({
            "tir": {
                "version": "v1beta0",
                "bytecode": hex::encode(ir),
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

        assert_eq!(err.code(), Error::CODE_MISSING_TX_ARG);
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

        assert_eq!(err.code(), ErrorCode::InvalidParams.code());
    }
}
