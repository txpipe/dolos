use std::ops::Deref;

use crate::uplc::{
    script_context::{ScriptContext, TxInfo, TxInfoV1},
    to_plutus_data::ToPlutusData,
};

use super::{
    error::Error,
    script_context::{
        find_script, DataLookupTable, ResolvedInput, ScriptVersion, SlotConfig, TxInfoV2, TxInfoV3,
    },
};
use miette::IntoDiagnostic;
use pallas::{
    codec::minicbor::to_vec,
    interop::utxorpc::spec::query::any_chain_params::Params,
    ledger::primitives::{
        conway::{MintedTx, Redeemer, Redeemers, RedeemersKey, RedeemersValue},
        PlutusData,
    },
};
use uplc::{bumpalo::Bump, data::PlutusData as PragmaPlutusData, term::Term};

pub struct TxEvalResult {
    pub cpu: i64,
    pub mem: i64,
}

pub fn map_pallas_data_to_pragma_data(arena: &Bump, data: PlutusData) -> &PragmaPlutusData<'_> {
    let bytes = to_vec(&data).expect("failed to encode data");
    PragmaPlutusData::from_cbor(arena, &bytes).expect("failed to decode data")
}

pub fn plutus_data_to_pragma_term(arena: &Bump, data: PlutusData) -> &Term<'_> {
    let pragma_data = map_pallas_data_to_pragma_data(arena, data);
    Term::data(arena, pragma_data)
}

pub fn eval_tx(
    tx: &MintedTx,
    _protocol_params: &Params, // For Cost Models
    utxos: &[ResolvedInput],
    slot_config: &SlotConfig,
) -> Result<TxEvalResult, Error> {
    let lookup_table = DataLookupTable::from_transaction(tx, utxos);

    let redeemers = tx
        .transaction_witness_set
        .redeemer
        .as_ref()
        .unwrap()
        .deref();

    let redeemers = match redeemers {
        Redeemers::List(arr) => arr
            .deref()
            .iter()
            .map(|r| {
                (
                    RedeemersKey {
                        tag: r.tag,
                        index: r.index,
                    },
                    RedeemersValue {
                        data: r.data.clone(),
                        ex_units: r.ex_units,
                    },
                )
            })
            .collect(),
        Redeemers::Map(arr) => arr.deref().clone(),
    };

    let redeemers = redeemers
        .iter()
        .map(|(k, v)| Redeemer {
            tag: k.tag,
            index: k.index,
            data: v.data.clone(),
            ex_units: v.ex_units,
        })
        .collect::<Vec<_>>();

    redeemers.iter().try_fold(
        TxEvalResult { cpu: 0, mem: 0 },
        |eval_result_acc, redeemer| match eval_redeemer(
            redeemer,
            tx,
            utxos,
            &lookup_table,
            slot_config,
        ) {
            Ok(result) => Ok(TxEvalResult {
                cpu: eval_result_acc.cpu + result.cpu,
                mem: eval_result_acc.mem + result.mem,
            }),
            Err(e) => Err(e),
        },
    )
}

pub fn eval_redeemer(
    redeemer: &Redeemer,
    tx: &MintedTx,
    utxos: &[ResolvedInput],
    lookup_table: &DataLookupTable,
    slot_config: &SlotConfig,
) -> Result<TxEvalResult, Error> {
    fn do_eval(
        tx_info: TxInfo,
        script_bytes: &[u8],
        datum: Option<PlutusData>,
        redeemer: &Redeemer,
    ) -> TxEvalResult {
        let script_context = tx_info
            .into_script_context(redeemer, datum.as_ref())
            .unwrap();

        let arena = Bump::with_capacity(1_024_000);
        let script_context_term =
            plutus_data_to_pragma_term(&arena, script_context.to_plutus_data());
        let redeemer_term = plutus_data_to_pragma_term(&arena, redeemer.to_plutus_data());
        let program = uplc::flat::decode(&arena, &script_bytes[2..])
            .into_diagnostic()
            .unwrap();

        let program = match script_context {
            ScriptContext::V1V2 { .. } => if let Some(datum) = datum {
                let datum_term = plutus_data_to_pragma_term(&arena, datum.to_plutus_data());
                program.apply(&arena, datum_term)
            } else {
                program
            }
            .apply(&arena, redeemer_term)
            .apply(&arena, script_context_term),

            ScriptContext::V3 { .. } => program.apply(&arena, script_context_term),
        };

        let result = program.eval(&arena);
        TxEvalResult {
            cpu: result.info.consumed_budget.cpu,
            mem: result.info.consumed_budget.mem,
        }
    }

    match find_script(redeemer, tx, utxos, lookup_table)? {
        (ScriptVersion::Native(_), _) => Err(Error::NativeScriptPhaseTwo),

        (ScriptVersion::V1(script), datum) => Ok(do_eval(
            TxInfoV1::from_transaction(tx, utxos, slot_config).unwrap(),
            script.as_ref(),
            datum,
            redeemer,
        )),

        (ScriptVersion::V2(script), datum) => Ok(do_eval(
            TxInfoV2::from_transaction(tx, utxos, slot_config).unwrap(),
            script.as_ref(),
            datum,
            redeemer,
        )),

        (ScriptVersion::V3(script), datum) => Ok(do_eval(
            TxInfoV3::from_transaction(tx, utxos, slot_config).unwrap(),
            script.as_ref(),
            datum,
            redeemer,
        )),
    }
}
