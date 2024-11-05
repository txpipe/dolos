use std::ops::Deref;

use crate::uplc::{script_context, to_plutus_data::ToPlutusData};

use super::{
    error::Error,
    script_context::{
        find_script, DataLookupTable, ResolvedInput, ScriptVersion, SlotConfig, TxInfoV3,
    },
};
use miette::IntoDiagnostic;
use pallas::{
    codec::minicbor::{self, to_vec},
    interop::utxorpc::spec::query::any_chain_params::Params,
    ledger::primitives::{
        conway::{MintedTx, Redeemer, Redeemers, RedeemersKey, RedeemersValue},
        PlutusData,
    },
};
use uplc::{bumpalo::Bump, data::PlutusData as PragmaPlutusData, machine::EvalResult, term::Term};

pub struct TxEvalResult {
    pub cpu: i64,
    pub mem: i64,
}

pub fn map_pallas_data_to_pragma_data(arena: &Bump, data: PlutusData) -> &PragmaPlutusData<'_> {
    let bytes = to_vec(&data).expect("failed to encode data");
    PragmaPlutusData::from_cbor(arena, &bytes).expect("failed to decode data")
}

pub fn eval_tx(
    tx: &MintedTx,
    protocol_params: Params,
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
    match find_script(redeemer, tx, utxos, lookup_table)? {
        (ScriptVersion::Native(_), _) => Err(Error::NativeScriptPhaseTwo),

        (ScriptVersion::V1(script), datum) => todo!(),

        (ScriptVersion::V2(script), datum) => todo!(),

        (ScriptVersion::V3(script), datum) => {
            let tx_info = TxInfoV3::from_transaction(tx, utxos, slot_config).unwrap();
            let script_context = tx_info
                .into_script_context(redeemer, datum.as_ref())
                .unwrap();
            let script_bytes = script.as_ref();

            // print in hex
            println!("Script Cbor Hex: {:?}", hex::encode(script_bytes));

            let arena = uplc::bumpalo::Bump::with_capacity(1_024_000);

            // One-liner to cbor decode the Flat-encoded script (assuming its just a CBOR bytestring)
            let mut program = uplc::flat::decode(&arena, &script_bytes[2..])
                .into_diagnostic()
                .unwrap();

            let script_context_term =
                map_pallas_data_to_pragma_data(&arena, script_context.to_plutus_data());

            let script_context_term = Term::data(&arena, script_context_term);
            program = program.apply(&arena, script_context_term);

            let result = program.eval(&arena);

            Ok(TxEvalResult {
                cpu: result.info.consumed_budget.cpu,
                mem: result.info.consumed_budget.mem,
            })
        }
    }
}
