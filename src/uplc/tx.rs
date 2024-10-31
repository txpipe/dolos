use std::ops::Deref;

use super::{error::Error, script_context::{find_script, DataLookupTable, ResolvedInput, ScriptVersion, SlotConfig, TxInfoV3}};
use pallas::{
    codec::minicbor::to_vec,
    interop::utxorpc::spec::query::any_chain_params::Params,
    ledger::primitives::{
        conway::{MintedTx, Redeemer, Redeemers, RedeemersKey, RedeemersValue},
        PlutusData,
    },
};
use uplc::{bumpalo::Bump, data::PlutusData as PragmaPlutusData};

pub fn map_pragma_data_to_pallas_data(arena: &Bump, data: PlutusData) -> &PragmaPlutusData<'_> {
    let bytes = to_vec(&data).expect("failed to encode data");
    PragmaPlutusData::from_cbor(arena, &bytes).expect("failed to decode data")
}

pub fn eval_tx(
    tx: &MintedTx,
    protocol_params: Params,
    utxos: &[ResolvedInput],
    slot_config: &SlotConfig,
) {
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

    redeemers.iter().for_each(|redeemer| {
        eval_redeemer(redeemer, tx, utxos, slot_config, &lookup_table);
    });
}

pub fn eval_redeemer(
    redeemer: &Redeemer,
    tx: &MintedTx,
    utxos: &[ResolvedInput],
    slot_config: &SlotConfig,
    lookup_table: &DataLookupTable,
) -> Result<(), Error> {
    let tx_info = TxInfoV3::from_transaction(tx, utxos, slot_config).unwrap();

    match find_script(redeemer, tx, utxos, lookup_table)? {
        (ScriptVersion::Native(_), _) => Err(Error::NativeScriptPhaseTwo),

        (ScriptVersion::V1(script), datum) => todo!(),

        (ScriptVersion::V2(script), datum) => todo!(),

        (ScriptVersion::V3(script), datum) => todo!(),
    }
}
