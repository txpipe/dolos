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
    to_plutus_data::convert_tag_to_constr,
};
use pallas::{
    interop::utxorpc::spec::query::any_chain_params::Params,
    ledger::primitives::{
        conway::{MintedTx, Redeemer, Redeemers, RedeemersKey, RedeemersValue},
        PlutusData,
    },
};
use rug::Integer;
use uplc::{
    binder::DeBruijn,
    bumpalo::{collections::Vec as BumpVec, Bump},
    data::PlutusData as PragmaPlutusData,
    term::Term,
};

pub struct TxEvalResult {
    pub cpu: i64,
    pub mem: i64,
}

pub fn map_pallas_data_to_pragma_data(arena: &Bump, data: PlutusData) -> &PragmaPlutusData<'_> {
    match data {
        PlutusData::Constr(constr) => PragmaPlutusData::constr(
            arena,
            convert_tag_to_constr(constr.tag).unwrap(),
            BumpVec::from_iter_in(
                constr
                    .fields
                    .iter()
                    .map(|f| map_pallas_data_to_pragma_data(arena, f.clone())),
                arena,
            ),
        ),
        PlutusData::Map(key_value_pairs) => {
            let key_value_pairs = BumpVec::from_iter_in(
                key_value_pairs.iter().map(|(k, v)| {
                    (
                        map_pallas_data_to_pragma_data(arena, k.clone()),
                        map_pallas_data_to_pragma_data(arena, v.clone()),
                    )
                }),
                arena,
            );
            PragmaPlutusData::map(arena, key_value_pairs)
        }
        PlutusData::BigInt(big_int) => match big_int {
            pallas::ledger::primitives::BigInt::Int(int) => {
                let val: i128 = int.into();
                PragmaPlutusData::integer_from(arena, val)
            }
            // @TODO: recheck this implementations correctness
            pallas::ledger::primitives::BigInt::BigUInt(big_num_bytes) => {
                let big_num_bytes_string: String = big_num_bytes.into();
                let big_num_byte_array = hex::decode(big_num_bytes_string).unwrap();
                let val = arena.alloc(Integer::from_digits(
                    &big_num_byte_array,
                    rug::integer::Order::MsfBe,
                ));
                PragmaPlutusData::integer(arena, val)
            }
            pallas::ledger::primitives::BigInt::BigNInt(big_num_bytes) => {
                let big_num_bytes_string: String = big_num_bytes.into();
                let big_num_byte_array = hex::decode(big_num_bytes_string).unwrap();
                let mut val = Integer::from_digits(&big_num_byte_array, rug::integer::Order::MsfBe);
                val = -(&val + Integer::from(1));
                PragmaPlutusData::integer(arena, arena.alloc(val))
            }
        },
        PlutusData::BoundedBytes(bounded_bytes) => {
            let bounded_bytes_string: String = bounded_bytes.into();
            let bounded_bytes_byte_array =
                BumpVec::from_iter_in(hex::decode(bounded_bytes_string).unwrap(), arena);
            PragmaPlutusData::byte_string(arena, bounded_bytes_byte_array)
        }
        PlutusData::Array(maybe_indef_array) => {
            let items = match maybe_indef_array {
                pallas::codec::utils::MaybeIndefArray::Def(xs) => BumpVec::from_iter_in(
                    xs.iter()
                        .map(|x| map_pallas_data_to_pragma_data(arena, x.clone())),
                    arena,
                ),
                pallas::codec::utils::MaybeIndefArray::Indef(xs) => BumpVec::from_iter_in(
                    xs.iter()
                        .map(|x| map_pallas_data_to_pragma_data(arena, x.clone())),
                    arena,
                ),
            };
            PragmaPlutusData::list(arena, items)
        }
    }
}

pub fn plutus_data_to_pragma_term(arena: &Bump, data: PlutusData) -> &Term<'_, DeBruijn> {
    Term::data(arena, map_pallas_data_to_pragma_data(arena, data))
}

pub fn eval_tx(
    tx: &MintedTx,
    _protocol_params: &Params, // For Cost Models
    utxos: &[ResolvedInput],
    slot_config: &SlotConfig,
) -> Result<Vec<Redeemer>, Error> {
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
        .into_iter()
        .map(
            |redeemer| match eval_redeemer(&redeemer, tx, utxos, &lookup_table, slot_config) {
                Ok(result) => {
                    let mut updated_redeemer = redeemer.clone();
                    updated_redeemer.ex_units.steps = result.cpu as u64;
                    updated_redeemer.ex_units.mem = result.mem as u64;
                    Ok(updated_redeemer)
                }
                Err(e) => Err(e),
            },
        )
        .collect::<Result<Vec<_>, _>>()?;

    Ok(redeemers)
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
    ) -> Result<TxEvalResult, Error> {
        let script_context = tx_info
            .into_script_context(redeemer, datum.as_ref())
            .ok_or_else(|| Error::ScriptContextBuildError)?;

        let arena = Bump::with_capacity(1_024_000);
        let script_context_term =
            plutus_data_to_pragma_term(&arena, script_context.to_plutus_data());
        let redeemer_term = plutus_data_to_pragma_term(&arena, redeemer.to_plutus_data());
        let program = uplc::flat::decode(&arena, &script_bytes[2..])?;
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

        Ok(TxEvalResult {
            cpu: result.info.consumed_budget.cpu * 11 / 10,
            mem: result.info.consumed_budget.mem * 11 / 10,
        })
    }

    match find_script(redeemer, tx, utxos, lookup_table)? {
        (ScriptVersion::Native(_), _) => Err(Error::NativeScriptPhaseTwo),

        (ScriptVersion::V1(script), datum) => Ok(do_eval(
            TxInfoV1::from_transaction(tx, utxos, slot_config)?,
            script.as_ref(),
            datum,
            redeemer,
        )?),

        (ScriptVersion::V2(script), datum) => Ok(do_eval(
            TxInfoV2::from_transaction(tx, utxos, slot_config)?,
            script.as_ref(),
            datum,
            redeemer,
        )?),

        (ScriptVersion::V3(script), datum) => Ok(do_eval(
            TxInfoV3::from_transaction(tx, utxos, slot_config)?,
            script.as_ref(),
            datum,
            redeemer,
        )?),
    }
}