use super::script_context::TxInInfo;
use pallas::{
    codec::minicbor::{self, to_vec, Encode},
    interop::utxorpc::spec::query::any_chain_params::Params,
    ledger::{primitives::PlutusData, traverse::MultiEraTx},
};
use uplc::{bumpalo::Bump, data::PlutusData as PragmaPlutusData};

pub fn map_pragma_data_to_pallas_data<'a>(
    arena: &'a Bump,
    data: PlutusData,
) -> &'a PragmaPlutusData<'a> {
    let bytes = to_vec(&data).expect("failed to encode data");
    PragmaPlutusData::from_cbor(arena, &bytes).expect("failed to decode data")
}

// pub fn eval_tx(tx: MultiEraTx, protocol_params: Params, resolved_inputs: Vec<TxInInfo>) {
//     let redeemers = tx.redeemers();

//     let results = redeemers
//         .iter()
//         .map(|r| match r {
//             pallas::ledger::traverse::MultiEraRedeemer::AlonzoCompatible(r) => match r.tag {
//                 pallas::ledger::primitives::alonzo::RedeemerTag::Spend => {
//                     let redeemer_index = r.index;
//                     let redeemer_data = r.data;
//                 }
//                 pallas::ledger::primitives::alonzo::RedeemerTag::Mint => todo!(),
//                 pallas::ledger::primitives::alonzo::RedeemerTag::Cert => todo!(),
//                 pallas::ledger::primitives::alonzo::RedeemerTag::Reward => todo!(),
//             },
//             pallas::ledger::traverse::MultiEraRedeemer::Conway(r_keys, r_values) => todo!(),
//             _ => todo!(),
//         })
//         .collect();
// }
