use crate::prelude::*;
use dolos_core::{IndexStore, StateStore};
use pallas::codec::utils::{AnyCbor, AnyUInt, KeyValuePairs};
use pallas::ledger::addresses::Address;
use pallas::ledger::primitives::conway::DatumOption;
use pallas::ledger::primitives::{BigInt as LedgerBigInt, PlutusData};
use pallas::ledger::traverse::{Era, MultiEraOutput, OriginalHash};
use pallas::network::miniprotocols::localstate::queries_v16 as q16;
use std::collections::HashSet;
use tracing::debug;

pub fn build_utxo_by_address_response<D: Domain>(
    domain: &D,
    addrs: &q16::Addrs,
) -> Result<AnyCbor, Error> {
    let mut utxo_pairs: Vec<(q16::UTxO, q16::TransactionOutput)> = Vec::new();
    let mut all_refs = HashSet::new();

    for addr in addrs.iter() {
        let addr_bytes: &[u8] = addr.as_ref();
        debug!(addr_len = addr_bytes.len(), addr_hex = %hex::encode(addr_bytes), "looking up utxos for address");

        let mut refs = domain
            .indexes()
            .utxos_by_tag("address", addr_bytes)
            .map_err(|e| Error::server(format!("failed to get utxos by address: {}", e)))?;

        debug!(num_refs = refs.len(), "found utxo refs by full address");

        if refs.is_empty() {
            if let Ok(Address::Shelley(shelley_addr)) = Address::from_bytes(addr_bytes) {
                let payment_bytes = shelley_addr.payment().to_vec();
                debug!(payment_hex = %hex::encode(&payment_bytes), "trying payment credential lookup");
                refs = domain
                    .indexes()
                    .utxos_by_tag("payment", &payment_bytes)
                    .map_err(|e| Error::server(format!("failed to get utxos by payment: {}", e)))?;
                debug!(
                    num_refs = refs.len(),
                    "found utxo refs by payment credential"
                );
            }
        }

        all_refs.extend(refs);
    }

    debug!(
        total_refs = all_refs.len(),
        "total unique utxo refs to fetch"
    );

    let refs_vec: Vec<_> = all_refs.into_iter().collect();
    let utxos = domain
        .state()
        .get_utxos(refs_vec.clone())
        .map_err(|e| Error::server(format!("failed to get utxos: {}", e)))?;

    debug!(fetched_utxos = utxos.len(), "fetched utxo data");

    for utxo_ref in refs_vec {
        if let Some(era_cbor) = utxos.get(&utxo_ref) {
            let output = MultiEraOutput::try_from(era_cbor.as_ref())
                .map_err(|e| Error::server(format!("failed to decode utxo: {}", e)))?;
            let q16_utxo = q16::UTxO {
                transaction_id: utxo_ref.0,
                index: AnyUInt::U32(utxo_ref.1),
            };

            let q16_output = convert_output_to_q16(&output)?;
            utxo_pairs.push((q16_utxo, q16_output));
        }
    }

    debug!(num_utxos = utxo_pairs.len(), "returning utxos");

    let response: KeyValuePairs<q16::UTxO, q16::TransactionOutput> = KeyValuePairs::Def(utxo_pairs);

    Ok(AnyCbor::from_encode((response,)))
}

fn convert_output_to_q16(output: &MultiEraOutput) -> Result<q16::TransactionOutput, Error> {
    use pallas::codec::utils::NonEmptyKeyValuePairs;

    let address = output.address().map_err(Error::server)?.to_vec();
    let value_data = output.value();
    let lovelace = AnyUInt::U64(value_data.coin());

    let assets = value_data.assets();
    let has_assets = !assets.is_empty();

    let value = if has_assets {
        let mut policy_map: Vec<(
            pallas::crypto::hash::Hash<28>,
            NonEmptyKeyValuePairs<pallas::codec::utils::Bytes, AnyUInt>,
        )> = vec![];

        for policy_assets in assets {
            let policy_id = *policy_assets.policy();
            let mut asset_entries: Vec<(pallas::codec::utils::Bytes, AnyUInt)> = vec![];

            for asset in policy_assets.assets() {
                let name = asset.name();
                let amount = asset.output_coin().unwrap_or(0);
                asset_entries.push((name.to_vec().into(), AnyUInt::U64(amount)));
            }

            if !asset_entries.is_empty() {
                policy_map.push((policy_id, NonEmptyKeyValuePairs::Def(asset_entries)));
            }
        }

        if policy_map.is_empty() {
            q16::Value::Coin(lovelace)
        } else {
            q16::Value::Multiasset(lovelace, NonEmptyKeyValuePairs::Def(policy_map))
        }
    } else {
        q16::Value::Coin(lovelace)
    };

    let inline_datum = output.datum().map(|d| match d {
        DatumOption::Hash(h) => q16::DatumOption::Hash(h),
        DatumOption::Data(data) => {
            q16::DatumOption::Data(pallas::codec::utils::CborWrap(convert_plutus_data(&data.0)))
        }
    });

    let datum_hash = output.datum().map(|d| match d {
        DatumOption::Hash(h) => h,
        DatumOption::Data(data) => data.original_hash(),
    });

    if output.era() >= Era::Alonzo {
        Ok(q16::TransactionOutput::Current(
            q16::PostAlonsoTransactionOutput {
                address: address.into(),
                amount: value,
                inline_datum,
                script_ref: None,
            },
        ))
    } else {
        Ok(q16::TransactionOutput::Legacy(
            q16::LegacyTransactionOutput {
                address: address.into(),
                amount: value,
                datum_hash,
            },
        ))
    }
}

fn convert_plutus_data(data: &PlutusData) -> q16::PlutusData {
    match data {
        PlutusData::Constr(constr) => {
            let fields = constr
                .fields
                .iter()
                .map(convert_plutus_data)
                .collect::<Vec<_>>();
            q16::PlutusData::Constr(q16::Constr {
                tag: constr.tag,
                any_constructor: constr.any_constructor,
                fields: pallas::codec::utils::MaybeIndefArray::Indef(fields),
            })
        }
        PlutusData::Map(kvs) => {
            let mapped = kvs
                .iter()
                .map(|(k, v)| (convert_plutus_data(k), convert_plutus_data(v)))
                .collect::<Vec<_>>();
            q16::PlutusData::Map(KeyValuePairs::Def(mapped))
        }
        PlutusData::BigInt(bi) => match bi {
            LedgerBigInt::Int(i) => q16::PlutusData::BigInt(q16::BigInt::Int(*i)),
            LedgerBigInt::BigUInt(bytes) => {
                let raw: Vec<u8> = bytes.clone().into();
                q16::PlutusData::BigInt(q16::BigInt::BigUInt(raw.into()))
            }
            LedgerBigInt::BigNInt(bytes) => {
                let raw: Vec<u8> = bytes.clone().into();
                q16::PlutusData::BigInt(q16::BigInt::BigNInt(raw.into()))
            }
        },
        PlutusData::BoundedBytes(bytes) => {
            let raw: Vec<u8> = bytes.clone().into();
            q16::PlutusData::BoundedBytes(raw.into())
        }
        PlutusData::Array(arr) => {
            let items = arr.iter().map(convert_plutus_data).collect::<Vec<_>>();
            q16::PlutusData::Array(pallas::codec::utils::MaybeIndefArray::Indef(items))
        }
    }
}
