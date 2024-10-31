use pallas::ledger::primitives::{Constr, PlutusData};

pub struct Data;

// TODO: See about moving these builders upstream to Pallas?
impl Data {
    pub fn to_hex(data: PlutusData) -> String {
        let mut bytes = Vec::new();
        pallas::codec::minicbor::Encoder::new(&mut bytes)
            .encode(data)
            .expect("failed to encode Plutus Data as cbor?");
        hex::encode(bytes)
    }
    // pub fn integer(i: BigInt) -> PlutusData {
    //     match i.to_i128().map(|n| n.try_into()) {
    //         Some(Ok(i)) => PlutusData::BigInt(alonzo::BigInt::Int(i)),
    //         _ => {
    //             let (sign, bytes) = i.to_bytes_be();
    //             match sign {
    //                 num_bigint::Sign::Minus => {
    //                     PlutusData::BigInt(alonzo::BigInt::BigNInt(bytes.into()))
    //                 }
    //                 _ => PlutusData::BigInt(alonzo::BigInt::BigUInt(bytes.into())),
    //             }
    //         }
    //     }
    // }

    pub fn bytestring(bytes: Vec<u8>) -> PlutusData {
        PlutusData::BoundedBytes(bytes.into())
    }

    pub fn map(kvs: Vec<(PlutusData, PlutusData)>) -> PlutusData {
        PlutusData::Map(kvs.into())
    }

    pub fn list(xs: Vec<PlutusData>) -> PlutusData {
        PlutusData::Array(pallas::codec::utils::MaybeIndefArray::Def(xs))
    }

    pub fn constr(ix: u64, fields: Vec<PlutusData>) -> PlutusData {
        // NOTE: see https://github.com/input-output-hk/plutus/blob/9538fc9829426b2ecb0628d352e2d7af96ec8204/plutus-core/plutus-core/src/PlutusCore/Data.hs#L139-L155
        if ix < 7 {
            PlutusData::Constr(Constr {
                tag: 121 + ix,
                any_constructor: None,
                fields: pallas::codec::utils::MaybeIndefArray::Def(fields),
            })
        } else if ix < 128 {
            PlutusData::Constr(Constr {
                tag: 1280 + ix - 7,
                any_constructor: None,
                fields: pallas::codec::utils::MaybeIndefArray::Def(fields),
            })
        } else {
            PlutusData::Constr(Constr {
                tag: 102,
                any_constructor: Some(ix),
                fields: pallas::codec::utils::MaybeIndefArray::Def(fields),
            })
        }
    }
}