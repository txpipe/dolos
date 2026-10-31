//! Field codecs that give a published value one encoding.
//!
//! The profile's value encoding (PROFILE.md, "Value encoding") puts map keys
//! in bytewise encoded-key order (RFC 8949 §4.2.1) and fixes the order of the
//! lists whose order means nothing. A Rust `BTreeMap` orders keys by their
//! `Ord`, which is not the encoded order once keys differ in length or the
//! type declares its variants out of index order, so the codecs here sort at
//! encode and refuse any other order at decode.

/// A `BTreeMap` encoded with its entries in bytewise encoded-key order.
///
/// Decode refuses keys that are out of that order or repeated, and a map of
/// indefinite length. The in-memory map keeps its own `Ord`.
pub mod canonical_map {
    use std::collections::BTreeMap;

    use pallas::codec::minicbor::{self, Decode, Decoder, Encode, Encoder};

    pub fn encode<Ctx, W, K, V>(
        map: &BTreeMap<K, V>,
        e: &mut Encoder<W>,
        ctx: &mut Ctx,
    ) -> Result<(), minicbor::encode::Error<W::Error>>
    where
        W: minicbor::encode::Write,
        K: Encode<Ctx>,
        V: Encode<Ctx>,
    {
        let mut entries = Vec::with_capacity(map.len());

        for (key, value) in map {
            let bytes = minicbor::to_vec_with(key, ctx)
                .map_err(|_| minicbor::encode::Error::message("map key does not encode"))?;
            entries.push((bytes, value));
        }

        entries.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

        e.map(entries.len() as u64)?;

        for (key, value) in entries {
            e.writer_mut()
                .write_all(&key)
                .map_err(minicbor::encode::Error::write)?;
            value.encode(e, ctx)?;
        }

        Ok(())
    }

    pub fn decode<'b, Ctx, K, V>(
        d: &mut Decoder<'b>,
        ctx: &mut Ctx,
    ) -> Result<BTreeMap<K, V>, minicbor::decode::Error>
    where
        K: Decode<'b, Ctx> + Ord,
        V: Decode<'b, Ctx>,
    {
        let Some(len) = d.map()? else {
            return Err(minicbor::decode::Error::message("map of indefinite length"));
        };

        let mut map = BTreeMap::new();
        let mut previous: Option<&'b [u8]> = None;

        for _ in 0..len {
            let start = d.position();
            let key = K::decode(d, ctx)?;
            let encoded = &d.input()[start..d.position()];

            if previous.is_some_and(|previous| previous >= encoded) {
                return Err(minicbor::decode::Error::message(
                    "map keys out of bytewise encoded-key order, or repeated",
                )
                .at(start));
            }

            previous = Some(encoded);

            let value = V::decode(d, ctx)?;
            map.insert(key, value);
        }

        Ok(map)
    }

    /// The same, for an `Option` of a map: `None` is CBOR null.
    pub mod option {
        use std::collections::BTreeMap;

        use pallas::codec::minicbor::{self, data::Type, Decode, Decoder, Encode, Encoder};

        pub fn encode<Ctx, W, K, V>(
            map: &Option<BTreeMap<K, V>>,
            e: &mut Encoder<W>,
            ctx: &mut Ctx,
        ) -> Result<(), minicbor::encode::Error<W::Error>>
        where
            W: minicbor::encode::Write,
            K: Encode<Ctx>,
            V: Encode<Ctx>,
        {
            match map {
                Some(map) => super::encode(map, e, ctx),
                None => e.null().map(|_| ()),
            }
        }

        pub fn decode<'b, Ctx, K, V>(
            d: &mut Decoder<'b>,
            ctx: &mut Ctx,
        ) -> Result<Option<BTreeMap<K, V>>, minicbor::decode::Error>
        where
            K: Decode<'b, Ctx> + Ord,
            V: Decode<'b, Ctx>,
        {
            if d.datatype()? == Type::Null {
                d.null()?;
                return Ok(None);
            }

            super::decode(d, ctx).map(Some)
        }

        pub fn nil<K, V>() -> Option<Option<BTreeMap<K, V>>> {
            Some(None)
        }

        pub fn is_nil<K, V>(map: &Option<BTreeMap<K, V>>) -> bool {
            map.is_none()
        }
    }
}

/// A list whose order carries no meaning, encoded sorted.
///
/// Decode refuses a list that is not sorted, and one of indefinite length.
pub mod sorted_list {
    use pallas::codec::minicbor::{self, Decode, Decoder, Encode, Encoder};

    pub fn encode<Ctx, W, T>(
        list: &[T],
        e: &mut Encoder<W>,
        ctx: &mut Ctx,
    ) -> Result<(), minicbor::encode::Error<W::Error>>
    where
        W: minicbor::encode::Write,
        T: Encode<Ctx> + Ord,
    {
        let mut sorted: Vec<&T> = list.iter().collect();
        sorted.sort_unstable();

        e.array(sorted.len() as u64)?;

        for item in sorted {
            item.encode(e, ctx)?;
        }

        Ok(())
    }

    pub fn decode<'b, Ctx, T>(
        d: &mut Decoder<'b>,
        ctx: &mut Ctx,
    ) -> Result<Vec<T>, minicbor::decode::Error>
    where
        T: Decode<'b, Ctx> + Ord,
    {
        let start = d.position();

        let Some(len) = d.array()? else {
            return Err(minicbor::decode::Error::message(
                "list of indefinite length",
            ));
        };

        let mut list: Vec<T> = Vec::new();

        for _ in 0..len {
            list.push(T::decode(d, ctx)?);
        }

        if !list.is_sorted() {
            return Err(minicbor::decode::Error::message("list not sorted").at(start));
        }

        Ok(list)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use pallas::{
        codec::minicbor::{self, Decode, Encode},
        ledger::primitives::{conway::DRep, StakeCredential},
    };

    #[derive(Debug, PartialEq, Eq, Encode, Decode)]
    struct Credentials {
        #[n(0)]
        #[cbor(with = "super::canonical_map")]
        map: BTreeMap<StakeCredential, u64>,
    }

    #[derive(Debug, PartialEq, Eq, Encode, Decode)]
    struct DReps {
        #[n(0)]
        #[cbor(with = "super::canonical_map")]
        map: BTreeMap<DRep, u64>,
    }

    #[derive(Debug, PartialEq, Eq, Encode, Decode)]
    struct MaybeMap {
        #[n(0)]
        #[cbor(default, with = "super::canonical_map::option", has_nil)]
        map: Option<BTreeMap<u64, u64>>,

        #[n(1)]
        tail: bool,
    }

    #[derive(Debug, PartialEq, Eq, Encode, Decode)]
    struct Sorted {
        #[n(0)]
        #[cbor(with = "super::sorted_list")]
        list: Vec<u64>,
    }

    fn key(byte: u8) -> StakeCredential {
        StakeCredential::AddrKeyhash([byte; 28].into())
    }

    fn script(byte: u8) -> StakeCredential {
        StakeCredential::ScriptHash([byte; 28].into())
    }

    /// Pallas orders `ScriptHash` first; the encoding puts `82 00…` first.
    #[test]
    fn credential_keys_encode_key_hashes_first() {
        let value = Credentials {
            map: BTreeMap::from([(script(1), 1), (key(2), 2)]),
        };

        let bytes = minicbor::to_vec(&value).unwrap();
        let first = bytes.windows(2).position(|w| w == [0x82, 0x00]).unwrap();
        let second = bytes.windows(2).position(|w| w == [0x82, 0x01]).unwrap();

        assert!(first < second, "{}", hex::encode(&bytes));
        assert_eq!(minicbor::decode::<Credentials>(&bytes).unwrap(), value);
    }

    /// `Abstain` encodes to `81 02`, shorter than a keyed DRep's `82 …`.
    #[test]
    fn drep_keys_encode_the_short_ones_first() {
        let value = DReps {
            map: BTreeMap::from([(DRep::Key([1; 28].into()), 1), (DRep::Abstain, 2)]),
        };

        let bytes = minicbor::to_vec(&value).unwrap();

        assert_eq!(
            &bytes[..4],
            [0x81, 0xa2, 0x81, 0x02],
            "{}",
            hex::encode(&bytes)
        );
        assert_eq!(minicbor::decode::<DReps>(&bytes).unwrap(), value);
    }

    #[test]
    fn keys_out_of_order_are_refused() {
        // [{[1, h1]: 1, [0, h2]: 2}]: the script hash before the key hash.
        let mut bytes = vec![0x81, 0xa2];
        bytes.extend(minicbor::to_vec(script(1)).unwrap());
        bytes.push(0x01);
        bytes.extend(minicbor::to_vec(key(2)).unwrap());
        bytes.push(0x02);

        let err = minicbor::decode::<Credentials>(&bytes).unwrap_err();
        assert!(err.to_string().contains("order"), "{err}");
    }

    #[test]
    fn repeated_keys_are_refused() {
        let mut bytes = vec![0x81, 0xa2];
        bytes.extend(minicbor::to_vec(key(1)).unwrap());
        bytes.push(0x01);
        bytes.extend(minicbor::to_vec(key(1)).unwrap());
        bytes.push(0x02);

        let err = minicbor::decode::<Credentials>(&bytes).unwrap_err();
        assert!(err.to_string().contains("repeated"), "{err}");
    }

    #[test]
    fn an_indefinite_map_is_refused() {
        let mut bytes = vec![0x81, 0xbf];
        bytes.extend(minicbor::to_vec(key(1)).unwrap());
        bytes.extend([0x01, 0xff]);

        assert!(minicbor::decode::<Credentials>(&bytes).is_err());
    }

    /// An optional map encodes as before: null when absent, the map when
    /// present, in the same order rules.
    #[test]
    fn an_optional_map_round_trips() {
        let absent = MaybeMap {
            map: None,
            tail: true,
        };
        let bytes = minicbor::to_vec(&absent).unwrap();
        assert_eq!(hex::encode(&bytes), "82f6f5");
        assert_eq!(minicbor::decode::<MaybeMap>(&bytes).unwrap(), absent);

        let present = MaybeMap {
            map: Some(BTreeMap::from([(24, 1), (1, 2)])),
            tail: true,
        };
        let bytes = minicbor::to_vec(&present).unwrap();
        assert_eq!(hex::encode(&bytes), "82a201021818 01f5".replace(' ', ""));
        assert_eq!(minicbor::decode::<MaybeMap>(&bytes).unwrap(), present);

        let unsorted = hex::decode("82a2181801 0102f5".replace(' ', "")).unwrap();
        assert!(minicbor::decode::<MaybeMap>(&unsorted).is_err());
    }

    #[test]
    fn a_list_encodes_sorted() {
        let value = Sorted {
            list: vec![3, 1, 2],
        };

        let bytes = minicbor::to_vec(&value).unwrap();

        assert_eq!(hex::encode(&bytes), "8183010203");
        assert_eq!(
            minicbor::decode::<Sorted>(&bytes).unwrap().list,
            vec![1, 2, 3]
        );
    }

    #[test]
    fn an_unsorted_list_is_refused() {
        let err = minicbor::decode::<Sorted>(&hex::decode("8183020103").unwrap()).unwrap_err();
        assert!(err.to_string().contains("not sorted"), "{err}");
    }

    fn cbor<T: Encode<()>>(value: T) -> Vec<u8> {
        minicbor::to_vec(value).unwrap()
    }

    /// `Committee.members`, one of the credential-keyed maps: written in
    /// encoded-key order, refused in Pallas order.
    #[test]
    fn committee_members_follow_the_encoded_key_order() {
        use crate::model::Committee;
        use pallas::ledger::primitives::RationalNumber;

        let threshold = RationalNumber {
            numerator: 2,
            denominator: 3,
        };

        let committee = Committee {
            members: BTreeMap::from([(script(1), 500), (key(2), 600)]),
            threshold: threshold.clone(),
        };

        let mut expected = vec![0x82, 0xa2];
        expected.extend(cbor(key(2)));
        expected.extend(cbor(600u64));
        expected.extend(cbor(script(1)));
        expected.extend(cbor(500u64));
        expected.extend(cbor(&threshold));

        assert_eq!(hex::encode(cbor(&committee)), hex::encode(&expected));
        assert_eq!(minicbor::decode::<Committee>(&expected).unwrap(), committee);

        let mut pallas_order = vec![0x82, 0xa2];
        pallas_order.extend(cbor(script(1)));
        pallas_order.extend(cbor(500u64));
        pallas_order.extend(cbor(key(2)));
        pallas_order.extend(cbor(600u64));
        pallas_order.extend(cbor(&threshold));

        assert!(minicbor::decode::<Committee>(&pallas_order).is_err());
    }

    #[test]
    fn pparams_encode_by_kind_and_refuse_any_other_order() {
        use crate::model::{PParamKind, PParamValue, PParamsSet};

        let mut set = PParamsSet::default();
        set.set(PParamValue::MinFeeB(155_381));
        set.set(PParamValue::KeyDeposit(2_000_000));
        set.set(PParamValue::MinFeeA(44));
        set.set(PParamValue::MinFeeB(155_382));

        let kinds: Vec<PParamKind> = set.iter().map(PParamValue::kind).collect();
        assert_eq!(
            kinds,
            [
                PParamKind::MinFeeA,
                PParamKind::MinFeeB,
                PParamKind::KeyDeposit
            ]
        );

        let list = |values: &[PParamValue]| {
            let mut bytes = vec![0x81, 0x80 + values.len() as u8];
            for value in values {
                bytes.extend(cbor(value));
            }
            bytes
        };

        let sorted = list(&[
            PParamValue::MinFeeA(44),
            PParamValue::MinFeeB(155_382),
            PParamValue::KeyDeposit(2_000_000),
        ]);
        assert_eq!(hex::encode(cbor(&set)), hex::encode(&sorted));
        assert_eq!(minicbor::decode::<PParamsSet>(&sorted).unwrap(), set);

        let unsorted = list(&[PParamValue::MinFeeB(155_382), PParamValue::MinFeeA(44)]);
        let repeated = list(&[PParamValue::MinFeeA(44), PParamValue::MinFeeA(45)]);

        for bytes in [unsorted, repeated] {
            let err = minicbor::decode::<PParamsSet>(&bytes).unwrap_err();
            assert!(err.to_string().contains("pparams"), "{err}");
        }
    }

    #[test]
    fn account_epoch_lists_encode_sorted_and_refuse_unsorted() {
        use crate::model::{AccountEpochLog, PoolHash};

        let pool = |byte: u8| PoolHash::from([byte; 28]);

        let row = AccountEpochLog {
            active_stake: Some(1),
            pool_id: Some(pool(1)),
            member_reward: Some(2),
            leader_rewards: vec![(pool(9), 30), (pool(3), 10)],
            deposit_refunds: vec![(pool(8), 500), (pool(2), 500)],
        };

        let mut sorted = row.clone();
        sorted.sort();

        let bytes = cbor(&row);
        assert_eq!(hex::encode(&bytes), hex::encode(cbor(&sorted)));
        assert_eq!(minicbor::decode::<AccountEpochLog>(&bytes).unwrap(), sorted);

        let mut unsorted = vec![0x85, 0x01];
        unsorted.extend(cbor(pool(1)));
        unsorted.push(0x02);
        unsorted.extend(cbor(vec![(pool(9), 30u64), (pool(3), 10u64)]));
        unsorted.push(0x80);

        let err = minicbor::decode::<AccountEpochLog>(&unsorted).unwrap_err();
        assert!(err.to_string().contains("not sorted"), "{err}");
    }

    #[test]
    fn a_sum_has_one_encoding() {
        use crate::model::Sum;

        let past_u64 = u64::MAX as u128 + 1;
        let canonical = hex::decode("c249010000000000000000").unwrap();

        assert_eq!(cbor(Sum(past_u64)), canonical);
        assert_eq!(minicbor::decode::<Sum>(&canonical).unwrap(), Sum(past_u64));
        assert_eq!(cbor(Sum(7)), [0x07]);

        for (bignum, refusal) in [
            ("c24107", "fits in u64"),
            ("c248ffffffffffffffff", "fits in u64"),
            ("c240", "fits in u64"),
            ("c24a00010000000000000000", "leading zeros"),
        ] {
            let err = minicbor::decode::<Sum>(&hex::decode(bignum).unwrap()).unwrap_err();
            assert!(err.to_string().contains(refusal), "{bignum}: {err}");
        }
    }
}
