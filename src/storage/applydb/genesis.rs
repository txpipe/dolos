use pallas::ledger::addresses::ByronAddress;
use pallas::ledger::configs::byron::GenesisUtxo;
use pallas::ledger::primitives::byron::TxOut;
use rocksdb::WriteBatch;
use tracing::info;

use crate::{
    prelude::*,
    storage::kvtable::{DBBytes, DBSerde, KVTable},
};

use super::{ApplyDB, UtxoKV, UtxoRef};

fn build_byron_txout(addr: ByronAddress, amount: u64) -> TxOut {
    TxOut {
        address: pallas::ledger::primitives::byron::Address {
            payload: addr.payload,
            crc: addr.crc,
        },
        amount,
    }
}

fn genesis_utxo_to_kv(utxo: GenesisUtxo) -> Result<(DBSerde<UtxoRef>, DBBytes), Error> {
    let (tx, addr, amount) = utxo;

    let key = DBSerde(UtxoRef(tx, 0));

    let txout = build_byron_txout(addr, amount);
    let txout = pallas::codec::minicbor::to_vec(txout).map_err(Error::config)?;
    let value = DBBytes(txout);

    Ok((key, value))
}

impl ApplyDB {
    pub fn insert_genesis_utxos(
        &self,
        byron: &pallas::ledger::configs::byron::GenesisFile,
    ) -> Result<(), Error> {
        let batch = pallas::ledger::configs::byron::genesis_utxos(&byron)
            .into_iter()
            .map(genesis_utxo_to_kv)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .fold(WriteBatch::default(), |mut batch, (k, v)| {
                info!(tx = %k.0 .0, "inserting genesis utxo");
                UtxoKV::stage_upsert(&self.db, k, v, &mut batch);
                batch
            });

        self.db.write(batch).map_err(Error::storage)
    }
}

#[cfg(test)]
mod tests {
    use pallas::crypto::hash::Hash;
    use std::str::FromStr;

    use super::*;
    use crate::storage::applydb::tests::with_tmp_db;

    fn assert_genesis_utxo_exists(db: &ApplyDB, tx_hex: &str, addr_base58: &str, amount: u64) {
        let tx = Hash::<32>::from_str(tx_hex).unwrap();

        let cbor = db.get_utxo(tx, 0).unwrap();

        assert!(cbor.is_some(), "utxo not found");
        let cbor = cbor.unwrap();

        let txout: Result<pallas::ledger::primitives::byron::TxOut, _> =
            pallas::codec::minicbor::decode(&cbor);

        assert!(txout.is_ok(), "can't parse utxo cbor");
        let txout = txout.unwrap();

        assert_eq!(txout.amount, amount, "utxo amount doesn't match");

        let addr = pallas::ledger::addresses::ByronAddress::new(
            txout.address.payload.as_ref(),
            txout.address.crc,
        );

        assert_eq!(addr.to_base58(), addr_base58);
    }

    #[test]
    fn test_mainnet_genesis_utxos() {
        with_tmp_db(|db| {
            let path = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
                .join("examples")
                .join("sync-mainnet")
                .join("byron.json");

            let byron = pallas::ledger::configs::byron::from_file(&path).unwrap();
            db.insert_genesis_utxos(&byron).unwrap();

            assert_genesis_utxo_exists(
                &db,
                "0ae3da29711600e94a33fb7441d2e76876a9a1e98b5ebdefbf2e3bc535617616",
                "Ae2tdPwUPEZKQuZh2UndEoTKEakMYHGNjJVYmNZgJk2qqgHouxDsA5oT83n",
                2_463_071_701_000_000,
            )
        });
    }

    #[test]
    fn test_preview_genesis_utxos() {
        with_tmp_db(|db| {
            let path = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
                .join("examples")
                .join("sync-preview")
                .join("byron.json");

            let byron = pallas::ledger::configs::byron::from_file(&path).unwrap();
            db.insert_genesis_utxos(&byron).unwrap();

            assert_genesis_utxo_exists(
                &db,
                "4843cf2e582b2f9ce37600e5ab4cc678991f988f8780fed05407f9537f7712bd",
                "FHnt4NL7yPXvDWHa8bVs73UEUdJd64VxWXSFNqetECtYfTd9TtJguJ14Lu3feth",
                30_000_000_000_000_000,
            );
        });
    }
}
