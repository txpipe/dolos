use std::ops::Deref;

use axum::http::StatusCode;
use blockfrost_openapi::models::asset_utxo_content_inner::AssetUtxoContentInner;
use pallas::{
    codec::minicbor,
    ledger::{primitives::conway::DatumOption, traverse::OriginalHash},
};

use super::{IntoModel, PlutusDataWrapper, UtxoOutputModelBuilder};

impl<'a> IntoModel<AssetUtxoContentInner> for UtxoOutputModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<AssetUtxoContentInner, StatusCode> {
        let inline_datum = self.output.datum().and_then(|x| match x {
            DatumOption::Hash(_) => None,
            DatumOption::Data(x) => Some(x),
        });

        let inline_datum_json = inline_datum
            .as_ref()
            .map(|x| PlutusDataWrapper(x.0.deref().clone()).as_value())
            .transpose()?;

        // the model requires the creating block; callers only build it for
        // outputs whose block the archive still holds.
        let block = self
            .block_data
            .as_ref()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
        let block_time = self.block_time.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let out = AssetUtxoContentInner {
            address: self.output.address().into_model()?,
            tx_hash: self.txo_ref.0.to_string(),
            output_index: try_into_or_500!(self.txo_ref.1),
            amount: self.output.value().into_model()?,
            block: block.hash.to_string(),
            block_height: try_into_or_500!(block.height),
            block_time: try_into_or_500!(block_time),
            data_hash: self.output.datum().map(|x| match x {
                DatumOption::Hash(x) => x.to_string(),
                DatumOption::Data(x) => x.original_hash().to_string(),
            }),
            // re-encode the raw datum so the hex matches the on-chain bytes
            inline_datum: inline_datum
                .map(|x| minicbor::to_vec(&x.0))
                .transpose()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .map(hex::encode),
            inline_datum_json,
            reference_script_hash: self
                .output
                .script_ref()
                .map(|h| h.into_model())
                .transpose()?,
        };

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_core::async_query::BlockRefMeta;
    use pallas::codec::utils::{CborWrap, KeepRaw};
    use pallas::crypto::hash::Hash;
    use pallas::ledger::primitives::conway::{PostAlonzoTransactionOutput, Value};
    use pallas::ledger::primitives::PlutusData;
    use pallas::ledger::traverse::Era;
    use pallas::ledger::traverse::MultiEraOutput;

    fn output_bytes() -> Vec<u8> {
        let output = PostAlonzoTransactionOutput {
            address: vec![0x60; 29].into(),
            value: Value::Coin(1_000_000),
            datum_option: None,
            script_ref: None,
        };

        minicbor::to_vec(&output).unwrap()
    }

    fn output_with_inline_datum_bytes() -> Vec<u8> {
        let datum = PlutusData::BigInt(pallas::ledger::primitives::BigInt::Int(42.into()));
        let output = PostAlonzoTransactionOutput {
            address: vec![0x60; 29].into(),
            value: Value::Coin(1_000_000),
            datum_option: Some(KeepRaw::from(DatumOption::Data(CborWrap(KeepRaw::from(
                datum,
            ))))),
            script_ref: None,
        };

        minicbor::to_vec(&output).unwrap()
    }

    /// The asset UTxO model carries the block height and time and decodes the
    /// inline datum into JSON next to its CBOR.
    #[test]
    fn asset_utxo_model_exposes_block_position_and_datum_json() {
        let bytes = output_with_inline_datum_bytes();
        let output = MultiEraOutput::decode(Era::Conway, &bytes).unwrap();

        let builder = UtxoOutputModelBuilder::from_output(Hash::from([0xaa; 32]), 3, output)
            .with_block_data(BlockRefMeta {
                slot: 100,
                hash: Hash::from([0x11; 32]),
                height: 7,
                tx_hash: Hash::from([0xaa; 32]),
                tx_index: 2,
            })
            .with_block_time(1_700_000_000);

        let model: AssetUtxoContentInner = builder.into_model().unwrap();

        assert_eq!(model.tx_hash, Hash::from([0xaa; 32]).to_string());
        assert_eq!(model.output_index, 3);
        assert_eq!(model.block, Hash::from([0x11; 32]).to_string());
        assert_eq!(model.block_height, 7);
        assert_eq!(model.block_time, 1_700_000_000);
        assert_eq!(model.inline_datum.as_deref(), Some("182a"));
        assert_eq!(
            model.inline_datum_json,
            Some(serde_json::json!({ "int": 42 }))
        );
        assert!(model.data_hash.is_some());
        assert_eq!(model.reference_script_hash, None);
    }

    /// The model never reports made-up block data: an output whose creation
    /// block was pruned is a caller bug here, not a row with zeroed fields.
    #[test]
    fn asset_utxo_model_refuses_missing_block_data() {
        let bytes = output_bytes();
        let output = MultiEraOutput::decode(Era::Conway, &bytes).unwrap();
        let builder = UtxoOutputModelBuilder::from_output(Hash::from([0xaa; 32]), 0, output);

        let model: Result<AssetUtxoContentInner, _> = builder.into_model();
        assert_eq!(model, Err(StatusCode::INTERNAL_SERVER_ERROR));
    }
}
