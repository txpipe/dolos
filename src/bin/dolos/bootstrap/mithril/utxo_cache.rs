use std::collections::HashMap;
use std::sync::Arc;

use dolos_redb3::redb::{backends::InMemoryBackend, Database, ReadableDatabase, TableDefinition};
use miette::IntoDiagnostic;
use pallas::ledger::traverse::MultiEraBlock;

use dolos::prelude::*;
use dolos_cardano::owned::OwnedMultiEraOutput;

type UtxosKey = (&'static [u8; 32], u32);
type UtxosValue = (u16, &'static [u8]);

const UTXOS_TABLE: TableDefinition<'static, UtxosKey, UtxosValue> = TableDefinition::new("utxos");

pub(super) struct UtxoCache {
    db: Database,
}

impl UtxoCache {
    pub(super) fn in_memory() -> miette::Result<Self> {
        let db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .into_diagnostic()?;
        let wx = db.begin_write().into_diagnostic()?;
        wx.open_table(UTXOS_TABLE).into_diagnostic()?;
        wx.commit().into_diagnostic()?;

        Ok(Self { db })
    }

    pub(super) fn insert_block_outputs(&self, block: &MultiEraBlock) -> miette::Result<()> {
        let wx = self.db.begin_write().into_diagnostic()?;
        {
            let mut table = wx.open_table(UTXOS_TABLE).into_diagnostic()?;

            for tx in block.txs() {
                let tx_hash = tx.hash();

                for (idx, output) in tx.produces() {
                    let txo_ref = TxoRef(tx_hash, idx as u32);
                    let eracbor: EraCbor = output.into();
                    let key: (&[u8; 32], u32) = (&txo_ref.0, txo_ref.1);
                    table
                        .insert(key, (eracbor.0, eracbor.1.as_slice()))
                        .into_diagnostic()?;
                }
            }
        }

        wx.commit().into_diagnostic()?;

        Ok(())
    }

    pub(super) fn resolve_block_inputs(
        &self,
        block: &MultiEraBlock,
    ) -> miette::Result<HashMap<TxoRef, OwnedMultiEraOutput>> {
        let rx = self.db.begin_read().into_diagnostic()?;
        let table = rx.open_table(UTXOS_TABLE).into_diagnostic()?;
        let mut resolved_inputs = HashMap::new();

        for tx in block.txs() {
            for input in tx.consumes() {
                let txo_ref: TxoRef = (&input).into();

                if resolved_inputs.contains_key(&txo_ref) {
                    continue;
                }

                let key: (&[u8; 32], u32) = (&txo_ref.0, txo_ref.1);
                if let Some(body) = table.get(key).into_diagnostic()? {
                    let (era, cbor) = body.value();
                    let eracbor = EraCbor(era, cbor.to_vec());
                    let resolved =
                        OwnedMultiEraOutput::decode(Arc::new(eracbor)).into_diagnostic()?;

                    resolved_inputs.insert(txo_ref, resolved);
                }
            }
        }

        Ok(resolved_inputs)
    }

    pub(super) fn remove_block_inputs(&self, block: &MultiEraBlock) -> miette::Result<()> {
        let wx = self.db.begin_write().into_diagnostic()?;
        {
            let mut table = wx.open_table(UTXOS_TABLE).into_diagnostic()?;

            for tx in block.txs() {
                for input in tx.consumes() {
                    let txo_ref: TxoRef = (&input).into();
                    let key: (&[u8; 32], u32) = (&txo_ref.0, txo_ref.1);
                    table.remove(key).into_diagnostic()?;
                }
            }
        }

        wx.commit().into_diagnostic()?;

        Ok(())
    }
}
