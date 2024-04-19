use pallas::ledger::{configs::shelley::GenesisFile, traverse::MultiEraBlock};

use super::LedgerSlice;

// Temporal workaround while we fix the GenesisValues mess we have in Pallas.
fn compute_epoch(genesis: &GenesisFile, block: &MultiEraBlock) -> u64 {
    let slot_length = genesis
        .slot_length
        .expect("shelley genesis didn't provide a slot length");

    let epoch_length = genesis
        .epoch_length
        .expect("shelley genesis didn't provide an epoch lenght");

    (block.slot() * slot_length as u64) / epoch_length as u64
}

fn ensure_pparams(&mut self, block: &MultiEraBlock) -> Result<(), WorkerError> {
    let epoch = self.compute_epoch(block);

    if self
        .current_pparams
        .as_ref()
        .is_some_and(|(current, _)| *current == epoch)
    {
        return Ok(());
    }

    let pparams = super::pparams::fold_pparams(
        crate::pparams::Genesis {
            byron: &self.byron,
            shelley: &self.shelley,
        },
        &self.ledger,
        epoch,
    )?;

    warn!(?pparams, "pparams for new epoch");

    let context = ValidationContext {
        block_slot: block.slot(),
        prot_magic: self.network_magic as u32,
        network_id: self.network_id,
        prot_params: pparams,
    };

    self.current_pparams = Some((epoch, context));

    Ok(())
}

pub fn execute_phase1_validation<'a>(
    block: &MultiEraBlock<'a>,
    ledger: impl LedgerSlice<'a>,
) -> Result<(), WorkerError> {
    let mut utxos2 = UTxOs::new();

    for (ref_, output) in utxos.iter() {
        let txin = pallas::ledger::primitives::byron::TxIn::Variant0(
            pallas::codec::utils::CborWrap((ref_.0, ref_.1 as u32)),
        );

        let key = MultiEraInput::Byron(
            <Box<Cow<'_, pallas::ledger::primitives::byron::TxIn>>>::from(Cow::Owned(txin)),
        );

        let era = Era::try_from(output.0).or_panic()?;
        let value = MultiEraOutput::decode(era, &output.1).or_panic()?;

        utxos2.insert(key, value);
    }

    let context = self
        .current_pparams
        .as_ref()
        .map(|(_, x)| x)
        .ok_or("no pparams available")
        .or_panic()?;

    for tx in block.txs().iter() {
        let res = validate(tx, &utxos2, &context);

        if let Err(err) = res {
            warn!(?err, "validation error");
        }
    }

    Ok(())
}
