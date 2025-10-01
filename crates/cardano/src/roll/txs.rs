use dolos_core::{batch::WorkDeltas, ChainError, SlotTags, TxoRef};
use pallas::{
    codec::{minicbor, utils::KeepRaw},
    ledger::{
        addresses::Address,
        primitives::{conway::DatumOption, Epoch, PlutusData},
        traverse::{
            ComputeHash, MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraRedeemer, MultiEraTx,
            MultiEraValue, OriginalHash as _,
        },
    },
};

use crate::{pallas_extras, roll::BlockVisitor, CardanoLogic, PParamsSet};

#[derive(Default, Clone)]
pub struct TxLogVisitor;

fn unpack_input(tags: &mut SlotTags, input: &MultiEraInput) {
    let txoref: TxoRef = input.into();
    tags.spent_txo.push(txoref.into());
}

fn unpack_address(tags: &mut SlotTags, address: &Address) {
    match address {
        Address::Shelley(x) => {
            tags.full_addresses.push(x.to_vec());
            tags.payment_addresses.push(x.payment().to_vec());

            if let Some(stake) = pallas_extras::shelley_address_to_stake_address(x) {
                tags.stake_addresses.push(stake.to_vec());
            }
        }
        Address::Stake(x) => {
            tags.full_addresses.push(x.to_vec());
            tags.stake_addresses.push(x.to_vec());
        }
        Address::Byron(x) => {
            tags.full_addresses.push(x.to_vec());
        }
    }
}

fn unpack_assets(tags: &mut SlotTags, assets: &MultiEraValue) {
    let assets = assets.assets();

    for ma in assets {
        tags.policies.push(ma.policy().to_vec());

        for asset in ma.assets() {
            let mut subject = asset.policy().to_vec();
            subject.extend(asset.name());

            tags.assets.push(subject);
        }
    }
}

fn unpack_datum(tags: &mut SlotTags, datum: &DatumOption) {
    match datum {
        DatumOption::Hash(hash) => {
            tags.datums.push(hash.to_vec());
        }
        DatumOption::Data(datum) => {
            tags.datums.push(datum.original_hash().to_vec());
        }
    }
}

fn unpack_cert(tags: &mut SlotTags, cert: &MultiEraCert) {
    if let Some(cred) = pallas_extras::cert_as_stake_registration(cert) {
        tags.account_certs.push(minicbor::to_vec(&cred).unwrap());
    }

    if let Some(cred) = pallas_extras::cert_as_stake_deregistration(cert) {
        tags.account_certs.push(minicbor::to_vec(&cred).unwrap());
    }

    if let Some(deleg) = pallas_extras::cert_as_stake_delegation(cert) {
        tags.account_certs
            .push(minicbor::to_vec(&deleg.delegator).unwrap());
    }
}

impl BlockVisitor for TxLogVisitor {
    fn visit_root(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        _: &PParamsSet,
        _: Epoch,
    ) -> Result<(), ChainError> {
        deltas.slot.number = Some(block.number());

        Ok(())
    }

    fn visit_tx(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        tx: &MultiEraTx,
    ) -> Result<(), ChainError> {
        deltas.slot.tx_hashes.push(tx.hash().to_vec());
        for (k, _) in tx.metadata().collect::<Vec<_>>() {
            deltas.slot.metadata.push(k);
        }

        Ok(())
    }

    fn visit_input(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        input: &MultiEraInput,
        resolved: &pallas::ledger::traverse::MultiEraOutput,
    ) -> Result<(), ChainError> {
        unpack_input(&mut deltas.slot, input);
        if let Ok(address) = resolved.address() {
            unpack_address(&mut deltas.slot, &address);
        }

        unpack_assets(&mut deltas.slot, &resolved.value());

        if let Some(datum) = resolved.datum() {
            unpack_datum(&mut deltas.slot, &datum);
        }

        Ok(())
    }

    fn visit_output(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: u32,
        output: &pallas::ledger::traverse::MultiEraOutput,
    ) -> Result<(), ChainError> {
        if let Ok(address) = output.address() {
            unpack_address(&mut deltas.slot, &address);
        }

        unpack_assets(&mut deltas.slot, &output.value());

        if let Some(datum) = output.datum() {
            unpack_datum(&mut deltas.slot, &datum);
        }

        Ok(())
    }

    fn visit_datums(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        datum: &KeepRaw<'_, PlutusData>,
    ) -> Result<(), ChainError> {
        deltas.slot.datums.push(datum.original_hash().to_vec());

        Ok(())
    }

    fn visit_cert(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &pallas::ledger::traverse::MultiEraCert,
    ) -> Result<(), ChainError> {
        unpack_cert(&mut deltas.slot, cert);

        Ok(())
    }

    fn visit_redeemers(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        redeemer: &MultiEraRedeemer,
    ) -> Result<(), ChainError> {
        // TODO: We should use a KeepRaw structure and original_hash
        deltas
            .slot
            .datums
            .push(redeemer.data().compute_hash().to_vec());

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use pallas::ledger::addresses::{
        Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
    };

    use crate::pallas_extras::shelley_address_to_stake_address;

    use super::*;

    #[test]
    fn test_unpack_address() {
        let mut tags = SlotTags::default();
        let shelley_address = ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::Key([1; 28].as_slice().into()),
            ShelleyDelegationPart::Key([2; 28].as_slice().into()),
        );
        let stake = shelley_address_to_stake_address(&shelley_address).unwrap();
        let address = Address::Shelley(shelley_address);
        unpack_address(&mut tags, &address);

        assert_eq!(tags.full_addresses.len(), 1);
        assert_eq!(tags.payment_addresses.len(), 1);
        assert_eq!(tags.stake_addresses.len(), 1);

        let mut other = SlotTags::default();
        let address = Address::Stake(stake);
        unpack_address(&mut other, &address);

        assert_eq!(other.full_addresses.len(), 1);
        assert_eq!(other.payment_addresses.len(), 0);
        assert_eq!(other.stake_addresses.len(), 1);

        // Two addresses with same stake part index that same thing
        assert_eq!(tags.stake_addresses.first(), other.stake_addresses.first());
    }
}
