use dolos_core::{State3Error, State3Store};
use pallas::ledger::{
    addresses::{Address, StakeAddress},
    primitives::StakeCredential,
    traverse::{MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraOutput, MultiEraTx},
};
use tracing::debug;

use crate::{
    model::{AccountActivity, AccountState, PoolDelegator},
    pallas_extras,
    roll::{BlockVisitor, DeltaBuilder, SliceBuilder},
};

pub struct SeenAddressesVisitor<'a, T>(&'a mut T);

impl<'a, T> From<&'a mut T> for SeenAddressesVisitor<'a, T> {
    fn from(value: &'a mut T) -> Self {
        Self(value)
    }
}

impl<T> SeenAddressesVisitor<'_, T> {
    fn extract_address(output: &MultiEraOutput) -> Option<(StakeAddress, Address)> {
        let full = output.address().ok()?;

        let stake = match &full {
            Address::Shelley(x) => StakeAddress::try_from(x.clone()).ok(),
            Address::Stake(x) => Some(x.clone()),
            _ => None,
        }?;

        Some((stake, full))
    }
}

impl<'a, S: State3Store> BlockVisitor for SeenAddressesVisitor<'a, SliceBuilder<'_, S>> {
    fn visit_input(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        let Some((stake, _)) = Self::extract_address(resolved) else {
            return Ok(());
        };

        let stake_bytes = stake.to_vec();

        self.0
            .slice
            .ensure_loaded_typed::<AccountState>(&stake_bytes, self.0.store)?;

        Ok(())
    }

    fn visit_output(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: u32,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        let Some((stake, _)) = Self::extract_address(output) else {
            return Ok(());
        };

        let stake_bytes = stake.to_vec();

        self.0
            .slice
            .ensure_loaded_typed::<AccountState>(&stake_bytes, self.0.store)?;

        Ok(())
    }
}

impl<'a> BlockVisitor for SeenAddressesVisitor<'a, DeltaBuilder<'_>> {
    fn visit_input(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        let Some((stake, _)) = Self::extract_address(resolved) else {
            return Ok(());
        };

        let stake_bytes = stake.to_vec();

        let current = self
            .0
            .state
            .get_entity_typed::<AccountState>(&stake_bytes)?;

        let mut new = current.clone().unwrap_or_default();

        // TODO: refactor into CRDT
        // TODO: check same-crawl delta changes
        // TODO: saturating sub shouldn't be necesary on the long run, it should be
        // treated as a invariant violation
        new.controlled_amount = new
            .controlled_amount
            .saturating_sub(resolved.value().coin());

        self.0
            .delta_mut()
            .override_entity(stake_bytes, new, current);

        Ok(())
    }

    fn visit_output(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: u32,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        let Some((stake, full_address)) = Self::extract_address(output) else {
            return Ok(());
        };

        let stake_bytes = stake.to_vec();

        let current = self
            .0
            .state
            .get_entity_typed::<AccountState>(&stake_bytes)?;

        let mut new = current.clone().unwrap_or_default();

        // TODO: refactor into CRDT
        // TODO: check same-crawl delta changes
        new.controlled_amount += output.value().coin();
        new.seen_addresses.insert(full_address.to_vec());

        self.0
            .delta_mut()
            .override_entity(stake_bytes, new, current);

        Ok(())
    }
}

pub struct AccountActivityVisitor<'a, T>(&'a mut T);

impl<'a, T> From<&'a mut T> for AccountActivityVisitor<'a, T> {
    fn from(value: &'a mut T) -> Self {
        Self(value)
    }
}

impl<'a, S: State3Store> BlockVisitor for AccountActivityVisitor<'a, SliceBuilder<'_, S>> {}

impl<'a> BlockVisitor for AccountActivityVisitor<'a, DeltaBuilder<'_>> {
    fn visit_cert(
        &mut self,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        let credential = pallas_extras::cert_as_stake_registration(cert);

        if let Some(credential) = credential {
            let key = match credential {
                StakeCredential::ScriptHash(x) => x.to_vec(),
                StakeCredential::AddrKeyhash(x) => x.to_vec(),
            };

            let value = AccountActivity(block.slot());

            self.0.delta_mut().append_entity(key, value);
        }

        Ok(())
    }
}

pub struct DelegationVisitor<'a, T>(&'a mut T);

impl<'a, T> From<&'a mut T> for DelegationVisitor<'a, T> {
    fn from(value: &'a mut T) -> Self {
        Self(value)
    }
}

impl<'a, S: State3Store> BlockVisitor for DelegationVisitor<'a, SliceBuilder<'_, S>> {
    fn visit_cert(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some(cred) = pallas_extras::cert_as_stake_registration(cert) {
            let stake_address = pallas_extras::stake_credential_to_address(self.0.network, &cred);

            let stake_bytes = stake_address.to_vec();

            self.0
                .slice
                .ensure_loaded_typed::<AccountState>(&stake_bytes, self.0.store)?;
        }

        if let Some(cred) = pallas_extras::cert_as_stake_deregistration(cert) {
            let stake_address = pallas_extras::stake_credential_to_address(self.0.network, &cred);

            let stake_bytes = stake_address.to_vec();

            self.0
                .slice
                .ensure_loaded_typed::<AccountState>(&stake_bytes, self.0.store)?;
        }

        if let Some(cert) = pallas_extras::cert_as_stake_delegation(cert) {
            let stake_address =
                pallas_extras::stake_credential_to_address(self.0.network, &cert.delegator);

            let stake_bytes = stake_address.to_vec();

            self.0
                .slice
                .ensure_loaded_typed::<AccountState>(&stake_bytes, self.0.store)?;
        }
        Ok(())
    }
}

impl<'a> BlockVisitor for DelegationVisitor<'a, DeltaBuilder<'_>> {
    fn visit_cert(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some(cert) = pallas_extras::cert_as_stake_delegation(cert) {
            debug!(%cert.pool, "new pool delegator");

            let stake_address =
                pallas_extras::stake_credential_to_address(self.0.network, &cert.delegator);

            let stake_bytes = stake_address.to_vec();

            let current = self
                .0
                .slice()
                .get_entity_typed::<AccountState>(&stake_bytes)?;

            let mut new = current.clone().unwrap_or_default();

            new.pool_id = Some(cert.pool.to_vec());

            self.0
                .delta_mut()
                .override_entity(stake_bytes, new, current);

            let entity = PoolDelegator(cert.delegator);

            self.0
                .delta_mut()
                .append_entity(cert.pool.as_slice(), entity);
        }

        if let Some(credential) = pallas_extras::cert_as_stake_registration(cert) {
            debug!("stake registration");

            let stake_address =
                pallas_extras::stake_credential_to_address(self.0.network, &credential);

            let stake_bytes = stake_address.to_vec();

            let current = self
                .0
                .slice()
                .get_entity_typed::<AccountState>(&stake_bytes)?;

            let mut new = current.clone().unwrap_or_default();

            new.active_epoch = Some(1);

            self.0
                .delta_mut()
                .override_entity(stake_bytes, new, current);
        }

        if let Some(credential) = pallas_extras::cert_as_stake_deregistration(cert) {
            debug!("stake deregistration");

            let stake_address =
                pallas_extras::stake_credential_to_address(self.0.network, &credential);

            let stake_bytes = stake_address.to_vec();

            let current = self
                .0
                .slice()
                .get_entity_typed::<AccountState>(&stake_bytes)?;

            let mut new = current.clone().unwrap_or_default();

            new.pool_id = None;
            new.active_epoch = None;

            self.0
                .delta_mut()
                .override_entity(stake_bytes, new, current);
        }

        Ok(())
    }
}
