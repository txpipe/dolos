//! Stats (decision 0050): what each block adds to the address, payment and
//! account stats of its epoch, staged until the epoch seals.
//!
//! A block's contributions are summed per key and staged once at its end, so
//! a key the block touches many times costs one WAL delta.

use std::collections::{HashMap, HashSet};

use dolos_core::{ChainError, InvariantViolation, StateError, TxoRef};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::Epoch,
        traverse::{MultiEraBlock, MultiEraInput, MultiEraOutput, MultiEraTx},
    },
};

use super::{BlockVisitor, WorkDeltas};
use crate::{
    model::stats::split_payment, owned::OwnedMultiEraOutput, periods::stats::tx_stats,
    periods::GroupKey, AccountKey, AccountStats, AddressKey, AddressStats, Genesis, PParamsSet,
    PaymentAddressSet, PaymentKey, PaymentStats, StageAccountStats, StageAddressStats,
    StagePaymentAddress, StagePaymentStats,
};

#[derive(Default)]
pub struct StatsVisitor {
    epoch: Option<Epoch>,
    addresses: HashMap<AddressKey, AddressStats>,
    members: HashSet<(Hash<28>, Vec<u8>)>,
    payments: HashMap<Hash<28>, u64>,
    accounts: HashMap<AccountKey, u64>,
}

impl BlockVisitor for StatsVisitor {
    fn visit_root(
        &mut self,
        _: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &Genesis,
        _: &PParamsSet,
        epoch: Epoch,
        _: u64,
        _: u16,
    ) -> Result<(), ChainError> {
        self.epoch = Some(epoch);

        Ok(())
    }

    fn visit_tx(
        &mut self,
        _: &mut WorkDeltas,
        _: &MultiEraBlock,
        tx: &MultiEraTx,
        utxos: &HashMap<TxoRef, OwnedMultiEraOutput>,
    ) -> Result<(), ChainError> {
        let moved = tx_stats(
            tx,
            |input: &MultiEraInput,
             visit: &mut dyn FnMut(&MultiEraOutput) -> Result<(), ChainError>| {
                let txoref = TxoRef::from(input);

                let output = utxos.get(&txoref).ok_or_else(|| {
                    StateError::InvariantViolation(InvariantViolation::InputNotFound(txoref))
                })?;

                output.with_dependent(|_, output| visit(output))
            },
        )?;

        let mut payments = HashSet::new();
        let mut accounts = HashSet::new();

        for (address, stats) in moved {
            if let Some((payment, tail)) = split_payment(&address) {
                payments.insert(payment);
                self.members.insert((payment, tail));
            }

            let key = AddressKey::of(&address);

            if let AddressKey::Staked { account, .. } = &key {
                accounts.insert(account.clone());
            }

            self.addresses.entry(key).or_default().add(&stats);
        }

        for payment in payments {
            *self.payments.entry(payment).or_default() += 1;
        }

        // a tx moving value through several of an account's addresses counts
        // once for the account
        for account in accounts {
            *self.accounts.entry(account).or_default() += 1;
        }

        Ok(())
    }

    fn flush(&mut self, deltas: &mut WorkDeltas) -> Result<(), ChainError> {
        let Some(epoch) = self.epoch else {
            return Ok(());
        };

        for (key, stats) in self.addresses.drain() {
            deltas.add_for_entity(StageAddressStats::new((epoch, key), stats));
        }

        for (payment, tail) in self.members.drain() {
            deltas.add_for_entity(StagePaymentAddress::new(PaymentAddressSet {
                payment,
                epoch,
                tail,
            }));
        }

        for (payment, tx_count) in self.payments.drain() {
            deltas.add_for_entity(StagePaymentStats::new(
                GroupKey {
                    group: PaymentKey(payment),
                    epoch,
                    member: (),
                },
                PaymentStats { tx_count },
            ));
        }

        for (account, tx_count) in self.accounts.drain() {
            deltas.add_for_entity(StageAccountStats::new(
                (epoch, account),
                AccountStats { tx_count },
            ));
        }

        Ok(())
    }
}
