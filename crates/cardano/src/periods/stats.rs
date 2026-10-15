//! Stats (decision 0050): what each address received and sent per unit in
//! each epoch, and the txs that moved it, serving `/addresses/{address}/total`
//! by address and by payment credential, and
//! `/accounts/{stake}/addresses/total`.
//!
//! - `address-stats` holds one row per (epoch, address), keyed stake-first.
//! - `payment-stats` lists, per (epoch, payment credential), the credential's
//!   addresses as tails, with its tx count.
//! - `account-stats` holds one row per (epoch, account) whose addresses moved
//!   value, with the account's tx count.
//!
//! Address and account rows are one type each in both stores, keyed epoch
//! first, and seal in chunks of rows, which may split an account's rows of
//! one epoch. So readers join the two stores per key, not per epoch; the
//! sealed row wins.

use std::{
    collections::{BTreeMap, HashMap},
    ops::RangeInclusive,
};

use dolos_core::{
    ArchiveStore as _, ArchiveWriter as _, ChainError, Domain, Entity as _, EntityKey, EntityValue,
    LogKey, LogRangeOf, RangeOf, StateStore as _, StateWriter as _,
};
use pallas::{
    codec::{
        minicbor::{self, Decode as _},
        utils::Bytes,
    },
    crypto::hash::Hash,
    ledger::{
        addresses::Address,
        primitives::{Epoch, StakeCredential},
        traverse::{MultiEraBlock, MultiEraInput, MultiEraOutput, MultiEraTx},
    },
};

use super::{oldest_staged, seal_epochs, StagedKey};
use crate::{
    model::stats::join_payment, pallas_extras, AccountKey, AccountStats, AddressKey, AddressStats,
    ChainSummary, FixedNamespace as _, PaymentKey, PaymentStats, StagedPaymentAddress,
    StagedPaymentStats, Sum, UnitSums,
};

/// Address rows read per round trip.
const READ_BATCH: usize = 4096;

/// The side of an address an output moves on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    Received,
    Sent,
}

/// Add what `output` carries to `units` on its `flow` side: lovelace under
/// the empty unit, each asset under `policy ‖ name`. A zero quantity adds
/// nothing.
pub fn add_output(units: &mut BTreeMap<Bytes, UnitSums>, flow: Flow, output: &MultiEraOutput) {
    let value = output.value();

    add_quantity(units, Vec::new(), flow, value.coin());

    for policy in value.assets() {
        for asset in policy.assets() {
            let unit = [policy.policy().as_slice(), asset.name()].concat();
            add_quantity(units, unit, flow, asset.output_coin().unwrap_or_default());
        }
    }
}

fn add_quantity(units: &mut BTreeMap<Bytes, UnitSums>, unit: Vec<u8>, flow: Flow, quantity: u64) {
    if quantity == 0 {
        return;
    }

    let sums = units.entry(Bytes::from(unit)).or_default();

    let side = match flow {
        Flow::Received => &mut sums.received,
        Flow::Sent => &mut sums.sent,
    };

    side.0 = side.0.saturating_add(quantity.into());
}

/// Hands the output an input consumes to `visit`, or skips an input it can't
/// resolve.
pub trait Consumed:
    FnMut(
    &MultiEraInput,
    &mut dyn FnMut(&MultiEraOutput) -> Result<(), ChainError>,
) -> Result<(), ChainError>
{
}

impl<F> Consumed for F where
    F: FnMut(
        &MultiEraInput,
        &mut dyn FnMut(&MultiEraOutput) -> Result<(), ChainError>,
    ) -> Result<(), ChainError>
{
}

/// What one tx moved through each address it touched, in order of first
/// touch, each address counting the tx once.
///
/// Produced outputs are received; the outputs its inputs consume, as
/// `consumed` resolves them, are sent. For a phase-2-invalid tx pallas
/// resolves both to the collateral pair, which is what the ledger moves.
///
/// This is the one definition of the sums: the roll stages it per address,
/// and [`sum_block_txs`] folds it over a set of addresses.
pub fn tx_stats(
    tx: &MultiEraTx,
    mut consumed: impl Consumed,
) -> Result<Vec<(Address, AddressStats)>, ChainError> {
    let mut out: Vec<(Address, AddressStats)> = Vec::new();
    let mut index: HashMap<Vec<u8>, usize> = HashMap::new();

    let mut add = |flow: Flow, output: &MultiEraOutput| -> Result<(), ChainError> {
        let address = output.address()?;

        let at = *index.entry(address.to_vec()).or_insert_with(|| {
            let stats = AddressStats {
                tx_count: 1,
                ..Default::default()
            };

            out.push((address, stats));
            out.len() - 1
        });

        add_output(&mut out[at].1.units, flow, output);

        Ok(())
    };

    for (_, output) in tx.produces() {
        add(Flow::Received, &output)?;
    }

    for input in tx.consumes() {
        consumed(&input, &mut |output| add(Flow::Sent, output))?;
    }

    Ok(out)
}

/// minibf's fold over one block for `/addresses/{address}/total`, lifted:
/// what each tx moved through the addresses `matches` accepts, and the txs
/// that moved any of them.
pub fn sum_block_txs(
    totals: &mut AddressStats,
    block: &MultiEraBlock,
    mut consumed: impl Consumed,
    matches: impl Fn(&Address) -> bool,
) -> Result<(), ChainError> {
    for tx in block.txs() {
        let mut matched = false;

        for (address, stats) in tx_stats(&tx, &mut consumed)? {
            if matches(&address) {
                totals.add(&AddressStats {
                    tx_count: 0,
                    ..stats
                });
                matched = true;
            }
        }

        if matched {
            totals.tx_count += 1;
        }
    }

    Ok(())
}

/// minibf's fold over one block for `/accounts/{stake}/addresses/total`,
/// lifted: [`sum_block_txs`] over the addresses of `account`.
pub fn sum_account_block_txs(
    totals: &mut AddressStats,
    block: &MultiEraBlock,
    consumed: impl Consumed,
    account: &StakeCredential,
) -> Result<(), ChainError> {
    sum_block_txs(totals, block, consumed, |address| {
        pallas_extras::address_account(address).as_ref() == Some(account)
    })
}

/// Seal the staged stats of every epoch `sealable` accepts.
pub(crate) fn seal<D: Domain>(
    state: &D::State,
    archive: &D::Archive,
    chain: &ChainSummary,
    sealable: &dyn Fn(Epoch) -> bool,
    batch: usize,
) -> Result<(), ChainError> {
    seal_epochs::<D, AddressStats, _>(state, archive, chain, sealable, batch)?;
    seal_payments::<D>(state, archive, chain, sealable, batch)?;
    seal_epochs::<D, AccountStats, _>(state, archive, chain, sealable, batch)
}

/// One (credential, epoch) of payment stats, staged.
#[derive(Default)]
struct StagedPayment {
    tails: Vec<Vec<u8>>,
    tx_count: u64,
}

/// Payment rows seal in chunks of whole credentials. A credential's tails and
/// tx count of an epoch are staged in two namespaces and seal into one row,
/// so a chunk deletes both only after the archive holds the row.
fn seal_payments<D: Domain>(
    state: &D::State,
    archive: &D::Archive,
    chain: &ChainSummary,
    sealable: &dyn Fn(Epoch) -> bool,
    batch: usize,
) -> Result<(), ChainError> {
    let mut staged: BTreeMap<(Hash<28>, Epoch), StagedPayment> = BTreeMap::new();

    for row in state.iter_entities_typed::<StagedPaymentAddress>(None)? {
        let (_, row) = row?;

        if sealable(row.epoch) {
            staged
                .entry((row.payment, row.epoch))
                .or_default()
                .tails
                .push(row.tail);
        }
    }

    for row in state.iter_entities_typed::<StagedPaymentStats>(None)? {
        let (key, row) = row?;

        if sealable(key.epoch) {
            staged.entry((key.group.0, key.epoch)).or_default().tx_count = row.tx_count;
        }
    }

    let mut staged = staged.into_iter().peekable();

    while staged.peek().is_some() {
        let mut chunk = Vec::new();
        let mut rows = 0;

        // close at a credential boundary once the chunk holds `batch` rows
        while let Some(((payment, _), _)) = staged.peek() {
            if rows >= batch && chunk.last().is_some_and(|((last, _), _)| last != payment) {
                break;
            }

            let next = staged.next().expect("peeked");
            rows += next.1.tails.len() + 1;
            chunk.push(next);
        }

        let writer = archive.start_writer()?;

        for ((payment, epoch), row) in &chunk {
            if row.tails.is_empty() && row.tx_count == 0 {
                continue;
            }

            let mut tails = row.tails.clone();
            tails.sort();

            writer.write_log_typed(
                chain.epoch_start(*epoch),
                &PaymentKey(*payment),
                &PaymentStats {
                    tails: tails.into_iter().map(Into::into).collect(),
                    tx_count: row.tx_count,
                },
            )?;
        }

        writer.commit()?;

        let writer = state.start_writer()?;

        for ((payment, epoch), row) in &chunk {
            for tail in &row.tails {
                let key = StagedPaymentAddress {
                    payment: *payment,
                    epoch: *epoch,
                    tail: tail.clone(),
                };

                writer.save_entity_typed::<StagedPaymentAddress>(
                    &super::StagedRow::key(&key),
                    None,
                )?;
            }

            writer.save_entity_typed::<StagedPaymentStats>(
                &StagedKey {
                    group: PaymentKey(*payment),
                    epoch: *epoch,
                    member: (),
                },
                None,
            )?;
        }

        writer.commit()?;
    }

    Ok(())
}

/// A running fold of address rows.
///
/// Keyed by a hash map, since the heaviest reads fold millions of unit
/// entries, and sorted once at the end.
#[derive(Default)]
struct Fold {
    units: HashMap<Vec<u8>, UnitSums>,
    tx_count: u64,
}

impl Fold {
    fn add_unit(&mut self, unit: &[u8], sums: &UnitSums) {
        match self.units.get_mut(unit) {
            Some(entry) => {
                entry.received.0 = entry.received.0.saturating_add(sums.received.0);
                entry.sent.0 = entry.sent.0.saturating_add(sums.sent.0);
            }
            None => {
                self.units.insert(unit.to_vec(), *sums);
            }
        }
    }

    fn add_stats(&mut self, stats: &AddressStats) {
        for (unit, sums) in &stats.units {
            self.add_unit(unit, sums);
        }

        self.tx_count = self.tx_count.saturating_add(stats.tx_count);
    }

    /// Fold a stored row without building it: `[{unit: [received, sent]},
    /// tx_count]`.
    fn add_raw(&mut self, raw: &[u8]) -> Result<(), ChainError> {
        let mut d = minicbor::Decoder::new(raw);

        d.array()?;

        let Some(len) = d.map()? else {
            return Err(minicbor::decode::Error::message("indefinite address stats").into());
        };

        for _ in 0..len {
            let unit = d.bytes()?;
            d.array()?;

            let sums = UnitSums {
                received: Sum::decode(&mut d, &mut ())?,
                sent: Sum::decode(&mut d, &mut ())?,
            };

            self.add_unit(unit, &sums);
        }

        self.tx_count = self.tx_count.saturating_add(d.u64()?);

        Ok(())
    }

    fn finish(self) -> AddressStats {
        AddressStats {
            units: self
                .units
                .into_iter()
                .map(|(unit, sums)| (Bytes::from(unit), sums))
                .collect(),
            tx_count: self.tx_count,
        }
    }
}

/// Fold the address rows at `keys` into `fold`, each from wherever it lives.
///
/// Per round trip, the staged rows are read before the sealed ones, and the
/// sealed row wins. A seal commits the archive before it deletes the staged
/// rows, so in that order a row missing from the staged read is in the
/// sealed one.
fn fold_address_rows<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    keys: &[(Epoch, AddressKey)],
    fold: &mut Fold,
) -> Result<(), ChainError> {
    let staged_from = oldest_staged::<_, AddressStats, _>(domain.state())?;

    for batch in keys.chunks(READ_BATCH) {
        let staged_keys: Vec<(usize, EntityKey)> = batch
            .iter()
            .enumerate()
            .filter(|(_, (epoch, _))| staged_from.is_some_and(|from| *epoch >= from))
            .map(|(i, key)| (i, EntityKey::of(key)))
            .collect();

        let staged_values = domain.state().read_entities(
            AddressStats::NS,
            &staged_keys.iter().map(|(_, key)| key).collect::<Vec<_>>(),
        )?;

        let mut staged: HashMap<usize, EntityValue> = staged_keys
            .into_iter()
            .zip(staged_values)
            .filter_map(|((i, _), value)| value.map(|value| (i, value)))
            .collect();

        let sealed_keys: Vec<LogKey> = batch
            .iter()
            .map(|(epoch, key)| LogKey::of(chain.epoch_start(*epoch), key))
            .collect();

        let sealed = domain
            .archive()
            .read_logs(AddressStats::NS, &sealed_keys.iter().collect::<Vec<_>>())?;

        for (i, sealed) in sealed.into_iter().enumerate() {
            if let Some(raw) = sealed.or_else(|| staged.remove(&i)) {
                fold.add_raw(&raw)?;
            }
        }
    }

    Ok(())
}

/// What `key`'s address received and sent over `epochs`, and its tx count.
pub fn address_totals<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    key: &AddressKey,
) -> Result<AddressStats, ChainError> {
    let keys: Vec<_> = epochs.map(|epoch| (epoch, key.clone())).collect();

    let mut fold = Fold::default();
    fold_address_rows(domain, chain, &keys, &mut fold)?;

    Ok(fold.finish())
}

/// What the addresses of `payment` received and sent over `epochs`, and the
/// txs that moved any of it.
///
/// Each epoch's tails and tx count come from one payment row, staged or
/// sealed, the sealed one winning; the sums from the address rows the tails
/// name.
pub fn payment_totals<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    payment: &PaymentKey,
) -> Result<AddressStats, ChainError> {
    let state = domain.state();

    let mut staged: BTreeMap<Epoch, StagedPayment> = BTreeMap::new();

    for row in state.iter_entities_typed::<StagedPaymentAddress>(Some(RangeOf::prefix(payment)))? {
        let (_, row) = row?;
        staged.entry(row.epoch).or_default().tails.push(row.tail);
    }

    for row in state.iter_entities_typed::<StagedPaymentStats>(Some(RangeOf::prefix(payment)))? {
        let (key, row) = row?;
        staged.entry(key.epoch).or_default().tx_count = row.tx_count;
    }

    let sealed =
        super::read_sealed::<_, PaymentStats>(domain.archive(), chain, epochs.clone(), |_| {
            *payment
        })?;

    let mut tx_count = 0u64;
    let mut keys = Vec::new();

    for epoch in epochs {
        let row = match sealed.get(&epoch) {
            Some(raw) => {
                let row = PaymentStats::decode_entity(PaymentStats::NS, raw)?;

                StagedPayment {
                    tails: row.tails.into_iter().map(Vec::from).collect(),
                    tx_count: row.tx_count,
                }
            }
            None => match staged.remove(&epoch) {
                Some(row) => row,
                None => continue,
            },
        };

        tx_count = tx_count.saturating_add(row.tx_count);

        for tail in row.tails {
            let address = Address::from_bytes(&join_payment(&payment.0, &tail))?;
            keys.push((epoch, AddressKey::of(&address)));
        }
    }

    let mut fold = Fold::default();
    fold_address_rows(domain, chain, &keys, &mut fold)?;

    let mut totals = fold.finish();
    totals.tx_count = tx_count;

    Ok(totals)
}

/// The tx count of `account` in each epoch of `epochs` it was active in.
///
/// Point reads, staged rows first and sealed ones second, the sealed row
/// winning. A seal commits the archive before it deletes from state, so in
/// that order a row missing from the staged read is in the sealed one.
fn account_epochs<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    account: &AccountKey,
) -> Result<BTreeMap<Epoch, u64>, ChainError> {
    let state = domain.state();
    let staged_from = oldest_staged::<_, AccountStats, _>(state)?;

    let staged_keys: Vec<(Epoch, AccountKey)> = epochs
        .clone()
        .filter(|epoch| staged_from.is_some_and(|from| *epoch >= from))
        .map(|epoch| (epoch, account.clone()))
        .collect();

    let staged =
        state.read_entities_typed::<AccountStats>(&staged_keys.iter().collect::<Vec<_>>())?;

    let mut out: BTreeMap<Epoch, u64> = staged_keys
        .iter()
        .zip(staged)
        .filter_map(|((epoch, _), row)| row.map(|row| (*epoch, row.tx_count)))
        .collect();

    let sealed = super::read_sealed::<_, AccountStats>(domain.archive(), chain, epochs, |_| {
        account.clone()
    })?;

    for (epoch, raw) in sealed {
        out.insert(
            epoch,
            AccountStats::decode_entity(AccountStats::NS, &raw)?.tx_count,
        );
    }

    Ok(out)
}

/// What the addresses of `account` received and sent over `epochs`, and the
/// txs that moved any of it.
///
/// The account rows give the tx count and name the epochs the account was
/// active in; one range scan per active epoch reads its address rows.
pub fn account_totals<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    account: &AccountKey,
) -> Result<AddressStats, ChainError> {
    let (state, archive) = (domain.state(), domain.archive());

    let active = account_epochs(domain, chain, epochs, account)?;
    let staged_from = oldest_staged::<_, AddressStats, _>(state)?;

    let mut fold = Fold::default();

    for &epoch in active.keys() {
        let mut staged: HashMap<Vec<u8>, AddressStats> = HashMap::new();

        if staged_from.is_some_and(|from| epoch >= from) {
            let range = RangeOf::prefix(&(epoch, account.clone()));

            for row in state.iter_entities_typed::<AddressStats>(Some(range))? {
                let ((_, key), row) = row?;
                staged.insert(dolos_core::keys::encode(&key).to_vec(), row);
            }
        }

        let range = LogRangeOf::<AddressKey>::prefix(chain.epoch_start(epoch), account);

        for row in archive.iter_logs(AddressStats::NS, range.into_raw())? {
            let (key, raw) = row?;
            staged.remove(key.entity());
            fold.add_raw(&raw)?;
        }

        for row in staged.values() {
            fold.add_stats(row);
        }
    }

    let mut totals = fold.finish();
    totals.tx_count = active.values().sum();

    Ok(totals)
}

#[cfg(test)]
mod tests {
    use dolos_core::{
        ArchiveStore as _, BlockSlot, Domain as _, StateStore as _, StateWriter as _,
    };
    use dolos_testing::{
        faults::{FaultyToyDomain, TestFault},
        toy_domain::ToyDomain,
    };
    use pallas::ledger::addresses::{
        Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
    };

    use super::*;
    use crate::{model::stats::split_payment, periods::is_sealable};

    /// The epoch the sealing boundary opens: 5 and 6 are immutable at it, 7 is
    /// not.
    const BOUNDARY: Epoch = 8;

    fn a() -> StakeCredential {
        StakeCredential::AddrKeyhash([0x11; 28].into())
    }

    fn b() -> StakeCredential {
        StakeCredential::ScriptHash([0x11; 28].into())
    }

    fn base(payment: u8, stake: StakeCredential) -> Address {
        let delegation = match stake {
            StakeCredential::AddrKeyhash(hash) => ShelleyDelegationPart::Key(hash),
            StakeCredential::ScriptHash(hash) => ShelleyDelegationPart::Script(hash),
        };

        Address::Shelley(ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::Key([payment; 28].into()),
            delegation,
        ))
    }

    fn enterprise(payment: u8) -> Address {
        Address::Shelley(ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::Key([payment; 28].into()),
            ShelleyDelegationPart::Null,
        ))
    }

    /// Account `a` holds three addresses, `b` one, and payment 0x40 is shared
    /// by an address of each and an enterprise address.
    fn addresses() -> Vec<Address> {
        vec![
            base(0x40, a()),
            base(0x41, a()),
            base(0x42, a()),
            base(0x40, b()),
            enterprise(0x40),
            enterprise(0x43),
        ]
    }

    fn chain(domain: &ToyDomain) -> ChainSummary {
        crate::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap()
    }

    fn sums(received: u128, sent: u128) -> UnitSums {
        UnitSums {
            received: Sum(received),
            sent: Sum(sent),
        }
    }

    /// Staged stats in epochs 5 to 7, sums past u64 included.
    fn seeded() -> ToyDomain {
        let domain = ToyDomain::new(None, None);
        let writer = domain.state().start_writer().unwrap();

        for epoch in 5..=7u64 {
            let mut payments: BTreeMap<Hash<28>, u64> = BTreeMap::new();

            for (i, address) in addresses().into_iter().enumerate() {
                let i = i as u64;

                let stats = AddressStats {
                    units: [
                        (Bytes::from(vec![]), sums((epoch * 10 + i).into(), i.into())),
                        (
                            Bytes::from(vec![0xaa; 30]),
                            sums(u128::from(u64::MAX) * u128::from(i + 1), 0),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                    tx_count: 1 + i,
                };

                writer
                    .write_entity_typed(&(epoch, AddressKey::of(&address)), &stats)
                    .unwrap();

                let (payment, tail) = split_payment(&address).unwrap();
                let row = StagedPaymentAddress {
                    payment,
                    epoch,
                    tail,
                };
                writer
                    .write_entity_typed(&super::super::StagedRow::key(&row), &row)
                    .unwrap();

                *payments.entry(payment).or_default() += 1 + i;
            }

            for (payment, tx_count) in payments {
                writer
                    .write_entity_typed(
                        &StagedKey {
                            group: PaymentKey(payment),
                            epoch,
                            member: (),
                        },
                        &StagedPaymentStats {
                            tx_count: tx_count - 1,
                        },
                    )
                    .unwrap();
            }

            // a tx moving value through several of `a`'s addresses counts
            // once for the account: `a`'s rows count 6 in each epoch
            for (account, tx_count) in [(a(), 10 - epoch), (b(), 4)] {
                writer
                    .write_entity_typed(&(epoch, AccountKey(account)), &AccountStats { tx_count })
                    .unwrap();
            }
        }

        writer.commit().unwrap();

        domain
    }

    fn seal_as<D: Domain>(domain: &D, at: &ToyDomain) -> Result<(), ChainError> {
        let chain = chain(at);
        let mutable = crate::utils::mutable_slots(&at.genesis());

        seal::<D>(
            domain.state(),
            domain.archive(),
            &chain,
            &|epoch| is_sealable(&chain, mutable, epoch, BOUNDARY),
            2,
        )
    }

    type Logs = (
        Vec<(BlockSlot, AddressKey, AddressStats)>,
        Vec<(BlockSlot, PaymentKey, PaymentStats)>,
        Vec<(BlockSlot, AccountKey, AccountStats)>,
    );

    fn logs(domain: &ToyDomain) -> Logs {
        let archive = domain.archive();

        (
            archive
                .iter_logs_typed::<AddressStats>(None)
                .unwrap()
                .map(Result::unwrap)
                .collect(),
            archive
                .iter_logs_typed::<PaymentStats>(None)
                .unwrap()
                .map(Result::unwrap)
                .collect(),
            archive
                .iter_logs_typed::<AccountStats>(None)
                .unwrap()
                .map(Result::unwrap)
                .collect(),
        )
    }

    fn staged(domain: &ToyDomain) -> (usize, usize, usize, usize) {
        let state = domain.state();

        (
            state
                .iter_entities_typed::<AddressStats>(None)
                .unwrap()
                .count(),
            state
                .iter_entities_typed::<StagedPaymentAddress>(None)
                .unwrap()
                .count(),
            state
                .iter_entities_typed::<StagedPaymentStats>(None)
                .unwrap()
                .count(),
            state
                .iter_entities_typed::<AccountStats>(None)
                .unwrap()
                .count(),
        )
    }

    /// Every total the seeded rows answer.
    fn totals(domain: &ToyDomain) -> Vec<AddressStats> {
        let chain = chain(domain);
        let epochs = || 0..=BOUNDARY;

        let mut out = Vec::new();

        for address in addresses() {
            let key = AddressKey::of(&address);
            out.push(address_totals(domain, &chain, epochs(), &key).unwrap());
        }

        for payment in [0x40, 0x41, 0x43, 0x99] {
            let payment = PaymentKey([payment; 28].into());
            out.push(payment_totals(domain, &chain, epochs(), &payment).unwrap());
        }

        for account in [a(), b()] {
            let account = AccountKey(account);
            out.push(account_totals(domain, &chain, epochs(), &account).unwrap());
        }

        out
    }

    #[test]
    fn a_seal_moves_immutable_epochs_to_the_archive() {
        let domain = seeded();
        let before = totals(&domain);

        seal_as(&domain, &domain).unwrap();

        // only epoch 7 stays staged
        assert_eq!(staged(&domain), (6, 6, 4, 2));

        let (addresses, payments, accounts) = logs(&domain);
        assert_eq!(
            (addresses.len(), payments.len(), accounts.len()),
            (12, 8, 4)
        );

        assert_eq!(totals(&domain), before);

        // account a: three addresses over three epochs, its tx count the
        // account rows' rather than the address rows' sum
        let a = &before[before.len() - 2];
        assert_eq!(a.tx_count, 3 * (1 + 2 + 3) - (1 + 2 + 3));

        // a sum past u64 survives the seal
        let wide = Bytes::from(vec![0xaa; 30]);
        assert_eq!(
            a.units[&wide].received.0,
            3 * u128::from(u64::MAX) * (1 + 2 + 3)
        );

        // payment 0x40: three addresses, its tx count its own
        let shared = &before[addresses_len()];
        assert_eq!(shared.tx_count, 3 * ((1 + 4 + 5) - 1));
        assert_eq!(shared.units[&Bytes::from(vec![])].sent.0, 3 * (3 + 4));

        // an unknown credential has nothing
        assert_eq!(before[addresses_len() + 3], AddressStats::default());
    }

    fn addresses_len() -> usize {
        addresses().len()
    }

    /// Seal idempotence: whichever commit the seal dies after, a re-run
    /// leaves exactly the rows of a seal that never crashed and nothing staged
    /// for the sealed epochs.
    #[test]
    fn a_crash_at_any_commit_seals_the_same_rows() {
        let clean = seeded();
        seal_as(&clean, &clean).unwrap();

        let expected = (logs(&clean), staged(&clean));

        let mut crash_after = 0;

        loop {
            let domain = seeded();
            let crashing =
                FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(crash_after));

            if seal_as(&crashing, &domain).is_ok() {
                break;
            }

            seal_as(&domain, &domain).expect("re-run");

            assert_eq!(
                (logs(&domain), staged(&domain)),
                expected,
                "after a crash following commit {crash_after}"
            );

            // no state row of a sealed epoch is left behind
            let state = domain.state();
            let epochs: Vec<Epoch> = state
                .iter_entities_typed::<AddressStats>(None)
                .unwrap()
                .map(|row| row.unwrap().0 .0)
                .chain(
                    state
                        .iter_entities_typed::<AccountStats>(None)
                        .unwrap()
                        .map(|row| row.unwrap().0 .0),
                )
                .collect();
            assert!(epochs.iter().all(|epoch| *epoch == 7), "{epochs:?}");

            crash_after += 1;
        }

        // address rows split mid-account at two rows a chunk
        assert!(crash_after >= 6, "only {crash_after} commits");
    }

    /// An account's address rows are read only in the epochs its account rows
    /// name: a row in any other epoch is not folded, staged or sealed.
    #[test]
    fn account_totals_scan_only_the_epochs_the_account_rows_name() {
        let domain = seeded();
        let before = totals(&domain);

        let stray = AddressStats {
            units: [(Bytes::from(vec![]), sums(1_000, 0))]
                .into_iter()
                .collect(),
            tx_count: 1,
        };
        let key = AddressKey::of(&base(0x44, b()));

        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(&(7, key.clone()), &stray)
            .unwrap();
        writer
            .save_entity_typed::<AccountStats>(&(7, AccountKey(b())), None)
            .unwrap();
        writer.commit().unwrap();

        let chain = chain(&domain);
        let writer = domain.archive().start_writer().unwrap();
        writer
            .write_log_typed(chain.epoch_start(3), &key, &stray)
            .unwrap();
        writer.commit().unwrap();

        // `b` lost epoch 7: its address row there and the stray rows go
        // unread
        let account = AccountKey(b());
        let after = account_totals(&domain, &chain, 0..=BOUNDARY, &account).unwrap();
        let b = &before[before.len() - 1];

        assert_eq!(after.tx_count, b.tx_count - 4);
        assert_eq!(
            after.units[&Bytes::from(vec![])].received.0,
            b.units[&Bytes::from(vec![])].received.0 - (7 * 10 + 3)
        );
    }

    /// A read landing between any two commits of a seal answers every total
    /// as a read before it: address rows of one account and epoch may be
    /// split between the stores, and the payment rows' two staged halves leave
    /// together.
    #[test]
    fn a_read_beside_a_seal_answers_the_same_totals() {
        let before = totals(&seeded());

        let mut crash_after = 0;

        loop {
            let domain = seeded();
            let crashing =
                FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(crash_after));
            let done = seal_as(&crashing, &domain).is_ok();

            assert_eq!(
                totals(&domain),
                before,
                "between commits {crash_after} and next"
            );

            if done {
                break;
            }

            crash_after += 1;
        }
    }
}
