//! Stats (decision 0050): what each address received and sent per unit in
//! each epoch, and the txs that moved it, serving `/addresses/{address}/total`
//! by address and by payment credential, and
//! `/accounts/{stake}/addresses/total`.
//!
//! - `address-stats` holds one row per (epoch, address), keyed stake-first.
//! - `payment-addresses` (a `Set`) and `payment-stats` hold, in state, a
//!   payment credential's addresses as tails and its tx count; they seal
//!   together into one `payment-address-chunks` row per (epoch, credential).
//! - `account-stats` holds one row per (epoch, account) whose addresses moved
//!   value, with the account's tx count.
//!
//! Address and account rows are one type each in both stores, keyed epoch
//! first, and seal in chunks of rows, which may split an account's rows of
//! one epoch. So readers join the two stores per key, not per epoch; the
//! sealed row wins.
//!
//! The routes read through the [`TOTALS`] cache namespace (decision 0059),
//! which keeps a credential's sums over its sealed epochs.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ops::RangeInclusive,
};

use dolos_core::{
    cache::Cost, ArchiveError, ArchiveStore as _, ArchiveWriter as _, CacheEntry, CacheError,
    CacheNamespace, CacheStore as _, ChainError, Domain, DomainError, Entity as _, EntityKey,
    EntityValue, KeyCodec as _, KeyError, KeyRange, LogKey, LogRangeOf, RangeOf, StateStore as _,
    StateWriter as _,
};
use pallas::{
    codec::{
        minicbor::{self, Decode as _},
        utils::Bytes,
    },
    crypto::hash::Hash,
    ledger::{
        addresses::{Address, ByronAddress},
        primitives::{Epoch, StakeCredential},
        traverse::{MultiEraBlock, MultiEraInput, MultiEraOutput, MultiEraTx},
    },
};

use super::{is_sealable, oldest_staged, seal_epochs, GroupKey};
use crate::{
    model::stats::{join_payment, split_payment},
    pallas_extras, AccountKey, AccountStats, AddressKey, AddressStats, ChainSummary,
    FixedNamespace as _, PaymentAddressChunk, PaymentAddressSet, PaymentKey, PaymentStats, Sum,
    UnitSums,
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

/// One (credential, epoch) of payment addresses and tx count, from either
/// store.
#[derive(Default)]
struct PaymentEpoch {
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
    let mut staged: BTreeMap<(Hash<28>, Epoch), PaymentEpoch> = BTreeMap::new();

    for row in state.iter_entities_typed::<PaymentAddressSet>(None)? {
        let (_, row) = row?;

        if sealable(row.epoch) {
            staged
                .entry((row.payment, row.epoch))
                .or_default()
                .tails
                .push(row.tail);
        }
    }

    for row in state.iter_entities_typed::<PaymentStats>(None)? {
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
                &PaymentAddressChunk {
                    tails: tails.into_iter().map(Into::into).collect(),
                    tx_count: row.tx_count,
                },
            )?;
        }

        writer.commit()?;

        let writer = state.start_writer()?;

        for ((payment, epoch), row) in &chunk {
            for tail in &row.tails {
                let key = PaymentAddressSet {
                    payment: *payment,
                    epoch: *epoch,
                    tail: tail.clone(),
                };

                writer.save_entity_typed::<PaymentAddressSet>(&super::GroupRow::key(&key), None)?;
            }

            writer.save_entity_typed::<PaymentStats>(
                &GroupKey {
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
    /// Unit entries folded: what the fold cost.
    entries: u64,
}

impl Fold {
    fn add_unit(&mut self, unit: &[u8], sums: &UnitSums) {
        self.entries += 1;

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
    fold_address(domain, chain, epochs, key).map(Fold::finish)
}

fn fold_address<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    key: &AddressKey,
) -> Result<Fold, ChainError> {
    let keys: Vec<_> = epochs.map(|epoch| (epoch, key.clone())).collect();

    let mut fold = Fold::default();
    fold_address_rows(domain, chain, &keys, &mut fold)?;

    Ok(fold)
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
    fold_payment(domain, chain, epochs, payment).map(Fold::finish)
}

fn fold_payment<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    payment: &PaymentKey,
) -> Result<Fold, ChainError> {
    let state = domain.state();

    let mut staged: BTreeMap<Epoch, PaymentEpoch> = BTreeMap::new();

    for row in state.iter_entities_typed::<PaymentAddressSet>(Some(RangeOf::prefix(payment)))? {
        let (_, row) = row?;
        staged.entry(row.epoch).or_default().tails.push(row.tail);
    }

    for row in state.iter_entities_typed::<PaymentStats>(Some(RangeOf::prefix(payment)))? {
        let (key, row) = row?;
        staged.entry(key.epoch).or_default().tx_count = row.tx_count;
    }

    let sealed = super::read_sealed::<_, PaymentAddressChunk>(
        domain.archive(),
        chain,
        epochs.clone(),
        |_| *payment,
    )?;

    let mut tx_count = 0u64;
    let mut keys = Vec::new();

    for epoch in epochs {
        let row = match sealed.get(&epoch) {
            Some(raw) => {
                let row = PaymentAddressChunk::decode_entity(PaymentAddressChunk::NS, raw)?;

                PaymentEpoch {
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
    fold.tx_count = tx_count;

    Ok(fold)
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
    fold_account(domain, chain, epochs, account).map(Fold::finish)
}

fn fold_account<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    account: &AccountKey,
) -> Result<Fold, ChainError> {
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

    fold.tx_count = active.values().sum();

    Ok(fold)
}

/// The cache namespace of lifetime totals (decision 0059): a credential's
/// sums over its sealed epochs, built once and extended as epochs seal.
///
/// An entry is kept once its fold read 100,000 unit entries.
pub const TOTALS: CacheNamespace = CacheNamespace::immutable("totals", 1).with_admission(100_000);

/// A credential is worth warming once its fold over one sealed epoch reads
/// this many unit entries.
pub const CANDIDATE_EPOCH_ENTRIES: u64 = 4_096;

/// Register [`TOTALS`], unless `sync.max_history` prunes the archive, whose
/// sealed stats it drops: there every read folds as without a cache. Answers
/// whether it registered.
pub fn register_totals_cache<D: Domain>(domain: &D) -> Result<bool, CacheError> {
    if domain.sync_config().max_history.is_some() {
        return Ok(false);
    }

    domain.cache().register(TOTALS)?;

    Ok(true)
}

/// The credential a totals entry sums.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TotalsOf {
    Address(AddressKey),
    Payment(PaymentKey),
    Account(AccountKey),
}

impl TotalsOf {
    fn family(&self) -> u8 {
        match self {
            Self::Address(_) => 0,
            Self::Payment(_) => 1,
            Self::Account(_) => 2,
        }
    }

    fn fold<D: Domain>(
        &self,
        domain: &D,
        chain: &ChainSummary,
        epochs: RangeInclusive<Epoch>,
    ) -> Result<Fold, ChainError> {
        match self {
            Self::Address(key) => fold_address(domain, chain, epochs, key),
            Self::Payment(payment) => fold_payment(domain, chain, epochs, payment),
            Self::Account(account) => fold_account(domain, chain, epochs, account),
        }
    }

    /// The epoch the routes start this credential's totals at: 0 for a Byron
    /// address, the first Shelley epoch for any other credential.
    pub fn start(&self, chain: &ChainSummary) -> Epoch {
        match self {
            Self::Address(key) if is_byron(&key.address()) => 0,
            _ => chain.first_shelley_epoch(),
        }
    }
}

fn is_byron(address: &[u8]) -> bool {
    matches!(Address::from_bytes(address), Ok(Address::Byron(_)))
}

/// A totals entry's key: `family (1 B) ‖ credential key ‖ start (8 B)`, the
/// start epoch big-endian.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TotalsKey {
    pub of: TotalsOf,
    /// The first epoch the totals sum.
    pub start: Epoch,
}

impl TotalsKey {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![self.of.family()];

        out.extend_from_slice(&match &self.of {
            TotalsOf::Address(key) => dolos_core::keys::encode(key),
            TotalsOf::Payment(key) => dolos_core::keys::encode(key),
            TotalsOf::Account(key) => dolos_core::keys::encode(key),
        });

        out.extend_from_slice(&self.start.to_be_bytes());

        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, KeyError> {
        let short = || KeyError::Short {
            expected: 1 + 8,
            found: bytes.len(),
        };

        let (&family, rest) = bytes.split_first().ok_or_else(short)?;
        let at = rest.len().checked_sub(8).ok_or_else(short)?;
        let (key, start) = rest.split_at(at);

        let of = match family {
            0 => TotalsOf::Address(AddressKey::decode_key(key)?),
            1 => TotalsOf::Payment(PaymentKey::decode_key(key)?),
            2 => TotalsOf::Account(AccountKey::decode_key(key)?),
            other => return Err(KeyError::Malformed(format!("totals family {other:#04x}"))),
        };

        let start = Epoch::from_be_bytes(start.try_into().expect("eight bytes"));

        Ok(Self { of, start })
    }

    /// The key of an address, a payment credential (`addr_vkh…`, `script…`)
    /// or a stake address, which names an account, starting where the routes
    /// do.
    pub fn parse(text: &str, chain: &ChainSummary) -> Result<Self, KeyError> {
        let of = parse_totals_of(text).ok_or_else(|| {
            KeyError::Malformed(format!(
                "not an address, payment credential or stake address: {text}"
            ))
        })?;

        let start = of.start(chain);

        Ok(Self { of, start })
    }
}

fn parse_totals_of(text: &str) -> Option<TotalsOf> {
    if text.starts_with("addr_vkh") || text.starts_with("script") {
        let (_, bytes) = bech32::decode(text).ok()?;
        let hash: [u8; 28] = bytes.try_into().ok()?;

        return Some(TotalsOf::Payment(PaymentKey(hash.into())));
    }

    let address = match Address::from_bech32(text) {
        Ok(address) => address,
        Err(_) => ByronAddress::from_base58(text).ok()?.into(),
    };

    Some(match address {
        Address::Stake(stake) => {
            TotalsOf::Account(AccountKey(pallas_extras::stake_address_to_cred(&stake)))
        }
        address => TotalsOf::Address(AddressKey::of(&address)),
    })
}

/// A totals entry: the fold over `start..=watermark` of its key, and the
/// unit entries that fold read.
///
/// Stored as the stats in their CBOR encoding, then the watermark and the
/// cost, each a big-endian `u64`, so both read without decoding the stats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TotalsEntry {
    pub stats: AddressStats,
    /// The last epoch the entry covers.
    pub watermark: Epoch,
    pub cost: Cost,
}

const TRAILER: usize = 16;

impl TotalsEntry {
    fn into_cache_entry(self) -> CacheEntry {
        let mut value = minicbor::to_vec(&self.stats).expect("infallible encode");
        value.extend_from_slice(&self.watermark.to_be_bytes());
        value.extend_from_slice(&self.cost.to_be_bytes());

        CacheEntry::new(value, self.cost)
    }

    pub fn decode(value: &[u8]) -> Result<Self, ChainError> {
        let (stats, watermark, cost) = split_trailer(value).ok_or_else(|| {
            minicbor::decode::Error::message("totals entry shorter than its trailer")
        })?;

        let mut d = minicbor::Decoder::new(stats);
        let decoded = AddressStats::decode(&mut d, &mut ())?;

        if d.position() != stats.len() {
            return Err(minicbor::decode::Error::message("trailing bytes in totals entry").into());
        }

        Ok(Self {
            stats: decoded,
            watermark,
            cost,
        })
    }

    /// The watermark of a stored entry, read from its trailer alone.
    pub fn watermark_of(value: &[u8]) -> Option<Epoch> {
        split_trailer(value).map(|(_, watermark, _)| watermark)
    }
}

fn split_trailer(value: &[u8]) -> Option<(&[u8], Epoch, Cost)> {
    let at = value.len().checked_sub(TRAILER)?;
    let (stats, trailer) = value.split_at(at);
    let (watermark, cost) = trailer.split_at(8);

    Some((
        stats,
        u64::from_be_bytes(watermark.try_into().ok()?),
        u64::from_be_bytes(cost.try_into().ok()?),
    ))
}

/// The last epoch a totals entry may cover with the tip in epoch `tip`: one
/// below the oldest staged address and account stats, and immutable at the
/// tip. None while no epoch is both.
pub fn totals_watermark<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    tip: Epoch,
) -> Result<Option<Epoch>, ChainError> {
    let mutable_slots = crate::utils::mutable_slots(&domain.genesis());
    let immutable = (0..tip)
        .rev()
        .find(|epoch| is_sealable(chain, mutable_slots, *epoch, tip));

    let state = domain.state();

    let staged = [
        oldest_staged::<_, AddressStats, _>(state)?,
        oldest_staged::<_, AccountStats, _>(state)?,
    ]
    .into_iter()
    .flatten()
    .min();

    let below_staged = match staged {
        Some(0) => return Ok(None),
        Some(oldest) => Some(oldest - 1),
        None => None,
    };

    Ok(immutable.map(|immutable| below_staged.map_or(immutable, |below| below.min(immutable))))
}

/// The entry of `key` up to `watermark`, built without the cache. A miss
/// builds through it, and so does warming the cache.
pub fn build_totals_entry<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    key: &TotalsKey,
    watermark: Epoch,
) -> Result<CacheEntry, ChainError> {
    let fold = key.of.fold(domain, chain, key.start..=watermark)?;

    let entry = TotalsEntry {
        cost: fold.entries,
        stats: fold.finish(),
        watermark,
    };

    Ok(entry.into_cache_entry())
}

/// `value`, an entry of `key`, extended to `watermark` by folding only the
/// epochs past its own. An entry past `watermark` is built afresh.
fn extend_totals_entry<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    key: &TotalsKey,
    value: &[u8],
    watermark: Epoch,
) -> Result<CacheEntry, ChainError> {
    let entry = TotalsEntry::decode(value)?;

    if entry.watermark > watermark {
        return build_totals_entry(domain, chain, key, watermark);
    }

    let from = (entry.watermark + 1).max(key.start);
    let fold = key.of.fold(domain, chain, from..=watermark)?;
    let cost = entry.cost.saturating_add(fold.entries);

    let mut stats = entry.stats;
    stats.add(&fold.finish());

    let entry = TotalsEntry {
        stats,
        watermark,
        cost,
    };

    Ok(entry.into_cache_entry())
}

/// The credentials whose fold over one sealed epoch reads at least
/// [`CANDIDATE_EPOCH_ENTRIES`] unit entries, in one pass over the sealed
/// address rows.
pub fn totals_candidates<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
) -> Result<Vec<TotalsKey>, ChainError> {
    #[derive(Default)]
    struct Epochs {
        current: Option<Epoch>,
        payments: HashMap<Hash<28>, u64>,
        accounts: HashMap<AccountKey, u64>,
        heavy: HashSet<TotalsOf>,
    }

    impl Epochs {
        fn close(&mut self) {
            let heavy = |entries: &u64| *entries >= CANDIDATE_EPOCH_ENTRIES;

            for (payment, _) in self.payments.drain().filter(|(_, n)| heavy(n)) {
                self.heavy.insert(TotalsOf::Payment(PaymentKey(payment)));
            }

            for (account, _) in self.accounts.drain().filter(|(_, n)| heavy(n)) {
                self.heavy.insert(TotalsOf::Account(account));
            }
        }
    }

    let mut epochs = Epochs::default();

    for row in domain
        .archive()
        .iter_logs(AddressStats::NS, KeyRange::all())?
    {
        let (key, raw) = row?;
        let (epoch, _) = chain.slot_epoch(key.slot());

        if epochs.current != Some(epoch) {
            epochs.close();
            epochs.current = Some(epoch);
        }

        let entries = unit_count(&raw)?;
        let address = AddressKey::decode_key(key.entity()).map_err(ArchiveError::from)?;

        if let AddressKey::Staked { account, .. } = &address {
            *epochs.accounts.entry(account.clone()).or_default() += entries;
        }

        if let Some((payment, _)) = Address::from_bytes(&address.address())
            .ok()
            .as_ref()
            .and_then(split_payment)
        {
            *epochs.payments.entry(payment).or_default() += entries;
        }

        if entries >= CANDIDATE_EPOCH_ENTRIES {
            epochs.heavy.insert(TotalsOf::Address(address));
        }
    }

    epochs.close();

    let mut keys: Vec<TotalsKey> = epochs
        .heavy
        .into_iter()
        .map(|of| TotalsKey {
            start: of.start(chain),
            of,
        })
        .collect();

    keys.sort_by_cached_key(TotalsKey::to_bytes);

    Ok(keys)
}

/// The units a stored address row lists, read from its map header.
fn unit_count(raw: &[u8]) -> Result<u64, ChainError> {
    let mut d = minicbor::Decoder::new(raw);
    d.array()?;

    d.map()?
        .ok_or_else(|| minicbor::decode::Error::message("indefinite address stats").into())
}

/// [`address_totals`], its sealed epochs read from the totals cache.
pub async fn cached_address_totals<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    key: &AddressKey,
) -> Result<AddressStats, DomainError> {
    cached_totals(domain, chain, epochs, TotalsOf::Address(key.clone())).await
}

/// [`payment_totals`], its sealed epochs read from the totals cache.
pub async fn cached_payment_totals<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    payment: &PaymentKey,
) -> Result<AddressStats, DomainError> {
    cached_totals(domain, chain, epochs, TotalsOf::Payment(*payment)).await
}

/// [`account_totals`], its sealed epochs read from the totals cache.
pub async fn cached_account_totals<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    account: &AccountKey,
) -> Result<AddressStats, DomainError> {
    cached_totals(domain, chain, epochs, TotalsOf::Account(account.clone())).await
}

/// The totals of `of` over `epochs`, the last of which holds the tip.
///
/// The epochs up to the entry's watermark come from the cache: a miss folds
/// them into the entry it offers, at the cost of the unit entries it read.
/// The epochs after it fold as without a cache, and an entry behind the
/// current watermark is extended in the background. With the namespace
/// unregistered, or no epoch of `epochs` under the watermark, the read folds
/// as without a cache.
///
/// Async, and never inside a blocking closure: a miss waits on a fill that
/// takes a query permit of its own.
async fn cached_totals<D: Domain>(
    domain: &D,
    chain: &ChainSummary,
    epochs: RangeInclusive<Epoch>,
    of: TotalsOf,
) -> Result<AddressStats, DomainError> {
    let (start, tip) = (*epochs.start(), *epochs.end());

    let watermark = blocking(domain, chain, move |domain, chain| {
        totals_watermark(domain, chain, tip)
    })
    .await?;

    let uncached = |of: TotalsOf| {
        let epochs = epochs.clone();

        blocking(domain, chain, move |domain, chain| {
            Ok(of.fold(domain, chain, epochs)?.finish())
        })
    };

    let Some(watermark) = watermark.filter(|watermark| *watermark >= start) else {
        return uncached(of).await;
    };

    let key = TotalsKey { of, start };
    let raw = key.to_bytes();

    let fill = {
        let (domain, chain, key) = (domain.clone(), chain.clone(), key.clone());
        move || build_totals_entry(&domain, &chain, &key, watermark)
    };

    let value = match domain.cache().get_or_fill(TOTALS.name, &raw, fill).await {
        Ok(value) => value,
        Err(CacheError::Fill(error)) => {
            return Err(DomainError::Internal(format!("totals fold: {error}")))
        }
        Err(CacheError::UnknownNamespace(_)) => return uncached(key.of).await,
        Err(error) => {
            tracing::warn!(%error, "totals cache unavailable, folding uncached");
            return uncached(key.of).await;
        }
    };

    let (totals, entry_watermark) = blocking(domain, chain, {
        let (of, value) = (key.of.clone(), value.clone());

        move |domain, chain| {
            let entry = TotalsEntry::decode(&value)?;

            if entry.watermark > watermark {
                let fold = of.fold(domain, chain, epochs)?;
                return Ok((fold.finish(), entry.watermark));
            }

            let from = (entry.watermark + 1).max(start);
            let rest = of.fold(domain, chain, from..=tip)?;

            let mut totals = entry.stats;
            totals.add(&rest.finish());

            Ok((totals, entry.watermark))
        }
    })
    .await?;

    if entry_watermark != watermark {
        let extend = {
            let (domain, chain) = (domain.clone(), chain.clone());
            move || extend_totals_entry(&domain, &chain, &key, &value, watermark)
        };

        if let Err(error) = domain.cache().refill(TOTALS.name, &raw, extend) {
            tracing::warn!(%error, "totals cache entry not extended");
        }
    }

    Ok(totals)
}

/// Run `f` on the blocking pool under the domain's query limiter.
async fn blocking<D, T, F>(domain: &D, chain: &ChainSummary, f: F) -> Result<T, DomainError>
where
    D: Domain,
    T: Send + 'static,
    F: FnOnce(&D, &ChainSummary) -> Result<T, ChainError> + Send + 'static,
{
    let (inner, chain) = (domain.clone(), chain.clone());

    Ok(domain
        .query_limiter()
        .run_blocking(move || f(&inner, &chain))
        .await??)
}

#[cfg(test)]
mod tests {
    use dolos_core::{
        config::SyncConfig, ArchiveStore as _, BlockSlot, CacheStore as _, Domain as _,
        StateStore as _, StateWriter as _,
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
                let row = PaymentAddressSet {
                    payment,
                    epoch,
                    tail,
                };
                writer
                    .write_entity_typed(&super::super::GroupRow::key(&row), &row)
                    .unwrap();

                *payments.entry(payment).or_default() += 1 + i;
            }

            for (payment, tx_count) in payments {
                writer
                    .write_entity_typed(
                        &GroupKey {
                            group: PaymentKey(payment),
                            epoch,
                            member: (),
                        },
                        &PaymentStats {
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
        seal_at(domain, at, BOUNDARY)
    }

    fn seal_at<D: Domain>(domain: &D, at: &ToyDomain, boundary: Epoch) -> Result<(), ChainError> {
        let chain = chain(at);
        let mutable = crate::utils::mutable_slots(&at.genesis());

        seal::<D>(
            domain.state(),
            domain.archive(),
            &chain,
            &|epoch| is_sealable(&chain, mutable, epoch, boundary),
            2,
        )
    }

    type Logs = (
        Vec<(BlockSlot, AddressKey, AddressStats)>,
        Vec<(BlockSlot, PaymentKey, PaymentAddressChunk)>,
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
                .iter_logs_typed::<PaymentAddressChunk>(None)
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
                .iter_entities_typed::<PaymentAddressSet>(None)
                .unwrap()
                .count(),
            state
                .iter_entities_typed::<PaymentStats>(None)
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

    /// Every credential the seeded rows answer for, of each family.
    fn every_key() -> Vec<TotalsOf> {
        let addresses = addresses()
            .into_iter()
            .map(|address| TotalsOf::Address(AddressKey::of(&address)));

        let payments = [0x40, 0x41, 0x43, 0x99]
            .into_iter()
            .map(|payment| TotalsOf::Payment(PaymentKey([payment; 28].into())));

        let accounts = [a(), b()]
            .into_iter()
            .map(|account| TotalsOf::Account(AccountKey(account)));

        addresses.chain(payments).chain(accounts).collect()
    }

    fn uncached(domain: &ToyDomain, of: &TotalsOf, tip: Epoch) -> AddressStats {
        let chain = chain(domain);

        match of {
            TotalsOf::Address(key) => address_totals(domain, &chain, 0..=tip, key),
            TotalsOf::Payment(key) => payment_totals(domain, &chain, 0..=tip, key),
            TotalsOf::Account(key) => account_totals(domain, &chain, 0..=tip, key),
        }
        .unwrap()
    }

    async fn cached(domain: &ToyDomain, of: &TotalsOf, tip: Epoch) -> AddressStats {
        let chain = chain(domain);

        match of {
            TotalsOf::Address(key) => cached_address_totals(domain, &chain, 0..=tip, key).await,
            TotalsOf::Payment(key) => cached_payment_totals(domain, &chain, 0..=tip, key).await,
            TotalsOf::Account(key) => cached_account_totals(domain, &chain, 0..=tip, key).await,
        }
        .unwrap()
    }

    fn key(of: &TotalsOf) -> TotalsKey {
        TotalsKey {
            of: of.clone(),
            start: 0,
        }
    }

    fn entry(domain: &ToyDomain, of: &TotalsOf) -> Option<TotalsEntry> {
        let value = domain.cache().get(TOTALS.name, &key(of).to_bytes())?;
        Some(TotalsEntry::decode(&value).unwrap())
    }

    fn register(domain: &ToyDomain, admission: Cost) {
        domain
            .cache()
            .register(TOTALS.with_admission(admission))
            .unwrap();
    }

    fn watermark(domain: &ToyDomain, tip: Epoch) -> Option<Epoch> {
        totals_watermark(domain, &chain(domain), tip).unwrap()
    }

    /// Epochs 5 and 6 sealed, 7 staged, and the totals cache keeping every
    /// entry.
    fn sealed_and_cached() -> ToyDomain {
        let domain = seeded();
        seal_as(&domain, &domain).unwrap();
        register(&domain, 0);
        domain
    }

    /// A miss answers the uncached fold and keeps the sealed epochs; a hit
    /// adds the staged ones and answers the same.
    #[tokio::test]
    async fn a_hit_across_sealed_and_staged_epochs_answers_the_fold() {
        let domain = sealed_and_cached();
        assert_eq!(watermark(&domain, BOUNDARY), Some(6));

        for of in every_key() {
            let expected = uncached(&domain, &of, BOUNDARY);

            assert_eq!(
                cached(&domain, &of, BOUNDARY).await,
                expected,
                "{of:?} miss"
            );
            assert_eq!(entry(&domain, &of).unwrap().watermark, 6, "{of:?}");
            assert_eq!(cached(&domain, &of, BOUNDARY).await, expected, "{of:?} hit");
        }
    }

    /// A hit takes its sealed epochs from the entry: rows rewritten under it
    /// change the fold and not the cached answer.
    #[tokio::test]
    async fn a_hit_reads_its_sealed_epochs_from_the_entry() {
        let domain = sealed_and_cached();
        let mut before = Vec::new();

        for of in every_key() {
            before.push(cached(&domain, &of, BOUNDARY).await);
        }

        let chain = chain(&domain);
        let writer = domain.archive().start_writer().unwrap();

        for address in addresses() {
            let rewritten = AddressStats {
                units: [(Bytes::from(vec![0xbb]), sums(1, 0))]
                    .into_iter()
                    .collect(),
                tx_count: 1,
            };

            writer
                .write_log_typed(chain.epoch_start(5), &AddressKey::of(&address), &rewritten)
                .unwrap();
        }

        writer.commit().unwrap();

        for (of, before) in every_key().iter().zip(before) {
            assert_eq!(cached(&domain, of, BOUNDARY).await, before, "{of:?}");

            if before.tx_count > 0 {
                assert_ne!(uncached(&domain, of, BOUNDARY), before, "{of:?}");
            }
        }
    }

    /// An entry left behind by a seal answers the fold all the same, and is
    /// extended in the background to what a fresh build of the new watermark
    /// stores.
    #[tokio::test]
    async fn an_entry_extended_past_a_seal_answers_a_fresh_fold() {
        let domain = seeded();
        seal_at(&domain, &domain, 7).unwrap();
        register(&domain, 0);
        assert_eq!(watermark(&domain, 7), Some(5));

        for of in every_key() {
            assert_eq!(cached(&domain, &of, 7).await, uncached(&domain, &of, 7));
            assert_eq!(entry(&domain, &of).unwrap().watermark, 5, "{of:?}");
        }

        seal_at(&domain, &domain, BOUNDARY).unwrap();
        assert_eq!(watermark(&domain, BOUNDARY), Some(6));

        let chain = chain(&domain);

        for of in every_key() {
            let expected = uncached(&domain, &of, BOUNDARY);
            assert_eq!(cached(&domain, &of, BOUNDARY).await, expected, "{of:?}");

            let extended = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let value = domain.cache().get(TOTALS.name, &key(&of).to_bytes());

                    match value {
                        Some(value) if TotalsEntry::watermark_of(&value) == Some(6) => {
                            return value
                        }
                        _ => tokio::time::sleep(std::time::Duration::from_millis(1)).await,
                    }
                }
            })
            .await
            .expect("the entry was not extended");

            let fresh = build_totals_entry(&domain, &chain, &key(&of), 6).unwrap();
            assert_eq!(extended, fresh.value, "{of:?}");

            assert_eq!(cached(&domain, &of, BOUNDARY).await, expected, "{of:?}");
        }
    }

    /// A miss offers its entry at the cost of the unit entries it folded,
    /// and the namespace keeps it only at or above its threshold.
    #[tokio::test]
    async fn a_miss_is_kept_only_at_or_above_the_threshold() {
        let domain = seeded();
        seal_as(&domain, &domain).unwrap();
        let chain = chain(&domain);

        for of in every_key() {
            let expected = uncached(&domain, &of, BOUNDARY);
            let cost = build_totals_entry(&domain, &chain, &key(&of), 6)
                .unwrap()
                .cost;

            register(&domain, cost + 1);
            assert_eq!(cached(&domain, &of, BOUNDARY).await, expected, "{of:?}");
            assert_eq!(entry(&domain, &of), None, "{of:?}");

            register(&domain, cost);
            assert_eq!(cached(&domain, &of, BOUNDARY).await, expected, "{of:?}");
            assert_eq!(entry(&domain, &of).unwrap().cost, cost, "{of:?}");
        }

        // two sealed epochs of two units a row
        let address = TotalsOf::Address(AddressKey::of(&addresses()[0]));
        assert_eq!(entry(&domain, &address).unwrap().cost, 4);
    }

    #[tokio::test]
    async fn with_max_history_the_totals_cache_is_off() {
        let mut sync = SyncConfig::default();
        sync.max_history = Some(1_000);

        let domain = seeded().with_sync_config(sync);
        seal_as(&domain, &domain).unwrap();

        assert!(!register_totals_cache(&domain).unwrap());

        for of in every_key() {
            assert_eq!(
                cached(&domain, &of, BOUNDARY).await,
                uncached(&domain, &of, BOUNDARY),
                "{of:?}"
            );
        }

        assert!(domain.cache().stats().unwrap().namespaces.is_empty());

        let unpruned = seeded();
        assert!(register_totals_cache(&unpruned).unwrap());

        let stats = unpruned.cache().stats().unwrap();
        assert_eq!(stats.namespaces[0].name, TOTALS.name);
        assert_eq!(TOTALS.admission, 100_000);
    }

    #[test]
    fn totals_keys_round_trip_and_parse_from_text() {
        for of in every_key() {
            let key = TotalsKey { of, start: 7 };
            assert_eq!(TotalsKey::from_bytes(&key.to_bytes()).unwrap(), key);
        }

        assert!(TotalsKey::from_bytes(&[3; 9]).is_err());

        let domain = seeded();
        let chain = chain(&domain);
        let shelley = chain.first_shelley_epoch();

        let address = &addresses()[0];
        let parsed = TotalsKey::parse(&address.to_bech32().unwrap(), &chain).unwrap();
        assert_eq!(parsed.of, TotalsOf::Address(AddressKey::of(address)));
        assert_eq!(parsed.start, shelley);

        let hrp = bech32::Hrp::parse("addr_vkh").unwrap();
        let payment = bech32::encode::<bech32::Bech32>(hrp, &[0x40; 28]).unwrap();
        let parsed = TotalsKey::parse(&payment, &chain).unwrap();
        assert_eq!(parsed.of, TotalsOf::Payment(PaymentKey([0x40; 28].into())));

        let stake = pallas_extras::stake_credential_to_address(Network::Testnet, &a());
        let parsed = TotalsKey::parse(&stake.to_bech32().unwrap(), &chain).unwrap();
        assert_eq!(parsed.of, TotalsOf::Account(AccountKey(a())));
        assert_eq!(parsed.start, shelley);

        let byron = "Ae2tdPwUPEZLs4HtbuNey7tK4hTKrwNwYtGqp7bDfCy2WdR3P6735W5Yfpe";
        let parsed = TotalsKey::parse(byron, &chain).unwrap();
        assert!(matches!(
            parsed.of,
            TotalsOf::Address(AddressKey::Unstaked(_))
        ));
        assert_eq!(parsed.start, 0);

        assert!(TotalsKey::parse("not a key", &chain).is_err());
    }

    /// A row of `units` distinct units.
    fn wide(units: u32) -> AddressStats {
        AddressStats {
            units: (0..units)
                .map(|unit| (Bytes::from(unit.to_be_bytes().to_vec()), sums(1, 0)))
                .collect(),
            tx_count: 1,
        }
    }

    /// Candidates come from one sealed epoch's fold, per address, payment
    /// credential and account; staged rows are not scanned.
    #[test]
    fn totals_candidates_reach_the_threshold_in_one_sealed_epoch() {
        let domain = seeded();
        seal_as(&domain, &domain).unwrap();
        let chain = chain(&domain);
        let c = StakeCredential::AddrKeyhash([0x22; 28].into());

        assert!(totals_candidates(&domain, &chain).unwrap().is_empty());

        let threshold = CANDIDATE_EPOCH_ENTRIES as u32;
        let writer = domain.archive().start_writer().unwrap();

        let sealed = |epoch: Epoch, address: &Address, stats: &AddressStats| {
            writer
                .write_log_typed(chain.epoch_start(epoch), &AddressKey::of(address), stats)
                .unwrap();
        };

        // one row reaching it: the address, its payment and its account
        sealed(5, &base(0x44, b()), &wide(threshold));
        // two halves of one epoch: the account only
        sealed(6, &base(0x45, c.clone()), &wide(threshold / 2));
        sealed(6, &base(0x46, c.clone()), &wide(threshold / 2));
        // one short of it, and the same again in another epoch
        sealed(
            5,
            &base(0x47, StakeCredential::ScriptHash([0x23; 28].into())),
            &wide(threshold - 1),
        );
        sealed(
            6,
            &base(0x47, StakeCredential::ScriptHash([0x23; 28].into())),
            &wide(threshold - 1),
        );

        writer.commit().unwrap();

        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(
                &(7, AddressKey::of(&enterprise(0x48))),
                &wide(2 * threshold),
            )
            .unwrap();
        writer.commit().unwrap();

        let found: HashSet<TotalsKey> = totals_candidates(&domain, &chain)
            .unwrap()
            .into_iter()
            .collect();

        let shelley = chain.first_shelley_epoch();
        let at = |of| TotalsKey { of, start: shelley };

        let expected: HashSet<TotalsKey> = [
            at(TotalsOf::Address(AddressKey::of(&base(0x44, b())))),
            at(TotalsOf::Payment(PaymentKey([0x44; 28].into()))),
            at(TotalsOf::Account(AccountKey(b()))),
            at(TotalsOf::Account(AccountKey(c))),
        ]
        .into_iter()
        .collect();

        assert_eq!(found, expected);
    }
}
