use std::{
    collections::{BTreeSet, HashMap},
    ops::Deref,
};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    account_addresses_assets_inner::AccountAddressesAssetsInner,
    account_addresses_content_inner::AccountAddressesContentInner,
    account_addresses_total::AccountAddressesTotal,
    account_addresses_total_received_sum_inner::AccountAddressesTotalReceivedSumInner,
    account_content::AccountContent,
    account_delegation_content_inner::AccountDelegationContentInner,
    account_registration_content_inner::{AccountRegistrationContentInner, Action},
    account_reward_content_inner::AccountRewardContentInner,
    account_transactions_content_inner::AccountTransactionsContentInner,
    account_withdrawal_content_inner::AccountWithdrawalContentInner,
    address_utxo_content_inner::AddressUtxoContentInner,
};

use dolos_cardano::{
    indexes::{AsyncCardanoQueryExt, CardanoIndexExt, SlotOrder},
    model::{AccountState, DRepState},
    pallas_extras, AccountEpochLog, ChainSummary, FixedNamespace, PoolHash,
};
use dolos_core::{
    async_query::BlockRefMeta, ArchiveStore as _, Domain, EntityKey, LogKey, StateStore as _,
    TemporalKey, TxHash,
};
use futures::future::join_all;
use futures_util::StreamExt;
use itertools::Itertools;
use pallas::{
    codec::minicbor,
    crypto::hash::{Hash, Hasher},
    ledger::{
        addresses::{Address, Network, StakeAddress, StakePayload},
        primitives::Epoch,
        traverse::{MultiEraBlock, MultiEraCert, MultiEraTx},
    },
};

use pallas::ledger::primitives::alonzo::Certificate as AlonzoCert;
use pallas::ledger::primitives::conway::Certificate as ConwayCert;

use crate::{
    error::Error,
    inputs::{for_each_touched_output, InputDeps, InputResolver},
    mapping::{self, bech32_drep, bech32_pool, AssetTotals, IntoModel},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

struct AccountKeyParam {
    address: StakeAddress,
    entity_key: Vec<u8>,
}

fn stake_address_from_cip_19_credential(address: &str, network: Network) -> Option<StakeAddress> {
    let (hrp, payload) = bech32::decode(address).ok()?;

    let payload = match hrp.as_str() {
        // Ed25519 verification key.
        "stake_vk" => {
            let key: [u8; 32] = <[u8; 32]>::try_from(&payload[..]).ok()?;
            let hash: Hash<28> = Hasher::<224>::hash(&key);
            StakePayload::Stake(hash)
        }
        // Raw key-hash credential.
        "stake_vkh" => {
            StakePayload::Stake(Hash::<28>::from(<[u8; 28]>::try_from(&payload[..]).ok()?))
        }
        // Raw script-hash credential.
        "script" => {
            StakePayload::Script(Hash::<28>::from(<[u8; 28]>::try_from(&payload[..]).ok()?))
        }
        _ => return None,
    };

    Some(StakeAddress::new(network, payload))
}

fn parse_account_key_param(address: &str, network: Network) -> Result<AccountKeyParam, Error> {
    let address =
        if let Some(stake_address) = stake_address_from_cip_19_credential(address, network) {
            stake_address
        } else {
            let parsed = pallas::ledger::addresses::Address::from_bech32(address)
                .map_err(|_| Error::InvalidStakeAddress)?;

            let stake_address = match parsed {
                Address::Shelley(x) => pallas_extras::shelley_address_to_stake_address(&x),
                Address::Stake(x) => Some(x),
                _ => None,
            };

            stake_address.ok_or(Error::InvalidStakeAddress)?
        };

    let stake_cred = dolos_cardano::pallas_extras::stake_address_to_cred(&address);

    let entity_key =
        minicbor::to_vec(&stake_cred).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(AccountKeyParam {
        address,
        entity_key,
    })
}

struct AccountModelBuilder<'a> {
    account_state: dolos_cardano::model::AccountState,
    stake_address: Option<StakeAddress>,
    tip_slot: Option<u64>,
    chain: Option<&'a ChainSummary>,
}

impl<'a> IntoModel<AccountContent> for AccountModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<AccountContent, StatusCode> {
        let tip_slot = self.tip_slot.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let chain = self.chain.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let stake_address = self
            .stake_address
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
            .to_bech32()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let (current_epoch, _) = chain.slot_epoch(tip_slot);

        let active_epoch = self
            .account_state
            .registered_at
            .or(self.account_state.deregistered_at)
            .map(|x| chain.slot_epoch(x))
            .map(|(x, _)| x);

        let pool_id = self
            .account_state
            .delegated_pool_at(current_epoch)
            .or(self.account_state.retired_pool.as_ref())
            .map(bech32_pool)
            .transpose()?;

        let drep_id = self
            .account_state
            .delegated_drep_at(current_epoch)
            .map(bech32_drep)
            .transpose()?;

        let active = pool_id.is_some();

        let stake = self.account_state.stake.live().cloned().unwrap_or_default();

        let out = AccountContent {
            stake_address,
            active,
            registered: self.account_state.is_registered(),
            active_epoch: active_epoch.map(|x| x as i32),
            controlled_amount: stake.total().to_string(),
            rewards_sum: stake.rewards_sum.to_string(),
            withdrawals_sum: stake.withdrawals_sum.to_string(),
            reserves_sum: "0".to_string(),
            treasury_sum: "0".to_string(),
            withdrawable_amount: stake.withdrawable().to_string(),
            pool_id,
            drep_id,
        };

        Ok(out)
    }
}

impl<'a> IntoModel<Vec<AccountAddressesContentInner>> for AccountModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<AccountAddressesContentInner>, StatusCode> {
        let out: Vec<_> = vec![]
            .into_iter()
            .map(|x| AccountAddressesContentInner { address: x })
            .collect();

        Ok(out)
    }
}

pub async fn by_stake<D>(
    Path(stake_address): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<AccountContent>, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<DRepState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&stake_address, network)?;

    let state = domain
        .read_cardano_entity::<AccountState>(account_key.entity_key.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let tip_slot = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(|(slot, _)| slot)
        .unwrap_or_default();

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let model = AccountModelBuilder {
        account_state: state,
        stake_address: Some(account_key.address),
        tip_slot: Some(tip_slot),
        chain: Some(&chain),
    }
    .into_model()?;

    Ok(Json(model))
}

pub async fn by_stake_addresses<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountAddressesContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;
    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&stake_address, network)?;
    if !domain.cardano_entity_exists::<AccountState>(account_key.entity_key.as_slice())? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let (start_slot, end_slot) = pagination.start_and_end_slots(&domain).await?;
    let stream = domain.query().blocks_by_stake_stream(
        &account_key.address.to_vec(),
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    );

    let mut items = vec![];
    let mut skipped = 0;
    let mut seen = BTreeSet::new();

    let mut stream = Box::pin(stream);

    while let Some(res) = stream.next().await {
        if items.len() >= pagination.count {
            break;
        }

        let (_slot, block) = res.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let Some(block) = block else {
            continue;
        };

        let block = MultiEraBlock::decode(&block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        for (_, utxo) in block.txs().iter().flat_map(|tx| tx.produces()) {
            let address = utxo
                .address()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            if match &address {
                Address::Shelley(shelley) => {
                    pallas_extras::shelley_address_to_stake_address(shelley)
                        .map(|x| x.to_vec() == account_key.address.to_vec())
                        .unwrap_or(false)
                }
                Address::Stake(stake) => stake.to_vec() == account_key.address.to_vec(),
                Address::Byron(_) => false,
            } && seen.insert(address.to_string())
            {
                if skipped < (pagination.page as usize - 1) * pagination.count {
                    skipped += 1;
                } else {
                    items.push(AccountAddressesContentInner {
                        address: address.to_string(),
                    });
                    if items.len() >= pagination.count {
                        break;
                    }
                }
            }
        }
        if items.len() >= pagination.count {
            break;
        }
    }

    Ok(Json(items))
}

/// Fold one block's txs into an account's lifetime totals.
///
/// Produced outputs are received and resolved inputs are sent, the same split
/// `/addresses/{address}/total` makes for a single address; a tx counts once
/// however many of the account's addresses it touches.
async fn sum_account_block_txs<D>(
    domain: &Facade<D>,
    deps: &mut InputDeps,
    account: &[u8],
    block: &[u8],
    received: &mut AssetTotals,
    sent: &mut AssetTotals,
    tx_count: &mut usize,
) -> Result<(), StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let txs = block.txs();

    let mut resolver = deps.prepare(domain, txs.iter()).await?;

    for tx in txs.iter() {
        let mut matched = false;

        for (_, output) in tx.produces() {
            let address = output
                .address()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            if address_belongs_to_account(&address, account) {
                received.add_output(&output);
                matched = true;
            }
        }

        for input in tx.consumes() {
            if let Some(output) = resolver.resolve(&input)? {
                let address = output
                    .address()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                if address_belongs_to_account(&address, account) {
                    sent.add_output(&output);
                    matched = true;
                }
            }
        }

        if matched {
            *tx_count += 1;
        }
    }

    Ok(())
}

fn account_amounts(totals: AssetTotals) -> Vec<AccountAddressesTotalReceivedSumInner> {
    totals
        .into_amounts()
        .into_iter()
        .map(|amount| AccountAddressesTotalReceivedSumInner {
            unit: amount.unit,
            quantity: amount.quantity,
        })
        .collect()
}

/// `GET /accounts/{stake_address}/addresses/total`: lifetime sums and tx count
/// across every address of an account.
///
/// The totals are folded from the archive on each request rather than kept as
/// state: a per-account asset breakdown is unbounded — one row grows with every
/// distinct asset the account ever touched — and it would ride along in every
/// stele and every state rebuild. The account therefore pays the same full
/// scan `/addresses/{address}/total` already pays for a single address, which
/// also means the answer only covers the history the archive still holds.
pub async fn by_stake_addresses_total<D>(
    Path(stake_address): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<AccountAddressesTotal>, Error>
where
    Option<AccountState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&stake_address, network)?;

    if !domain.cardano_entity_exists::<AccountState>(account_key.entity_key.as_slice())? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let account = account_key.address.to_vec();
    let end_slot = domain.get_tip_slot()?;

    let stream = domain
        .query()
        .blocks_by_stake_stream(&account, 0, end_slot, SlotOrder::Asc);

    let mut received = AssetTotals::default();
    let mut sent = AssetTotals::default();
    let mut tx_count: usize = 0;
    let mut deps = InputDeps::default();

    let mut stream = Box::pin(stream);

    while let Some(res) = stream.next().await {
        let (_slot, block) = res.map_err(|err| {
            tracing::error!(?err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let Some(block) = block else {
            continue;
        };

        sum_account_block_txs(
            &domain,
            &mut deps,
            &account,
            &block,
            &mut received,
            &mut sent,
            &mut tx_count,
        )
        .await?;
    }

    let stake_address = account_key
        .address
        .to_bech32()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let model = AccountAddressesTotal {
        stake_address,
        received_sum: account_amounts(received),
        sent_sum: account_amounts(sent),
        tx_count: tx_count as i32,
    };

    Ok(Json(model))
}

pub async fn by_stake_utxos<D>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressUtxoContentInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&address, network)?;

    let refs = domain
        .indexes()
        .utxos_by_stake(&account_key.address.to_vec())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let utxos = super::utxos::load_utxo_models(&domain, refs, pagination).await?;

    Ok(Json(utxos))
}

pub async fn by_stake_addresses_assets<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountAddressesAssetsInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&stake_address, network)?;

    if !domain.cardano_entity_exists::<AccountState>(account_key.entity_key.as_slice())? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let refs = domain
        .indexes()
        .utxos_by_stake(&account_key.address.to_vec())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let utxos = domain
        .state()
        .get_utxos(refs.into_iter().collect())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // chain position of each utxo's tx: Blockfrost orders assets by the
    // position of the oldest (asc) or newest (desc) utxo holding them
    let tx_deps: Vec<TxHash> = utxos.keys().map(|txo_ref| txo_ref.0).unique().collect();

    // one facade for the whole fan-out so its blocking-task limiter is shared
    let query = domain.query();

    let block_deps: HashMap<TxHash, BlockRefMeta> = join_all(tx_deps.iter().map(|tx| {
        let tx = *tx;
        let query = &query;
        async move {
            match query.block_meta_by_tx_hash(tx.to_vec()).await {
                Ok(Some(block_data)) => Some(Ok((tx, block_data))),
                Ok(None) => None,
                Err(_) => Some(Err(StatusCode::INTERNAL_SERVER_ERROR)),
            }
        }
    }))
    .await
    .into_iter()
    .flatten()
    .collect::<Result<_, _>>()?;

    let by_unit = mapping::aggregate_account_assets(&utxos, &block_deps)?;

    let mut entries: Vec<(String, mapping::AssetAggregate)> = by_unit.into_iter().collect();

    // mirrors the Blockfrost ordering: asc by the oldest utxo holding the
    // asset, desc by the newest (deliberately not reverses of each other).
    // ties resolve unit-ascending: entries iterate out of the BTreeMap in
    // unit order and sort_by is stable, so equal positions keep it
    match pagination.order {
        Order::Asc => entries.sort_by(|x, y| x.1.oldest.cmp(&y.1.oldest)),
        Order::Desc => entries.sort_by(|x, y| y.1.newest.cmp(&x.1.newest)),
    }

    let assets = entries
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|(unit, agg)| AccountAddressesAssetsInner {
            unit,
            quantity: agg.quantity.to_string(),
        })
        .collect();

    Ok(Json(assets))
}

fn build_delegation<D: Domain>(
    ctx: &AccountActionContext<'_, D>,
    stake_address: &StakeAddress,
    tx: &MultiEraTx,
    cert: &MultiEraCert,
) -> Result<Option<AccountDelegationContentInner>, StatusCode> {
    let (cred, pool) = match cert {
        MultiEraCert::AlonzoCompatible(cert) => match cert.deref().deref() {
            AlonzoCert::StakeDelegation(cred, pool) => (cred, pool),
            _ => return Ok(None),
        },
        MultiEraCert::Conway(cert) => match cert.deref().deref() {
            ConwayCert::StakeDelegation(cred, pool) => (cred, pool),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };

    let address = mapping::stake_cred_to_address(cred, ctx.network);

    if address != *stake_address {
        return Ok(None);
    }

    let pool = mapping::bech32_pool(pool)?;

    Ok(Some(AccountDelegationContentInner {
        active_epoch: (ctx.epoch + 2) as i32,
        tx_hash: tx.hash().to_string(),
        amount: tx
            .outputs()
            .iter()
            .map(|x| x.value().coin())
            .sum::<u64>()
            .to_string(),
        pool_id: pool,
        tx_slot: ctx.block.slot() as i32,
        block_time: ctx.chain.slot_time(ctx.block.slot()) as i32,
        block_height: ctx.block.number() as i32,
    }))
}

fn build_registration<D: Domain>(
    ctx: &AccountActionContext<'_, D>,
    stake_address: &StakeAddress,
    tx: &MultiEraTx,
    cert: &MultiEraCert,
) -> Result<Option<AccountRegistrationContentInner>, StatusCode> {
    let key_deposit = || -> Result<String, StatusCode> {
        Ok(ctx
            .domain
            .get_effective_pparams_for_epoch(ctx.epoch, ctx.chain)?
            .key_deposit()
            .unwrap_or_default()
            .to_string())
    };

    let (cred, is_registration, deposit) = match cert {
        MultiEraCert::AlonzoCompatible(cert) => match cert.deref().deref() {
            AlonzoCert::StakeRegistration(cred) => (cred, true, Some(key_deposit()?)),
            AlonzoCert::StakeDeregistration(cred) => (cred, false, None),
            _ => return Ok(None),
        },
        MultiEraCert::Conway(cert) => match cert.deref().deref() {
            ConwayCert::StakeRegistration(cred) => (cred, true, Some(key_deposit()?)),
            ConwayCert::StakeDeregistration(cred) => (cred, false, None),
            ConwayCert::Reg(cred, coin) => (cred, true, Some(coin.to_string())),
            ConwayCert::UnReg(cred, _) => (cred, false, None),
            ConwayCert::StakeRegDeleg(cred, _, coin) => (cred, true, Some(coin.to_string())),
            ConwayCert::StakeVoteRegDeleg(cred, _, _, coin) => (cred, true, Some(coin.to_string())),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };

    let address = mapping::stake_cred_to_address(cred, ctx.network);

    if address != *stake_address {
        return Ok(None);
    }

    Ok(Some(AccountRegistrationContentInner {
        tx_hash: tx.hash().to_string(),
        action: if is_registration {
            Action::Registered
        } else {
            Action::Deregistered
        },
        deposit,
        tx_slot: ctx.block.slot() as i32,
        block_time: ctx.chain.slot_time(ctx.block.slot()) as i32,
        block_height: ctx.block.number() as i32,
    }))
}

fn find_withdrawals_in_block(
    stake_address: &StakeAddress,
    chain: &ChainSummary,
    pagination: &Pagination,
    block: &[u8],
) -> Result<Vec<AccountWithdrawalContentInner>, StatusCode> {
    let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let account = stake_address.to_vec();

    let mut matches = vec![];

    for (idx, tx) in block.txs().iter().enumerate() {
        if pagination.should_skip(block.number(), idx) {
            continue;
        }

        let withdrawals = tx.withdrawals();
        let withdrawals: Vec<_> = withdrawals.collect();

        if let Some(amount) = withdrawals
            .into_iter()
            .find_map(|(address, amount)| (address == account.as_slice()).then_some(amount))
        {
            matches.push(AccountWithdrawalContentInner {
                tx_hash: tx.hash().to_string(),
                amount: amount.to_string(),
                tx_slot: block.slot() as i32,
                block_time: chain.slot_time(block.slot()) as i32,
                block_height: block.number() as i32,
            });
        }
    }

    if matches!(pagination.order, Order::Desc) {
        matches.reverse();
    }

    Ok(matches)
}

struct AccountActivityModelBuilder<T> {
    stake_address: StakeAddress,
    network: Network,
    page_size: usize,
    page_number: usize,
    skipped: usize,
    items: Vec<T>,
}

struct AccountActionContext<'a, D: Domain> {
    domain: &'a Facade<D>,
    epoch: Epoch,
    network: Network,
    block: &'a MultiEraBlock<'a>,
    chain: &'a ChainSummary,
}

impl<T> AccountActivityModelBuilder<T> {
    fn new(
        stake_address: StakeAddress,
        network: Network,
        page_size: usize,
        page_number: usize,
    ) -> Self {
        Self {
            stake_address,
            network,
            page_size,
            page_number,
            skipped: 0,
            items: vec![],
        }
    }

    fn should_skip(&self) -> bool {
        self.skipped < (self.page_number - 1) * self.page_size
    }

    fn add(&mut self, item: T) {
        if self.should_skip() {
            self.skipped += 1;
        } else {
            self.items.push(item);
        }
    }

    fn needs_more(&self) -> bool {
        self.items.len() < self.page_size
    }

    fn scan_block_certs<D, F>(
        &mut self,
        domain: &Facade<D>,
        epoch: Epoch,
        block: &MultiEraBlock,
        chain: &ChainSummary,
        mapper: F,
        order: crate::pagination::Order,
    ) -> Result<(), StatusCode>
    where
        D: Domain,
        F: Fn(
            &AccountActionContext<'_, D>,
            &StakeAddress,
            &MultiEraTx,
            &MultiEraCert,
        ) -> Result<Option<T>, StatusCode>,
    {
        let txs = block.txs();
        let mut block_items = vec![];
        let ctx = AccountActionContext {
            domain,
            epoch,
            network: self.network,
            block,
            chain,
        };

        for tx in txs {
            for cert in tx.certs() {
                if let Some(model) = mapper(&ctx, &self.stake_address, &tx, &cert)? {
                    block_items.push(model);
                }
            }
        }

        if matches!(order, crate::pagination::Order::Desc) {
            block_items.reverse();
        }

        for item in block_items {
            if !self.needs_more() {
                break;
            }
            self.add(item);
        }

        Ok(())
    }
}

impl IntoModel<Vec<AccountDelegationContentInner>>
    for AccountActivityModelBuilder<AccountDelegationContentInner>
{
    type SortKey = ();

    fn into_model(self) -> Result<Vec<AccountDelegationContentInner>, StatusCode> {
        Ok(self.items)
    }
}

impl IntoModel<Vec<AccountAddressesContentInner>>
    for AccountActivityModelBuilder<AccountAddressesContentInner>
{
    type SortKey = ();

    fn into_model(self) -> Result<Vec<AccountAddressesContentInner>, StatusCode> {
        Ok(self.items)
    }
}

async fn by_stake_actions<D, F, T>(
    stake_address: &str,
    pagination: Pagination,
    domain: Facade<D>,
    mapper: F,
) -> Result<Vec<T>, Error>
where
    Option<AccountState>: From<D::Entity>,
    F: Fn(
        &AccountActionContext<'_, D>,
        &StakeAddress,
        &MultiEraTx,
        &MultiEraCert,
    ) -> Result<Option<T>, StatusCode>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(stake_address, network)?;

    if !domain.cardano_entity_exists::<AccountState>(account_key.entity_key.as_slice())? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let network = domain
        .get_network_id()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut builder = AccountActivityModelBuilder::new(
        account_key.address,
        network,
        pagination.count,
        pagination.page as usize,
    );

    let (start_slot, end_slot) = pagination.start_and_end_slots(&domain).await?;
    let stream = domain.query().blocks_by_account_certs_stream(
        &account_key.entity_key,
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    );

    let mut stream = Box::pin(stream);

    while let Some(res) = stream.next().await {
        if !builder.needs_more() {
            break;
        }

        let (slot, block) = res.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let Some(block) = block else {
            continue;
        };

        let (epoch, _) = chain.slot_epoch(slot);

        let block = MultiEraBlock::decode(&block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        builder.scan_block_certs(&domain, epoch, &block, &chain, &mapper, pagination.order)?;
    }

    Ok(builder.items)
}

pub async fn by_stake_delegations<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountDelegationContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let items = by_stake_actions::<D, _, AccountDelegationContentInner>(
        &stake_address,
        pagination,
        domain,
        build_delegation,
    )
    .await?;

    Ok(Json(items))
}

pub async fn by_stake_registrations<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountRegistrationContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let items = by_stake_actions::<D, _, AccountRegistrationContentInner>(
        &stake_address,
        pagination,
        domain,
        build_registration,
    )
    .await?;

    Ok(Json(items))
}

/// Build the reward entries for one reward epoch.
///
/// `stake` contains the leader and member rewards for this epoch. `refund`
/// contains the deposit refunds from the row two epochs before this epoch.
///
/// Each `leader_rewards` item becomes a separate entry. This preserves all
/// leader rewards when one account operates more than one pool.
fn reward_entries(
    epoch: Epoch,
    stake: Option<&AccountEpochLog>,
    refund: Option<&AccountEpochLog>,
) -> Result<Vec<AccountRewardContentInner>, StatusCode> {
    use blockfrost_openapi::models::account_reward_content_inner::Type;

    fn entry(
        epoch: Epoch,
        pool: &PoolHash,
        amount: u64,
        r#type: Type,
    ) -> Result<AccountRewardContentInner, StatusCode> {
        Ok(AccountRewardContentInner {
            epoch: epoch as i32,
            amount: amount.to_string(),
            pool_id: mapping::bech32_pool(pool)?,
            r#type,
        })
    }

    let mut out = Vec::new();

    if let Some(stake) = stake {
        for (pool, amount) in &stake.leader_rewards {
            if *amount > 0 {
                out.push(entry(epoch, pool, *amount, Type::Leader)?);
            }
        }

        if let (Some(pool), Some(amount)) = (stake.pool_id.as_ref(), stake.member_reward) {
            if amount > 0 {
                out.push(entry(epoch, pool, amount, Type::Member)?);
            }
        }
    }

    if let Some(refund) = refund {
        for (pool, amount) in &refund.deposit_refunds {
            if *amount > 0 {
                out.push(entry(epoch, pool, *amount, Type::PoolDepositRefund)?);
            }
        }
    }

    Ok(out)
}

pub async fn by_stake_rewards<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountRewardContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&stake_address, network)?;
    if !domain.cardano_entity_exists::<AccountState>(account_key.entity_key.as_slice())? {
        return Err(StatusCode::NOT_FOUND.into());
    }
    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (epoch, _) = summary.slot_epoch(tip);

    let entity_key: EntityKey = account_key.entity_key.into();
    let mut items = Vec::new();
    let mut skipped = 0;
    let skip = pagination.skip();

    // Blockfrost reports the reward epoch as `earned_epoch`. The db-sync schema
    // uses the same term. Each reward epoch uses two merged rows:
    //
    //   * Row `e` contains the leader and member rewards for epoch `e`.
    //   * Row `e - 2` contains refunds that become spendable in epoch `e`.
    //
    // This order matches the db-sync order: leader, member, then refund.
    for reward_epoch in 0..=epoch {
        let stake_slot = summary.epoch_start(reward_epoch);
        let stake_key: LogKey = (TemporalKey::from(stake_slot), entity_key.clone()).into();

        // This call reads both rows at the same time. `read_logs_typed`
        // returns one result for each key, in the order of the keys. The last
        // result is the refund row. The result before it is the stake row.
        // Before epoch 2 there is no refund row. In that case the call reads
        // only the stake row.
        let (stake, refund) = if reward_epoch >= 2 {
            let refund_slot = summary.epoch_start(reward_epoch - 2);
            let refund_key: LogKey = (TemporalKey::from(refund_slot), entity_key.clone()).into();

            let mut rows = domain
                .archive()
                .read_logs_typed::<AccountEpochLog>(AccountEpochLog::NS, &[&stake_key, &refund_key])
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let refund = rows.pop().flatten();
            let stake = rows.pop().flatten();
            (stake, refund)
        } else {
            let stake = domain
                .archive()
                .read_log_typed::<AccountEpochLog>(AccountEpochLog::NS, &stake_key)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            (stake, None)
        };

        for item in reward_entries(reward_epoch, stake.as_ref(), refund.as_ref())? {
            if skipped < skip {
                skipped += 1;
                continue;
            }

            items.push(item);

            if items.len() >= pagination.count {
                return Ok(Json(items));
            }
        }
    }

    Ok(Json(items))
}

pub async fn by_stake_withdrawals<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountWithdrawalContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&stake_address, network)?;
    if !domain.cardano_entity_exists::<AccountState>(account_key.entity_key.as_slice())? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let (start_slot, end_slot) = pagination.start_and_end_slots(&domain).await?;
    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let stream = domain.query().blocks_by_account_withdrawals_stream(
        &account_key.address.to_vec(),
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    );

    let mut items = Vec::new();
    let mut stream = Box::pin(stream);

    while let Some(res) = stream.next().await {
        let (_slot, block) = res.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let Some(block) = block else {
            continue;
        };

        let mut withdrawals =
            find_withdrawals_in_block(&account_key.address, &chain, &pagination, &block)
                .map_err(Error::Code)?;
        items.append(&mut withdrawals);

        if items.len() >= pagination.from() + pagination.count {
            break;
        }
    }

    let items = items
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .collect();

    Ok(Json(items))
}

fn address_belongs_to_account(address: &Address, account: &[u8]) -> bool {
    match address {
        Address::Shelley(shelley) => pallas_extras::shelley_address_to_stake_address(shelley)
            .map(|x| x.to_vec() == account)
            .unwrap_or(false),
        Address::Stake(stake) => stake.to_vec() == account,
        Address::Byron(_) => false,
    }
}

fn account_addresses_in_tx(
    resolver: &mut InputResolver<'_>,
    account: &[u8],
    tx: &MultiEraTx<'_>,
) -> Result<BTreeSet<String>, StatusCode> {
    let mut addresses = BTreeSet::new();

    // never stops early: every touched output that belongs to the account
    // contributes its address to the set.
    for_each_touched_output(resolver, tx, |output| {
        let candidate = output
            .address()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if address_belongs_to_account(&candidate, account) {
            addresses.insert(candidate.to_string());
        }

        Ok(false)
    })?;

    Ok(addresses)
}

async fn find_account_txs_in_block<D>(
    domain: &Facade<D>,
    deps: &mut InputDeps,
    account: &[u8],
    chain: &ChainSummary,
    pagination: &Pagination,
    block: &[u8],
) -> Result<Vec<AccountTransactionsContentInner>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let txs = block.txs();

    // only the txs that will actually be scanned contribute dependencies
    let scanned = txs
        .iter()
        .enumerate()
        .filter(|(idx, _)| !pagination.should_skip(block.number(), *idx))
        .map(|(_, tx)| tx);

    let mut resolver = deps.prepare(domain, scanned).await?;

    let mut matches = vec![];

    for (idx, tx) in txs.iter().enumerate() {
        if pagination.should_skip(block.number(), idx) {
            continue;
        }

        let addresses = account_addresses_in_tx(&mut resolver, account, tx)?;

        let tx_hash = hex::encode(tx.hash().as_slice());
        let block_height = block.number() as i32;
        let block_time = chain.slot_time(block.slot()) as i32;

        for address in addresses {
            matches.push(AccountTransactionsContentInner {
                address,
                tx_hash: tx_hash.clone(),
                tx_index: idx as i32,
                block_height,
                block_time,
            });
        }
    }

    if matches!(pagination.order, Order::Desc) {
        matches.reverse();
    }

    Ok(matches)
}

pub async fn by_stake_transactions<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountTransactionsContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&stake_address, network)?;
    if !domain.cardano_entity_exists::<AccountState>(account_key.entity_key.as_slice())? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let account = account_key.address.to_vec();

    let (start_slot, end_slot) = pagination.start_and_end_slots(&domain).await?;
    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let stream = domain.query().blocks_by_stake_stream(
        &account,
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    );

    let mut matches = Vec::new();
    let mut deps = InputDeps::default();
    let mut stream = Box::pin(stream);

    while let Some(res) = stream.next().await {
        let (_slot, block) = res.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let Some(block) = block else {
            continue;
        };

        let mut txs =
            find_account_txs_in_block(&domain, &mut deps, &account, &chain, &pagination, &block)
                .await?;
        matches.append(&mut txs);

        if matches.len() >= pagination.from() + pagination.count {
            break;
        }
    }

    let transactions = matches
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .collect();

    Ok(Json(transactions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use blockfrost_openapi::models::{
        account_addresses_assets_inner::AccountAddressesAssetsInner,
        account_addresses_content_inner::AccountAddressesContentInner,
        account_content::AccountContent,
        account_delegation_content_inner::AccountDelegationContentInner,
        account_registration_content_inner::AccountRegistrationContentInner,
        account_reward_content_inner::AccountRewardContentInner,
        account_transactions_content_inner::AccountTransactionsContentInner,
        account_withdrawal_content_inner::AccountWithdrawalContentInner,
    };
    use dolos_core::{ArchiveWriter as _, EraCbor, StateWriter as _, UtxoSetDelta};
    use dolos_testing::{
        synthetic::SyntheticBlockConfig, toy_domain::ToyDomain, utxo_with_value, MIN_UTXO_AMOUNT,
    };
    use pallas::ledger::primitives::conway::{PositiveCoin, Value};
    use std::{collections::BTreeMap, sync::Arc};

    fn invalid_stake_address() -> &'static str {
        "not-a-stake"
    }

    fn missing_stake_address() -> &'static str {
        "stake_test1uqysjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgeeww5k"
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    fn encode_bech32(hrp: &str, payload: &[u8]) -> String {
        let hrp = bech32::Hrp::parse(hrp).expect("invalid hrp");
        bech32::encode::<bech32::Bech32>(hrp, payload).expect("failed to encode bech32")
    }

    #[test]
    fn cip_19_credential_forms_resolve_to_canonical_stake_address() {
        let network = Network::Testnet;

        let vk = [7u8; 32];
        let key_hash: Hash<28> = Hasher::<224>::hash(&vk);
        let script_hash = Hash::<28>::from([9u8; 28]);

        // `stake_vk`: Ed25519 key hashed into a key credential.
        let canonical_key = StakeAddress::new(network, StakePayload::Stake(key_hash));
        let expected_key =
            parse_account_key_param(&canonical_key.to_bech32().expect("bech32"), network)
                .expect("canonical key address")
                .entity_key;

        let stake_vk = parse_account_key_param(&encode_bech32("stake_vk", &vk), network)
            .expect("stake_vk resolves");
        assert_eq!(stake_vk.entity_key, expected_key);
        assert_eq!(stake_vk.address, canonical_key);

        // `stake_vkh`: raw key-hash credential.
        let stake_vkh =
            parse_account_key_param(&encode_bech32("stake_vkh", key_hash.as_ref()), network)
                .expect("stake_vkh resolves");
        assert_eq!(stake_vkh.entity_key, expected_key);
        assert_eq!(stake_vkh.address, canonical_key);

        // `script`: raw script-hash credential.
        let canonical_script = StakeAddress::new(network, StakePayload::Script(script_hash));
        let expected_script =
            parse_account_key_param(&canonical_script.to_bech32().expect("bech32"), network)
                .expect("canonical script address")
                .entity_key;

        let script =
            parse_account_key_param(&encode_bech32("script", script_hash.as_ref()), network)
                .expect("script resolves");
        assert_eq!(script.entity_key, expected_script);
        assert_eq!(script.address, canonical_script);
    }

    #[test]
    fn malformed_credential_forms_are_rejected() {
        let network = Network::Testnet;

        let assert_bad_request = |address: String| {
            let result = parse_account_key_param(&address, network).err();
            assert!(matches!(result, Some(Error::InvalidStakeAddress)));
        };

        // `stake_vk` requires a 32-byte key.
        assert_bad_request(encode_bech32("stake_vk", &[7u8; 16]));
        // `stake_vkh` requires a 28-byte hash.
        assert_bad_request(encode_bech32("stake_vkh", &[7u8; 20]));
        // `script` requires a 28-byte hash.
        assert_bad_request(encode_bech32("script", &[9u8; 32]));
        // An unknown prefix is not a credential and is not a valid address.
        assert_bad_request(encode_bech32("addr_vk", &[7u8; 32]));
    }

    fn asset_utxo(address: &str, assets: &[([u8; 28], u64)]) -> Arc<EraCbor> {
        let multi: BTreeMap<_, _> = assets
            .iter()
            .map(|(policy, quantity)| {
                (
                    Hash::from(*policy),
                    BTreeMap::from_iter([(
                        pallas::codec::utils::Bytes::from(b"tok".to_vec()),
                        PositiveCoin::try_from(*quantity).unwrap(),
                    )]),
                )
            })
            .collect();

        Arc::new(utxo_with_value(
            address,
            Value::Multiasset(MIN_UTXO_AMOUNT, multi),
        ))
    }

    fn asset_unit(policy: [u8; 28]) -> String {
        format!("{}{}", hex::encode(policy), hex::encode(b"tok"))
    }

    #[tokio::test]
    async fn accounts_by_stake_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: AccountContent =
            serde_json::from_slice(&bytes).expect("failed to parse account content");
    }

    #[tokio::test]
    async fn accounts_by_stake_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/addresses?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<AccountAddressesContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account addresses");
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_paginated() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path_page_1 = format!("/accounts/{stake_address}/addresses?page=1&count=1");
        let path_page_2 = format!("/accounts/{stake_address}/addresses?page=2&count=1");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AccountAddressesContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse account addresses page 1");
        let page_2: Vec<AccountAddressesContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse account addresses page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1[0].address, page_2[0].address);
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_assets_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let asset_unit = app.vectors().asset_unit.clone();

        let path = format!("/accounts/{stake_address}/addresses/assets");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let assets: Vec<AccountAddressesAssetsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account assets");

        let entry = assets
            .iter()
            .find(|a| a.unit == asset_unit)
            .expect("fixture asset missing from account assets");

        // cross-check the aggregate against the per-utxo amounts served by
        // the utxos endpoint for the same account
        let (status, bytes) = app
            .get_bytes(&format!("/accounts/{stake_address}/utxos?count=100"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let utxos: Vec<
            blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner,
        > = serde_json::from_slice(&bytes).expect("failed to parse account utxos");

        let expected: u128 = utxos
            .iter()
            .flat_map(|u| u.amount.iter())
            .filter(|a| a.unit == asset_unit)
            .map(|a| a.quantity.parse::<u128>().expect("bad quantity"))
            .sum();

        assert!(expected > 0, "fixture must hold the asset in some utxo");
        assert_eq!(entry.quantity, expected.to_string());
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_assets_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/addresses/assets", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_assets_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/addresses/assets", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_assets_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/addresses/assets");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_assets_orders_by_oldest_and_newest_utxos() {
        const ASSET_A: [u8; 28] = [0xAA; 28];
        const ASSET_B: [u8; 28] = [0xBB; 28];
        const ASSET_C: [u8; 28] = [0xCC; 28];

        let fixture_config = SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 1,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(fixture_config, |domain, vectors| {
            let stake_address = Address::from_bech32(&vectors.stake_address)
                .expect("invalid fixture stake address")
                .to_vec();
            let mut ordered_refs: Vec<_> = domain
                .indexes()
                .utxos_by_stake(&stake_address)
                .expect("failed to load fixture utxos")
                .into_iter()
                .collect();
            ordered_refs.sort_by_key(|txo_ref| {
                let (block_number, tx_index) = vectors.tx_position(&txo_ref.0.to_string());
                (block_number, tx_index, txo_ref.1)
            });
            assert_eq!(ordered_refs.len(), 3, "fixture needs three ordered utxos");

            let oldest_ref = ordered_refs[0].clone();
            let middle_ref = ordered_refs[1].clone();
            let newest_ref = ordered_refs[2].clone();

            // Three distinct assets are distributed across three UTxOs:
            // oldest UTxO: 5 units of ASSET_A
            // middle UTxO: 3 units of ASSET_B
            // newest UTxO: 7 units of ASSET_A and 1 unit of ASSET_C
            //
            // ASSET_A sorts first ascending because its oldest occurrence is
            // first, and first descending because its newest occurrence is
            // last.
            let produced_utxo = HashMap::from([
                (oldest_ref, asset_utxo(&vectors.address, &[(ASSET_A, 5)])),
                (middle_ref, asset_utxo(&vectors.address, &[(ASSET_B, 3)])),
                (
                    newest_ref,
                    asset_utxo(&vectors.address, &[(ASSET_A, 7), (ASSET_C, 1)]),
                ),
            ]);

            let writer = domain
                .state()
                .start_writer()
                .expect("failed to write state");
            writer
                .apply_utxoset(&UtxoSetDelta {
                    produced_utxo,
                    ..Default::default()
                })
                .expect("failed to replace fixture utxos");
            writer.commit().expect("failed to commit fixture utxos");
        });

        let stake_address = app.vectors().stake_address.as_str();
        let extract_units = |items: Vec<AccountAddressesAssetsInner>| {
            items.into_iter().map(|item| item.unit).collect::<Vec<_>>()
        };
        let asset_a_unit = asset_unit(ASSET_A);
        let asset_b_unit = asset_unit(ASSET_B);
        let asset_c_unit = asset_unit(ASSET_C);

        let (status, bytes) = app
            .get_bytes(&format!(
                "/accounts/{stake_address}/addresses/assets?order=asc"
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        let ascending_assets: Vec<AccountAddressesAssetsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse asc assets");
        assert_eq!(ascending_assets[0].quantity, "12");
        let ascending_units = extract_units(ascending_assets);

        let (status, bytes) = app
            .get_bytes(&format!(
                "/accounts/{stake_address}/addresses/assets?order=desc"
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        let descending_units =
            extract_units(serde_json::from_slice(&bytes).expect("failed to parse desc assets"));

        // Ascending uses each asset's oldest UTxO.
        assert_eq!(
            ascending_units,
            vec![
                asset_a_unit.clone(),
                asset_b_unit.clone(),
                asset_c_unit.clone(),
            ]
        );
        // Descending uses each asset's newest UTxO. ASSET_A and ASSET_C share
        // that position, so unit order breaks the tie.
        assert_eq!(
            descending_units,
            vec![asset_a_unit, asset_c_unit, asset_b_unit.clone()]
        );

        let (status, bytes) = app
            .get_bytes(&format!(
                "/accounts/{stake_address}/addresses/assets?order=asc&count=1&page=2"
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        let second_page: Vec<AccountAddressesAssetsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse second assets page");
        assert_eq!(extract_units(second_page), vec![asset_b_unit]);
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_order_asc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/addresses?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<AccountAddressesContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse addresses asc");
        if asc.is_empty() {
            return;
        }
        let address_bounds = |addr: &str| {
            app.vectors()
                .account_address_bounds
                .iter()
                .find_map(|(known, min, max)| (known == addr).then_some((*min, *max)))
                .expect("missing address in vectors")
        };

        let asc_blocks: Vec<_> = asc.iter().map(|x| address_bounds(&x.address).0).collect();

        assert!(asc_blocks.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_order_desc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/addresses?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<AccountAddressesContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse addresses desc");
        if desc.is_empty() {
            return;
        }
        let address_bounds = |addr: &str| {
            app.vectors()
                .account_address_bounds
                .iter()
                .find_map(|(known, min, max)| (known == addr).then_some((*min, *max)))
                .expect("missing address in vectors")
        };

        let desc_blocks: Vec<_> = desc.iter().map(|x| address_bounds(&x.address).1).collect();

        assert!(desc_blocks.windows(2).all(|w| w[0] >= w[1]));
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/addresses", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/addresses", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/addresses");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_delegations_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/delegations?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<AccountDelegationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account delegations");
    }

    #[tokio::test]
    async fn accounts_by_stake_delegations_slot_constrained() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!(
            "/accounts/{stake_address}/delegations?from={}&to={}",
            block.block_number, block.block_number
        );
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AccountDelegationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account delegations");
        for item in items {
            assert!(block.tx_hashes.contains(&item.tx_hash));
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_delegations_order_asc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/delegations?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<AccountDelegationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse delegations asc");
        if asc.len() < 2 {
            return;
        }
        let tx_pos = |hash: &str| {
            app.vectors()
                .blocks
                .iter()
                .find_map(|block| {
                    block
                        .tx_hashes
                        .iter()
                        .position(|x| x == hash)
                        .map(|idx| (block.block_number, idx))
                })
                .expect("missing tx hash in vectors")
        };
        let asc_pos: Vec<_> = asc.iter().map(|x| tx_pos(&x.tx_hash)).collect();
        assert!(asc_pos.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn accounts_by_stake_delegations_order_desc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/delegations?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<AccountDelegationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse delegations desc");
        if desc.len() < 2 {
            return;
        }
        let tx_pos = |hash: &str| {
            app.vectors()
                .blocks
                .iter()
                .find_map(|block| {
                    block
                        .tx_hashes
                        .iter()
                        .position(|x| x == hash)
                        .map(|idx| (block.block_number, idx))
                })
                .expect("missing tx hash in vectors")
        };
        let desc_pos: Vec<_> = desc.iter().map(|x| tx_pos(&x.tx_hash)).collect();
        assert!(
            desc_pos.windows(2).all(|w| w[0] >= w[1]),
            "positions should be in descending order: {:?}",
            desc_pos
        );
    }

    #[tokio::test]
    async fn accounts_by_stake_delegations_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/delegations", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_delegations_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/delegations", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_delegations_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/delegations");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/registrations?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<AccountRegistrationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account registrations");
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_include_resolved_deposit() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/registrations?page=1");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AccountRegistrationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account registrations");

        assert!(
            items.iter().any(|x| x.deposit.as_deref() != Some("0")),
            "expected at least one registration item with a resolved deposit, got: {items:?}"
        );
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_slot_constrained() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!(
            "/accounts/{stake_address}/registrations?from={}&to={}",
            block.block_number, block.block_number
        );
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AccountRegistrationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account registrations");
        for item in items {
            assert!(block.tx_hashes.contains(&item.tx_hash));
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_order_asc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/registrations?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<AccountRegistrationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse registrations asc");
        if asc.len() < 2 {
            return;
        }
        let tx_pos = |hash: &str| {
            app.vectors()
                .blocks
                .iter()
                .find_map(|block| {
                    block
                        .tx_hashes
                        .iter()
                        .position(|x| x == hash)
                        .map(|idx| (block.block_number, idx))
                })
                .expect("missing tx hash in vectors")
        };
        let asc_pos: Vec<_> = asc.iter().map(|x| tx_pos(&x.tx_hash)).collect();
        assert!(asc_pos.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_order_desc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/registrations?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<AccountRegistrationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse registrations desc");
        if desc.len() < 2 {
            return;
        }
        let tx_pos = |hash: &str| {
            app.vectors()
                .blocks
                .iter()
                .find_map(|block| {
                    block
                        .tx_hashes
                        .iter()
                        .position(|x| x == hash)
                        .map(|idx| (block.block_number, idx))
                })
                .expect("missing tx hash in vectors")
        };
        let desc_pos: Vec<_> = desc.iter().map(|x| tx_pos(&x.tx_hash)).collect();
        assert!(
            desc_pos.windows(2).all(|w| w[0] >= w[1]),
            "positions should be in descending order: {:?}",
            desc_pos
        );
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/registrations", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/registrations", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/registrations");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_rewards_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/rewards?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<AccountRewardContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account rewards");
    }

    #[tokio::test]
    async fn accounts_by_stake_rewards_reports_refund_at_spendable_epoch() {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state())
                .expect("era summary");
            let tip = domain
                .state()
                .read_cursor()
                .expect("cursor read failed")
                .expect("missing tip")
                .slot();
            let (tip_epoch, _) = summary.slot_epoch(tip);
            let source_epoch = tip_epoch.checked_sub(2).expect("tip before epoch 2");
            let account = parse_account_key_param(&vectors.stake_address, Network::Testnet)
                .expect("invalid fixture stake address");
            let log_key: LogKey = (
                TemporalKey::from(summary.epoch_start(source_epoch)),
                EntityKey::from(account.entity_key),
            )
                .into();
            let mut log = domain
                .archive()
                .read_log_typed::<AccountEpochLog>(AccountEpochLog::NS, &log_key)
                .expect("reward log read failed")
                .expect("missing reward log");
            let pool = log.pool_id.expect("missing reward pool");
            log.deposit_refunds.push((pool, 500));
            log.sort();

            let writer = domain.archive().start_writer().expect("archive writer");
            writer
                .write_log_typed(&log_key, &log)
                .expect("refund log write failed");
            writer.commit().expect("refund log commit failed");
        });

        let tip_epoch = app.tip_epoch();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/rewards");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);
        let items: Vec<AccountRewardContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account rewards");
        let refunds: Vec<_> = items
            .iter()
            .filter(|item| {
                matches!(
                    &item.r#type,
                    blockfrost_openapi::models::account_reward_content_inner::Type::PoolDepositRefund
                )
            })
            .collect();

        assert_eq!(refunds.len(), 1);
        assert_eq!(refunds[0].epoch, tip_epoch as i32);
        assert_eq!(refunds[0].amount, "500");
    }

    #[tokio::test]
    async fn accounts_by_stake_rewards_paginated() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path_page_1 = format!("/accounts/{stake_address}/rewards?page=1&count=1");
        let path_page_2 = format!("/accounts/{stake_address}/rewards?page=2&count=1");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AccountRewardContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse account rewards page 1");
        let page_2: Vec<AccountRewardContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse account rewards page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1[0].epoch, page_2[0].epoch);
    }

    #[tokio::test]
    async fn accounts_by_stake_rewards_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/rewards", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_rewards_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/rewards", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_rewards_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/rewards");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/withdrawals?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let items: Vec<AccountWithdrawalContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account withdrawals");

        let expected = &app.vectors().account_withdrawals;
        assert_eq!(items.len(), expected.len());
        for (item, expected) in items.iter().zip(expected.iter()) {
            assert_eq!(item.tx_hash, expected.tx_hash);
            assert_eq!(item.amount, expected.amount.to_string());
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_paginated() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path_page_1 = format!("/accounts/{stake_address}/withdrawals?page=1&count=1");
        let path_page_2 = format!("/accounts/{stake_address}/withdrawals?page=2&count=1");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AccountWithdrawalContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse account withdrawals page 1");
        let page_2: Vec<AccountWithdrawalContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse account withdrawals page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1[0].tx_hash, page_2[0].tx_hash);
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_order_asc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/withdrawals?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AccountWithdrawalContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse withdrawals asc");
        if items.len() < 2 {
            return;
        }

        let positions: Vec<_> = items
            .iter()
            .map(|x| app.vectors().tx_position(&x.tx_hash))
            .collect();
        assert!(positions.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_order_desc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/withdrawals?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AccountWithdrawalContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse withdrawals desc");
        if items.len() < 2 {
            return;
        }

        let positions: Vec<_> = items
            .iter()
            .map(|x| app.vectors().tx_position(&x.tx_hash))
            .collect();
        assert!(positions.windows(2).all(|w| w[0] >= w[1]));
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_slot_constrained() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!(
            "/accounts/{stake_address}/withdrawals?from={}&to={}",
            block.block_number, block.block_number
        );
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AccountWithdrawalContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account withdrawals");
        for item in items {
            assert!(block.tx_hashes.contains(&item.tx_hash));
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/withdrawals", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/withdrawals", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/withdrawals");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/transactions?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let items: Vec<AccountTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account transactions");
        assert!(!items.is_empty());
        for item in &items {
            assert!(!item.address.is_empty());
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_slot_constrained() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!(
            "/accounts/{stake_address}/transactions?from={}&to={}",
            block.block_number, block.block_number
        );
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AccountTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account transactions");
        for item in items {
            assert!(block.tx_hashes.contains(&item.tx_hash));
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_paginated() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path_page_1 = format!("/accounts/{stake_address}/transactions?page=1&count=2");
        let path_page_2 = format!("/accounts/{stake_address}/transactions?page=2&count=2");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AccountTransactionsContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse transactions page 1");
        let page_2: Vec<AccountTransactionsContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse transactions page 2");

        let key = |x: &AccountTransactionsContentInner| (x.tx_hash.clone(), x.address.clone());
        let page_1_keys: std::collections::HashSet<_> = page_1.iter().map(key).collect();
        let page_2_keys: std::collections::HashSet<_> = page_2.iter().map(key).collect();
        assert!(page_1_keys.is_disjoint(&page_2_keys));
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_order_asc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/transactions?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<AccountTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse transactions asc");
        if asc.is_empty() {
            return;
        }
        let asc_pos: Vec<_> = asc.iter().map(|x| (x.block_height, x.tx_index)).collect();
        assert!(asc_pos.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_order_desc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/transactions?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<AccountTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse transactions desc");
        if desc.is_empty() {
            return;
        }
        let desc_pos: Vec<_> = desc.iter().map(|x| (x.block_height, x.tx_index)).collect();
        assert!(desc_pos.windows(2).all(|w| w[0] >= w[1]));
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/transactions", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/transactions", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/transactions");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
