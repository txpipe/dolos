use axum::http::StatusCode;
use futures::future::join_all;
use itertools::Itertools;
use pallas::{
    codec::{minicbor, utils::Bytes},
    crypto::hash::Hash,
    ledger::{
        addresses::{Address, Network},
        primitives::{
            alonzo::{self, Certificate as AlonzoCert},
            conway::{Certificate as ConwayCert, DatumOption, RedeemerTag},
            Epoch, ExUnitPrices, StakeCredential,
        },
        traverse::{
            ComputeHash, MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraOutput,
            MultiEraRedeemer, MultiEraTx, OriginalHash,
        },
    },
};
use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};

use blockfrost_openapi::models::{
    tx_content::TxContent,
    tx_content_cbor::TxContentCbor,
    tx_content_delegations_inner::TxContentDelegationsInner,
    tx_content_metadata_cbor_inner::TxContentMetadataCborInner,
    tx_content_metadata_inner::TxContentMetadataInner,
    tx_content_metadata_inner_json_metadata::TxContentMetadataInnerJsonMetadata,
    tx_content_mirs_inner::{Pot, TxContentMirsInner},
    tx_content_pool_certs_inner::TxContentPoolCertsInner,
    tx_content_pool_certs_inner_metadata::TxContentPoolCertsInnerMetadata,
    tx_content_pool_retires_inner::TxContentPoolRetiresInner,
    tx_content_redeemers_inner::{Purpose, TxContentRedeemersInner},
    tx_content_required_signers_inner::TxContentRequiredSignersInner,
    tx_content_stake_addr_inner::TxContentStakeAddrInner,
    tx_content_utxo::TxContentUtxo,
    tx_content_utxo_inputs_inner::TxContentUtxoInputsInner,
    tx_content_withdrawals_inner::TxContentWithdrawalsInner,
};

use dolos_cardano::{
    indexes::{pool_certs_in, CardanoArchiveIndexExt as _, PoolCert},
    pallas_extras, pool_history, AccountState, ChainSummary, DRepState, PParamsSet, PoolHash,
};
use dolos_core::{ArchiveStore as _, Domain, EraCbor, TxHash, TxOrder, TxoRef};

use super::{
    bech32_pool, list_assets, pool_offchain_metadata, rational_to_f64, redeemer_fee,
    redeemer_script_hash, stake_cred_to_address, try_into_or_500, vkey_to_stake_address, IntoModel,
    PoolOffchainMetadata, UtxoOutputModelBuilder,
};
use crate::Facade;

pub struct UtxoInputModelBuilder<'a> {
    input: MultiEraInput<'a>,
    as_output: Option<MultiEraOutput<'a>>,
    is_reference: bool,
    is_collateral: bool,
}

impl<'a> IntoModel<TxContentUtxoInputsInner> for UtxoInputModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<TxContentUtxoInputsInner, StatusCode> {
        let mut out = TxContentUtxoInputsInner {
            tx_hash: self.input.hash().to_string(),
            output_index: try_into_or_500!(self.input.index()),
            collateral: self.is_collateral,
            reference: Some(self.is_reference),
            ..Default::default()
        };

        if let Some(o) = self.as_output {
            out = TxContentUtxoInputsInner {
                address: o.address().into_model()?,
                amount: o
                    .value()
                    .into_model()?
                    .into_iter()
                    .filter(|x| x.unit == "lovelace" || !self.is_collateral)
                    .collect(),
                reference_script_hash: o.script_ref().map(|h| h.into_model()).transpose()?,
                data_hash: o.datum().map(|x| match x {
                    DatumOption::Hash(x) => x.to_string(),
                    DatumOption::Data(x) => x.original_hash().to_string(),
                }),
                inline_datum: o
                    .datum()
                    .and_then(|x| match x {
                        DatumOption::Hash(_) => None,
                        DatumOption::Data(x) => Some(minicbor::to_vec(&x.0).unwrap()),
                    })
                    .map(hex::encode),
                ..out
            }
        }

        Ok(out)
    }
}

pub struct TxModelBuilder<'a> {
    chain: Option<ChainSummary>,
    pparams: Option<PParamsSet>,
    network: Option<Network>,
    block: MultiEraBlock<'a>,
    order: TxOrder,
    deps: HashMap<TxHash, MultiEraTx<'a>>,
    consumed_deps: HashMap<TxoRef, TxHash>,
    pool_metadata: HashMap<PoolHash, PoolOffchainMetadata>,
    deposit: Option<i64>,
    /// Whether each pool registration certificate of the tx, by index, takes
    /// a fresh registration; see [`TxModelBuilder::is_pool_registration`].
    pool_registrations: HashMap<usize, bool>,
}

impl<'a> TxModelBuilder<'a> {
    pub fn new(block: &'a [u8], order: TxOrder) -> Result<Self, StatusCode> {
        let block = MultiEraBlock::decode(block).map_err(|err| {
            tracing::error!(error = ?err, "failed to decode block in TxModelBuilder");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        Ok(Self {
            block,
            order,
            chain: None,
            pparams: None,
            network: None,
            deps: HashMap::new(),
            pool_registrations: HashMap::new(),
            consumed_deps: HashMap::new(),
            pool_metadata: HashMap::new(),
            deposit: None,
        })
    }

    pub fn with_chain(self, chain: ChainSummary) -> Self {
        Self {
            chain: Some(chain),
            ..self
        }
    }

    pub fn with_pparams(self, pparams: PParamsSet) -> Self {
        Self {
            pparams: Some(pparams),
            ..self
        }
    }

    pub fn with_historical_pparams<D: Domain>(
        self,
        facade: &Facade<D>,
    ) -> Result<Self, StatusCode> {
        let epoch = self.tx_epoch()?;
        let chain = self.chain_or_500()?;
        let pparams = facade.get_effective_pparams_for_epoch(epoch, chain)?;

        Ok(self.with_pparams(pparams))
    }

    pub fn with_network(self, network: Network) -> Self {
        Self {
            network: Some(network),
            ..self
        }
    }

    pub fn with_consumed_deps(self, consumed_deps: HashMap<TxoRef, TxHash>) -> Self {
        Self {
            consumed_deps,
            ..self
        }
    }

    /// The newest certificate of `operator` before certificate `cert_index`
    /// of this tx, in this block or an earlier one, `None` if it never
    /// registered. A pool the Shelley genesis stakes is registered from the
    /// start.
    fn previous_pool_cert<D: Domain>(
        &self,
        facade: &Facade<D>,
        operator: &PoolHash,
        cert_index: usize,
    ) -> Result<Option<PoolCert>, StatusCode> {
        let here = (self.block.slot(), self.order, cert_index);

        let in_block = pool_certs_in(&self.block, operator)
            .into_iter()
            .take_while(|(at, _)| *at < here)
            .last();

        if let Some((_, cert)) = in_block {
            return Ok(Some(cert));
        }

        let previous = facade
            .archive()
            .pool_cert_before(operator, self.block.slot())
            .map_err(crate::log_and_500(
                "failed to read a pool's previous certificate",
            ))?;

        if previous.is_none() && pool_history::genesis_pools(&facade.genesis()).contains(operator) {
            return Ok(Some(PoolCert::Registration));
        }

        Ok(previous)
    }

    /// The epoch of the block before this one, or this block's where the
    /// archive no longer holds it.
    fn previous_block_epoch<D: Domain>(&self, facade: &Facade<D>) -> Result<Epoch, StatusCode> {
        let previous = facade
            .archive()
            .get_range(None, Some(self.block.slot()))
            .map_err(crate::log_and_500("failed to read the previous block"))?
            .next_back();

        match previous {
            Some((slot, _)) => Ok(self.chain_or_500()?.slot_epoch(slot).0),
            None => self.tx_epoch(),
        }
    }

    /// Whether certificate `cert_index`, of `operator`, takes the pool
    /// deposit: when the pool is not registered as the ledger applies it,
    /// because it never registered or its retirement took effect by the
    /// block's epoch.
    fn takes_pool_deposit<D: Domain>(
        &self,
        facade: &Facade<D>,
        operator: &PoolHash,
        cert_index: usize,
    ) -> Result<bool, StatusCode> {
        Ok(
            match self.previous_pool_cert(facade, operator, cert_index)? {
                None => true,
                Some(PoolCert::Registration) => false,
                Some(PoolCert::Retirement(retiring)) => self.tx_epoch()? >= retiring,
            },
        )
    }

    /// db-sync's rule for a pool certificate (`isPoolRegistration`): a
    /// registration, active two epochs on, when the pool was not registered
    /// at the end of the previous block, because it never registered or its
    /// retirement had taken effect, and no earlier certificate in the block
    /// updated it. Otherwise an update, active three epochs on, which cancels
    /// a pending retirement.
    ///
    /// It reads the pools before the ledger ticks into the block's epoch, so
    /// in the first block of the epoch a retirement takes effect in, the
    /// certificate takes the deposit and is still an update.
    fn is_pool_registration<D: Domain>(
        &self,
        facade: &Facade<D>,
        operator: &PoolHash,
        cert_index: usize,
    ) -> Result<bool, StatusCode> {
        Ok(
            match self.previous_pool_cert(facade, operator, cert_index)? {
                None => true,
                Some(PoolCert::Registration) => false,
                Some(PoolCert::Retirement(retiring)) => {
                    self.previous_block_epoch(facade)? >= retiring
                }
            },
        )
    }

    pub fn set_pool_registrations<D: Domain>(
        &mut self,
        facade: &Facade<D>,
    ) -> Result<(), StatusCode> {
        let registrations: Vec<(usize, PoolHash)> = self
            .tx()?
            .certs()
            .iter()
            .enumerate()
            .filter_map(|(cert_index, cert)| {
                pallas_extras::cert_as_pool_registration(cert).map(|reg| (cert_index, reg.operator))
            })
            .collect();

        self.pool_registrations = registrations
            .into_iter()
            .map(|(cert_index, operator)| {
                Ok((
                    cert_index,
                    self.is_pool_registration(facade, &operator, cert_index)?,
                ))
            })
            .collect::<Result<_, StatusCode>>()?;

        Ok(())
    }

    pub async fn fetch_pool_metadata(&mut self) -> Result<(), StatusCode> {
        let pool_registrations = self
            .tx()?
            .certs()
            .iter()
            .filter_map(|cert| match cert {
                MultiEraCert::AlonzoCompatible(cow) => {
                    if let AlonzoCert::PoolRegistration {
                        operator,
                        ref pool_metadata,
                        ..
                    } = *(**cow).clone()
                    {
                        pool_metadata
                            .as_ref()
                            .map(|meta| (operator, meta.url.clone(), meta.hash.clone()))
                    } else {
                        None
                    }
                }
                MultiEraCert::Conway(cow) => {
                    if let ConwayCert::PoolRegistration {
                        operator,
                        ref pool_metadata,
                        ..
                    } = *(**cow).clone()
                    {
                        pool_metadata
                            .as_ref()
                            .map(|meta| (operator, meta.url.clone(), meta.hash.clone()))
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect_vec();

        self.pool_metadata = join_all(pool_registrations.iter().map(
            |(pool_hash, url, hash)| async move {
                pool_offchain_metadata(url, Some(hash.as_slice()))
                    .await
                    .map(|meta| (*pool_hash, meta))
            },
        ))
        .await
        .into_iter()
        .flatten()
        .collect();

        Ok(())
    }

    pub fn compute_deposit<D>(&mut self, facade: &Facade<D>) -> Result<(), StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
        Option<AccountState>: From<D::Entity>,
        Option<DRepState>: From<D::Entity>,
    {
        let pparms = self.pparams_or_500()?;

        let key_deposit = pparms.key_deposit().unwrap_or_default() as i64;
        let pool_deposit = pparms.pool_deposit().unwrap_or_default() as i64;

        let mut repeated = HashSet::new();
        let mut out = 0;

        for (cert_index, cert) in self.tx()?.certs().into_iter().enumerate() {
            if let Some(reg) = pallas_extras::cert_as_stake_registration(&cert) {
                let key = minicbor::to_vec(&reg).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                if repeated.insert((0, key.clone())) {
                    out += key_deposit;
                }
            }

            if let Some(reg) = pallas_extras::cert_as_pool_registration(&cert) {
                if repeated.insert((1, reg.operator.to_vec()))
                    && self.takes_pool_deposit(facade, &reg.operator, cert_index)?
                {
                    out += pool_deposit;
                }
            }

            if let Some(reg) = pallas_extras::cert_as_drep_registration(&cert) {
                let key =
                    minicbor::to_vec(&reg.cred).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                if repeated.insert((2, key)) {
                    out += reg.deposit as i64;
                }
            }

            if let Some(reg) = pallas_extras::cert_as_stake_deregistration(&cert) {
                let key = minicbor::to_vec(&reg).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                if repeated.insert((3, key.clone())) {
                    out -= key_deposit;
                }
            }

            if let Some(reg) = pallas_extras::cert_as_drep_unregistration(&cert) {
                let key =
                    minicbor::to_vec(&reg.cred).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                if repeated.insert((4, key)) {
                    out -= reg.deposit as i64;
                }
            }
        }

        self.deposit = Some(out);

        Ok(())
    }

    pub fn tx(&self) -> Result<MultiEraTx<'_>, StatusCode> {
        let tx = self
            .block
            .txs()
            .get(self.order)
            .ok_or(StatusCode::NOT_FOUND)?
            .clone();

        Ok(tx)
    }

    fn chain_or_500(&self) -> Result<&ChainSummary, StatusCode> {
        self.chain.as_ref().ok_or_else(|| {
            tracing::error!("chain summary not set on TxModelBuilder");
            StatusCode::INTERNAL_SERVER_ERROR
        })
    }

    fn pparams_or_500(&self) -> Result<&PParamsSet, StatusCode> {
        self.pparams.as_ref().ok_or_else(|| {
            tracing::error!("pparams not set on TxModelBuilder");
            StatusCode::INTERNAL_SERVER_ERROR
        })
    }

    fn tx_epoch(&self) -> Result<Epoch, StatusCode> {
        let (epoch, _) = self.chain_or_500()?.slot_epoch(self.block.slot());
        Ok(epoch)
    }

    pub fn required_deps(&self) -> Result<Vec<TxHash>, StatusCode> {
        let tx = self.tx()?;

        let mut deps = vec![];

        for i in tx.inputs() {
            deps.push(*i.hash());
        }

        for i in tx.collateral() {
            deps.push(*i.hash());
        }

        for i in tx.reference_inputs() {
            deps.push(*i.hash());
        }

        let unique = deps.into_iter().unique().collect();

        Ok(unique)
    }

    pub fn required_consumed_deps(&self) -> Result<Vec<TxoRef>, StatusCode> {
        let tx = self.tx()?;

        let mut deps = vec![];

        for (i, _) in tx.produces() {
            deps.push(TxoRef(tx.hash(), i as u32));
        }

        Ok(deps)
    }

    pub fn load_dep(&mut self, key: TxHash, cbor: &'a EraCbor) -> Result<(), StatusCode> {
        let era = try_into_or_500!(cbor.0);

        let tx = MultiEraTx::decode_for_era(era, &cbor.1)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        self.deps.insert(key, tx);

        Ok(())
    }

    fn new_input_builder<'b, 'c>(
        &'b self,
        input: MultiEraInput<'c>,
        is_collateral: bool,
        is_reference: bool,
    ) -> UtxoInputModelBuilder<'c>
    where
        'b: 'c,
    {
        let tx = self.deps.get(input.hash());
        let as_output = tx.and_then(|tx| tx.output_at(input.index() as usize));

        UtxoInputModelBuilder {
            input,
            as_output,
            is_reference,
            is_collateral,
        }
    }
}

impl<'a> IntoModel<TxContentUtxo> for TxModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<TxContentUtxo, StatusCode> {
        let tx = self.tx()?;

        let mut reference: Vec<_> = tx
            .reference_inputs()
            .into_iter()
            .map(|i| self.new_input_builder(i, false, true))
            .map(|b| b.into_model())
            .try_collect()?;

        reference.sort_by_key(|i| (i.tx_hash.clone(), i.output_index));

        let mut inputs: Vec<_> = tx
            .inputs()
            .into_iter()
            .map(|i| self.new_input_builder(i, false, false))
            .map(|b| b.into_model())
            .try_collect()?;

        inputs.sort_by_key(|i| (i.tx_hash.clone(), i.output_index));

        let mut collateral_inputs: Vec<_> = tx
            .collateral()
            .into_iter()
            .map(|i| self.new_input_builder(i, true, false))
            .map(|b| b.into_model())
            .try_collect()?;

        collateral_inputs.sort_by_key(|i| (i.tx_hash.clone(), i.output_index));

        let all_inputs = reference
            .into_iter()
            .chain(inputs)
            .chain(collateral_inputs)
            .collect();

        let outputs: Vec<_> = tx
            .outputs()
            .into_iter()
            .enumerate()
            .map(|(i, o)| UtxoOutputModelBuilder::from_output(tx.hash(), i as u32, o))
            .map(|b| {
                let builder = if let Some(consumed_by) = self.consumed_deps.get(&b.txo_ref()) {
                    b.with_consumed_by(*consumed_by)
                } else {
                    b
                };
                builder.into_model()
            })
            .try_collect()?;

        let collateral_outputs: Vec<_> = tx
            .collateral_return()
            .into_iter()
            .enumerate()
            .map(|(i, o)| {
                UtxoOutputModelBuilder::from_collateral(tx.hash(), outputs.len(), i as u32, o)
            })
            .map(|b| {
                let builder = if let Some(consumed_by) = self.consumed_deps.get(&b.txo_ref()) {
                    b.with_consumed_by(*consumed_by)
                } else {
                    b
                };
                builder.into_model()
            })
            .try_collect()?;

        let all_outputs = outputs.into_iter().chain(collateral_outputs).collect();

        Ok(TxContentUtxo {
            hash: tx.hash().to_string(),
            inputs: all_inputs,
            outputs: all_outputs,
        })
    }
}

macro_rules! match_certs {
    ($tx:expr, "alonzo", $cert:ident) => {
        $tx.certs()
            .iter()
            .enumerate()
            .filter_map(|(index, x)| x.as_alonzo().map(|x| (index, x)))
            .filter(|(_, x)| matches!(x, AlonzoCert::$cert { .. }))
    };

    ($tx:expr, "conway", $cert:ident) => {
        $tx.certs()
            .iter()
            .enumerate()
            .filter_map(|(index, x)| x.as_conway().map(|x| (index, x)))
            .filter(|(_, x)| matches!(x, ConwayCert::$cert { .. }))
    };
}

macro_rules! count_certs {
    ($tx:expr, "alonzo", $cert:ident) => {{
        let alonzo = match_certs!($tx, "alonzo", $cert).count();
        alonzo
    }};

    ($tx:expr, "conway", $cert:ident) => {{
        let conway = match_certs!($tx, "conway", $cert).count();
        conway
    }};

    ($tx:expr, $cert:ident) => {{
        let alonzo = match_certs!($tx, "alonzo", $cert).count();
        let conway = match_certs!($tx, "conway", $cert).count();

        alonzo + conway
    }};
}

impl IntoModel<TxContent> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<TxContent, StatusCode> {
        let tx = self.tx()?;
        let block = &self.block;
        let order = self.order;
        let txin = tx.inputs();
        let txouts = tx.produces();
        let chain = self.chain_or_500()?;

        let block_time = chain.slot_time(block.slot());
        let treasury_donation = tx
            .as_conway()
            .and_then(|x| x.transaction_body.donation.as_ref())
            .map(u64::from)
            .unwrap_or_default()
            .to_string();

        let tx = TxContent {
            hash: tx.hash().to_string(),
            block: block.hash().to_string(),
            block_height: try_into_or_500!(block.number()),
            slot: try_into_or_500!(block.slot()),
            index: try_into_or_500!(order),
            output_amount: list_assets(txouts.iter().map(|(_, x)| x)),
            fees: if tx.is_valid() {
                tx.fee_or_compute().to_string()
            } else {
                tx.total_collateral().unwrap_or_default().to_string()
            },
            size: try_into_or_500!(tx.size()),
            invalid_before: tx.validity_start().map(|v| v.to_string()),
            invalid_hereafter: tx.ttl().map(|v| v.to_string()),
            utxo_count: try_into_or_500!(txin.len() + txouts.len()),
            redeemer_count: try_into_or_500!(if tx.is_valid() {
                tx.redeemers().len()
            } else {
                0
            }),
            valid_contract: tx.is_valid(),
            block_time: try_into_or_500!(block_time),
            withdrawal_count: tx.withdrawals().collect::<Vec<_>>().len() as i32,
            mir_cert_count: count_certs!(tx, "alonzo", MoveInstantaneousRewardsCert) as i32,
            delegation_count: count_certs!(tx, StakeDelegation) as i32,
            stake_cert_count: count_certs!(tx, StakeRegistration) as i32,
            pool_update_count: count_certs!(tx, PoolRegistration) as i32,
            pool_retire_count: count_certs!(tx, PoolRetirement) as i32,
            asset_mint_or_burn_count: tx.mints().iter().flat_map(|x| x.assets()).count() as i32,
            deposit: self.deposit.unwrap_or_default().to_string(),
            treasury_donation,
        };

        Ok(tx)
    }
}

impl IntoModel<TxContentCbor> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<TxContentCbor, StatusCode> {
        let tx = self.tx()?;

        let cbor = tx.encode();
        let cbor = hex::encode(&cbor);

        let tx = TxContentCbor { cbor };

        Ok(tx)
    }
}

impl IntoModel<TxContentMetadataInnerJsonMetadata> for alonzo::Metadatum {
    type SortKey = ();

    fn into_model(self) -> Result<TxContentMetadataInnerJsonMetadata, StatusCode> {
        let out = match self {
            alonzo::Metadatum::Int(x) => TxContentMetadataInnerJsonMetadata::String(x.to_string()),
            alonzo::Metadatum::Bytes(x) => TxContentMetadataInnerJsonMetadata::String(format!(
                "0x{}",
                hex::encode(x.as_slice())
            )),
            alonzo::Metadatum::Text(x) => TxContentMetadataInnerJsonMetadata::String(x.to_string()),
            alonzo::Metadatum::Array(x) => {
                let items: Vec<_> = x.into_iter().map(|x| x.into_model()).try_collect()?;

                TxContentMetadataInnerJsonMetadata::Object(HashMap::from_iter([(
                    "array".to_string(),
                    serde_json::Value::Array(items),
                )]))
            }
            alonzo::Metadatum::Map(x) => {
                let items: HashMap<String, serde_json::Value> = x
                    .iter()
                    .map(|(k, v)| Ok((k.clone().into_model()?, v.clone().into_model()?)))
                    .try_collect()
                    .map_err(|_: StatusCode| StatusCode::INTERNAL_SERVER_ERROR)?;

                TxContentMetadataInnerJsonMetadata::Object(items)
            }
        };

        Ok(out)
    }
}

impl IntoModel<Vec<TxContentMetadataInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentMetadataInner>, StatusCode> {
        let tx = self.tx()?;
        let metadata = tx.metadata();

        let entries: Vec<_> = metadata.collect();

        let items = entries
            .into_iter()
            .map(|(label, metadatum)| {
                Ok(TxContentMetadataInner {
                    label: label.to_string(),
                    json_metadata: Box::new(metadatum.clone().into_model()?),
                })
            })
            .try_collect()
            .map_err(|_: StatusCode| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(items)
    }
}

impl IntoModel<Vec<TxContentMetadataCborInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentMetadataCborInner>, StatusCode> {
        let tx = self.tx()?;
        let metadata = tx.metadata();

        let entries: Vec<_> = metadata.collect();

        let items = entries
            .into_iter()
            .map(|(label, metadatum)| {
                let meta: alonzo::Metadata = vec![(label, metadatum.clone())].into_iter().collect();
                let encoded = hex::encode(minicbor::to_vec(meta).unwrap());
                Ok(TxContentMetadataCborInner {
                    label: label.to_string(),
                    metadata: Some(encoded.clone()),
                    cbor_metadata: Some(format!("\\x{encoded}")),
                })
            })
            .try_collect()
            .map_err(|_: StatusCode| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(items)
    }
}

/// The Blockfrost purpose of a tx-content redeemer.
pub fn tx_redeemer_purpose(tag: RedeemerTag) -> Purpose {
    match tag {
        RedeemerTag::Spend => Purpose::Spend,
        RedeemerTag::Mint => Purpose::Mint,
        RedeemerTag::Cert => Purpose::Cert,
        RedeemerTag::Reward => Purpose::Reward,
        RedeemerTag::Vote => Purpose::Vote,
        RedeemerTag::Propose => Purpose::Propose,
    }
}

impl TxModelBuilder<'_> {
    fn find_output_for_input(&self, input: &MultiEraInput<'_>) -> Option<MultiEraOutput<'_>> {
        let tx_hash = input.hash();
        let index = input.index() as usize;

        let source = self.deps.get(tx_hash)?;

        let outputs = source.outputs();
        let output = outputs.get(index)?;

        Some(output.clone())
    }

    fn find_redeemer_script(
        &self,
        redeemer: &MultiEraRedeemer<'_>,
    ) -> Result<Option<Hash<28>>, StatusCode> {
        let tx = self.tx()?;

        redeemer_script_hash(&tx, redeemer, &mut |input| {
            let Some(output) = self.find_output_for_input(input) else {
                return Ok(None);
            };

            output
                .address()
                .map(Some)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        })
    }

    fn build_redeemer_inner(
        &self,
        redeemer: &MultiEraRedeemer<'_>,
        prices: &ExUnitPrices,
    ) -> Result<TxContentRedeemersInner, StatusCode> {
        let units = redeemer.ex_units();

        let fee = redeemer_fee(&units, prices)?;

        let out = TxContentRedeemersInner {
            purpose: tx_redeemer_purpose(redeemer.tag()),
            tx_index: redeemer.index() as i32,
            // TODO: we should change this in Pallas to ensure that we have a KeepRaw wrapping the
            // redeemer data
            redeemer_data_hash: redeemer.data().compute_hash().to_string(),
            fee: fee.to_string(),
            unit_mem: units.mem.to_string(),
            unit_steps: units.steps.to_string(),
            // deliberate divergence from deployed Blockfrost: db-sync leaves
            // vote and propose redeemers unattributed (script_hash ""), we
            // resolve them from the ledger.
            script_hash: self
                .find_redeemer_script(redeemer)?
                .map(|x| x.to_string())
                .unwrap_or_default(),
            datum_hash: redeemer.data().compute_hash().to_string(),
        };

        Ok(out)
    }
}

impl IntoModel<Vec<TxContentRedeemersInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentRedeemersInner>, StatusCode> {
        let tx = self.tx()?;
        let redeemers = tx.redeemers();

        let pparms = self.pparams_or_500()?;

        let prices = pparms
            .execution_costs()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let items = redeemers
            .into_iter()
            .map(|x| self.build_redeemer_inner(&x, &prices))
            .try_collect()
            .map_err(|_: StatusCode| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(items)
    }
}

impl IntoModel<Vec<TxContentWithdrawalsInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentWithdrawalsInner>, StatusCode> {
        let tx = self.tx()?;
        let withdrawals = tx.withdrawals();
        let withdrawals: Vec<_> = withdrawals.collect();

        let items = withdrawals
            .into_iter()
            .map(|(address, amount)| {
                let address =
                    Address::from_bytes(address).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                Ok(TxContentWithdrawalsInner {
                    address: address.to_string(),
                    amount: amount.to_string(),
                })
            })
            .try_collect()
            .map_err(|_: StatusCode| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(items)
    }
}

impl IntoModel<Vec<TxContentRequiredSignersInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentRequiredSignersInner>, StatusCode> {
        let tx = self.tx()?;
        let signers = tx.required_signers();

        let items = signers
            .collect::<Vec<_>>()
            .into_iter()
            .map(|hash| TxContentRequiredSignersInner {
                witness_hash: hash.to_string(),
            })
            .collect();

        Ok(items)
    }
}

fn build_delegation_inner(
    index: usize,
    cred: &StakeCredential,
    pool: &Hash<28>,
    network: Network,
    active_epoch: i32,
) -> Result<TxContentDelegationsInner, StatusCode> {
    let pool_hrp = bech32::Hrp::parse("pool").map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let pool_id = bech32::encode::<bech32::Bech32>(pool_hrp, pool.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let address = stake_cred_to_address(cred, network);

    let address = address
        .to_bech32()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(TxContentDelegationsInner {
        index: index as i32,
        address,
        pool_id,
        active_epoch: active_epoch + 1,
        // DEPRECATED
        cert_index: index as i32,
    })
}

impl IntoModel<Vec<TxContentDelegationsInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentDelegationsInner>, StatusCode> {
        let tx = self.tx()?;

        let certs = tx.certs();

        let network = self.network.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        // TODO: we're hardcoding a ledger rule here by saying that the active epoch is
        // the epoch number + 1. Although this is correct, the mapping layer
        // shouldn't be the one defining this.
        let active_epoch = self
            .chain
            .as_ref()
            .map(|c| c.slot_epoch(self.block.slot()))
            .map(|(a, _)| (a + 1) as i32)
            .unwrap_or_default();

        let items =
            certs
                .into_iter()
                .enumerate()
                .filter_map(|(index, cert)| match cert {
                    MultiEraCert::AlonzoCompatible(cert) => {
                        match &**cert {
                            AlonzoCert::StakeDelegation(cred, pool) => Some(
                                build_delegation_inner(index, cred, pool, network, active_epoch),
                            ),
                            _ => None,
                        }
                    }
                    MultiEraCert::Conway(cert) => {
                        match &**cert {
                            ConwayCert::StakeDelegation(cred, pool) => Some(
                                build_delegation_inner(index, cred, pool, network, active_epoch),
                            ),
                            ConwayCert::StakeRegDeleg(cred, pool, _) => Some(
                                build_delegation_inner(index, cred, pool, network, active_epoch),
                            ),
                            ConwayCert::StakeVoteRegDeleg(cred, pool, _, _) => Some(
                                build_delegation_inner(index, cred, pool, network, active_epoch),
                            ),
                            _ => None,
                        }
                    }
                    _ => None,
                })
                .try_collect()?;

        Ok(items)
    }
}

fn build_mir_inners(
    index: usize,
    mir: &alonzo::Certificate,
    network: Network,
) -> Result<Vec<TxContentMirsInner>, StatusCode> {
    let AlonzoCert::MoveInstantaneousRewardsCert(mir) = mir else {
        return Ok(vec![]);
    };

    let pot = match mir.source {
        alonzo::InstantaneousRewardSource::Reserves => Pot::Reserve,
        alonzo::InstantaneousRewardSource::Treasury => Pot::Treasury,
    };

    let targets = match &mir.target {
        alonzo::InstantaneousRewardTarget::StakeCredentials(creds) => creds.iter().collect(),
        _ => vec![],
    };

    let items = targets
        .into_iter()
        .map(|(cred, amount)| {
            let address = stake_cred_to_address(cred, network);

            let address = address
                .to_bech32()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            Ok::<_, StatusCode>(TxContentMirsInner {
                pot,
                cert_index: index as i32,
                address,
                amount: amount.to_string(),
            })
        })
        .try_collect()?;

    Ok(items)
}

impl IntoModel<Vec<TxContentMirsInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentMirsInner>, StatusCode> {
        let tx = self.tx()?;

        let network = self.network.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let items = match_certs!(tx, "alonzo", MoveInstantaneousRewardsCert)
            .map(|(index, cert)| build_mir_inners(index, cert, network))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(items)
    }
}

impl IntoModel<Vec<TxContentPoolRetiresInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentPoolRetiresInner>, StatusCode> {
        let tx = self.tx()?;

        let alonzo = match_certs!(tx, "alonzo", PoolRetirement)
            .filter_map(|(index, cert)| {
                let AlonzoCert::PoolRetirement(pool, epoch) = cert else {
                    return None;
                };

                Some(TxContentPoolRetiresInner {
                    pool_id: bech32_pool(pool.as_slice()).ok()?,
                    cert_index: index as i32,
                    retiring_epoch: *epoch as i32,
                })
            })
            .collect::<Vec<_>>();

        let conway = match_certs!(tx, "conway", PoolRetirement)
            .filter_map(|(index, cert)| {
                let ConwayCert::PoolRetirement(pool, epoch) = cert else {
                    return None;
                };

                Some(TxContentPoolRetiresInner {
                    pool_id: bech32_pool(pool.as_slice()).ok()?,
                    cert_index: index as i32,
                    retiring_epoch: *epoch as i32,
                })
            })
            .collect::<Vec<_>>();

        let items = alonzo.into_iter().chain(conway).collect();

        Ok(items)
    }
}

struct PoolUpdateModelBuilder {
    operator: Hash<28>,
    vrf_keyhash: Hash<32>,
    pledge: u64,
    cost: u64,
    margin: alonzo::RationalNumber,
    reward_account: Bytes,
    pool_owners: Vec<Hash<28>>,
    relays: Vec<alonzo::Relay>,
    pool_metadata: Option<alonzo::PoolMetadata>,
    offchain_pool_metadata: Option<PoolOffchainMetadata>,
    cert_index: usize,
    network: Network,
    current_epoch: i32,
    is_registration: bool,
}

impl PoolUpdateModelBuilder {
    fn new_from_alonzo(
        cert: AlonzoCert,
        cert_index: usize,
        network: Network,
        current_epoch: i32,
        is_registration: bool,
    ) -> Option<Self> {
        let AlonzoCert::PoolRegistration {
            operator,
            vrf_keyhash,
            pledge,
            cost,
            margin,
            reward_account,
            pool_owners,
            relays,
            pool_metadata,
        } = cert
        else {
            return None;
        };

        Some(Self {
            operator,
            vrf_keyhash,
            pledge,
            cost,
            margin,
            reward_account,
            pool_owners,
            relays,
            pool_metadata,
            cert_index,
            network,
            current_epoch,
            offchain_pool_metadata: None,
            is_registration,
        })
    }

    fn new_from_conway(
        cert: ConwayCert,
        cert_index: usize,
        network: Network,
        current_epoch: i32,
        is_registration: bool,
    ) -> Option<Self> {
        let ConwayCert::PoolRegistration {
            operator,
            vrf_keyhash,
            pledge,
            cost,
            margin,
            reward_account,
            pool_owners,
            relays,
            pool_metadata,
        } = cert
        else {
            return None;
        };

        Some(Self {
            operator,
            vrf_keyhash,
            pledge,
            cost,
            margin,
            reward_account,
            pool_owners: pool_owners.to_vec(),
            relays,
            pool_metadata,
            cert_index,
            network,
            current_epoch,
            offchain_pool_metadata: None,
            is_registration,
        })
    }

    fn new(
        cert: MultiEraCert,
        cert_index: usize,
        network: Network,
        current_epoch: i32,
        is_registration: bool,
    ) -> Option<Self> {
        match cert {
            MultiEraCert::AlonzoCompatible(cow) => Self::new_from_alonzo(
                (**cow).clone(),
                cert_index,
                network,
                current_epoch,
                is_registration,
            ),
            MultiEraCert::Conway(cow) => Self::new_from_conway(
                (**cow).clone(),
                cert_index,
                network,
                current_epoch,
                is_registration,
            ),
            _ => None,
        }
    }

    fn with_offchain(&mut self, offchain_pool_metadata: Option<PoolOffchainMetadata>) {
        self.offchain_pool_metadata = offchain_pool_metadata
    }
}

impl IntoModel<TxContentPoolCertsInner> for PoolUpdateModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<TxContentPoolCertsInner, StatusCode> {
        let reward_account =
            vkey_to_stake_address(self.reward_account.as_slice()[1..].into(), self.network)
                .to_bech32()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let owners: Vec<_> = self
            .pool_owners
            .iter()
            .map(|owner| vkey_to_stake_address(*owner, self.network))
            .map(|owner| {
                owner
                    .to_bech32()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
            })
            .try_collect()?;

        Ok(TxContentPoolCertsInner {
            vrf_key: self.vrf_keyhash.to_string(),
            pledge: self.pledge.to_string(),
            margin_cost: rational_to_f64::<3>(&self.margin),
            fixed_cost: self.cost.to_string(),
            reward_account,
            owners,
            metadata: Some(Box::new(TxContentPoolCertsInnerMetadata {
                url: self.pool_metadata.as_ref().map(|x| x.url.clone()),
                hash: self.pool_metadata.as_ref().map(|x| x.hash.to_string()),
                ticker: self
                    .offchain_pool_metadata
                    .as_ref()
                    .map(|x| x.ticker.clone()),
                name: self.offchain_pool_metadata.as_ref().map(|x| x.name.clone()),
                description: self
                    .offchain_pool_metadata
                    .as_ref()
                    .map(|x| x.description.clone()),
                homepage: self
                    .offchain_pool_metadata
                    .as_ref()
                    .map(|x| x.homepage.clone()),
            })),
            relays: self
                .relays
                .iter()
                .map(|relay| relay.clone().into_model())
                .try_collect()?,
            cert_index: self.cert_index as i32,
            pool_id: bech32_pool(self.operator.as_slice())?,
            active_epoch: self.current_epoch + if self.is_registration { 2 } else { 3 },
        })
    }
}

impl IntoModel<Vec<TxContentPoolCertsInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentPoolCertsInner>, StatusCode> {
        let tx = self.tx()?;

        let network = self.network.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
        let chain = self
            .chain
            .as_ref()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let epoch = chain.slot_epoch(self.block.slot()).0;
        let items = tx
            .certs()
            .into_iter()
            .enumerate()
            .filter_map(|(cert_index, cert)| {
                let is_registration = self
                    .pool_registrations
                    .get(&cert_index)
                    .copied()
                    .unwrap_or(true);
                let builder = PoolUpdateModelBuilder::new(
                    cert,
                    cert_index,
                    network,
                    epoch as i32,
                    is_registration,
                );
                builder.map(|mut x| {
                    x.with_offchain(self.pool_metadata.get(&x.operator).cloned());
                    x
                })
            })
            .map(|builder| builder.into_model())
            .try_collect()?;

        Ok(items)
    }
}

struct StakeCertModelBuilder {
    stake_credential: StakeCredential,
    is_registration: bool,
    cert_index: usize,
    network: Network,
}

impl StakeCertModelBuilder {
    fn new(cert: MultiEraCert, cert_index: usize, network: Network) -> Option<Self> {
        match cert {
            MultiEraCert::AlonzoCompatible(cow) => match cow.deref().deref() {
                AlonzoCert::StakeRegistration(stake_credential) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: true,
                    cert_index,
                    network,
                }),
                AlonzoCert::StakeDeregistration(stake_credential) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: false,
                    cert_index,
                    network,
                }),
                _ => None,
            },
            MultiEraCert::Conway(cow) => match cow.deref().deref() {
                ConwayCert::StakeRegistration(stake_credential) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: true,
                    cert_index,
                    network,
                }),
                ConwayCert::StakeDeregistration(stake_credential) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: false,
                    cert_index,
                    network,
                }),
                ConwayCert::Reg(stake_credential, _) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: true,
                    cert_index,
                    network,
                }),
                ConwayCert::StakeRegDeleg(stake_credential, _, _) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: true,
                    cert_index,
                    network,
                }),
                ConwayCert::StakeVoteRegDeleg(stake_credential, _, _, _) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: true,
                    cert_index,
                    network,
                }),
                ConwayCert::UnReg(stake_credential, _) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: false,
                    cert_index,
                    network,
                }),
                _ => None,
            },
            _ => None,
        }
    }
}

impl IntoModel<TxContentStakeAddrInner> for StakeCertModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<TxContentStakeAddrInner, StatusCode> {
        let address = stake_cred_to_address(&self.stake_credential, self.network)
            .to_bech32()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let out = TxContentStakeAddrInner {
            address,
            registration: self.is_registration,
            cert_index: self.cert_index as i32,
        };

        Ok(out)
    }
}

impl IntoModel<Vec<TxContentStakeAddrInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentStakeAddrInner>, StatusCode> {
        let tx = self.tx()?;

        let network = self.network.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let items = tx
            .certs()
            .into_iter()
            .enumerate()
            .filter_map(|(index, cert)| StakeCertModelBuilder::new(cert, index, network))
            .map(|builder| builder.into_model())
            .try_collect()?;

        Ok(items)
    }
}
