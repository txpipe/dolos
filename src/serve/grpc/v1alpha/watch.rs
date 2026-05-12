use futures_core::Stream;
use futures_util::StreamExt;
use pallas::interop::utxorpc::v1alpha::{self as interop, spec as u5c};
use pallas::interop::utxorpc::LedgerContext;
use pallas::{
    interop::utxorpc::v1alpha::spec::watch::any_chain_tx_pattern::Chain,
    ledger::{addresses::Address, traverse::MultiEraBlock},
};
use std::pin::Pin;
use tonic::{Request, Response, Status};

use crate::serve::grpc::stream::ChainStream;
use crate::prelude::*;

fn outputs_match_address(
    pattern: &u5c::cardano::AddressPattern,
    outputs: &[u5c::cardano::TxOutput],
) -> bool {
    let exact_matches = pattern.exact_address.is_empty()
        || outputs.iter().any(|o| o.address == pattern.exact_address);

    let delegation_matches = pattern.delegation_part.is_empty()
        || outputs.iter().any(|o| {
            let addr = Address::from_bytes(&o.address).unwrap();
            match addr {
                Address::Shelley(s) => s.delegation().to_vec().eq(&pattern.delegation_part),
                _ => false,
            }
        });
    let payment_matches = pattern.payment_part.is_empty()
        || outputs.iter().any(|o| {
            let addr = Address::from_bytes(&o.address).unwrap();
            match addr {
                Address::Shelley(s) => s.payment().to_vec().eq(&pattern.payment_part),
                _ => false,
            }
        });

    exact_matches && delegation_matches && payment_matches
}

fn outputs_match_asset(
    asset_pattern: &u5c::cardano::AssetPattern,
    outputs: &[u5c::cardano::TxOutput],
) -> bool {
    outputs
        .iter()
        .any(|o| matches_asset(asset_pattern, &o.assets))
}

fn matches_asset(
    asset_pattern: &u5c::cardano::AssetPattern,
    assets: &[u5c::cardano::Multiasset],
) -> bool {
    assets.iter().any(|ma| {
        if !asset_pattern.policy_id.is_empty() && asset_pattern.policy_id.ne(&ma.policy_id) {
            return false;
        }
        if asset_pattern.asset_name.is_empty() {
            return true;
        }
        ma.assets
            .iter()
            .any(|ma| asset_pattern.asset_name.eq(&ma.name))
    })
}

fn matches_output(
    pattern: &u5c::cardano::TxOutputPattern,
    outputs: &[u5c::cardano::TxOutput],
) -> bool {
    let address_match = pattern
        .address
        .as_ref()
        .is_none_or(|addr_pattern| outputs_match_address(addr_pattern, outputs));

    let asset_match = pattern
        .asset
        .as_ref()
        .is_none_or(|asset_pattern| outputs_match_asset(asset_pattern, outputs));

    address_match && asset_match
}

fn credential_hash_eq(cred: &u5c::cardano::StakeCredential, hash: &[u8]) -> bool {
    use u5c::cardano::stake_credential::StakeCredential as SC;
    match &cred.stake_credential {
        Some(SC::AddrKeyHash(h) | SC::ScriptHash(h)) => h == hash,
        None => false,
    }
}

fn drep_hash_eq(drep: &u5c::cardano::DRep, hash: &[u8]) -> bool {
    use u5c::cardano::d_rep::Drep;
    match &drep.drep {
        Some(Drep::AddrKeyHash(h) | Drep::ScriptHash(h)) => h == hash,
        _ => false,
    }
}

fn matches_stake_credential_pattern(
    pattern: &u5c::cardano::StakeCredential,
    actual: &u5c::cardano::StakeCredential,
) -> bool {
    pattern.stake_credential.is_none() || pattern.stake_credential == actual.stake_credential
}

fn cert_involves_stake_credential(
    cert: &u5c::cardano::certificate::Certificate,
    hash: &[u8],
) -> bool {
    use u5c::cardano::certificate::Certificate as C;
    match cert {
        C::StakeRegistration(c) | C::StakeDeregistration(c) => credential_hash_eq(c, hash),
        C::StakeDelegation(c) => c
            .stake_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        C::RegCert(c) => c
            .stake_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        C::UnregCert(c) => c
            .stake_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        C::VoteDelegCert(c) => c
            .stake_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        C::StakeVoteDelegCert(c) => c
            .stake_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        C::StakeRegDelegCert(c) => c
            .stake_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        C::VoteRegDelegCert(c) => c
            .stake_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        C::StakeVoteRegDelegCert(c) => c
            .stake_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        C::AuthCommitteeHotCert(c) => {
            c.committee_cold_credential
                .as_ref()
                .is_some_and(|sc| credential_hash_eq(sc, hash))
                || c.committee_hot_credential
                    .as_ref()
                    .is_some_and(|sc| credential_hash_eq(sc, hash))
        }
        C::ResignCommitteeColdCert(c) => c
            .committee_cold_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        C::RegDrepCert(c) => c
            .drep_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        C::UnregDrepCert(c) => c
            .drep_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        C::UpdateDrepCert(c) => c
            .drep_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        C::MirCert(c) => c.to.iter().any(|t| {
            t.stake_credential
                .as_ref()
                .is_some_and(|sc| credential_hash_eq(sc, hash))
        }),
        _ => false,
    }
}

fn cert_involves_pool(cert: &u5c::cardano::certificate::Certificate, hash: &[u8]) -> bool {
    use u5c::cardano::certificate::Certificate as C;
    match cert {
        C::StakeDelegation(c) => c.pool_keyhash == hash,
        C::PoolRegistration(c) => c.operator == hash,
        C::PoolRetirement(c) => c.pool_keyhash == hash,
        C::StakeVoteDelegCert(c) => c.pool_keyhash == hash,
        C::StakeRegDelegCert(c) => c.pool_keyhash == hash,
        C::StakeVoteRegDelegCert(c) => c.pool_keyhash == hash,
        _ => false,
    }
}

fn cert_involves_drep(cert: &u5c::cardano::certificate::Certificate, hash: &[u8]) -> bool {
    use u5c::cardano::certificate::Certificate as C;
    match cert {
        C::VoteDelegCert(c) => c.drep.as_ref().is_some_and(|d| drep_hash_eq(d, hash)),
        C::StakeVoteDelegCert(c) => c.drep.as_ref().is_some_and(|d| drep_hash_eq(d, hash)),
        C::VoteRegDelegCert(c) => c.drep.as_ref().is_some_and(|d| drep_hash_eq(d, hash)),
        C::StakeVoteRegDelegCert(c) => c.drep.as_ref().is_some_and(|d| drep_hash_eq(d, hash)),
        C::RegDrepCert(c) => c
            .drep_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        C::UnregDrepCert(c) => c
            .drep_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        C::UpdateDrepCert(c) => c
            .drep_credential
            .as_ref()
            .is_some_and(|sc| credential_hash_eq(sc, hash)),
        _ => false,
    }
}

fn matches_certificate_pattern(
    pattern: &u5c::cardano::CertificatePattern,
    certs: &[u5c::cardano::Certificate],
) -> bool {
    use u5c::cardano::certificate::Certificate as Cert;
    use u5c::cardano::certificate_pattern::CertificateType;

    let Some(ref cert_type) = pattern.certificate_type else {
        return true;
    };

    certs.iter().any(|cert| {
        let Some(ref c) = cert.certificate else {
            return false;
        };

        match cert_type {
            CertificateType::StakeRegistration(pat) => {
                matches!(c, Cert::StakeRegistration(cred) if matches_stake_credential_pattern(pat, cred))
            }
            CertificateType::StakeDeregistration(pat) => {
                matches!(c, Cert::StakeDeregistration(cred) if matches_stake_credential_pattern(pat, cred))
            }
            CertificateType::StakeDelegation(pat) => {
                if let Cert::StakeDelegation(deleg) = c {
                    let cred_match = pat.stake_credential.as_ref().is_none_or(|p| {
                        deleg
                            .stake_credential
                            .as_ref()
                            .is_some_and(|a| matches_stake_credential_pattern(p, a))
                    });
                    let pool_match =
                        pat.pool_keyhash.is_empty() || pat.pool_keyhash == deleg.pool_keyhash;
                    cred_match && pool_match
                } else {
                    false
                }
            }
            CertificateType::PoolRegistration(pat) => {
                if let Cert::PoolRegistration(reg) = c {
                    let operator_match = pat.operator.is_empty() || pat.operator == reg.operator;
                    let pool_match =
                        pat.pool_keyhash.is_empty() || pat.pool_keyhash == reg.operator;
                    operator_match && pool_match
                } else {
                    false
                }
            }
            CertificateType::PoolRetirement(pat) => {
                if let Cert::PoolRetirement(ret) = c {
                    let pool_match =
                        pat.pool_keyhash.is_empty() || pat.pool_keyhash == ret.pool_keyhash;
                    let epoch_match = pat.epoch == 0 || pat.epoch == ret.epoch;
                    pool_match && epoch_match
                } else {
                    false
                }
            }
            CertificateType::AnyStakeCredential(hash) => {
                cert_involves_stake_credential(c, hash)
            }
            CertificateType::AnyPoolKeyhash(hash) => cert_involves_pool(c, hash),
            CertificateType::AnyDrep(hash) => cert_involves_drep(c, hash),
        }
    })
}

fn matches_cardano_pattern(tx_pattern: &u5c::cardano::TxPattern, tx: &u5c::cardano::Tx) -> bool {
    let has_address_match = tx_pattern.has_address.as_ref().is_none_or(|addr_pattern| {
        let outputs: Vec<_> = tx.outputs.to_vec();
        let inputs: Vec<_> = tx
            .inputs
            .iter()
            .filter_map(|x| x.as_output.as_ref().cloned())
            .collect();

        outputs_match_address(addr_pattern, &inputs)
            || outputs_match_address(addr_pattern, &outputs)
    });

    let consumes_match = tx_pattern.consumes.as_ref().is_none_or(|out_pattern| {
        let inputs: Vec<_> = tx
            .inputs
            .iter()
            .filter_map(|x| x.as_output.as_ref().cloned())
            .collect();
        matches_output(out_pattern, &inputs)
    });

    let mints_asset_match = tx_pattern
        .mints_asset
        .as_ref()
        .is_none_or(|asset_pattern| matches_asset(asset_pattern, &tx.mint));

    let moves_asset_match = tx_pattern.moves_asset.as_ref().is_none_or(|asset_pattern| {
        let inputs: Vec<_> = tx
            .inputs
            .iter()
            .filter_map(|x| x.as_output.as_ref().cloned())
            .collect();
        outputs_match_asset(asset_pattern, &inputs)
            || outputs_match_asset(asset_pattern, &tx.outputs)
    });

    let produces_match = tx_pattern
        .produces
        .as_ref()
        .is_none_or(|out_pattern| matches_output(out_pattern, &tx.outputs));

    let has_certificate_match = tx_pattern
        .has_certificate
        .as_ref()
        .is_none_or(|cert_pattern| matches_certificate_pattern(cert_pattern, &tx.certificates));

    has_address_match
        && consumes_match
        && mints_asset_match
        && moves_asset_match
        && produces_match
        && has_certificate_match
}

fn matches_chain(chain: &Chain, tx: &u5c::cardano::Tx) -> bool {
    match chain {
        Chain::Cardano(tx_pattern) => matches_cardano_pattern(tx_pattern, tx),
    }
}

fn apply_predicate(predicate: &u5c::watch::TxPredicate, tx: &u5c::cardano::Tx) -> bool {
    let tx_matches = predicate
        .r#match
        .as_ref()
        .and_then(|pattern| pattern.chain.as_ref())
        .is_none_or(|chain| matches_chain(chain, tx));

    let not_clause = predicate.not.iter().any(|p| apply_predicate(p, tx));

    let and_clause = predicate.all_of.iter().all(|p| apply_predicate(p, tx));

    let or_clause =
        predicate.any_of.is_empty() || predicate.any_of.iter().any(|p| apply_predicate(p, tx));

    tx_matches && !not_clause && and_clause && or_clause
}

fn block_to_txs<C: LedgerContext>(
    block: &RawBlock,
    mapper: &interop::Mapper<C>,
    request: &u5c::watch::WatchTxRequest,
) -> Vec<u5c::watch::AnyChainTx> {
    let bytes = block;
    let block = MultiEraBlock::decode(block).unwrap();
    let txs = block.txs();

    txs.iter()
        .map(|x: &pallas::ledger::traverse::MultiEraTx<'_>| mapper.map_tx(x))
        .filter(|tx| {
            request
                .predicate
                .as_ref()
                .is_none_or(|predicate| apply_predicate(predicate, tx))
        })
        .map(|x| u5c::watch::AnyChainTx {
            chain: Some(u5c::watch::any_chain_tx::Chain::Cardano(x)),
            block: Some(u5c::watch::AnyChainBlock {
                native_bytes: bytes.to_vec().into(),
                chain: Some(u5c::watch::any_chain_block::Chain::Cardano(
                    mapper.map_block(&block),
                )),
            }),
        })
        .collect()
}

fn raw_to_blockref<C: LedgerContext>(
    mapper: &interop::Mapper<C>,
    raw: &[u8],
) -> Option<u5c::watch::BlockRef> {
    let block = mapper.map_block_cbor(raw);
    let header = block.header?;

    Some(u5c::watch::BlockRef {
        slot: header.slot,
        hash: header.hash,
        height: header.height,
    })
}

fn roll_to_watch_response<C: LedgerContext>(
    mapper: &interop::Mapper<C>,
    log: &TipEvent,
    request: &u5c::watch::WatchTxRequest,
) -> impl Stream<Item = u5c::watch::WatchTxResponse> {
    let txs: Vec<_> = match log {
        TipEvent::Apply(_, block) => {
            let txs = block_to_txs(block, mapper, request);
            if txs.is_empty() {
                let block_ref = raw_to_blockref(mapper, block);
                if let Some(r) = block_ref {
                    vec![u5c::watch::WatchTxResponse {
                        action: Some(u5c::watch::watch_tx_response::Action::Idle(r)),
                    }]
                } else {
                    vec![]
                }
            } else {
                txs.into_iter()
                    .map(u5c::watch::watch_tx_response::Action::Apply)
                    .map(|x| u5c::watch::WatchTxResponse { action: Some(x) })
                    .collect()
            }
        }
        TipEvent::Undo(_, block) => block_to_txs(block, mapper, request)
            .into_iter()
            .map(u5c::watch::watch_tx_response::Action::Undo)
            .map(|x| u5c::watch::WatchTxResponse { action: Some(x) })
            .collect(),
        // TODO: shouldn't we have a u5c event for origin?
        TipEvent::Mark(..) => vec![],
    };

    tokio_stream::iter(txs)
}

pub struct WatchServiceImpl<D, C>
where
    D: Domain + LedgerContext,
    C: CancelToken,
{
    domain: D,
    mapper: interop::Mapper<D>,
    cancel: C,
}

impl<D, C> WatchServiceImpl<D, C>
where
    D: Domain + LedgerContext,
    C: CancelToken,
{
    pub fn new(domain: D, cancel: C) -> Self {
        let mapper = interop::Mapper::new(domain.clone());

        Self {
            domain,
            mapper,
            cancel,
        }
    }
}

#[async_trait::async_trait]
impl<D, C> u5c::watch::watch_service_server::WatchService for WatchServiceImpl<D, C>
where
    D: Domain + LedgerContext,
    C: CancelToken,
{
    type WatchTxStream = Pin<
        Box<dyn Stream<Item = Result<u5c::watch::WatchTxResponse, tonic::Status>> + Send + 'static>,
    >;

    async fn watch_tx(
        &self,
        request: Request<u5c::watch::WatchTxRequest>,
    ) -> Result<Response<Self::WatchTxStream>, Status> {
        let inner_req = request.into_inner();

        let intersect = inner_req
            .intersect
            .iter()
            .map(|x| ChainPoint::Specific(x.slot, x.hash.to_vec().as_slice().into()))
            .collect::<Vec<ChainPoint>>();

        let stream =
            ChainStream::start::<D, _>(self.domain.clone(), intersect, self.cancel.clone());

        let mapper = self.mapper.clone();

        let stream = stream
            .flat_map(move |log| roll_to_watch_response(&mapper, &log, &inner_req))
            .map(Ok);

        Ok(Response::new(Box::pin(stream)))
    }
}
