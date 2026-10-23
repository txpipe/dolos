use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::account_mir_content_inner::AccountMirContentInner;
use dolos_cardano::{
    model::{AccountState, ProposalState},
    pallas_extras,
};
use dolos_core::Domain;
use pallas::ledger::{
    addresses::StakeAddress,
    primitives::alonzo::InstantaneousRewardTarget,
    traverse::{MultiEraCert, MultiEraTx},
};

use crate::{
    error::Error,
    mapping,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{by_stake_actions, AccountActionContext};

/// One row per MIR certificate that pays the account.
///
/// Blockfrost lists each certificate as its own row: a tx carrying two MIR
/// certificates for the same account yields two rows. The response does not
/// say whether the funds came from the reserves or the treasury, and a
/// negative amount (a correction) keeps its sign.
fn build_mir<D: Domain>(
    ctx: &AccountActionContext<'_, D>,
    stake_address: &StakeAddress,
    tx: &MultiEraTx,
    cert: &MultiEraCert,
) -> Result<Option<AccountMirContentInner>, StatusCode> {
    let Some(mir) = pallas_extras::cert_as_mir_certificate(cert) else {
        return Ok(None);
    };

    // The other MIR form moves funds between reserves and treasury and
    // pays no account.
    let InstantaneousRewardTarget::StakeCredentials(targets) = mir.target else {
        return Ok(None);
    };

    let amount = targets.iter().find_map(|(cred, amount)| {
        (mapping::stake_cred_to_address(cred, ctx.network) == *stake_address).then_some(*amount)
    });

    let Some(amount) = amount else {
        return Ok(None);
    };

    Ok(Some(AccountMirContentInner {
        tx_hash: tx.hash().to_string(),
        amount: amount.to_string(),
        tx_slot: ctx.block.slot() as i32,
        block_time: ctx.chain.slot_time(ctx.block.slot()) as i32,
        block_height: ctx.block.number() as i32,
    }))
}

/// `GET /accounts/{stake_address}/mirs`: the MIR payments the account ever
/// received, oldest first.
pub async fn by_stake_mirs<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountMirContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<ProposalState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let items = by_stake_actions::<D, _, AccountMirContentInner>(
        &stake_address,
        pagination,
        domain,
        build_mir,
    )
    .await?;

    Ok(Json(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::accounts::testing::*,
        test_support::{TestApp, TestFault},
    };
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use pallas::{
        crypto::hash::Hash,
        ledger::{addresses::Network, primitives::alonzo::Certificate as AlonzoCert},
    };
    use std::collections::BTreeMap;

    fn mir_cred() -> pallas::ledger::primitives::StakeCredential {
        pallas::ledger::primitives::StakeCredential::AddrKeyhash(Hash::from([0x6d; 28]))
    }

    fn mir_stake_address() -> String {
        mapping::stake_cred_to_address(&mir_cred(), Network::Testnet)
            .to_bech32()
            .expect("failed to encode stake address")
    }

    fn mir_pay(
        source: pallas::ledger::primitives::alonzo::InstantaneousRewardSource,
        amount: i64,
    ) -> AlonzoCert {
        use pallas::ledger::primitives::alonzo::MoveInstantaneousReward;

        AlonzoCert::MoveInstantaneousRewardsCert(MoveInstantaneousReward {
            source,
            target: InstantaneousRewardTarget::StakeCredentials(BTreeMap::from([(
                mir_cred(),
                amount,
            )])),
        })
    }

    struct MirBlockVector {
        tx_hash: Hash<32>,
        number: u64,
        slot: u64,
    }

    /// Two Alonzo blocks that follow the synthetic chain's tip: the first
    /// registers the account and pays it a reserves MIR, the second carries a
    /// treasury MIR and nothing else. The second block only reaches the
    /// listing when MIR targets get their own account tag — the registration
    /// tag of the first block cannot find it. Deterministic, so tests rebuild
    /// the vector to learn tx hashes and block coordinates.
    fn alonzo_mir_blocks(
        vectors: &dolos_testing::synthetic::SyntheticVectors,
    ) -> (Vec<dolos_core::RawBlock>, Vec<MirBlockVector>) {
        use pallas::ledger::primitives::alonzo::InstantaneousRewardSource::*;

        let last = vectors.blocks.last().expect("chain has no blocks");
        let mut prev: Hash<32> = last.block_hash.parse().expect("failed to parse block hash");

        let cert_sets = [
            vec![
                AlonzoCert::StakeRegistration(mir_cred()),
                mir_pay(Reserves, 19_296_735),
            ],
            vec![mir_pay(Treasury, 42)],
        ];

        let mut raws = Vec::new();
        let mut rows = Vec::new();

        for (offset, certs) in cert_sets.into_iter().enumerate() {
            let number = last.block_number + 1 + offset as u64;
            let slot = last.slot + 1 + offset as u64;

            // spend a live output of the synthetic chain: the roll pipeline
            // resolves every input it applies
            let input = pallas::ledger::primitives::TransactionInput {
                transaction_id: vectors.blocks[offset].tx_hashes[0]
                    .parse()
                    .expect("failed to parse tx hash"),
                index: 0,
            };

            let (raw, block_hash, tx_hash) = dolos_testing::synthetic::sample_alonzo_block(
                number,
                slot,
                Some(prev),
                input,
                certs,
                None,
            );

            prev = block_hash;
            raws.push(raw);
            rows.push(MirBlockVector {
                tx_hash,
                number,
                slot,
            });
        }

        (raws, rows)
    }

    fn mir_app() -> TestApp {
        use dolos_core::import::ImportExt as _;

        let cfg = SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 1,
            ..Default::default()
        };

        TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let (raws, _) = alonzo_mir_blocks(vectors);
            domain
                .import_blocks(raws)
                .expect("failed to import alonzo blocks");
        })
    }

    async fn get_mirs(app: &TestApp, path: &str) -> Vec<AccountMirContentInner> {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("failed to parse mirs")
    }

    #[tokio::test]
    async fn account_mirs_happy_path() {
        let app = mir_app();
        let (_, blocks) = alonzo_mir_blocks(app.vectors());
        let stake = mir_stake_address();

        let rows = get_mirs(&app, &format!("/accounts/{stake}/mirs")).await;

        // one row per MIR certificate, oldest first; the second row's block
        // carries no other account activity, so it proves the MIR tag
        assert_eq!(rows.len(), 2);

        let amounts = ["19296735", "42"];
        for ((row, block), amount) in rows.iter().zip(&blocks).zip(amounts) {
            assert_eq!(row.amount, amount);
            assert_eq!(row.tx_hash, block.tx_hash.to_string());
            assert_eq!(row.tx_slot, block.slot as i32);
            assert_eq!(row.block_height, block.number as i32);
            assert!(row.block_time > 0);
        }
    }

    #[tokio::test]
    async fn account_mirs_orders_and_paginates() {
        let app = mir_app();
        let stake = mir_stake_address();
        let base = format!("/accounts/{stake}/mirs");
        let expected = get_mirs(&app, &base).await;

        let desc = get_mirs(&app, &format!("{base}?order=desc")).await;
        assert_eq!(
            desc,
            expected.iter().rev().cloned().collect::<Vec<_>>(),
            "desc is the ascending listing read backwards"
        );

        let page = get_mirs(&app, &format!("{base}?count=1&page=2")).await;
        assert_eq!(page, expected[1..].to_vec());

        // a page past the end is empty, not an error
        let page = get_mirs(&app, &format!("{base}?page=9")).await;
        assert!(page.is_empty());
    }

    #[tokio::test]
    async fn account_mirs_without_rows() {
        let app = mir_app();

        // an account that exists and never received a MIR
        let stake = app.vectors().stake_address.clone();
        let rows = get_mirs(&app, &format!("/accounts/{stake}/mirs")).await;
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn account_mirs_bad_request() {
        let app = mir_app();
        let path = format!("/accounts/{}/mirs", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;

        let stake = mir_stake_address();
        let base = format!("/accounts/{stake}/mirs");
        assert_status(&app, &format!("{base}?count=0"), StatusCode::BAD_REQUEST).await;
        assert_status(
            &app,
            &format!("{base}?order=sideways"),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn account_mirs_not_found() {
        let app = mir_app();
        let path = format!("/accounts/{}/mirs", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn account_mirs_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/mirs");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
