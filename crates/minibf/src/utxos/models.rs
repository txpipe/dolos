use axum::http::StatusCode;
use pallas::ledger::traverse::MultiEraOutput;
use std::sync::Arc;

use dolos_cardano::ChainSummary;
use dolos_core::{Domain, TxoRef, UtxoValue};

use crate::{
    mapping::{IntoModel, UtxoOutputModelBuilder},
    Facade,
};

/// The models of a page's rows, in the rows' order. The block hash comes from
/// the block at each row's creation position; a row without one (genesis,
/// configured) or whose block was pruned gets none.
///
/// The lookup and the value read are not one snapshot. When a rollback removes
/// a row's block after its value was read, the row lists without a block hash
/// for one response, unless a fork block at that slot already carries its
/// transaction at the same index. A request after the fork reads the row's new
/// position and its block.
///
/// With `chain`, a row whose block the archive holds also gets that block's
/// time; the rows of the listings whose model reports none skip the summary.
pub(super) async fn into_models<D, T>(
    domain: &Facade<D>,
    rows: Vec<(TxoRef, Arc<UtxoValue>)>,
    chain: Option<&ChainSummary>,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let located = rows
        .iter()
        .filter_map(|(txo_ref, value)| Some((txo_ref.0, value.position()?)))
        .collect::<Vec<_>>();

    let block_deps = domain
        .query()
        .block_meta_by_positions(located)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut out = Vec::with_capacity(rows.len());
    for (txo_ref, value) in rows {
        let output = MultiEraOutput::try_from(value.as_ref())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let TxoRef(tx_hash, txo_idx) = txo_ref;
        let builder = UtxoOutputModelBuilder::from_output(tx_hash, txo_idx, output);

        let builder = match (block_deps.get(&tx_hash).cloned(), chain) {
            (Some(x), Some(chain)) => {
                let block_time = chain.slot_time(x.slot);
                builder.with_block_data(x).with_block_time(block_time)
            }
            (Some(x), None) => builder.with_block_data(x),
            (None, _) => builder,
        };

        out.push(<UtxoOutputModelBuilder<'_> as IntoModel<T>>::into_model(
            builder,
        )?);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
    use dolos_core::{config::MinibfConfig, import::ImportExt as _, Domain};
    use dolos_testing::{
        measured::MeasuredStores,
        synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
        toy_domain::{MemoryStores, ToyDomain, ToyStores as _},
    };
    use http_body_util::BodyExt as _;
    use std::sync::Arc;
    use tower::util::ServiceExt as _;

    /// Block reads follow the rows a page returns, not the rows the address
    /// holds: a one-row page reads a twentieth of what the twenty-row page
    /// reads.
    #[tokio::test]
    async fn a_page_fetches_only_its_own_blocks() {
        let block_count = 20;
        let (blocks, vectors, chain_config) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count,
            txs_per_block: 1,
            slot: 1,
            ..Default::default()
        });
        let domain = ToyDomain::with_stores(
            Arc::new(dolos_cardano::include::preview::load()),
            chain_config,
            None,
            None,
            MeasuredStores::new(MemoryStores::open()),
        );
        domain
            .import_blocks(blocks)
            .expect("import synthetic blocks");

        let router = crate::build_router(
            MinibfConfig::new("[::]:0".parse().expect("listen address")),
            domain.clone(),
        );

        let mut block_reads = Vec::new();
        for count in [1, 100] {
            domain.archive().counters.reset();
            let path = format!("/addresses/{}/utxos?count={count}", vectors.address);
            let res = router
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);

            let bytes = res.into_body().collect().await.unwrap().to_bytes();
            let rows: Vec<AddressUtxoContentInner> = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(rows.len(), count.min(block_count));

            block_reads.push(domain.archive().counters.snapshot().block_reads);
        }

        assert!(block_reads[0] > 0);
        assert_eq!(block_reads[1], block_reads[0] * block_count as u64);
    }
}
