//! Dynamic chain sampling to test endpoints across chain history.

use anyhow::Result;
use reqwest::Client;

/// Historical samples from different chain positions
#[derive(Debug, Clone)]
pub struct HistoricalSamples {
    pub early: ChainSample,
    pub mid: ChainSample,
    pub recent: ChainSample,
}

#[derive(Debug, Clone)]
pub struct ChainSample {
    pub block_hash: String,
    pub block_number: u64,
    pub slot: u64,
    pub epoch: u64,
    pub txs: Vec<String>,
}

impl HistoricalSamples {
    /// Build samples from pre-computed reference blocks in test vectors,
    /// fetching block details and txs via the API by block height.
    pub async fn from_vectors(
        client: &Client,
        base_url: &str,
        refs: &crate::minibf_bench::vectors::ReferenceBlocks,
    ) -> Result<Self> {
        Ok(Self {
            early: fetch_sample(client, base_url, refs.early.height).await?,
            mid: fetch_sample(client, base_url, refs.mid.height).await?,
            recent: fetch_sample(client, base_url, refs.recent.height).await?,
        })
    }
}

async fn fetch_sample(client: &Client, base_url: &str, height: i64) -> Result<ChainSample> {
    let url = format!("{}/blocks/{}", base_url, height);
    let resp: serde_json::Value = client.get(&url).send().await?.json().await?;

    let block_hash = resp["hash"].as_str().unwrap_or_default().to_string();
    let block_number = resp["height"].as_u64().unwrap_or(height as u64);
    let slot = resp["slot"].as_u64().unwrap_or_default();
    let epoch = resp["epoch"].as_u64().unwrap_or_default();

    // Get transactions for this block
    let txs_url = format!("{}/blocks/{}/txs?count=100", base_url, block_hash);
    let txs_resp: serde_json::Value = client.get(&txs_url).send().await?.json().await?;

    let txs = match txs_resp.as_array() {
        Some(arr) => arr
            .iter()
            .filter_map(|v| v["tx_hash"].as_str().or_else(|| v.as_str()).map(String::from))
            .take(20)
            .collect(),
        None => Vec::new(),
    };

    Ok(ChainSample {
        block_hash,
        block_number,
        slot,
        epoch,
        txs,
    })
}
