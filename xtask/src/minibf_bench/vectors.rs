//! Test vector management: query DBSync and cache results.

use anyhow::{Context, Result};
use postgres::{Client, NoTls};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use crate::config::Network;

const CACHE_TTL_HOURS: u64 = 24;
const VECTORS_DIR: &str = "src/minibf_bench/vectors";

/// Test vectors for a specific network
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestVectors {
    pub network: String,
    pub generated_at: String,
    pub dbsync_source: String,
    pub addresses: Vec<AddressVector>,
    pub stake_addresses: Vec<StakeAddressVector>,
    pub reference_blocks: ReferenceBlocks,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressVector {
    pub address: String,
    pub address_type: String,
    pub tx_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeAddressVector {
    pub stake_address: String,
    pub delegation_count: i64,
    pub reward_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferenceBlocks {
    pub early: BlockRef,
    pub mid: BlockRef,
    pub recent: BlockRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockRef {
    pub height: i64,
    pub hash: String,
    pub epoch: i64,
    pub slot: i64,
}

pub struct VectorGenerator {
    client: Client,
}

impl VectorGenerator {
    pub fn new(dbsync_url: &str) -> Result<Self> {
        let client = Client::connect(dbsync_url, NoTls).context("Failed to connect to DBSync")?;
        Ok(Self { client })
    }

    pub fn generate_vectors(
        &mut self,
        network: &Network,
        dbsync_host: &str,
    ) -> Result<TestVectors> {
        let network_str = network.as_str();

        Ok(TestVectors {
            network: network_str.to_string(),
            generated_at: chrono::Utc::now().to_rfc3339(),
            dbsync_source: dbsync_host.to_string(),
            addresses: self.query_addresses(network)?,
            stake_addresses: self.query_stake_addresses()?,
            reference_blocks: self.query_reference_blocks()?,
        })
    }

    fn query_addresses(&mut self, network: &Network) -> Result<Vec<AddressVector>> {
        let pattern = match network {
            Network::Mainnet => "addr1%",
            Network::Preprod => "addr_test1%",
            Network::Preview => "addr_test1%",
        };

        let rows = self.client.query(
            "SELECT DISTINCT tx_out.address, sa.view as stake_address, COUNT(tx.id) as tx_count
             FROM tx_out
             LEFT JOIN stake_address sa ON tx_out.stake_address_id = sa.id
             JOIN tx ON tx_out.tx_id = tx.id
             WHERE tx_out.address LIKE $1
             GROUP BY tx_out.address, sa.view
             ORDER BY tx_count DESC
             LIMIT 50",
            &[&pattern],
        )?;

        rows.iter()
            .map(|row| {
                let address: String = row.get(0);
                let stake: Option<String> = row.get(1);
                let tx_count: i64 = row.get(2);

                let address_type = if stake.is_some() {
                    "shelley_payment_stake"
                } else {
                    "shelley_payment_only"
                };

                Ok(AddressVector {
                    address,
                    address_type: address_type.to_string(),
                    tx_count,
                })
            })
            .collect()
    }

    fn query_stake_addresses(&mut self) -> Result<Vec<StakeAddressVector>> {
        let rows = self.client.query(
            "SELECT sa.view,
                    COALESCE(d.cnt, 0) as delegation_count,
                    COALESCE(r.cnt, 0) as reward_count
             FROM stake_address sa
             LEFT JOIN (SELECT addr_id, COUNT(*) as cnt FROM delegation GROUP BY addr_id) d ON sa.id = d.addr_id
             LEFT JOIN (SELECT addr_id, COUNT(*) as cnt FROM reward GROUP BY addr_id) r ON sa.id = r.addr_id
             WHERE COALESCE(d.cnt, 0) > 0 OR COALESCE(r.cnt, 0) > 0
             ORDER BY delegation_count DESC, reward_count DESC
             LIMIT 30",
            &[],
        )?;

        rows.iter()
            .map(|row| {
                Ok(StakeAddressVector {
                    stake_address: row.get(0),
                    delegation_count: row.get(1),
                    reward_count: row.get(2),
                })
            })
            .collect()
    }

    fn query_reference_blocks(&mut self) -> Result<ReferenceBlocks> {
        // Get tip
        let tip_row = self.client.query_one(
            "SELECT block_no, hash::text, epoch_no, slot_no
             FROM block
             WHERE block_no IS NOT NULL
             ORDER BY block_no DESC
             LIMIT 1",
            &[],
        )?;

        let tip_height: i32 = tip_row.get(0);

        // Calculate positions for 10%, 50%, 90%
        let early_height = tip_height / 10;
        let mid_height = tip_height / 2;
        let recent_height = (tip_height * 9) / 10;

        Ok(ReferenceBlocks {
            early: self.get_block_at_height(early_height)?,
            mid: self.get_block_at_height(mid_height)?,
            recent: self.get_block_at_height(recent_height)?,
        })
    }

    fn get_block_at_height(&mut self, height: i32) -> Result<BlockRef> {
        let row = self.client.query_one(
            "SELECT block_no, hash::text, epoch_no, slot_no
             FROM block
             WHERE block_no = $1",
            &[&height],
        )?;

        let height: i32 = row.get(0);
        let epoch: i32 = row.get(2);
        let slot: i64 = row.get(3);

        Ok(BlockRef {
            height: height as i64,
            hash: row.get(1),
            epoch: epoch as i64,
            slot,
        })
    }
}

/// Load or generate test vectors for a network
pub fn load_vectors(
    network: &Network,
    dbsync_url: &str,
    force_refresh: bool,
) -> Result<TestVectors> {
    let vectors_path = PathBuf::from(VECTORS_DIR).join(format!("{}.json", network.as_str()));

    // Check if we can use cached vectors
    if !force_refresh && vectors_path.exists() {
        let metadata = fs::metadata(&vectors_path)?;
        let modified = metadata.modified()?;
        let age = SystemTime::now().duration_since(modified)?;

        if age < Duration::from_secs(CACHE_TTL_HOURS * 3600) {
            let content = fs::read_to_string(&vectors_path)?;
            return Ok(serde_json::from_str(&content)?);
        }
    }

    // Generate new vectors
    println!(
        "Generating test vectors for {} from DBSync...",
        network.as_str()
    );

    let mut generator = VectorGenerator::new(dbsync_url)?;
    let dbsync_host = dbsync_url
        .split('@')
        .nth(1)
        .and_then(|s| s.split('/').next())
        .unwrap_or("unknown")
        .to_string();

    let vectors = generator.generate_vectors(network, &dbsync_host)?;

    // Ensure directory exists
    fs::create_dir_all(&vectors_path.parent().unwrap())?;

    // Cache to file
    let json = serde_json::to_string_pretty(&vectors)?;
    fs::write(&vectors_path, json)?;
    println!("Cached test vectors to {}", vectors_path.display());

    Ok(vectors)
}
