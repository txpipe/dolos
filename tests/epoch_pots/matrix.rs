//! Checks `matrix.json`, the entries the Epoch Tests workflow runs, without
//! any fixtures. Each CI job downloads one seed and one upstream range, so an
//! entry must name the ones the harness picks when every fixture is present.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use serde::Deserialize;

use super::{nearest_lower_seed, upstream_dir_for};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    network: String,
    seed_epoch: u64,
    ground_truth_epoch: u64,
    upstream_range: String,
    /// Read only by the workflow's filter.
    #[serde(default)]
    #[allow(dead_code)]
    tags: Vec<String>,
}

fn parse_range(range: &str) -> (u64, u64) {
    let (start, end) = range
        .split_once('-')
        .unwrap_or_else(|| panic!("upstream_range {range} is not start-end"));
    let parse = |x: &str| {
        x.parse()
            .unwrap_or_else(|_| panic!("upstream_range {range} is not start-end"))
    };
    (parse(start), parse(end))
}

#[test]
fn matrix_entries_name_the_fixtures_the_harness_picks() {
    let entries: Vec<Entry> =
        serde_json::from_str(include_str!("matrix.json")).expect("parsing matrix.json");

    let mut seeds: HashMap<String, Vec<u64>> = HashMap::new();
    let mut upstreams: HashMap<String, Vec<(u64, u64, PathBuf)>> = HashMap::new();

    for entry in &entries {
        let (start, end) = parse_range(&entry.upstream_range);
        let dir = PathBuf::from(format!("{}-{}", entry.network, entry.upstream_range));
        seeds
            .entry(entry.network.clone())
            .or_default()
            .push(entry.seed_epoch);
        upstreams
            .entry(entry.network.clone())
            .or_default()
            .push((start, end, dir));
    }

    // Same order as `discover_upstreams`, which decides the first match.
    for ranges in upstreams.values_mut() {
        ranges.sort();
        ranges.dedup();
    }

    let mut subjects = HashSet::new();

    for entry in &entries {
        let name = format!("{}-{}", entry.network, entry.ground_truth_epoch);
        let (start, end) = parse_range(&entry.upstream_range);

        assert!(
            matches!(entry.network.as_str(), "mainnet" | "preprod" | "preview"),
            "{name}: unknown network"
        );
        assert!(subjects.insert(name.clone()), "{name}: listed twice");

        // A seed stopped at epoch S has already crossed the S-1 boundary, so
        // the rewards that the subject's comparison reads would be missing.
        assert!(
            entry.seed_epoch < entry.ground_truth_epoch,
            "{name}: the seed must be an earlier epoch than the subject"
        );
        assert!(
            start <= entry.seed_epoch && entry.ground_truth_epoch <= end,
            "{name}: upstream {} must cover the seed and the subject",
            entry.upstream_range
        );

        assert_eq!(
            nearest_lower_seed(
                &seeds[&entry.network],
                entry.ground_truth_epoch,
                &entry.network
            ),
            entry.seed_epoch,
            "{name}: with every seed present the harness picks another seed"
        );
        assert_eq!(
            upstream_dir_for(&upstreams, &entry.network, entry.ground_truth_epoch),
            PathBuf::from(format!("{}-{}", entry.network, entry.upstream_range)),
            "{name}: with every range present the harness picks another upstream"
        );
    }
}
