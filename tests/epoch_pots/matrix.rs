//! Checks `matrix.json`, the entries the Epoch Tests workflow runs, without
//! any fixtures. Each CI job downloads one seed and one upstream range, so an
//! entry must name the ones the harness picks when every fixture is present.

use std::collections::HashSet;

use serde::Deserialize;

use super::{discover_seeds, discover_upstreams, nearest_lower_seed, upstream_dir_for};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    network: String,
    seed_epoch: u64,
    ground_truth_epoch: u64,
    upstream_range: String,
    #[serde(default)]
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

/// The tags the workflow's `tag` input can select, read from its choices.
fn workflow_tags() -> HashSet<&'static str> {
    let workflow = include_str!("../../.github/workflows/epoch-tests.yml");
    let (_, tag_input) = workflow
        .split_once("\n      tag:\n")
        .expect("epoch-tests.yml has a tag input");
    let options = tag_input
        .lines()
        .find_map(|line| line.trim().strip_prefix("options: ["))
        .and_then(|line| line.strip_suffix(']'))
        .expect("the tag input lists its options as [a, b]");

    options
        .split(',')
        .map(str::trim)
        .filter(|tag| *tag != "all")
        .collect()
}

#[test]
fn matrix_entries_name_the_fixtures_the_harness_picks() {
    let entries: Vec<Entry> =
        serde_json::from_str(include_str!("matrix.json")).expect("parsing matrix.json");

    // Lay out every fixture the matrix names as an empty directory, so the
    // harness's own discovery decides which seed and range each entry gets.
    let fixtures = tempfile::tempdir().expect("creating fixture dir");
    let seed_base = fixtures.path().join("seeds");
    let upstream_base = fixtures.path().join("upstream");

    for entry in &entries {
        let seed = format!("{}-{}", entry.network, entry.seed_epoch);
        let upstream = format!("{}-{}", entry.network, entry.upstream_range);
        std::fs::create_dir_all(seed_base.join(seed)).expect("creating seed dir");
        std::fs::create_dir_all(upstream_base.join(upstream)).expect("creating upstream dir");
    }

    let seeds = discover_seeds(&seed_base);
    let upstreams = discover_upstreams(&upstream_base);
    let tags = workflow_tags();

    let mut subjects = HashSet::new();

    for entry in &entries {
        let name = format!("{}-{}", entry.network, entry.ground_truth_epoch);
        let (start, end) = parse_range(&entry.upstream_range);

        assert!(
            matches!(entry.network.as_str(), "mainnet" | "preprod" | "preview"),
            "{name}: unknown network"
        );
        assert!(subjects.insert(name.clone()), "{name}: listed twice");

        for tag in &entry.tags {
            assert!(
                tags.contains(tag.as_str()),
                "{name}: the workflow's tag input can't select {tag:?}"
            );
        }

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
            upstream_base.join(format!("{}-{}", entry.network, entry.upstream_range)),
            "{name}: with every range present the harness picks another upstream"
        );
    }
}
