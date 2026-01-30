//! Assertion helpers for comparing Dolos state against ground-truth fixtures.

use anyhow::Result;
use dolos_cardano::{eras::ChainSummary, EpochState, EraSummary, FixedNamespace};
use dolos_core::{ArchiveStore, LogKey, TemporalKey};

pub(crate) fn format_value(value: &impl std::fmt::Debug) -> String {
    let raw = format!("{:?}", value);

    if raw.starts_with("Hash<") {
        if let Some(start) = raw.find('"') {
            if let Some(end) = raw.rfind('"') {
                if end > start {
                    return raw[start + 1..end].to_string();
                }
            }
        }
    }

    raw
}

pub(crate) fn format_mismatch(
    context: &str,
    field: &str,
    expected: &impl std::fmt::Debug,
    actual: &impl std::fmt::Debug,
) -> String {
    format!(
        "{} {} expected {} got {}",
        context,
        field,
        format_value(expected),
        format_value(actual)
    )
}

#[macro_export]
macro_rules! compare_fields {
    ($context:expr, $expected:expr, $actual:expr, $report:expr, [$(($field:expr, $expected_field:expr, $actual_field:expr)),+ $(,)?]) => {{
        $(
            if $expected_field != $actual_field {
                $report.mismatches += 1;
                if $report.mismatch_samples.len() < 20 {
                    $report
                        .mismatch_samples
                        .push(crate::harness::assertions::format_mismatch(
                            &$context,
                            $field,
                            &$expected_field,
                            &$actual_field,
                        ));
                }
            } else {
                $report.matches += 1;
            }
        )+
        anyhow::Result::<()>::Ok(())
    }};
}

/// Load eras from a JSON fixture file.
pub fn load_eras_fixture(path: &std::path::Path) -> Result<Vec<EraSummary>> {
    let content = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str::<Vec<EraSummary>>(&content)?)
}

/// Load all epoch states from a JSON fixture file.
pub fn load_epochs_fixture(path: &std::path::Path) -> Result<Vec<EpochState>> {
    let content = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str::<Vec<EpochState>>(&content)?)
}

pub fn load_epoch_from_archive<A: ArchiveStore>(
    archive: &A,
    eras: &ChainSummary,
    epoch: u64,
) -> Result<EpochState> {
    let epoch_start_slot = eras.epoch_start(epoch);
    let logkey = LogKey::from(TemporalKey::from(epoch_start_slot));
    let actual = archive
        .read_log_typed::<EpochState>(EpochState::NS, &logkey)?
        .ok_or_else(|| anyhow::anyhow!("epoch {} not found in archive", epoch))?;
    Ok(actual)
}
