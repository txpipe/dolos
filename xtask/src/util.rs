//! Shared utilities for xtask commands.

use anyhow::{Context, Result};
use csv_diff::csv::Csv;
use csv_diff::csv_diff::CsvByteDiffLocalBuilder;
use csv_diff::diff_row::DiffByteRecord;
use std::fs::File;
use std::path::{Path, PathBuf};

/// Resolve a path relative to a base directory.
///
/// If the path is absolute, returns it as-is.
/// If the path is relative, joins it with the base directory.
pub fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

/// Check if a directory exists and has at least one entry.
pub fn dir_has_entries(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }

    let mut entries =
        std::fs::read_dir(path).with_context(|| format!("reading dir: {}", path.display()))?;
    Ok(entries.next().is_some())
}

/// Compare two CSV files by primary key columns, printing differences.
///
/// Returns an error if any differences are found or if something goes wrong.
pub fn compare_csvs(file1: &Path, file2: &Path, key_columns: &[usize], max_rows: usize) -> Result<usize> {
    // Count data rows in file1 (excluding header)
    let total_rows = {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(file1)
            .with_context(|| format!("counting rows in {}", file1.display()))?;
        rdr.records().count()
    };

    let csv_left = Csv::with_reader_seek(
        File::open(file1).with_context(|| format!("opening {}", file1.display()))?,
    );
    let csv_right = Csv::with_reader_seek(
        File::open(file2).with_context(|| format!("opening {}", file2.display()))?,
    );

    let differ = CsvByteDiffLocalBuilder::new()
        .primary_key_columns(key_columns.iter().copied())
        .build()
        .context("building csv differ")?;

    let diff_results = differ.diff(csv_left, csv_right).context("diffing csvs")?;

    let headers: Vec<String> = diff_results
        .headers()
        .headers_left()
        .map(|h| {
            h.iter()
                .map(|f| String::from_utf8_lossy(f).to_string())
                .collect()
        })
        .unwrap_or_default();

    let mut count = 0usize;
    let mut total = 0usize;

    for record in diff_results.as_slice() {
        total += 1;
        if count >= max_rows && max_rows > 0 {
            continue; // keep counting total but stop printing
        }
        count += 1;

        match &record {
            DiffByteRecord::Add(info) => {
                let rec = info.byte_record();
                let fields: Vec<String> = rec.iter().map(|f| String::from_utf8_lossy(f).to_string()).collect();
                println!("  + [Add] {}", format_record(&headers, &fields));
            }
            DiffByteRecord::Modify { delete, add, field_indices } => {
                let old_fields: Vec<String> = delete.byte_record().iter().map(|f| String::from_utf8_lossy(f).to_string()).collect();
                let new_fields: Vec<String> = add.byte_record().iter().map(|f| String::from_utf8_lossy(f).to_string()).collect();
                println!("  ~ [Modify] changed columns: {:?}", field_indices.iter().map(|&i| headers.get(i).cloned().unwrap_or_else(|| i.to_string())).collect::<Vec<_>>());
                println!("    - {}", format_record(&headers, &old_fields));
                println!("    + {}", format_record(&headers, &new_fields));
            }
            DiffByteRecord::Delete(info) => {
                let rec = info.byte_record();
                let fields: Vec<String> = rec.iter().map(|f| String::from_utf8_lossy(f).to_string()).collect();
                println!("  - [Delete] {}", format_record(&headers, &fields));
            }
        }
    }

    if max_rows > 0 && total > max_rows {
        println!("  ... and {} more differences (showing {}/{})", total - max_rows, max_rows, total);
    }

    let matched = total_rows.saturating_sub(total);
    println!("  {} rows matched, {} differences", matched, total);

    Ok(total)
}

fn format_record(headers: &[String], fields: &[String]) -> String {
    headers
        .iter()
        .zip(fields.iter())
        .map(|(h, v)| format!("{}={}", h, v))
        .collect::<Vec<_>>()
        .join(", ")
}
