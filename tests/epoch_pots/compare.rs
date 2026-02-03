use std::path::Path;

use anyhow::{Context, Result};
use csv_diff::csv::Csv;
use csv_diff::csv_diff::CsvByteDiffLocalBuilder;
use csv_diff::diff_row::DiffByteRecord;

pub fn compare_csvs_with_ignore(
    file1: &Path,
    file2: &Path,
    key_columns: &[usize],
    max_rows: usize,
    ignore: impl Fn(&DiffByteRecord) -> bool,
) -> Result<usize> {
    let total_rows = {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(file1)
            .with_context(|| format!("counting rows in {}", file1.display()))?;
        rdr.records().count()
    };

    let csv_left = Csv::with_reader_seek(
        std::fs::File::open(file1).with_context(|| format!("opening {}", file1.display()))?,
    );
    let csv_right = Csv::with_reader_seek(
        std::fs::File::open(file2).with_context(|| format!("opening {}", file2.display()))?,
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
    let mut ignored = 0usize;

    for record in diff_results.as_slice() {
        if ignore(&record) {
            ignored += 1;
            continue;
        }

        total += 1;
        if count >= max_rows && max_rows > 0 {
            continue;
        }
        count += 1;

        match &record {
            DiffByteRecord::Add(info) => {
                let rec = info.byte_record();
                let fields: Vec<String> = rec
                    .iter()
                    .map(|f| String::from_utf8_lossy(f).to_string())
                    .collect();
                eprintln!("  + [Add] {}", format_record(&headers, &fields));
            }
            DiffByteRecord::Modify {
                delete,
                add,
                field_indices,
            } => {
                let old_fields: Vec<String> = delete
                    .byte_record()
                    .iter()
                    .map(|f| String::from_utf8_lossy(f).to_string())
                    .collect();
                let new_fields: Vec<String> = add
                    .byte_record()
                    .iter()
                    .map(|f| String::from_utf8_lossy(f).to_string())
                    .collect();
                eprintln!(
                    "  ~ [Modify] changed columns: {:?}",
                    field_indices
                        .iter()
                        .map(|&i| headers.get(i).cloned().unwrap_or_else(|| i.to_string()))
                        .collect::<Vec<_>>()
                );
                eprintln!("    - {}", format_record(&headers, &old_fields));
                eprintln!("    + {}", format_record(&headers, &new_fields));
            }
            DiffByteRecord::Delete(info) => {
                let rec = info.byte_record();
                let fields: Vec<String> = rec
                    .iter()
                    .map(|f| String::from_utf8_lossy(f).to_string())
                    .collect();
                eprintln!("  - [Delete] {}", format_record(&headers, &fields));
            }
        }
    }

    if max_rows > 0 && total > max_rows {
        eprintln!(
            "  ... and {} more differences (showing {}/{})",
            total - max_rows,
            max_rows,
            total
        );
    }

    let matched = total_rows.saturating_sub(total + ignored);
    eprintln!("  {} rows matched, {} differences", matched, total);

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

/// Extract a single row from a ground-truth CSV by matching the first column.
pub fn extract_row_from_csv(csv_content: &str, key_value: &str, path: &Path) -> Result<()> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_content.as_bytes());

    let headers = rdr.headers().context("reading headers")?.clone();

    let mut wtr = csv::Writer::from_path(path)
        .with_context(|| format!("creating {}", path.display()))?;

    wtr.write_record(headers.iter())?;

    for record in rdr.records() {
        let record = record.context("reading record")?;
        if record.get(0) == Some(key_value) {
            wtr.write_record(record.iter())?;
            break;
        }
    }

    wtr.flush()?;
    Ok(())
}

/// Write a ground-truth string to a file path.
pub fn write_fixture(content: &str, path: &Path) -> Result<()> {
    std::fs::write(path, content)
        .with_context(|| format!("writing fixture to {}", path.display()))?;
    Ok(())
}

/// Check if a CSV string has data rows (not just a header).
pub fn has_data(csv: &str) -> bool {
    csv.lines().count() > 1
}
