//! Dictionaries as the maintenance commands see them: the `.dict` file the
//! store reads, plus a `.json` manifest beside it recording the network and
//! the sample the dictionary was trained on, so a later seal can refuse a
//! dictionary trained for another network and an operator can reproduce it.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use dolos_fjall::flatfiles::compressed::{
    Dictionary, DictionaryDir, DictionaryId, DictionarySource as _,
};
use dolos_fjall::flatfiles::DICTIONARIES_DIR;
use miette::{bail, IntoDiagnostic, WrapErr};
use serde::{Deserialize, Serialize};

/// What a dictionary was trained on. Written once, beside the dictionary,
/// under its identity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub network_magic: u64,
    pub from_segment: u32,
    pub to_segment: u32,
    /// Blocks the range held when the sample was drawn.
    pub population: usize,
    /// Blocks the trainer saw.
    pub samples: usize,
    pub sample_bytes: u64,
    pub sample_budget_bytes: u64,
    pub seed: u64,
    pub max_size: usize,
    pub zstd_version: u32,
}

pub fn dir(segments_dir: &Path) -> DictionaryDir {
    DictionaryDir::new(segments_dir.join(DICTIONARIES_DIR))
}

pub fn manifest_path(dictionaries: &DictionaryDir, id: &DictionaryId) -> PathBuf {
    dictionaries.path().join(format!("{id}.json"))
}

pub fn parse_id(text: &str) -> miette::Result<DictionaryId> {
    match DictionaryId::from_hex(text.trim()) {
        Some(id) => Ok(id),
        None => bail!(
            "{text:?} is not a dictionary identity: expected the 64 hex digits of the \
             dictionary's SHA-256"
        ),
    }
}

pub fn read_manifest(
    dictionaries: &DictionaryDir,
    id: &DictionaryId,
) -> miette::Result<Option<Manifest>> {
    let path = manifest_path(dictionaries, id);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e)
                .into_diagnostic()
                .wrap_err_with(|| format!("reading {}", path.display()))
        }
    };
    serde_json::from_str(&text)
        .into_diagnostic()
        .wrap_err_with(|| format!("parsing {}", path.display()))
        .map(Some)
}

/// Write the manifest beside its dictionary, durably; an existing manifest
/// is kept, since the dictionary it describes cannot have changed.
pub fn write_manifest(
    dictionaries: &DictionaryDir,
    id: &DictionaryId,
    manifest: &Manifest,
) -> miette::Result<PathBuf> {
    let target = manifest_path(dictionaries, id);
    if target.exists() {
        return Ok(target);
    }
    let staging = dictionaries.path().join(format!("{id}.json.tmp"));
    let text = serde_json::to_string_pretty(manifest).into_diagnostic()?;
    let write = || -> std::io::Result<()> {
        let mut file = fs::File::create(&staging)?;
        file.write_all(text.as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        fs::rename(&staging, &target)?;
        if cfg!(unix) {
            fs::File::open(dictionaries.path())?.sync_all()?;
        }
        Ok(())
    };
    write()
        .into_diagnostic()
        .wrap_err_with(|| format!("writing {}", target.display()))?;
    Ok(target)
}

/// Load the dictionary a seal compresses with, refusing one that is not
/// installed, does not hash to its name, or was trained for another network.
pub fn resolve(
    dictionaries: &DictionaryDir,
    id: &DictionaryId,
    network_magic: u64,
) -> miette::Result<Dictionary> {
    let dictionary = match dictionaries.dictionary(id) {
        Ok(Some(dictionary)) => dictionary,
        Ok(None) => bail!(
            "dictionary {id} is not installed at {}: train one with `dolos data \
             archive-compression train-dictionary`, or restore its .dict file from a backup",
            dictionaries.file(id).display()
        ),
        Err(e) => bail!("dictionary {id} cannot be used: {e}"),
    };
    match read_manifest(dictionaries, id)? {
        Some(manifest) if manifest.network_magic != network_magic => bail!(
            "dictionary {id} was trained on network magic {} and this archive is network \
             magic {network_magic}; refusing to seal with it",
            manifest.network_magic
        ),
        Some(_) => {}
        None => eprintln!(
            "warning: dictionary {id} has no manifest at {}; its network cannot be checked",
            manifest_path(dictionaries, id).display()
        ),
    }
    Ok(dictionary)
}

/// One installed dictionary, as `inspect` reports it.
#[derive(Debug, Serialize)]
pub struct Installed {
    pub id: String,
    pub bytes: u64,
    pub path: PathBuf,
    /// `None` when the file hashes to its name; the error otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub problem: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<Manifest>,
}

/// Every `.dict` file under the dictionaries directory, ascending by
/// identity.
pub fn installed(dictionaries: &DictionaryDir) -> miette::Result<Vec<Installed>> {
    let entries = match fs::read_dir(dictionaries.path()) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(e)
                .into_diagnostic()
                .wrap_err_with(|| format!("listing {}", dictionaries.path().display()))
        }
    };
    let mut ids = Vec::new();
    for entry in entries {
        let entry = entry.into_diagnostic()?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(id) = name.strip_suffix(".dict").and_then(DictionaryId::from_hex) {
            ids.push(id);
        }
    }
    ids.sort();

    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        let path = dictionaries.file(&id);
        let bytes = fs::metadata(&path).into_diagnostic()?.len();
        let problem = match dictionaries.dictionary(&id) {
            Ok(Some(_)) => None,
            Ok(None) => Some("file disappeared while listing".to_string()),
            Err(e) => Some(e.to_string()),
        };
        out.push(Installed {
            id: id.to_string(),
            bytes,
            path,
            problem,
            manifest: read_manifest(dictionaries, &id)?,
        });
    }
    Ok(out)
}
