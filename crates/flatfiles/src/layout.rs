//! Segment file names, transition records, and the recovery that resolves a
//! segment directory to one authoritative representation per segment.
//!
//! A segment owns up to six files, all named by its six-digit number:
//!
//! ```text
//! 000012.segment          raw representation
//! 000012.zseg             compressed representation
//! 000012.transition       transition record: the word `seal` or `thaw`
//! 000012.segment.tmp      raw output being staged by a thaw
//! 000012.zseg.tmp         compressed output being staged by a seal
//! 000012.transition.tmp   transition record being staged
//! ```
//!
//! At rest a segment has exactly one representation file and nothing else.
//! A transition publishes the other representation and retires the current
//! one through the steps in [`recover`]'s table; the transition record is
//! what tells an interrupted store which of two representation files is the
//! authoritative one, so authority is never inferred from file presence
//! alone. `LIFECYCLE.md` beside this crate walks the same table with the
//! restart authority after every step.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Subdirectory of the segments directory where dictionaries are installed.
pub const DICTIONARIES_DIR: &str = "dictionaries";

const RAW_EXTENSION: &str = "segment";
const COMPRESSED_EXTENSION: &str = "zseg";
const TRANSITION_EXTENSION: &str = "transition";
const STAGING_SUFFIX: &str = ".tmp";

/// How a segment's bytes are stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Representation {
    /// The concatenated block bodies, as appended.
    Raw,
    /// The same logical stream in the [`compressed`](crate::compressed) layout.
    Compressed,
}

impl Representation {
    fn extension(self) -> &'static str {
        match self {
            Representation::Raw => RAW_EXTENSION,
            Representation::Compressed => COMPRESSED_EXTENSION,
        }
    }

    pub fn other(self) -> Self {
        match self {
            Representation::Raw => Representation::Compressed,
            Representation::Compressed => Representation::Raw,
        }
    }
}

impl fmt::Display for Representation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Representation::Raw => "raw",
            Representation::Compressed => "compressed",
        })
    }
}

/// The direction of a representation change, as the transition record spells
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    /// Raw to compressed.
    Seal,
    /// Compressed to raw.
    Thaw,
}

impl Transition {
    pub fn from(self) -> Representation {
        match self {
            Transition::Seal => Representation::Raw,
            Transition::Thaw => Representation::Compressed,
        }
    }

    pub fn to(self) -> Representation {
        self.from().other()
    }

    fn label(self) -> &'static str {
        match self {
            Transition::Seal => "seal",
            Transition::Thaw => "thaw",
        }
    }

    fn parse(content: &[u8]) -> Option<Self> {
        match content.strip_suffix(b"\n").unwrap_or(content) {
            b"seal" => Some(Transition::Seal),
            b"thaw" => Some(Transition::Thaw),
            _ => None,
        }
    }
}

impl fmt::Display for Transition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// What a file name inside the segments directory denotes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentFile {
    Representation(Representation),
    Staging(Representation),
    Transition,
    TransitionStaging,
}

/// Parse a segment file name into the segment it belongs to and its role.
/// Anything else in the directory is not ours and yields `None`.
pub fn parse_filename(name: &str) -> Option<(u32, SegmentFile)> {
    let (id, rest) = name.split_at_checked(6)?;
    let id = id.parse::<u32>().ok()?;
    if !id_is_canonical(id, name) {
        return None;
    }
    let rest = rest.strip_prefix('.')?;
    let (stem, staging) = match rest.strip_suffix(STAGING_SUFFIX) {
        Some(stem) => (stem, true),
        None => (rest, false),
    };
    let file = match (stem, staging) {
        (RAW_EXTENSION, false) => SegmentFile::Representation(Representation::Raw),
        (COMPRESSED_EXTENSION, false) => SegmentFile::Representation(Representation::Compressed),
        (RAW_EXTENSION, true) => SegmentFile::Staging(Representation::Raw),
        (COMPRESSED_EXTENSION, true) => SegmentFile::Staging(Representation::Compressed),
        (TRANSITION_EXTENSION, false) => SegmentFile::Transition,
        (TRANSITION_EXTENSION, true) => SegmentFile::TransitionStaging,
        _ => return None,
    };
    Some((id, file))
}

fn id_is_canonical(id: u32, name: &str) -> bool {
    name.starts_with(&format!("{id:06}"))
}

/// Every path one segment can own.
#[derive(Debug, Clone)]
pub struct SegmentPaths {
    dir: PathBuf,
    id: u32,
}

impl SegmentPaths {
    pub fn new(dir: &Path, id: u32) -> Self {
        Self {
            dir: dir.to_path_buf(),
            id,
        }
    }

    fn named(&self, extension: &str, staging: bool) -> PathBuf {
        let suffix = if staging { STAGING_SUFFIX } else { "" };
        self.dir.join(format!("{:06}.{extension}{suffix}", self.id))
    }

    pub fn representation(&self, representation: Representation) -> PathBuf {
        self.named(representation.extension(), false)
    }

    pub fn staging(&self, representation: Representation) -> PathBuf {
        self.named(representation.extension(), true)
    }

    pub fn transition(&self) -> PathBuf {
        self.named(TRANSITION_EXTENSION, false)
    }

    fn transition_staging(&self) -> PathBuf {
        self.named(TRANSITION_EXTENSION, true)
    }

    /// Look at the directory and report which of this segment's files exist.
    pub fn found(&self) -> io::Result<Found> {
        let exists = |path: PathBuf| match fs::symlink_metadata(&path) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        };
        let transition = match fs::read(self.transition()) {
            Ok(content) => Some(Transition::parse(&content).ok_or_else(|| {
                invalid(format!(
                    "segment {:06}: transition record {} is unreadable",
                    self.id,
                    self.transition().display()
                ))
            })?),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };
        Ok(Found {
            raw: exists(self.representation(Representation::Raw))?,
            compressed: exists(self.representation(Representation::Compressed))?,
            raw_staging: exists(self.staging(Representation::Raw))?,
            compressed_staging: exists(self.staging(Representation::Compressed))?,
            transition_staging: exists(self.transition_staging())?,
            transition,
        })
    }

    /// Remove every file of this segment, whatever state it is in.
    pub fn remove_all(&self) -> io::Result<()> {
        for path in [
            self.staging(Representation::Raw),
            self.staging(Representation::Compressed),
            self.transition_staging(),
            self.representation(Representation::Raw),
            self.representation(Representation::Compressed),
            self.transition(),
        ] {
            remove_if_present(&path)?;
        }
        sync_dir(&self.dir)
    }

    /// Resolve this segment's files to its authoritative representation,
    /// finishing or abandoning an interrupted transition on the way.
    pub fn recover(&self) -> io::Result<Option<Representation>> {
        recover(self, self.found()?)
    }
}

/// Which of a segment's files exist.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Found {
    pub raw: bool,
    pub compressed: bool,
    pub raw_staging: bool,
    pub compressed_staging: bool,
    pub transition_staging: bool,
    pub transition: Option<Transition>,
}

impl Found {
    fn has(&self, representation: Representation) -> bool {
        match representation {
            Representation::Raw => self.raw,
            Representation::Compressed => self.compressed,
        }
    }
}

/// Decide which representation is authoritative and make the directory say
/// so, given what is on disk.
///
/// | record | files                | authority | action                              |
/// |--------|----------------------|-----------|-------------------------------------|
/// | none   | one representation   | that one  | remove staging files                |
/// | none   | none                 | absent    | remove staging files                |
/// | none   | both representations | —         | error: no record to decide by       |
/// | T      | `T.to()` present     | `T.to()`  | finish: unlink `T.from()`, record   |
/// | T      | only `T.from()`      | `T.from()`| abort: unlink staging, record       |
/// | T      | neither              | —         | error: the segment is lost          |
///
/// The destination file exists only after the transition verified it and
/// renamed it into place, so its presence beside a record is the commit
/// point; without the record two representation files mean the directory
/// was edited by something other than this crate, and the store refuses to
/// guess which one the index describes.
pub fn recover(paths: &SegmentPaths, found: Found) -> io::Result<Option<Representation>> {
    let id = paths.id;

    if found.transition_staging {
        remove_if_present(&paths.transition_staging())?;
    }

    let authority = match found.transition {
        None => match (found.raw, found.compressed) {
            (true, true) => {
                return Err(invalid(format!(
                "segment {id:06} has both a raw and a compressed file and no transition record; \
                     remove the one the archive index does not describe"
            )))
            }
            (true, false) => Some(Representation::Raw),
            (false, true) => Some(Representation::Compressed),
            (false, false) => None,
        },
        Some(transition) if found.has(transition.to()) => {
            remove_if_present(&paths.representation(transition.from()))?;
            sync_dir(&paths.dir)?;
            Some(transition.to())
        }
        Some(transition) if found.has(transition.from()) => Some(transition.from()),
        Some(transition) => {
            return Err(invalid(format!(
                "segment {id:06} has a {transition} record but neither representation file"
            )))
        }
    };

    if found.raw_staging {
        remove_if_present(&paths.staging(Representation::Raw))?;
    }
    if found.compressed_staging {
        remove_if_present(&paths.staging(Representation::Compressed))?;
    }
    if found.transition.is_some() {
        remove_if_present(&paths.transition())?;
    }
    if found.transition.is_some()
        || found.raw_staging
        || found.compressed_staging
        || found.transition_staging
    {
        sync_dir(&paths.dir)?;
    }

    Ok(authority)
}

/// A transition in flight: the durable record plus the cleanup its
/// abandonment requires.
///
/// Dropped before [`publish`](Self::publish) it removes the staged output
/// and the record, leaving the source representation as it was. After the
/// publish the destination is authoritative and the guard cleans nothing
/// on drop: what remains is the source and the record, which
/// [`recover`] finishes on the next open if [`finish`](Self::finish) does
/// not get to.
pub struct TransitionGuard<'a> {
    paths: &'a SegmentPaths,
    transition: Transition,
    published: bool,
}

impl<'a> TransitionGuard<'a> {
    /// Write the record durably, refusing if one is already present.
    pub fn begin(paths: &'a SegmentPaths, transition: Transition) -> io::Result<Self> {
        let record = paths.transition();
        if fs::symlink_metadata(&record).is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "segment {:06} already has a transition record at {}",
                    paths.id,
                    record.display()
                ),
            ));
        }
        let staging = paths.transition_staging();
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&staging)?;
        file.write_all(transition.label().as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        fs::rename(&staging, &record)?;
        sync_dir(&paths.dir)?;
        Ok(Self {
            paths,
            transition,
            published: false,
        })
    }

    /// Where the destination is staged.
    pub fn staging(&self) -> PathBuf {
        self.paths.staging(self.transition.to())
    }

    /// Move the verified staged output into place. From here on the
    /// destination is authoritative — the rename is the commit point, so a
    /// failed directory sync after it reports [`published`](Self::published)
    /// all the same and leaves the record for [`recover`] to finish.
    pub fn publish(&mut self) -> io::Result<()> {
        fs::rename(
            self.staging(),
            self.paths.representation(self.transition.to()),
        )?;
        self.published = true;
        sync_dir(&self.paths.dir)
    }

    /// Whether the destination has been renamed into place.
    pub fn published(&self) -> bool {
        self.published
    }

    /// Retire the source representation and the record.
    pub fn finish(self) -> io::Result<()> {
        debug_assert!(self.published);
        remove_if_present(&self.paths.representation(self.transition.from()))?;
        sync_dir(&self.paths.dir)?;
        remove_if_present(&self.paths.transition())?;
        sync_dir(&self.paths.dir)
    }
}

impl Drop for TransitionGuard<'_> {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        let _ = remove_if_present(&self.staging());
        let _ = remove_if_present(&self.paths.transition());
        let _ = sync_dir(&self.paths.dir);
    }
}

pub fn remove_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Make directory entry changes durable. Directories cannot be opened for
/// syncing on Windows, where the rename itself is the durability point.
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    if cfg!(unix) {
        File::open(dir)?.sync_all()
    } else {
        Ok(())
    }
}

fn invalid(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filenames_parse_to_their_role() {
        assert_eq!(
            parse_filename("000012.segment"),
            Some((12, SegmentFile::Representation(Representation::Raw)))
        );
        assert_eq!(
            parse_filename("000012.zseg"),
            Some((12, SegmentFile::Representation(Representation::Compressed)))
        );
        assert_eq!(
            parse_filename("000012.segment.tmp"),
            Some((12, SegmentFile::Staging(Representation::Raw)))
        );
        assert_eq!(
            parse_filename("000012.zseg.tmp"),
            Some((12, SegmentFile::Staging(Representation::Compressed)))
        );
        assert_eq!(
            parse_filename("000012.transition"),
            Some((12, SegmentFile::Transition))
        );
        assert_eq!(
            parse_filename("000012.transition.tmp"),
            Some((12, SegmentFile::TransitionStaging))
        );
        assert_eq!(parse_filename("12.segment"), None);
        assert_eq!(parse_filename("000012.segment.bak"), None);
        assert_eq!(parse_filename("dictionaries"), None);
        assert_eq!(parse_filename("+00012.segment"), None);
    }

    #[test]
    fn transition_records_round_trip() {
        for transition in [Transition::Seal, Transition::Thaw] {
            let text = format!("{transition}\n");
            assert_eq!(Transition::parse(text.as_bytes()), Some(transition));
        }
        assert_eq!(Transition::parse(b"melt"), None);
    }
}
