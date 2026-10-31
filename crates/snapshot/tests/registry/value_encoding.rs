//! The value encoding, checked on the bytes.
//!
//! PROFILE.md's "Value encoding" section holds every record value to the SPEC
//! encoding profile, with two tags allowed: shortest-form integers and
//! lengths, definite lengths, map keys in bytewise encoded-key order (RFC 8949
//! §4.2.1), no floats, and no tags but 2 (an unsigned bignum above u64, no
//! leading zeros) and 30 (a rational). [`violations`] walks one encoded value
//! and lists every place it breaks a rule; the registry runs it over every
//! golden, so a canary that encodes outside the profile fails by the place it
//! does.

use std::fmt;

/// One break of the value encoding, at a path into the value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// `$` is the value; `[i]` an array element; `{i}` and `{i}:` the key
    /// and the value of a map's `i`-th entry; `#t` the content of tag `t`.
    pub path: String,
    pub rule: Rule,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rule {
    /// An integer, length or tag number not in its shortest form.
    NotShortest,
    /// An indefinite-length string, array or map.
    Indefinite,
    /// A tag other than 2 or 30.
    Tag,
    /// A tag 2 whose content is not a byte string above u64 without leading
    /// zeros.
    Bignum,
    /// A tag 30 whose content is not an array of two unsigned integers.
    Rational,
    /// A map key not after the previous one in bytewise encoded-key order.
    KeyOrder,
    /// A float.
    Float,
    /// A simple value other than false, true and null.
    Simple,
    /// Bytes the walk could not read: cut short, reserved, or trailing.
    Malformed,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} at {}", self.rule, self.path)
    }
}

/// The value encoding's exceptions in the registry's goldens: `(namespace,
/// path, rule)`.
///
/// Exactly one. `pool-epochs` field 2 is `PoolEpochLog.relative_size`, an
/// f64, which
/// [record-residue](../../../../plans/
/// stelae-dolos-profile-release-gate-freeze-hardening-record-residue.md)
/// removes; its entry goes with it.
pub const EXCEPTIONS: &[(&str, &str, Rule)] = &[("pool-epochs", "$[2]", Rule::Float)];

/// Every break of the value encoding in `bytes`, one encoded value.
pub fn violations(bytes: &[u8]) -> Vec<Violation> {
    let mut walk = Walk {
        bytes,
        at: 0,
        found: Vec::new(),
    };

    if walk.item("$".to_string()).is_err() || walk.at != bytes.len() {
        walk.flag("$", Rule::Malformed);
    }

    walk.found
}

struct Walk<'a> {
    bytes: &'a [u8],
    at: usize,
    found: Vec<Violation>,
}

/// The walk cannot continue: the bytes are cut short or reserved.
struct Stop;

impl<'a> Walk<'a> {
    fn flag(&mut self, path: &str, rule: Rule) {
        self.found.push(Violation {
            path: path.to_string(),
            rule,
        });
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], Stop> {
        let end = self.at.checked_add(len).ok_or(Stop)?;
        let slice = self.bytes.get(self.at..end).ok_or(Stop)?;
        self.at = end;
        Ok(slice)
    }

    /// The head of an item: its major type and argument, `None` for an
    /// indefinite length. Flags an argument longer than it needs to be.
    fn head(&mut self, path: &str) -> Result<(u8, Option<u64>), Stop> {
        let initial = self.take(1)?[0];
        let major = initial >> 5;
        let info = initial & 0x1f;

        let (argument, minimum) = match info {
            0..=23 => return Ok((major, Some(info as u64))),
            24 => (self.take(1)?[0] as u64, 24),
            25 => (
                u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as u64,
                0x100,
            ),
            26 => (
                u32::from_be_bytes(self.take(4)?.try_into().unwrap()) as u64,
                0x1_0000,
            ),
            27 => (
                u64::from_be_bytes(self.take(8)?.try_into().unwrap()),
                0x1_0000_0000,
            ),
            31 => return Ok((major, None)),
            _ => return Err(Stop),
        };

        // Major type 7 uses the same info values for simple values and
        // floats, which are no shortest-form arguments.
        if major != 7 && argument < minimum {
            self.flag(path, Rule::NotShortest);
        }

        Ok((major, Some(argument)))
    }

    fn item(&mut self, path: String) -> Result<(), Stop> {
        let start = self.at;
        let (major, argument) = self.head(&path)?;

        match (major, argument) {
            (0 | 1, Some(_)) => Ok(()),
            (0 | 1 | 6, None) => Err(Stop),
            (2 | 3, Some(len)) => self.take(len as usize).map(|_| ()),
            (2 | 3, None) => {
                self.flag(&path, Rule::Indefinite);
                self.chunks(major)
            }
            (4, len) => {
                if len.is_none() {
                    self.flag(&path, Rule::Indefinite);
                }

                let mut index = 0;

                while self.more(len, index)? {
                    self.item(format!("{path}[{index}]"))?;
                    index += 1;
                }

                Ok(())
            }
            (5, len) => {
                if len.is_none() {
                    self.flag(&path, Rule::Indefinite);
                }

                let mut index = 0;
                let mut previous: Option<(usize, usize)> = None;

                while self.more(len, index)? {
                    let key_start = self.at;
                    self.item(format!("{path}{{{index}}}"))?;
                    let key = (key_start, self.at);

                    if let Some((from, to)) = previous {
                        if self.bytes[from..to] >= self.bytes[key.0..key.1] {
                            self.flag(&format!("{path}{{{index}}}"), Rule::KeyOrder);
                        }
                    }

                    previous = Some(key);
                    self.item(format!("{path}{{{index}}}:"))?;
                    index += 1;
                }

                Ok(())
            }
            (6, Some(tag)) => self.tagged(path, tag),
            (7, _) => {
                match self.bytes[start] & 0x1f {
                    20..=22 => (),
                    25..=27 => self.flag(&path, Rule::Float),
                    31 => return Err(Stop),
                    _ => self.flag(&path, Rule::Simple),
                }

                Ok(())
            }
            _ => Err(Stop),
        }
    }

    /// Whether a container holds another item: `len` items when definite,
    /// up to the break byte when not.
    fn more(&mut self, len: Option<u64>, index: u64) -> Result<bool, Stop> {
        match len {
            Some(len) => Ok(index < len),
            None => {
                if *self.bytes.get(self.at).ok_or(Stop)? == 0xff {
                    self.at += 1;
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
        }
    }

    /// The chunks of an indefinite string, up to the break byte.
    fn chunks(&mut self, major: u8) -> Result<(), Stop> {
        loop {
            if *self.bytes.get(self.at).ok_or(Stop)? == 0xff {
                self.at += 1;
                return Ok(());
            }

            let (chunk, len) = self.head("")?;

            match (chunk == major, len) {
                (true, Some(len)) => self.take(len as usize)?,
                _ => return Err(Stop),
            };
        }
    }

    fn tagged(&mut self, path: String, tag: u64) -> Result<(), Stop> {
        let content = format!("{path}#{tag}");

        match tag {
            2 => {
                let start = self.at;
                self.item(content.clone())?;

                if !is_bignum(&self.bytes[start..self.at]) {
                    self.flag(&content, Rule::Bignum);
                }

                Ok(())
            }
            30 => {
                let start = self.at;
                self.item(content.clone())?;
                let item = &self.bytes[start..self.at];

                if !is_rational(item) {
                    self.flag(&content, Rule::Rational);
                }

                Ok(())
            }
            _ => {
                self.flag(&path, Rule::Tag);
                self.item(content)
            }
        }
    }
}

/// A definite byte string of more than 8 bytes, the first not zero: tag 2's
/// content, above u64 and without leading zeros.
fn is_bignum(item: &[u8]) -> bool {
    let mut walk = Walk {
        bytes: item,
        at: 0,
        found: Vec::new(),
    };

    match walk.head("") {
        Ok((2, Some(len))) => len > 8 && item.get(walk.at) != Some(&0),
        _ => false,
    }
}

/// `[uint, uint]`, the shape of tag 30's content.
fn is_rational(item: &[u8]) -> bool {
    let Some((0x82, rest)) = item.split_first() else {
        return false;
    };

    let mut walk = Walk {
        bytes: rest,
        at: 0,
        found: Vec::new(),
    };

    for _ in 0..2 {
        match walk.head("") {
            Ok((0, Some(_))) => (),
            _ => return false,
        }
    }

    walk.at == rest.len()
}
