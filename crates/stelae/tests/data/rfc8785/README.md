# RFC 8785 (JCS) conformance vectors

Vendored, unmodified, from the reference implementation repository that
accompanies RFC 8785:

- Source: <https://github.com/cyberphone/json-canonicalization>, `testdata/input`
  and `testdata/output`.
- Retrieved: 2026-07-31.
- Licence: Apache-2.0 — same licence as this repository.

`input/{name}.json` is the document to canonicalize; `output/{name}.json` is the
expected canonical form, compared **byte for byte** (the outputs carry raw UTF-8
and a raw `0x7f`, so they are read as bytes, never as text).

What each one pins down:

| Vector | What it would catch |
|---|---|
| `arrays` | array order preserved while object keys are sorted |
| `french` | sorting is by code unit, never by locale collation |
| `structures` | nested objects sorted independently; empty objects; `\n` escaping |
| `unicode` | no Unicode normalization — `Å` stays decomposed |
| `values` | ECMAScript number rendering, and the JSON string escape set |
| `weird` | sorting by **UTF-16** code units: `😂` (surrogate pair `D83D DE02`) sorts *before* `דּ`, which bytewise UTF-8 ordering would get backwards |

The suite's third file, `es6testfile100m.txt` (100 million number samples), is
deliberately not vendored. The number-rendering edge cases it covers are checked
instead from the table in RFC 8785 Appendix B, which is reproduced in
`tests/rfc8785.rs`.
