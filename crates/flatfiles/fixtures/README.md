# Compressed segment fixtures

Reference files for `COMPRESSED.md`, read by `tests/compressed.rs`. Both
segments carry the same 139,418-byte logical stream: 96 generated blocks of
300–2,500 bytes, reproduced in the test from a seeded generator, so no raw
copy is committed.

| file | mode | dictionary |
|---|---|---|
| `chunked.zseg` | chunked, 4 KiB target, zstd level 3 | none |
| `per-block-dict.zseg` | per-block, zstd level 3 | `sample.dict` |
| `sample.dict` | 4 KiB zstd dictionary trained on 3,000 generated blocks | — |

The tests parse each file with the crate's reader, walk its frames with
libzstd alone, and decode it with a plain streaming decoder. The `zstd`
command line reads them too:

```sh
zstd -d -c chunked.zseg | shasum -a 256
zstd -d -c -D sample.dict per-block-dict.zseg | shasum -a 256
```

Both print the same digest. Regenerate the files only when the format
changes, with
`cargo test -p dolos-flatfiles --test compressed regenerate -- --ignored`,
and update this table if the parameters move.
