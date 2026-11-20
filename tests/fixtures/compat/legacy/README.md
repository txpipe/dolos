# Legacy datasets

Each directory holds `dataset.tar.zst`: the node directory a short preview
sync by a published binary left behind (its `dolos.toml`, the preview genesis
files and `data/`). `tests/compat_legacy.rs` unpacks each one and checks that
the current binary refuses it before any read or write.

| Tag | Commit | Blocks applied | Archive size | SHA-256 of `dataset.tar.zst` |
| --- | --- | --- | --- | --- |
| `v1.6.3` | `f01f9eca6f645661004085a70621b2e88ffb59a5` | 156 (to slot 3100) | 119289 B | `dbfbcfd738da077a33ab866d6003e6612ee1f073386f5c74fc95ff9cf09f7dec` |
| `v1.7.0-alpha.1` | `8faa9476628afc15be638777f3457ef950b1a48b` | 200 (to slot 3980) | 149266 B | `f567f5bdf1cf10101c9a56ef0bbb71113dd2a5d768c6ce5e35f008ff228a4802` |
| `v2.0.0-alpha.0` | `a08c9d130e6b6d4cdc43fe54228ff6dd4e0f88e4` | 200 (to slot 3980) | 148751 B | `d835bb56cb4002ee73291a43be0f21eff9d5e291907b8ed3db8952ad261de8ae` |

The v1 releases keep the WAL and archive index in redb files and the state
and index in fjall databases; `v2.0.0-alpha.0` keeps state and archive in
fjall databases that record no identity. A `v2-alpha.2` store needs no old
binary: the test stamps one with the current fjall API.

## Regenerating a fixture

Run the recipe once per tag, with `$TAG` set to it, from a scratch directory
outside the repository. The same steps hold for every tag.

1. Build the binary from the tag, outside the repository:

   ```sh
   mkdir -p src/$TAG
   git -C <dolos checkout> archive $TAG | tar -x -C src/$TAG
   (cd src/$TAG && cargo build --locked --bin dolos)
   ```

2. Take the tag's own preview example as the node directory, and give it a
   block quota so the sync stops by itself:

   ```sh
   mkdir node
   cp src/$TAG/examples/sync-preview/{dolos.toml,byron.json,shelley.json,alonzo.json,conway.json} node/
   cd node
   sed -i '' 's/^max_rollback = 25920$/max_rollback = 25920\nsync_limit = { MaxBlocks = 150 }/' dolos.toml
   sed -i '' 's/^max_level = "DEBUG"$/max_level = "INFO"/' dolos.toml
   ```

   (`sed -i ''` is the macOS form; GNU sed takes `sed -i`.) Nothing else in
   the example changes: it syncs from the preview relay it names,
   `relay.cnode-m1.demeter.run:3002`.

3. Sync. The command exits on its own once the pull stage has fetched the
   quota; the apply stage stops with it, so the applied count varies from run
   to run (the table records each fixture's, the last `rolling forward` line
   of the log):

   ```sh
   ../src/$TAG/target/debug/dolos sync > ../sync.log 2>&1
   ```

4. Pack the directory. GNU tar's sparse format keeps the fjall journals,
   preallocated at 64 MiB, sparse on unpack:

   ```sh
   gtar --format=gnu --sparse --sort=name --owner=0 --group=0 --numeric-owner \
     -cf - dolos.toml byron.json shelley.json alonzo.json conway.json data \
     | zstd -19 -o <dolos checkout>/tests/fixtures/compat/legacy/$TAG/dataset.tar.zst
   ```

5. Update the table above with the applied count, the size and
   `shasum -a 256 dataset.tar.zst`. Keep each archive under 5 MB.
