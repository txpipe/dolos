# Archived compatibility fixtures

Each directory here is a fixture a stable release wrote, named
`v<major>.<minor>`. `tests/compat_fixtures.rs` opens every one with the
current build, and so does CI's `compat-fixtures` job, on every push to
`main` and on every pull request. A fixture holds:

- `dolos.toml`, a configuration that sets every table a default one leaves
  out, and `config.json`, the meaning that release gave it;
- `data.tar.zst`, its `data/` (the stores after importing a short synthetic
  chain), packed because the store journals are preallocated sparse files;
- `cli/`, one golden per row of the supported-surface inventory on
  `docs/content/reference/compatibility.mdx`: the arguments (`.args`), the exit
  code (`.exit`), for a command with machine-readable output its JSON
  (`.json`), and for a row that reads the WAL the command run before it
  (`.before`);
- `FIXTURE.md`, the binary's version, commit and contract, the tip the
  stores stand at, and what the goldens leave out and why: the WAL, the
  mempool and the cache's disk tier are outside the contract, and some
  reports are not deterministic.

The harness gives the stores and each golden their own copy of `data/`, so
no command sees what another wrote.

## Archiving a fixture

From 2.0.0, at each stable minor release, the release coordinator archives
one fixture from the tagged binary and commits it:

```sh
git checkout v<major>.<minor>.0
cargo run --locked -p xtask -- compat fixture v<major>.<minor>
```

Run it through `cargo run` from the tagged checkout, not an installed
`cargo xtask`: the goldens and the configuration are compiled into the
xtask, and an installed one stays at the commit it was built from. The
command refuses to run any other way. It builds the checkout's `dolos` (or
takes `--bin <path>`), writes
`tests/fixtures/compat/archive/v<major>.<minor>/`, and refuses, leaving
nothing behind, when a command exits other than it should or prints no JSON
where it should. Commit the
directory as written, and confirm in the release preflight that every
archived fixture still opens. Never edit an archived fixture: a change that
breaks one breaks that release's users.

Before 2.0.0 this directory holds no fixture: prerelease identifiers promise
nothing to the next prerelease. The harness still writes a fixture with the
current build at test time and checks it the same way.
