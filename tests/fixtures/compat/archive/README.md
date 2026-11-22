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
  code (`.exit`) and, for a command with machine-readable output, its JSON
  (`.json`);
- `FIXTURE.md`, the binary's version, commit and contract, and the tip the
  stores stand at.

## Archiving a fixture

From 2.0.0, at each stable minor release, the release coordinator archives
one fixture from the tagged binary and commits it:

```sh
git checkout v<major>.<minor>.0
cargo xtask compat fixture v<major>.<minor>
```

The command builds the checkout's `dolos` (or takes `--bin <path>`) and
writes `tests/fixtures/compat/archive/v<major>.<minor>/`. Commit the
directory as written, and confirm in the release preflight that every
archived fixture still opens. Never edit an archived fixture: a change that
breaks one breaks that release's users.

Before 2.0.0 this directory holds no fixture: prerelease identifiers promise
nothing to the next prerelease. The harness still writes a fixture with the
current build at test time and checks it the same way.
