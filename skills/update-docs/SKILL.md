---
name: update-docs
description: Reconcile the user-facing documentation under docs/content/ with the current source-of-truth in the codebase. Use whenever code changes touched config fields, CLI subcommands/args, MiniBF routes, MiniKupo routes, or any other user-visible behavior whose docs may now be stale.
---

# Update User-Facing Docs

The `docs/content/` tree is hand-written and easy to drift from the code. This skill reconciles it section by section against the actual source of truth. Always finish by re-reading both sides and confirming they agree — do not assume the docs already match.

## Scope

The user-facing docs live in `docs/content/`. Each section has exactly one source of truth in the codebase:

| Doc area                        | Doc file(s)                                  | Source of truth                                                         |
| ------------------------------- | -------------------------------------------- | ----------------------------------------------------------------------- |
| Configuration schema            | `docs/content/configuration/schema.mdx`      | `crates/core/src/config.rs` (`RootConfig` and the per-section structs)  |
| CLI subcommands & operation modes | `docs/content/operations/modes.mdx`        | `src/bin/dolos/main.rs` (`Command` enum) + `src/bin/dolos/<cmd>.rs` Args |
| MiniBF endpoint coverage        | `docs/content/apis/minibf.mdx`               | `crates/minibf/src/lib.rs` (`build_router_with_facade`)                 |
| MiniKupo endpoint coverage      | `docs/content/apis/minikupo.mdx`             | `crates/minikupo/src/lib.rs` (`api_router`)                             |
| Bootstrap, installation, etc.   | `docs/content/bootstrap/*`, `installation/*` | Behavior in `src/bin/dolos/bootstrap/`, `init.rs`, README, etc.         |

The four bullets in the typical request — config schema, CLI, MiniBF list, MiniKupo list — map to the first four rows. Always include them. Add other rows when the change touches them.

## Workflow

For each area you intend to update, do these three things in order:

1. Read the source of truth fully. Do not eyeball it — list out the actual fields/routes/args.
2. Read the current doc fully and build the same list from it.
3. Diff the two lists. For each delta:
   - Item present in code, missing from docs → add it.
   - Item present in docs, missing from code → remove it (it was renamed or deleted).
   - Item present in both with different name/type/default/description → update the doc.

Edit the docs surgically with `Edit`. Do not rewrite a section just to "tidy" it — keep diffs reviewable.

## Section: Configuration schema

Source: `crates/core/src/config.rs`.

The schema doc must reflect:

- Every field of `RootConfig` (top-level TOML sections).
- For each top-level section struct (e.g. `MinibfConfig`, `StorageConfig`, `SyncConfig`, `LoggingConfig`, `TelemetryConfig`, etc.), every public field plus its type and any default returned by an accessor like `fn permissive_cors(&self) -> bool { self.permissive_cors.unwrap_or(true) }`.
- `Option<T>` fields are documented as optional. Fields with `#[serde(default)]` use the `Default` impl.
- Nested structs under `storage.*` (`storage.wal`, `storage.state`, `storage.archive`, `storage.index`, `storage.mempool`) each have their own subsection.
- The example TOML at the top of `schema.mdx` should remain a valid, representative sample — if you add a section, add it here too in alphabetical-ish order matching the rest of the doc.

Common drift patterns to look for:
- A new field added to a config struct but never appearing in the table or bullet list.
- A field renamed in code (e.g. `cache_mb` → `cache`) while the doc still uses the old name.
- A default changed in the accessor (`unwrap_or(...)`) while the doc still cites the old default.
- A `#[serde(rename = "...")]` that means the TOML key differs from the Rust field name.

## Section: CLI subcommands & operation modes

Source: `src/bin/dolos/main.rs` `enum Command` plus the corresponding `mod` (`daemon.rs`, `sync.rs`, `serve.rs`, `data/`, `eval.rs`, `doctor/`, `bootstrap/`, `init.rs`, `minibf.rs`, `minikupo.rs`).

Steps:
1. List every variant of `Command`. Note any `#[cfg(feature = "...")]` gates — `Init`, `Data`, `Bootstrap`, `Minibf`, `Minikupo` are all feature-gated, so the docs should describe them as available "when built with the corresponding feature" if the gate is non-default. (As of this writing, all five features are on by default in `Cargo.toml`.)
2. For each subcommand, open its module's `Args` struct (or sub-`Subcommand` enum, e.g. `data::Args`, `doctor::Args`) and confirm the doc reflects the actual flags/args.
3. Update `docs/content/operations/modes.mdx` to keep the "Summary of Modes" table aligned with what `Command` actually exposes.

Note that `modes.mdx` today only documents `daemon`, `sync`, and `serve`. If a new top-level subcommand appears that an end user needs to know about (not internal helpers), add it. Internal/utility subcommands like `data` and `doctor` are typically documented elsewhere or via `--help`; do not add them to `modes.mdx` unless that's the intent of the change.

## Section: MiniBF endpoints

Source: `crates/minibf/src/lib.rs`, function `build_router_with_facade`. Each `.route("...", get(...))` or `.route("...", post(...))` line is one endpoint.

Steps:
1. Extract the full list of route paths from `build_router_with_facade`. A reliable shell snippet:
   ```bash
   rg -N '\.route\(' crates/minibf/src/lib.rs | sed -E 's/.*route\("([^"]+)".*/\1/'
   ```
2. Compare against the table in `docs/content/apis/minibf.mdx` under "Coverage".
3. Add any missing routes, remove any deleted routes, and keep the table sorted in the same order it currently uses (roughly: root/health/metrics first, then alphabetical-ish by resource: `accounts`, `addresses`, `assets`, `blocks`, `epochs`, `genesis`, `governance`, `metadata`, `network`, `pools`, `scripts`, `tx`, `txs`).
4. Each row's description should be a short, user-meaningful summary — match the style of existing rows; do not paste internal handler names.
5. If the change added a route that requires a new config field on `MinibfConfig`, also update the configuration sections in both `schema.mdx` and the "Configuration" block inside `minibf.mdx`.

## Section: MiniKupo endpoints

Source: `crates/minikupo/src/lib.rs`, function `api_router`. Routes live there; the top-level `build_router_with_facade` merges and `nest("/v1", ...)` so the `/v1/...` mount point is implicit — document the un-versioned path (matching existing style).

Steps:
1. Extract the route list:
   ```bash
   rg -N '\.route\(' crates/minikupo/src/lib.rs | sed -E 's/.*route\("([^"]+)".*/\1/'
   ```
2. Reconcile with the "Coverage" table in `docs/content/apis/minikupo.mdx`.
3. Update the "Not supported" list too if a previously unsupported endpoint was added (move it from the bottom list to the top table) or vice versa.
4. As with MiniBF, propagate any new `MinikupoConfig` fields into `schema.mdx` and the doc's own "Configuration" block.

## Verification

After editing, do all of the following:

1. `git diff docs/content/` — read every change as if you were the reviewer. Confirm there is no fabricated content (route, field, or default that doesn't exist in code).
2. Re-extract the route list from each crate (commands above) and grep for each entry inside the corresponding `.mdx`. Every route in code must have a row.
3. For config: open the relevant struct and confirm every public field is mentioned in `schema.mdx`. Pay special attention to fields with `#[serde(default)]`, `Option<T>`, and any custom `#[serde(rename)]` or `#[serde(skip_serializing_if)]`.
4. If the docs build script exists in the repo (check `docs/package.json` or similar), do not run it unless the user asks — schema/route consistency is the goal here, not rendering.

## What NOT to do

- Don't restructure or rename doc sections that weren't part of the request — drift fixes only.
- Don't invent descriptions. If the source of truth doesn't make the purpose obvious, ask the user or read the handler/struct comments instead of guessing.
- Don't document private fields, internal helpers, or test-only routes.
- Don't add a new top-level doc page for a feature unless explicitly asked; prefer extending the existing schema/api/operations files.
