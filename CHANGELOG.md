# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0-rc.1] - 2025-11-06

### 🚀 Features

- *(cli)* Add proposal state dump command
- *(cli)* Add single entitty dump command
- *(cli)* Add update entity command

### 🐛 Bug Fixes

- *(cardano)* Move era transition to estart (#777)
- *(cardano)* Force protocol version value on forks
- *(cardano)* Adjust pot logic to support pre-shelley eras
- *(cardano)* Skip nonce calc before Shelley
- *(cardano)* Take into account Byron tx fees
- *(cardano)* Support era transition hacks
- *(cardano)* Assign rewards using only account state at ewrap
- *(cardano)* Compute eta using mark pparams
- Handle default wal log on emulator pipeline (#780)
- Take into account empty WAL during origin sync
- *(cli)* Remove double async runtime in bootstrap command

### 🚜 Refactor

- *(core)* Remove chain-specific batching from core
- *(cardano)* Use genesis data for protocol constants instead of epoch state
- *(cardano)* Move params migration to inside delta on estart
- *(cardano)* Simplify genesis pot logic
- *(cardano)* Treat pre-conway protocol updates as proposals
- *(cardano)* Materialize proposal lifecycle as state
- *(core)* Make chain logic safe for async runtimes

### ⚙️ Miscellaneous Tasks

- *(cardano)* Remove excessive logs
- Fix lint warnings
- Bump Pallas to latest main branch (#779)

## [1.0.0-beta.8] - 2025-10-27

### 🐛 Bug Fixes

- *(cardano)* Use correct timing for pool params activation (#764)
- *(cardano)* Skip retired pools from active stake snapshot (#765)
- *(cardano)* Treat pool retire epoch as inclusive
- *(cardano)* Adjust multiple reward-related issues (#766)
- *(cardano)* Fix multiple accounting details (#767)
- *(cardano)* Make sure all staking rules are followed (#771)
- *(cardano)* Take into account preprod pointer addresses
- *(minibf)* Fix `/network` endpoint mappings (#773)

### ⚙️ Miscellaneous Tasks

- *(cardano)* Avoid panic on undo logic (#772)

### Release

- V1.0.0-beta.8

## [1.0.0-beta.7] - 2025-10-17

### 🐛 Bug Fixes

- *(cardano)* Treat pool params as an epoch-dependant value (#759)
- *(cardano)* Move reward apply after epoch snapshot (#761)
- *(minibf)* Use inaccurate but compatible pool fees value (#762)
- *(cardano)* Allow delegation to unregistered dreps (#763)
- *(cardano)* Fix incentives calc on epoch 0

### Release

- V1.0.0-beta.7

## [1.0.0-beta.6] - 2025-10-16

### 🚀 Features

- *(cardano)* Implement pixel-perfect reward calc (#757)

### 🐛 Bug Fixes

- *(cardano)* Don't forget about Byron pparams updates (#752)
- *(minibf)* Offset constructor index on datums (#755)
- *(cardano)* Make reward data optional on ewrap

### ⚙️ Miscellaneous Tasks

- Fix broken tests and lint warning (#753)
- Disable publish for test-vectors crate
- Fix failing tests and lint warnings (#754)
- Formalize v2 as new internal storage version (#758)
- *(cli)* Make storage v2 the default during init

### Release

- V1.0.0-beta.6

## [1.0.0-beta.5] - 2025-10-06

### 🚀 Features

- *(minibf)* Implement `/epochs/{epoch}/blocks` endpoint (#731)
- *(minibf)* Implement `scripts/datum/{datum_hash}` endpoint (#734)
- *(minibf)* Implement more `/assets/{asset}/*` endpoints (#735)
- *(cardano)* Track detailed pool logs and expose via minibf  (#736)
- *(archive)* Add index for tx metadata (#741)

### 🐛 Bug Fixes

- *(minibf)* Handle cost models updates from proposals (#737)
- *(minibf)* Fetch correct epoch for params endpoints (#738)
- *(cardano)* Avoid using push for epoch value
- *(cardano)* Reset epoch minted blocks on boundary
- *(cardano)* Improve reward logic accuracy (#745)
- *(cardano)* Treat current pparams as effective (#747)
- *(cardano)* Interpret Byron epoch length correctly (#748)
- *(cardano)* Consider all Conway certs (#742)
- *(cardano)* Be explicit about hardfork versions (#749)
- *(minibf)* Avoid rounding errors on fee calc (#750)
- *(minibf)* Skip pool history with empty stake (#751)

### 🚜 Refactor

- *(cardano)* Tidy up reward calculation logic (#740)
- *(cardano)* Move reward calc into its own module (#746)

### ⚙️ Miscellaneous Tasks

- Fix lint warnings across the board (#732)
- Fix build errors on support crates

### Release

- V1.0.0-beta.5

## [1.0.0-beta.4] - 2025-09-29

### 🚀 Features

- *(cardano)* Track governance artifacts (#714)
- *(cli)* Introduce dump-logs data command

### 🐛 Bug Fixes

- *(cardano)* Nonce after Conway uses different eta_h (#712)
- *(minibf)* Redeemers fee calculation (#716)
- *(minibf)* Add missing mapping of redeemer datum hash (#718)
- *(cardano)* Add missing proposal entity sweep (#722)
- *(cardano)* Stop epoch before boundary work (#726)
- *(cardano)* Use big rationals for reward calc (#727)
- *(cardano)* Handle bootstrap for all networks (#728)
- *(cardano)* Handle pledge mismatch during pool reward calc (#729)
- *(minibf)* Adjust drep active model mapping (#730)

### 🚜 Refactor

- *(archive)* Support time-series logs of arbitrary shape (#713)
- *(cardano)* Use deltas for the sweep process (#717)
- *(cardano)* Interleave logs in boundary visitors (#719)
- *(cardano)* Track total vs epoch minted blocks (#720)
- *(cardano)* Log ending epoch on sweep (#721)
- *(cardano)* Track deposits explicitly in each entity (#723)
- *(core)* Move sweep / genesis ops inside chain logic (#724)
- *(cardano)* Unify track & log config sections (#725)

### Release

- V1.0.0-beta.4

## [1.0.0-beta.3] - 2025-09-22

### 🐛 Bug Fixes

- *(minibf)* Handle block 0 edge case for preview network (#701)
- *(minibf)* Save pool register slot for later sorting (#702)
- *(cardano)* Handle drep activity during roll (#703)
- *(cardano)* Clear delegators on retiring pools (#705)
- *(cardano)* Clear delegators on retiring drep (#707)
- *(cardano)* Make sure to evolve pparams during forks (#708)
- *(minibf)* Handle drep_id and include nonce in parameters (#706)
- *(cardano)* Fix Byron check logic for initial epochs
- *(cardano)* Use epoch+1 for era end boundary
- *(minibf)* Avoid edge case of pool bech32 passing as hex
- *(minibf)* Load pparams before reading redeemers

### 🚜 Refactor

- *(cardano)* Handle drep and pool retirement during roll (#704)
- *(cardano)* Deprecate redundant drep id state value (#709)
- *(minibf)* Implement same era hardcoding as BF (#700)

### ⚙️ Miscellaneous Tasks

- Improve trace level for verbose logs
- Fix lint warnings across the board

### Release

- V1.0.0-beta.3

## [1.0.0-beta.2] - 2025-09-19

### 🚀 Features

- Track account certs as a slot tag (#697)

### 🐛 Bug Fixes

- *(archive)* Include missing full-address filter insert
- *(cardano)* Make epoch boundary work for Byron (#698)

### 🚜 Refactor

- *(cardano)* Interleave member rewards calc during pool sweep step (#694)
- Use xxh3 hash algorithm for entity keys (#695)

### ⚙️ Miscellaneous Tasks

- Update Redb to v3 (#696)

### Release

- V1.0.0-beta.2

## [1.0.0-beta.1] - 2025-09-17

### 🚀 Features

- *(cardano)* Track all state by default (#691)
- Track slot inputs tags (#692)

### 🐛 Bug Fixes

- *(cardano)* Contemplate negative utxo delta on epoch state roll (#687)
- *(cardano)* Track block number during roll visitor
- *(cardano)* Use fingerprint for asset keys (#693)

### 🚜 Refactor

- *(cardano)* Unpack address tags during roll (#679)
- *(cardano)* Unpack asset tags inside a roll visitor (#680)
- *(cardano)* Track datums archive tags in roll visitor (#681)
- *(archive)* Use writer semantic for batched updates (#683)
- *(core)* Switch to state-undo rollback mechanism (#684)
- *(cardano)* Compute nonce as part of the sweep process (#688)
- *(core)* Use WAL to store consumed inputs (#690)

### 🧪 Testing

- Add drep test vectors import (#678)
- Add era summaries to test vectors (#682)
- Fix test after epoch state changes

### ⚙️ Miscellaneous Tasks

- Fix lint warnings across the board (#689)

### Release

- V1.0.0-beta.1

## [1.0.0-beta.0] - 2025-09-15

### 🚀 Features

- *(bin)* Add helper command to query state and spdd
- *(cardano)* Support forced stop epoch via config (#673)
- *(cli)* Add clear-state data command (#677)
- *(minibf)* Use active pool for delegation filtering

### 🐛 Bug Fixes

- *(cardano)* Use stake address for pool delegator visitor
- *(minibf)* Adjust query params to new entity key size
- *(minibf)* Fix logic for account active flag
- *(cardano)* Contemplate edge as first era
- *(minibf)* Use correct header for drep ids
- *(cardano)* Fix bad filtering of stake certs
- *(minibf)* Load pparams when doing deposit calc
- *(core)* Parse origin chain point bytes correctly
- *(cli)* Always fast-forward wal if behind
- *(minibf)* Extend max scan window for metadata
- *(minibf)* Fix mappings of stake values
- *(cli)* Use active pool for spdd command

### 🚜 Refactor

- *(cardano)* Support visitor pattern for inputs
- *(cardano)* Improve seen addresses visitor style
- *(cardano)* Use rayon for input decoding
- *(cardano)* Handle epoch boundary as a block visitor
- *(cardano)* Move visitors to their own module
- Overhaul state using custom delta types
- Turn WAL into a sequential delta store
- Move tip broadcast to the domain
- *(cardano)* Include withdrawals and vote delegation in roll logic
- *(cardano)* Move pparams to state storage
- *(cardano)* Compute pparams on roll (#668)
- *(cardano)* Revisit epoch rotation approach (#669)
- *(core)* Merge utxoset and state storage (#670)
- *(cardano)* Avoid excessive data in account entity (#672)
- *(cardano)* Group all boundary work in a single struct (#674)
- *(cardano)* Support block visitors with state (#675)
- *(core)* Introduce state writer approach (#676)

### 🧪 Testing

- Introduce test-vectors crate (#666)

### ⚙️ Miscellaneous Tasks

- Fix post-rebase compiler errors
- *(minibf)* Add root and health endpoints (#663)
- Remove excessive dbg! statements

### Release

- V1.0.0-beta.0

## [0.32.0] - 2025-09-03

### 🚀 Features

- *(u5c)* Implement readtx call and update specs (#657)

### 🐛 Bug Fixes

- *(cardano)* Track minted blocks per pool during roll (#661)
- *(minibf)* Compute deposits correctly on tx mappings (#659)
- *(minibf)* Improve epoch params mappings (#660)
- *(cardano)* Compute drep state values during roll (#658)

### Release

- V0.32.0

## [0.31.1] - 2025-08-25

### 🐛 Bug Fixes

- *(bin)* Fix bad logic in Mithril bootstrap start

### Release

- V0.31.1

## [0.31.0] - 2025-08-25

### 🐛 Bug Fixes

- *(minifb)* Improve conformance on `/addresses` endpoints (#644)
- *(minibf)* Use correct mappings for assets metadata CIP68 and CIP25 (#649)
- Fix windows compile-time errors (#656)
- Calculate correct tx size fixed in Pallas (#655)

### 🚜 Refactor

- Go through the domain for rebuilding stores (#645)
- *(core)* Split slice loading from delta in state3 (#653)
- *(bin)* Improve Mithril bootstrap procedure (#654)

### 🔧 Continuous Integration

- Bring back windows to the test matrix

### 🧪 Testing

- *(minibf)* Fix conformance tests for metadata endpoints (#651)

### ⚙️ Miscellaneous Tasks

- Update tx3 to v0.11.3
- Bump tx3 and pallas versions
- Fix new lint warnings
- Remove phase2 flag (#650)

### Release

- V0.31.0

## [0.30.2] - 2025-08-14

### 🚀 Features

- *(trp)* Parametrize extra_fees via config value (#647)

### 🐛 Bug Fixes

- *(minibf)* Improve conformance tests coverage (#641)
- Support asset quantities larger than u64 (#643)
- Use latest Pallas with better validation

### 🧪 Testing

- Fix outdated test

### Release

- V0.30.2

## [0.30.1] - 2025-08-11

### 🐛 Bug Fixes

- *(minibf)* Improve conformance tests coverage (#635)
- *(minibf)* Improve conformance test results (#637)

### 🔧 Continuous Integration

- Reinstate windows builds (#639)
- Handle windows phase2 as a conditional compile (#640)

### Release

- V0.30.1

## [0.30.0] - 2025-08-04

### 🚀 Features

- *(trp)* Improve error data (#625)
- Make rich state configurable (#633)

### 🚜 Refactor

- *(trp)* Move error code to new module
- Support having chain config values (#632)

### 📚 Documentation

- Include changes regarding minibf endpoints (#634)

### Release

- V0.30.0

## [0.29.1] - 2025-07-31

### 🐛 Bug Fixes

- *(minibf)* Improve conformance test coverage (#629)

### Release

- V0.29.1

## [0.29.0] - 2025-07-31

### 🚀 Features

- *(minibf)* Implement `txs/{hash}/stakes` endpoint (#605)
- *(minibf)* Implement `accounts/{stake}` endpoint (#608)
- *(minibf)* Implement `accounts/{stake}/addresses` endpoint (#610)
- *(minibf)* Implement `metadata/txs/{label}` endpoints (#611)
- *(minibf)* Implement `/assets/{asset}` endpoint (#612)
- *(minibf)* Implement `/pools/extended` endpoint (#613)
- *(minibf)* Implement `/pool/{id}/delegators` endpoint (#614)
- *(minibf)* Implement `/network/eras` endpoint (#615)
- *(minibf)* Implement `/network` endpoint (#617)
- *(minibf)* Implement `accounts/{stake_address}/delegations` endpoint (#618)
- *(minibf)* Implement `/accounts/{stake_address}/registrations` endpoint (#619)
- *(minibf)* Implement `txs/{hash}/redeemers` endpoint (#556)
- *(minibf)* Implement `accounts/{stake_address}/rewards` endpoint (#628)
- *(minibf)* Implement `governance/dreps/{drepid}` endpoint (#626)

### 🐛 Bug Fixes

- *(minibf)* Improve endpoint conformance (#627)

### 🚜 Refactor

- Introduce chain-agnostic state interface (#607)

### 🧪 Testing

- Fix fake block hash mismatch

### ⚙️ Miscellaneous Tasks

- Fix lint warnings (#609)
- *(minibf)* Use correct json & range  mappings (#620)

### Release

- V0.29.0

## [0.28.0] - 2025-07-25

### 🚀 Features

- *(minibf)* Implement `tx/{hash}/mirs` endpoint (#602)
- *(minibf)* Implement `txs/{hash}/pool_retires` endpoint (#603)
- *(minibf)* Implement `txs/{hash}/pool_updates` endpoint (#604)

### 🐛 Bug Fixes

- Fallback to archive if WAL intersect fails (#606)

### 🚜 Refactor

- Use tx3 built-in resolver (#601)

### Release

- V0.28.0

## [0.27.1] - 2025-07-16

### 🐛 Bug Fixes

- Handle phase-2 eval errors (#598)

### ⚙️ Miscellaneous Tasks

- Fix cargo release config
- Unify cargo metadata across all crates

### Release

- V0.27.1

## [0.27.0] - 2025-07-14

### 🚀 Features

- *(minibf)* Implement `address/{address}/transactions` endpoint (#593)

### 🐛 Bug Fixes

- *(u5c)* Use chain storage for DumpHistory requests (#568)
- *(cardano)* Use uplc fork that support lists (#596)

### 🧪 Testing

- *(cardano)* Add pparams timeline data

### ⚙️ Miscellaneous Tasks

- Skip required code when no default features available (#595)

### Release

- V{{version}}

## [0.26.1] - 2025-07-11

### 🚀 Features

- *(trp)* Implement submit endpoint (#592)

### 🐛 Bug Fixes

- *(minibf)* Match cost model keys with official API (#589)

### 🚜 Refactor

- *(trp)* Parse json args using tx3 sdk (#588)

### Release

- V0.26.1

## [0.26.0] - 2025-07-07

### 🚀 Features

- Move TRP into crate and add OTEL (#572)
- *(minibf)* Implement `epoch/latest/parameters` endpoint (#584)
- *(minibf)* Implement `epoch/{number}/parameters` endpoint (#585)
- *(minibf)* Implement `genesis` endpoint (#586)

### 🚜 Refactor

- Decouple u5c trait from state store (#577)
- Move genesis include files to cardano crate (#581)

### 🧪 Testing

- *(trp)* Cover input resolver logic (#578)
- Introduce toy domain artifact (#580)
- Improve toy domain features (#583)

### ⚙️ Miscellaneous Tasks

- Fix new lint warnings (#579)
- *(trp)* Update tx3-lang to v0.7.2 (#587)

### Release

- V0.26.0

## [0.25.0] - 2025-07-03

### 🚀 Features

- *(minibf)* Implement `txs/{hash}/meatadata` endpoint (#554)
- *(minibf)* Implement `txs/{hash}/metadata/cbor` endpoint (#555)
- *(minibf)* Implement `txs/{hash}/withdrawals` endpoint (#557)
- Add local storage stats command (#558)
- Add housekeeping command (#561)
- Add copy-ledger command (#560)
- *(redb)* Support iterating multi-value keys (#565)
- *(minibf)* Implement `txs/{hash}/delegations` endpoint (#571)

### 🐛 Bug Fixes

- *(core)* Use correct default value for max ledger history (#559)
- *(minibf)* Use correct status code in `/blocks` endpoint (#563)
- *(minibf)* Parse hash or number param corectly (#567)

### 🚜 Refactor

- Move testing artifacts to new crate (#564)
- *(minibf)* Use open-api specs for `blocks/` endpoints (#570)

### ⚙️ Miscellaneous Tasks

- Update tx3 to v0.7.0 (#575)
- Release v0.25.0

## [0.24.0] - 2025-06-19

### 🚀 Features

- *(trp)* Improve argument decoding (#512)
- *(trp)* Make arg decoding more  flexible (#519)
- Support sync limit via config (#526)
- Add reset-genesis command (#532)
- Improve data summary command (#536)
- *(minibf)* Implement `txs/{hash}` endpoint (#537)
- *(minibf)* Implement `txs/{hash}/utxos` endpoint (#538)
- *(redb)* Use quick repair on all write transactions (#551)

### 🐛 Bug Fixes

- Improve shutdown of stuck pipelines (#531)
- *(minibf)* Implement cert count in tx model (#539)
- *(minibf)* Include pagination in addresses utxos endpoint (#540)
- *(minibf)* Handle trailing backlash (#541)
- *(minibf)* Use full name for asset unit in utxos by address (#542)
- *(minibf)* Sort utxos by slot in `addresses/{address}/utxos` endpoint (#543)
- *(minibf)* Include block data in `/addresses` endpoints (#547)
- *(minibf)* Include tx index in utxo ordering (#548)
- *(minibf)* Show disaggregated output amounts  (#549)
- *(redb)* Skip index for addresses with empty delegation (#552)

### 🚜 Refactor

- Split Cardano ledger into new crate (#514)
- Split minibf into new crate (#515)
- Abstract core domain as trait (#521)
- Introduce driver abstraction (#528)

### 📚 Documentation

- Replace old docs with new structure (#513)

### 🔧 Continuous Integration

- Improve PR workflow speed (#525)
- Run tests for the whole workspace (#553)

### 🧪 Testing

- Introduce xtask for smoke tests (#524)
- Add daemon sync smoke test (#527)
- *(redb)* Add state store tests (#550)

### ⚙️ Miscellaneous Tasks

- Update tx3 lib to v0.6.0 (#530)
- Avoid commit of genesis files in test scenarios (#533)
- Release v0.24.0

## [0.23.0] - 2025-05-23

### 🐛 Bug Fixes

- Graceful shutdown when GRPC streams are being served (#499)
- *(minibf)* Handle Byron addresses correctly (#498)
- Load Plutus v2 cost models if available in config (#505)
- *(minibf)* Make cost model names more flexible (#506)
- Apply latest changes to Mithril flow (#510)

### ⚙️ Miscellaneous Tasks

- *(trp)* Decouple tx3 pallas version from the rest (#503)
- Fix lint warnings (#507)
- Release dolos version 0.23.0

## [0.22.0] - 2025-05-12

### 🚀 Features

- *(trp)* Enforce valid TIR version (#497)

### 🐛 Bug Fixes

- *(trp)* Resolve reference inputs (#487)
- *(minibf)* Use new syntax for capture groups (#492)
- *(trp)* Fix input selection algorithm (#494)
- *(trp)* Make cost models index match language view (#495)
- *(trp)* Implement mapping of datum to IR (#496)

### ⚙️ Miscellaneous Tasks

- Update Pallas to v1.0.0-alpha.2 (#491)
- Release dolos version 0.22.0

## [0.21.1] - 2025-04-22

### 🐛 Bug Fixes

- Use Conway structs for genesis UTxOs (#480)
- *(trp)* Use input query criteria correctly (#481)

### 🔧 Continuous Integration

- Update cargo dist version

### ⚙️ Miscellaneous Tasks

- Release dolos version 0.21.1

## [0.21.0] - 2025-04-21

### 🚀 Features

- *(serve)* Add TRP server (#462)
- Introduce support for ephemeral devents (#475)

### 🐛 Bug Fixes

- *(minibf)* Add missing fields on protocol parameters (#474)

### 📚 Documentation

- Add MiniBF api section (#473)
- Propose ADR for light consensus (#477)

### 🔧 Continuous Integration

- Trigger docker on `demeter` branch push (#470)
- Use github ARM runner (#471)

### ⚙️ Miscellaneous Tasks

- Update docs to match v0.20 (#468)
- *(serve)* Split services into compile-time features (#479)
- Release dolos version 0.21.0

## [0.20.0] - 2025-04-01

### 🚀 Features

- Implement mini-blockfrost emulator (#447)
- Implement new chain store (#451)
- *(minibf)* Implement `/txs/<hash>/cbor` endpoint (#465)

### 🐛 Bug Fixes

- Remove deprecated GenesisValues (#454)
- Use look-ahead WAL reader for the ledger rebuild procedure (#453)
- Prompt for storage upgrade on init (#464)

### 📚 Documentation

- Propose ADR for P2P features (#460)

### ⚙️ Miscellaneous Tasks

- Fix lint warnings (#459)
- Update Pallas to latest main (#463)
- Add minibf prompt to init (#466)
- Refresh example configs (#467)
- Release dolos version 0.20.0

## [0.19.1] - 2025-01-25

### 🐛 Bug Fixes

- Avoid glibc issues by fixing runner os (#442)

### 🔧 Continuous Integration

- Use ubuntu-22.04 tag for runners (#443)

### ⚙️ Miscellaneous Tasks

- Release dolos version 0.19.1

## [0.19.0] - 2025-01-24

### 🚀 Features

- *(grpc)* Apply predicate on WatchTx (#384)
- *(serve)* Implement u5c chain-sync read-tip method (#391)
- *(grpc)* Honor intersect in watch module (#408)
- Introduce chain summary struct (#410)
- Support forcing protocol version at epoch 0 (#417)
- Implement phase-2 validation / evaluation (#421)
- Enable npm installer (#425)
- Call Pallas phase-1 validation during submit (#426)
- Support shelley genesis utxos (#437)
- Use hacks to keep gov action pparams updated (#436)
- Allow configurable ledger prune height (#440)

### 🐛 Bug Fixes

- Add detail to IO error (#392)
- Relax mutability requirements on key structs (#414)
- Apply ledger genesis when WAL is at origin (#415)
- *(serve)* Allow pparams query on origin ledger (#416)
- Adjust mithril-client features for windows target (#418)
- Hardcode ada_per_utxo_byte transformation from words to bytes (#423)

### 🚜 Refactor

- Revisit pparams folding approach (#430)

### 📚 Documentation

- Add missing conway_path in config section (#393)
- Add custom network example (#419)

### 🔧 Continuous Integration

- Disable windows builds until upstream issues are resolved (#441)
- Build docker image on push to main branch  (#428)

### 🧪 Testing

- Fix genesis utxo tests (#439)

### ⚙️ Miscellaneous Tasks

- Fix lint issues (#388)
- Fix lint warnings from latest clippy (#390)
- Add conway genesis files to examples (#380)
- Fix lint warnings (#438)
- Release dolos version 0.19.0

## [0.18.2] - 2024-11-02

### 🐛 Bug Fixes

- Use s3 accelerate endpoint for snapshots (#377)

### ⚙️ Miscellaneous Tasks

- Release dolos version 0.18.2

## [0.18.1] - 2024-10-25

### 🐛 Bug Fixes

- Adjust ignored failing test after refactor (#372)
- Include Conway genesis in Docker image (#373)

### ⚙️ Miscellaneous Tasks

- Fix lint warnings (#374)
- Release dolos version 0.18.1

## [0.18.0] - 2024-10-24

### 🚀 Features

- Add flag to quit when reaching tip (#369)

### 🐛 Bug Fixes

- Fix openssl dependency regression (#368)
- Avoid double open of wal during bootstrap (#371)

### ⚙️ Miscellaneous Tasks

- Release dolos version 0.18.0

## [0.17.0] - 2024-10-17

### 🚀 Features

- Skip redundant bootstrap in overriding init (#364)

### 🐛 Bug Fixes

- Apply missing pparams in update logic (#361)

### ⚙️ Miscellaneous Tasks

- Release dolos version 0.17.0

## [0.16.0] - 2024-10-13

### 🚀 Features

- Introduce direct snapshot bootstrap (#336)
- Automate WAL housekeeping procedure (#347)
- Allow manual wal pruning via CLI (#349)
- Ask for history pruning during init (#351)
- Improve bootstrap experience (#353)
- Trigger bootstrap right after init (#354)

### 🐛 Bug Fixes

- Fix typo in bootstrap question (#271)
- Fix build / lint issues (#346)
- Make CORS config optional (#348)
- Avoid crashing on WAL pruning edge case (#350)
- Use saturating substract in housekeeping logic (#355)

### 📚 Documentation

- Add reference to wal history setting (#352)

### ⚙️ Miscellaneous Tasks

- Update blaze docs with latest version (#338)
- Add support for CORS in gRPC-web (#339)
- Fix lint warnings (#356)
- Release dolos version 0.16.0

## [0.15.1] - 2024-09-07

### 🐛 Bug Fixes

- Use u5c mapping that supports Conway (#333)
- Use CF backbone as default relay (#335)

### 📚 Documentation

- Add Blaze example (#334)

### ⚙️ Miscellaneous Tasks

- Release dolos version 0.15.1

## [0.15.0] - 2024-09-06

### 🐛 Bug Fixes

- Provide initial mempool tx stage when asked (#330)

### 🚜 Refactor

- Move pparams mapping to Pallas (#324)
- Simplify tx submit pipeline (#327)

### ⚙️ Miscellaneous Tasks

- *(serve)* Implement ReadParams method in u5c server (#304)
- Update Pallas to v0.30.1 (#328)
- Release dolos version 0.15.0

## [0.14.1] - 2024-08-03

### 🐛 Bug Fixes

- Don't panic on Conway certs (#319)

### ⚙️ Miscellaneous Tasks

- Release dolos version 0.14.1

## [0.14.0] - 2024-08-02

### 🚀 Features

- Add find WAL sequence utility (#317)

### 🐛 Bug Fixes

- *(serve)* Use correct function to filter utxos by asset (#313)
- *(serve)* Skip the first block in the follow-tip grpc API (#298)
- *(serve)* Send reset event on follow-tip intersect (#318)

### ⚙️ Miscellaneous Tasks

- Upgrade Pallas to use u5c v0.8 (#315)
- Release dolos version 0.14.0

## [0.13.1] - 2024-07-19

### 🐛 Bug Fixes

- *(bootstrap)* Save ledger to disk before attempting upgrade (#310)

### 🚜 Refactor

- *(state)* Treat address parsing as a fallible operation (#309)

### ⚙️ Miscellaneous Tasks

- Use Pallas edge with long address fix (#311)
- Release dolos version 0.13.1

## [0.13.0] - 2024-07-18

### 🚀 Features

- Bootstrap in-memory and with defered indexes (#308)

### 🐛 Bug Fixes

- *(state)* Use cursor value to decide if db is empty (#302)

### ⚙️ Miscellaneous Tasks

- Release dolos version 0.13.0

## [0.12.0] - 2024-07-16

### 🚀 Features

- *(serve)* Include raw-bytes in follow-tip method (#299)
- Implement state storage v2 (#295)

### ⚙️ Miscellaneous Tasks

- Update Pallas with u5c spec v0.7.0 (#300)
- Update Pallas to v0.29 (#301)
- Release dolos version 0.12.0

## [0.11.1] - 2024-07-14

### 🐛 Bug Fixes

- *(serve)* Don't break socket listening loop on connect error (#297)

### ⚙️ Miscellaneous Tasks

- Update Pallas with input ordering interop fix (#296)
- Release dolos version 0.11.1

## [0.11.0] - 2024-07-13

### 🚜 Refactor

- Split ledger into state and transition (#288)
- Support side-by-side state schemas (#290)

### ⚙️ Miscellaneous Tasks

- Update u5c spec to v0.6 (#289)
- Update Pallas with redeemer interop fix (#291)
- Release dolos version 0.11.0

## [0.10.1] - 2024-07-05

### 🐛 Bug Fixes

- Change Mithril dep to use rustls (#284)

### 📚 Documentation

- Simplify readme (#281)

### ⚙️ Miscellaneous Tasks

- Release dolos version 0.10.1

## [0.10.0] - 2024-06-24

### 🚀 Features

- Implement input resolver for gRPC endpoint (#149)
- Implement utxorpc watch module (#270)
- Integrate tokio traces & debug console (#275)
- *(storage)* Allow configurable cache size (#277)

### 🐛 Bug Fixes

- Handle SIGTERM for graceful shutdown (#273)
- *(grpc)* Avoid panic in hash value parsing (#274)

### 🚜 Refactor

- *(grpc)* Improve sync mapping (#276)

### 📚 Documentation

- Improve configuration docs (#260)
- Add bootstrap instructions (#261)
- Add quickstart guide (#262)
- Improve running instructions (#263)
- Improve API documentation (#264)
- Document latest config changes (#279)

### 🔧 Continuous Integration

- Remove mdbook workflow (#203)

### 🧪 Testing

- Implement pparams testing (#217)

### ⚙️ Miscellaneous Tasks

- Update Pallas to v0.27 (#265)
- Reset example configs (#269)
- Release dolos version 0.10.0

## [0.9.0] - 2024-05-28

### 🚀 Features

- Improve cli entry-point ux (#232)
- Implement ledger compact logic (#235)
- Improve bootstrap procedure (#237)
- Add wal integrity doctor utility (#240)
- Add util to output wal data (#242)
- Add utility to trim wal db (#244)
- Implement n2c chainsync (#248)
- Introduce config init command (#249)
- Add next step msg on init finish (#251)

### 🐛 Bug Fixes

- Fix bad equality op messing up rollbacks (#243)
- Implement missing chainsync logic (#245)
- Ensure graceful shutdown (#250)
- Use filepath to check empty stores (#252)

### 🚜 Refactor

- Use unapplied deltas in ledger slice (#234)
- Remove chain db in favor of wal (#238)
- Ensure wal is initialized on every open (#239)

### 📚 Documentation

- Improve installation instructions (#233)

### 🔧 Continuous Integration

- Enable linux aarch64 builds (#231)
- Restore docker builds (#255)
- Fix docker arm build (#256)

### 🧪 Testing

- Add WAL tests (#241)

### ⚙️ Miscellaneous Tasks

- Fix lint warnings (#246)
- Tidy up info-level tracing (#247)
- Migrate to git-cliff for changelog updates (#253)
- Remove legacy ci files (#254)
- Exclude heavy features from Pallas (#257)
- Move release config to cargo toml (#258)
- Update example config files (#259)
- Release dolos version 0.9.0

## [0.8.0] - 2024-05-18

### 🚀 Features

- Create bootstrap download dir if it doesn't exist (#211)
- Improve bootstrap command (#222)
- Implement chainsync ouroboros server (#88)
- Add ledger repair warning (#227)
- Bring back `serve` command (#229)

### 🐛 Bug Fixes

- Fix example configs after refactor (#209)

### 🔧 Continuous Integration

- Add alonzo genesis files to docker (#224)
- Migrate to cargo-dist (#225)
- Fix arm release builds

### ⚙️ Miscellaneous Tasks

- Scaffold stateful u5c interop (#226)
- Update gasket to v0.8 (#228)
- Apply lint warnings (#230)
- Release dolos version 0.8.0

## [0.7.0] - 2024-04-20

### 🚀 Features

- Introduce query db (#192)
- Introduce `doctor` command (#204)
- Implement basic u5c query module (#205)

### 🐛 Bug Fixes

- *(applydb)* Revert wrong invalid tx filter (#195)

### 🚜 Refactor

- Merge submit endpoint into general gRPC server (#196)
- Use Redb for ledger data (#199)
- Improve config field naming (#206)

### 📚 Documentation

- Migrate to Nextra (#201)
- Update docs with latest config changes (#207)

### 🔧 Continuous Integration

- Update go to 1.21 (#198)

### ⚙️ Miscellaneous Tasks

- Remove hardcoded protocol params (#191)
- Fix lint warnings for v0.7 (#208)

### Release

- V0.7.0

## [0.7.0-alpha.5] - 2024-04-01

### 🚀 Features

- Implement submit pipeline (#150)

### 🐛 Bug Fixes

- Compute epoch directly from Shelley genesis (#186)
- *(sync)* Restart stage on blockfetch failure (#187)

### 🚜 Refactor

- *(sync)* Fetch blocks in batches (#189)

### 📚 Documentation

- Draft ADR for extra ledger queries (#178)

### 🔧 Continuous Integration

- Migrate to artifact action v4 (#188)
- Add new testnet genesis to docker build

### Release

- V0.7.0-alpha.5

## [0.7.0-alpha.4] - 2024-02-21

### 🚀 Features

- Integrate Babbage phase-1 validations (#169)

### ⚙️ Miscellaneous Tasks

- Update Pallas with latest Babbage validations (#170)

### Release

- V0.7.0-alpha.4

## [0.7.0-alpha.3] - 2024-02-14

### 🚀 Features

- Mithril client increments snapshot statistics (#157)
- Add shelley configs to docker image (#168)

### 🐛 Bug Fixes

- *(serve)* Handle intersection arg in follow tip (#162)

### 📚 Documentation

- Add installation guide link to README (#153)

### ⚙️ Miscellaneous Tasks

- Update u5c specs to v0.3 (#159)

### Release

- V0.7.0-alpha.3 (#163)

## [0.7.0-alpha.2] - 2024-01-20

### 🚀 Features

- *(serve)* Add gRPC reflection (#147)
- Integrate Alonzo phase-1 validations (#151)

### Release

- V0.7.0-alpha.2 (#152)

## [0.7.0-alpha.1] - 2023-12-16

### 🚀 Features

- Provide install script (#120)
- Implement Mithril bootstrap mechanism (#129)
- Integrate Pallas phase-1 validation function (#127)
- Implement protocol params update logic (#135)
- Make phase-1 validations optional (#136)

### 🐛 Bug Fixes

- Execute wal pruning after a few blocks (#137)
- Make Mithril dependency optional (#140)
- Remove Mithril from cross builds (#141)

### 📚 Documentation

- Improve documentation outline (#121)
- Add october long-running test results (#122)
- Add Byron phase-1 resource footprint benchmark (#124)
- Fix missing items in ToC (#125)

### 🔧 Continuous Integration

- Fix arm linux compile target (#139)

### Release

- V0.7.0-alpha.1 (#138)

## [0.6.0] - 2023-10-19

### 🚀 Features

- Include genesis files in docker image (#107)
- Improve sync progress logs (#110)
- Add basic data status command (#111)
- Improve logging options (#117)

### 🐛 Bug Fixes

- Use new debain as docker base image (#106)
- Ensure consistency between chain and ledger (#108)
- *(storage)* Take into account same-block utxos (#116)
- Avoid stage timeout when waiting for blocks (#119)

### ⚙️ Miscellaneous Tasks

- Add preprod sync example (#109)

### Release

- V0.6.0 (#118)

## [0.5.0] - 2023-10-14

### 🚀 Features

- Allow retry policy via config (#81)
- Serve Ouroboros BlockFetch N2N miniprotocol (#83)
- Compute genesis utxos (#89)

### 🚜 Refactor

- *(rolldb)* Use iterator for chain rollback delete (#85)
- *(sync)* Turn apply stage into end of pipeline (#86)
- *(serve)* Unify serve bootstrapping procedure (#87)
- Migrate to Pallas RollDB (#102)

### 📚 Documentation

- Fix daemon example (#80)

### 🔧 Continuous Integration

- Improve release workflow (#104)

### ⚙️ Miscellaneous Tasks

- Remove unused deps (#92)
- Fix lint warnings (#105)

### Release

- V0.5.0

## [0.4.1] - 2023-07-27

### Release

- V0.4.1

## [0.4.0] - 2023-07-26

### 🚀 Features

- Implement follow-tip as part of pipeline (#74)
- Add async streaming for rolldb (#75)

### 🚜 Refactor

- Implement follow-tip using rolldb streaming (#77)

### Release

- V0.4.0

## [0.3.0] - 2023-06-21

### 🚀 Features

- Introduce StateDB (#66)
- *(sync)* Introduce apply stage (#67)
- Enable gRPC-web connections (#69)
- Add client auth via TLS (#70)

### 🚜 Refactor

- *(rolldb)* Replace macro with trait (#50)
- Use Pallas interop for u5c mappings (#65)

### 📚 Documentation

- Add usage section (#71)
- Add long-running benchmark (#72)

### 🧪 Testing

- Add upstream integration test (#64)

### ⚙️ Miscellaneous Tasks

- Prepare for multiple dbs (#63)
- Fix lint issues (#73)

### Release

- V0.3.0

## [0.2.1] - 2023-05-11

### 🚀 Features

- Introduce grpc serve config (#48)

### 🐛 Bug Fixes

- Use info log level by default (#49)

## [0.2.0] - 2023-05-10

### 🚀 Features

- Add PoC gRPC endpoint (#25)
- *(rolldb)* Implement wal compaction (#26)
- *(rolldb)* Implement paged chain reads (#27)
- *(downstream)* Introduce dump-history endpoint (#28)
- Introduce daemon (sync+serve) command (#29)
- Use rolldb to define chainsync intersect (#45)

### 🚜 Refactor

- Use macros for rolldb (#24)

### 📚 Documentation

- Add installation section (#21)
- Fix book title (#22)

### 🔧 Continuous Integration

- Enable dependabot (#32)

### ⚙️ Miscellaneous Tasks

- Upgrade gasket / pallas versions (#23)
- Add community docs (#31)
- Improve module naming conventions (#46)
- Add example configuration (#47)

## [0.1.0] - 2023-01-11

### 🚀 Features

- Introduce ChainSync stage (#2)
- Introduce storage module (#3)
- Implement upstream block-fetch stage (#7)
- Introduce RollDB for chain persistence (#8)
- Introduce upstream reducer stage (#9)
- Introduce 'sync' entrypoint (#11)
- Introduce config mechanism (#15)

### 🐛 Bug Fixes

- Fix test concurrency issues (#18)

### 🚜 Refactor

- Use Rayon for plexer stage (#5)

### 🔧 Continuous Integration

- Implement validation workflow (#13)
- Implement release workflow (#19)
- Add mdbook workflow (#20)

### 🧪 Testing

- Add chainsync integration test (#6)

### ⚙️ Miscellaneous Tasks

- Scaffold Rust project (#1)
- Implement multiplexer stage (#4)
- Automate changelog generation (#12)
- Use remote deps instead of local (#14)
- Trace errors instead of dropping them (#16)
- Fix lint warnings (#17)

<!-- generated by git-cliff -->
