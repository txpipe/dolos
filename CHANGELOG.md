# Changelog

All notable changes to this project will be documented in this file.

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
