# Changelog

All notable changes to this project will be documented in this file.

## [2.0.0-beta.1] - 2026-11-23

### 🚀 Features

- Expose headless bulk replay engine (#1327)
- *(snapshot)* Expose headless snapshot facade (#1334)
- *(minibf)* Governance proposals parameters (#1281)
- *(minibf)* Add the `/governance/dreps/{drep_id}/metadata` endpoint (#1343)
- *(minibf)* Add the `/governance/committee` endpoint (#1350)
- *(minibf)* Add `/addresses/{address}/extended` endpoint (#1319)
- *(minibf)* Add `/governance/dreps/{drep_id}/delegators` (#1341)
- *(minibf)* Add `/accounts/{stake_address}/mirs` (#1321)
- *(minibf)* Add `/governance/dreps/{drep_id}/votes` (#1368)
- *(minibf)* Add `/pools/{pool_id}/votes` (#1376)
- *(minibf)* Add `/governance/dreps/{drep_id}/updates` (#1383)
- *(minibf)* Add the `/pools/{pool_id}/blocks` endpoint (#1365)
- *(cardano)* Index governance votes by voter and action (#1392)
- *(cardano)* Keep at most two votes per voter on each proposal (#1393)
- *(minibf)* Add `/scripts/{script_hash}/redeemers` endpoint (#1199)
- *(minibf)* Add `/governance/proposals/*/votes` routes (#1320)
- *(cardano)* First-mint period logs serving /assets and /assets/policy (#1417)
- *(cardano)* Stake-address period logs serving /accounts/{stake}/addresses (#1425)
- *(core)* One process-wide query limiter, carried by the domain (#1431)
- *(cardano)* Address, payment and account Stats serving the totals endpoints (#1429)
- Use jemalloc as the global allocator on Linux (#1416)
- *(minibf)* [**breaking**] Add the constitutional committee vote endpoints (#1369)
- *(minibf)* [**breaking**] Add `/metadata/txs/labels` (#1389)
- *(minibf)* Implement `/scripts` endpoint (#1367)
- *(minibf)* Add `/assets/{asset}/history` (#1398)
- *(minibf)* Add `/governance/dreps` (#1121)
- *(core)* Add a disposable cache store with a memory tier (#1455)
- *(redb3)* Add the cache's redb disk tier (#1456)
- *(cardano)* Serve the totals routes from a lifetime totals cache (#1458)
- *(cache)* Add the `dolos cache` commands and docs (#1461)
- *(cardano)* Persist the active-stake distribution at ESTART (#1462)
- *(cardano)* A known-network catalog with embedded genesis (#1463)
- *(bootstrap)* [**breaking**] Take bootstrap and publisher inputs from the invocation (#1466)
- *(cardano)* Keep the last seated committee after a no-confidence (#1473)
- *(config)* [**breaking**] The v2 configuration schema, loader and compatibility (#1468)
- *(config)* Init writes v2 at the selected path; add `config check` (#1476)
- *(storage)* [**breaking**] Supported store combinations and their lifecycle (#1479)
- *(archive)* [**breaking**] V2 key layouts for EBB block numbers, metadata tags and proposals (#1482)
- *(cardano)* Keep the CIP-151 calidus key on the pool (#1475)
- *(snapshot)* Publish only from the epoch-boundary cut (#1471)
- *(fjall)* [**breaking**] A fully specified `standard` preset for both stores (#1478)
- *(storage)* [**breaking**] Stores record their compatibility identity (#1481)
- *(cardano)* Archive per-epoch DRep voting power in a drep-epochs log (#1474)
- *(minibf)* Serve every route under an optional `route_prefix` (#1496)
- *(cardano)* [**breaking**] Drop stored fields nothing reads (#1500)
- *(storage)* [**breaking**] Store the UTxO creation position and page listings by it (#1498)
- *(cardano)* Write the current and previous epoch's stake rows at ESTART (#1499)
- *(config)* [**breaking**] Remove serve.grpc.tls_client_ca_root (#1504)
- *(cardano)* [**breaking**] Flag the asset mints whose CIP-25 metadata names the asset (#1507)
- *(storage)* [**breaking**] A seal-written entity index, its stele layers and the origin publish gate (#1511)
- *(cardano)* [**breaking**] Derive the API-only DRep and pool fields on read (#1508)
- *(cardano)* [**breaking**] A final-row log, and resolved proposals out of state (#1513)
- *(cardano)* [**breaking**] Move ScriptState out of state, the entity index as its witness (#1509)
- *(cardano)* [**breaking**] Retired pools out of state, to the final-row log (#1523)
- *(cardano)* [**breaking**] Move AssetState out of state, derived through the cache (#1524)
- *(cardano)* [**breaking**] Prune dead accounts from state at ESTART (#1525)
- *(cardano)* [**breaking**] Stake credentials in the entity index, the /accounts existence witness (#1526)
- *(cardano)* [**breaking**] Unregistered DReps out of state, to the final-row log (#1522)
- *(cli)* [**breaking**] `config write-genesis`, and `init` that only writes configuration (#1527)
- *(cli)* Non-mutating store access and shared output conventions (#1529)
- *(snapshot)* [**breaking**] Name payload media types `application/vnd.dolos.stelae.{kind}.v1+zstd` (#1532)
- *(bootstrap)* [**breaking**] Three sources over one replay and one data policy (#1528)
- *(snapshot)* [**breaking**] Restart every stele schema revision at 1 (#1536)
- *(snapshot)* Specify the record value encoding (#1540)
- *(cli)* [**breaking**] The `sync` group owns WAL status, recovery, dump and rollback (#1535)
- *(cardano)* [**breaking**] Drop pre-v2 residue and sync bookkeeping from published records (#1541)
- *(cli)* `service install | uninstall` on systemd and launchd (#1533)
- *(cli)* [**breaking**] The `data` group regrouped, bounded and non-mutating (#1534)
- *(cli)* [**breaking**] `sync catchup` succeeds only once the observed tip is committed (#1542)
- *(cardano)* [**breaking**] Apply the evolution conventions to the v2 data model (#1549)
- *(cli)* [**breaking**] Remove `snapshot`, the API wrappers and low-level utilities; add `cargo xtask debug` (#1538)
- *(minibf)* Serve epoch C + 1 in account history from the mark snapshot (#1506)
- *(snapshot)* [**breaking**] Adopt the Stelae snapshot envelope (#1553)
- *(snapshot)* Stage index and entity layers once per family (#1563)
- *(snapshot)* [**breaking**] Prepare a boundary durably and finalize it later (#1564)
- *(bootstrap)* Plain stele restore progress off a terminal (#1582)
- *(snapshot)* [**breaking**] Record a restored layer only once it is durable (#1583)
- *(bootstrap)* [**breaking**] Resume an interrupted stele restore with `--continue` (#1585)
- *(snapshot)* [**breaking**] Read stele layers ahead of the one a restore writes (#1588)
- *(minibf)* Add `/assets/{asset}/utxos` endpoint (#1594)
- *(minibf,minikupo)* Heavy-scan limit and request deadline (#1555)
- *(fjall)* Build fjall from the txpipe fork and report L0 run counts (#1599)
- *(snapshot)* Name the blocks snapshot in `position` (#1602)
- *(snapshot)* Assign each layer to the diff chain's diff or tip (#1604)
- *(snapshot)* [**breaking**] Export the diff chain and restore a snapshot through its chain (#1610)
- *(snapshot)* [**breaking**] Restore closed epochs' blocks from the blocks repository (#1613)
- *(bootstrap)* [**breaking**] A stelae source names the network root (#1616)
- *(snapshot)* [**breaking**] Closed epochs' blocks leave the Dolos export (#1619)
- *(telemetry)* [**breaking**] Export metrics over OTLP and remove MiniBF's /metrics route (#1626)
- *(grpc)* Record request and stream metrics for every call (#1631)
- *(trp)* [**breaking**] Record request metrics for every TRP call (#1636)
- *(minibf, minikupo)* [**breaking**] Record request metrics and remove MiniKupo's /health route (#1634)
- *(telemetry)* Export query limiter occupancy, waits and blocking closures (#1635)
- *(telemetry)* Export build info, uptime and Linux process metrics (#1630)
- *(mempool)* Export size, lifecycle, confirmation and submission metrics (#1632)
- *(telemetry)* Export storage disk, compaction, range, commit and housekeeping metrics (#1633)
- *(storage)* [**breaking**] Move to contract v2-beta.1 and record a store's internal format (#1646)
- *(xtask)* Add store relabel to overwrite a store's identity and format (#1650)
- *(storage)* Reset an incompatible mempool or cache file instead of failing to start (#1647)
- *(wal)* Reset an unreadable WAL at open, after state and archive are admitted (#1649)
- *(snapshot)* Grow the blocks stream's read-ahead share with depth, fit the default to a staging budget (#1657)
- *(snapshot)* Read 64 layers ahead by default (#1659)
- *(telemetry)* Export sync stage, tip lag, rollback and upstream metrics (#1668)

### 🐛 Bug Fixes

- *(bootstrap)* Derive UTxO tags during snapshot restore (#1332)
- *(cli)* Explain mid-epoch supply gaps instead of reporting value creation (#1326)
- *(minibf)* Reject hexadecimal DRep ids (#1346)
- *(minibf)* Account addresses BF-parity edge cases (#1339)
- *(archive)* Limit index work in each maintenance call (#1382)
- *(minibf)* Parse pool IDs like Blockfrost (#1384)
- *(minibf)* Answer unmatched paths with Blockfrost's global 400 (#1349)
- *(cardano)* Ratify against the boundary DRep snapshot (#1390)
- *(cardano)* Drop DRep votes the ledger removed at deregistration (#1396)
- *(minibf)* Filter lovelace UTxOs before cutting the page (#1400)
- *(minibf)* Order same-epoch pool retirements like Blockfrost (#1391)
- *(cardano)* Leave cleared DRep delegations out of the boundary distribution (#1399)
- *(cardano)* Commit EWRAP archive logs before the shard's state (#1415)
- *(cardano)* Leave proposal deposits refunded at the boundary out of the SPO distribution (#1401)
- *(xtask)* Give each PeakAlloc counter its own cache line (#1424)
- *(minibf)* Order account addresses by first appearance and bound the address scan (#1414)
- *(minibf)* Answer malformed epoch and slot numbers with Blockfrost's 400 body (#1408)
- *(cardano)* Enact preview's no-quorum cost model update at epoch 8 (#1407)
- *(minikupo)* Keep /matches store work off the async workers (#1432)
- *(minibf)* Run scan work in chunks under the query limiter (#1438)
- *(minibf)* Bound /epochs/{n}/stakes by max_scan_items (#1440)
- *(minibf)* Share one detached, limited fetch per cache entry (#1437)
- *(cardano)* Settle PV9 DRep delegator sets from a hardcoded table (#1443)
- *(cardano)* Tag archive inputs by what a tx consumes, not by batch residency (#1442)
- *(cardano)* Store the fee a Byron tx paid, not the policy minimum (#1472)
- *(minibf)* Restrict pool metadata requests (#1483)
- *(minibf)* Stabilize cost-model JSON key order (#1488)
- *(cardano)* Follow the entering epoch's rules for epoch nonces (#1477)
- *(deps)* Update TLS dependencies to resolve emSign advisories (#1486)
- *(cardano)* [**breaking**] Zero-quantity mints do not count (#1480)
- *(cardano)* A PV9 Clear entry clears a same-transaction delegation (#1492)
- *(minibf)* Recognize proposal return accounts (#1490)
- *(cardano)* [**breaking**] Keep slot zero in epoch statistics (#1485)
- *(minibf)* Apply several fixes on pool history (#1430)
- *(cardano)* Log the first stake epoch's distribution (#1497)
- *(cardano)* Order pending-rewards pairs by pool (#1512)
- *(snapshot)* Shard each state namespace on its first hash byte (#1539)
- *(minibf)* Deterministic metadata key order in `/assets/{asset}` and `/txs/{hash}/metadata` (#1547)
- *(cardano)* Count every hash-datum output in the datum refcount (#1546)
- *(crawl)* Return an error when the crawler can't cross from archive to WAL (#1569)
- *(sync)* Offer unacknowledged txs again on a new submit session (#1571)
- *(serve)* Return the full era table over gRPC and Ogmios (#1572)
- *(minibf)* Drop the spent-by lookup from UTxO listings and pin the per-page cost (#1573)
- *(minibf)* List vote-registration-delegation certs in account registrations (#1576)
- *(cli)* Flush OpenTelemetry spans before a command exits (#1580)
- *(snapshot)* [**breaking**] Size the restore space preflight from an on-disk estimate (#1593)
- *(cardano)* Require the Technical DRep threshold for an empty cost-model update (#1595)
- *(minibf)* Reverse a block's transactions for `order=desc` in `/metadata/txs/labels/{label}` (#1567)
- *(cli)* Log other components' errors; test that a failed persist stops a restore (#1607)
- *(core)* Restore delta data and refuse WAL gaps in catch-up (#1590)
- *(core)* Catch-up and rollback follow the cursor and chain order, not the slot (#1608)
- *(core)* The archive and sync status keep a same-slot block pair apart (#1621)
- *(minibf)* /network never serves the previous epoch's pots after a boundary (#1620)
- *(minibf)* Honour order=desc on account rewards (#1643)
- *(minibf)* Skip invalid-tx pool updates and order same-block pools by certificate (#1644)
- *(minibf)* [**breaking**] Report only the configured public URL at the root, drop revision (#1648)
- *(minibf)* Check an anchor's content type and word its errors as Blockfrost (#1645)
- *(cardano)* Join CIP-68 byte-string parts (#1623)
- *(minibf)* Correct account transaction address order (#1627)
- *(minibf)* Drop delegations that a pool retirement removed (#1611)
- *(minibf)* Convert metadata to JSON at any nesting depth (#1589)
- *(minibf)* Blocks edge cases, walking the archive by chain point (#1651)
- *(minibf)* Read address and account summaries off the async workers (#1654)
- *(minibf)* Show paid byron transaction fees (#1655)
- *(cli)* Refuse to run on an interrupted snapshot restore (#1661)
- *(bootstrap)* Check a registry source before --force clears the node (#1660)
- *(deps)* Bump url to 2.5.9 for RUSTSEC-2026-0592 (#1664)
- *(cache)* Warn when a namespace's format version change wipes it (#1671)

### 🚜 Refactor

- *(snapshot)* Retire publisher commands and orchestration (#1335)
- *(minibf)* Blocks (#1354)
- *(minibf)* Epochs (#1402)
- *(minibf)* `network` `health` and `metrics` layout (#1422)
- *(core)* Typed, variable-length entity and log keys (#1419)
- *(minibf)* `txs` (#1433)
- *(cardano)* Entity naming renames — EraState, PoolEpochLog, kebab-case pending namespaces (#1444)
- *(cardano)* One stake-first AddressKey, with AccountKey as its prefix (#1448)
- *(cardano)* [**breaking**] One AddressStats and AccountStats type each, with a dense AccountStats (#1450)
- *(cardano)* [**breaking**] Name the period-log sets and the chunks they seal into (#1451)
- *(cardano)* [**breaking**] Register only state-keyed types as state namespaces (#1452)
- *(minibf)* Serve `/network` through the cache store (#1457)
- *(minibf)* Serve off-chain DRep metadata through the cache store (#1460)
- *(minibf)* Governance, genesis and utils layout (#1484)
- *(minibf)* Accounts layout (#1503)
- *(minibf)* Addresses layout (#1516)
- *(minibf)* Assets layout (#1517)
- *(snapshot)* [**breaking**] Drop the pre-v2 residue from the stele profile (#1531)
- *(cardano)* [**breaking**] Rename policy-mint-chunks to policy-first-mint-chunks (#1544)
- *(minibf)* Metadata layout (#1552)
- *(minibf)* Pools scripts layout (#1558)
- *(minibf)* Utxos layout (#1596)
- *(snapshot)* Stop cutting and adopting retained state dumps (#1601)
- *(snapshot)* [**breaking**] Remove the unused `digests` kind (#1603)
- *(snapshot)* Stop reporting retained state dumps on restore (#1600)
- *(snapshot)* [**breaking**] Remove the retained state-epoch list (#1606)
- *(snapshot)* [**breaking**] Remove restricted plans and the bundle horizon (#1612)
- *(snapshot)* [**breaking**] Say "snapshot" instead of "stele" in code and messages (#1625)

### 📚 Documentation

- *(minibf)* Document endpoints missing from coverage table (#1336)
- *(snapshot)* Stop hardcoding counts of kinds, layers and namespaces (#1449)
- *(data-layer)* Record first-mint-chunks and stats footprints (#1453)
- *(cache)* Document the totals namespace's mainnet footprint (#1465)
- *(config)* V2 reference, examples and migration guide (#1494)
- *(architecture)* The frozen v2 data model, with mainnet footprints (#1530)
- *(snapshot)* Freeze the io.txpipe.dolos.cardano v1 profile (#1537)
- *(snapshot)* State the PROFILE.md contract in Dolos's own terms (#1545)
- *(cli)* Add the CLI reference, help fixtures and the v2 migration guide (#1551)
- Add llms.txt for integrators (#1578)
- *(minibf)* Publish a Blockfrost conformance summary (#1579)
- *(snapshot)* Describe the diff chain in the profile, ADR-004 and the docs (#1629)
- *(agents)* Never edit the changelog by hand, git-cliff generates it (#1640)
- *(telemetry)* Add the metric reference and a telemetry page (#1639)
- *(agents)* The full checks are the merge bar, not every local run (#1652)
- *(reference)* The compatibility policy, and --version names the contract (#1642)
- *(snapshot)* Freeze the profile again for the diff chain (#1658)
- *(snapshot)* Settle the snapshot wording in help, example and guide (#1662)
- *(compat)* State restore stamping and that there is no release registry (#1669)
- *(telemetry)* Add the mempool and storage series and node health queries to the metric reference (#1672)

### 🔧 Continuous Integration

- Move the macOS test job to macos-15 (#1570)
- Cap the test jobs at 60 minutes (#1598)

### ⚡ Performance

- *(minibf)* Batch metadata and live replay work (#1333)
- *(fjall)* Bound and lazily read archive tag slot iteration (#1445)
- *(minibf)* Write the `/total` bodies directly (#1454)
- *(minibf)* Fetch dependency txs as one grouped, concurrent batch (#1441)
- *(cardano)* Rotate ESTART accounts in one streamed pass (#1495)
- *(cardano)* Read staged stats and first scripts from the staged floor (#1548)
- *(snapshot)* Ingest block and log layers as sorted runs, with larger restore batches (#1615)
- *(snapshot)* Resolve the blocks chain beside the Dolos chain (#1656)

### 🧪 Testing

- *(cache)* Read the open cache file through a memory map (#1467)
- *(cardano)* Drop oversized payment stats fixture (#1550)
- *(epoch_pots)* Cover the Conway hard forks on every network (#1581)
- *(cardano)* Replay every delta variant from the WAL (#1609)
- *(compat)* Refuse datasets written by published releases, before any write (#1653)
- *(storage)* The store-contract enforcement matrix, and admit both stores before either opens writable (#1663)
- *(compat)* Archive forward-upgrade fixtures and open them in CI (#1666)
- *(bootstrap)* Match the forced-restore refusal across wrapped lines (#1670)

### ⚙️ Miscellaneous Tasks

- Bump pallas to 1.3.0 and render script JSON as text (#1361)
- *(minibf)* Drop the stale openapi.yaml copy and link the upstream spec (#1363)
- Bump rustls to 0.23.46 (#1439)
- *(build)* Keep only line tables in dev and test debuginfo (#1459)
- *(fjall)* Pin fjall 3.1.13 (#1597)
- *(changelog)* Regenerate with git-cliff (#1641)

## [2.0.0-alpha.0] - 2026-09-11

### 🚀 Features

- *(minibf)* Implement `/governance/proposals` endpoint (#1275)
- *(minibf)* Implement `/utils/addresses/xpub/{xpub}/{role}/{index}` (#1278)
- *(minibf)* Add `/assets/{subject}/txs` endpoint (#1220)
- *(minibf)* Add `/accounts/{stake_address}/history` endpoint (#1293)
- *(minibf)* Add `/assets` endpoints (#1294)
- *(config)* [**breaking**] Remove the non-live redb state and index backends (#1301)
- *(archive)* [**breaking**] Retire the redb archive backend (#1302)
- *(archive)* Prune the archive index entries with the blocks (#1306)
- *(minibf)* Governance proposals withdrawals (#1279)
- *(archive)* Compressed segment codec with a seekable zstd layout (#1308)
- *(archive)* Mixed raw and compressed segments with a recoverable transition (#1309)
- *(archive)* Offline sealing, dictionary training and restore commands (#1310)
- *(minibf)* Add `/governance/proposals/{tx_hash}/{cert_index}`, /governance/proposals/{gov_action_id} endpoints (#1219)
- *(archive)* Write benchmarks and a bundled zstd dictionary (#1311)
- *(archive)* [**breaking**] Append every block as one compressed frame at its physical location (#1312)
- *(storage)* Bump the storage version to v4 (#1282)
- *(archive)* Automatically select bounded parallel encoding (#1317)
- *(bench)* Add minibf performance regression harness (#1318)
- *(minibf)* Add `/governance/proposals/*/metadata` routes (#1295)
- *(minibf)* Add `/accounts/{stake_address}/addresses/total` (#1195)
- *(minibf)* Add `/blocks/epoch/{epoch_number}/slot/{slot_number}` (#1322)

### 🐛 Bug Fixes

- *(ci)* Keep the Windows job off `getrusage` (#1267)
- *(cli)* Don't swallow driver errors (#999)
- *(cli)* Let a fatal driver failure reach the exit status (#1269)
- *(snapshot)* Size backfill's immutable chunks from the chain's own k (#1268)
- *(minibf)* Report null active stake for the preprod epoch gap (#1263)
- *(cardano)* Gate the roll crawl on transaction validity (#1266)
- *(minibf)* Attribute pool-deposit refunds to the correct reward epoch (#1276)
- *(snapshot)* The state cursor marks the whole restore, not the tip (#1297)
- *(snapshot)* Improve pre-flight space check to account for already downloaded bytes (#1299)
- *(storage)* Close index dissolution follow-ups (#1307)
- *(core)* Share block metadata resolution for queries (#1316)
- *(ci)* Respect unavailable Windows CPU counters (#1324)

### 🚜 Refactor

- *(stelae)* Move the dolos-free residue out of the profile (#1285)
- *(stelae)* Make the chained-publish lifecycle profile-generic (#1286)
- *(stelae)* Make the snapshot and restore commands parse and render (#1288)
- *(stelae)* Move the backfill daemon into the profile crate (#1289)
- *(stelae)* Close out the repo-split leftovers (#1290)
- *(stelae)* Consume the stelae crates as a pinned git dependency (#1291)
- *(state)* [**breaking**] Host the live-UTxO tags in the state store (#1303)
- *(archive)* [**breaking**] Host the archive tags and exact lookups in the archive store (#1304)
- *(storage)* [**breaking**] Remove the standalone index store (#1305)

### 📚 Documentation

- *(minibf)* Reconcile the endpoint coverage table with the router (#1261)

### 🔧 Continuous Integration

- Pin the cargo fmt nightly to 2026-08-27 (#1284)
- Lint PR titles for conventional commits (#1300)

### ⚡ Performance

- *(snapshot)* Produce a publish's layers from a pool of workers (#1272)
- *(stelae)* Send 4 MiB upload chunks instead of 1 MiB (#1273)
- *(stelae)* Push a layer that fits as one request (#1277)

### 🧪 Testing

- Compare the cross-backend walk against the seeded rows directly (#1258)
- *(archive)* Accept the direct-write cutover end to end and move the benchmarks into xtask (#1313)

### ⚙️ Miscellaneous Tasks

- Officialize next version as v2 (#1328)

### Release

- V2.0.0-alpha.0

## [1.7.0-alpha.1] - 2026-08-24

### 🚀 Features

- *(minibf)* Implement `/pools/retired` endpoint (#1218)
- *(cli)* Add doctor command to regenerate the state store (#1222)
- *(cli)* Improve data checks of pots at epoch boundaries (#1235)
- *(minibf)* Bump Blockfrost OpenAPI to v0.1.91 (#1223)
- *(minibf)* Add `/txs/{hash}/required_signers` endpoint (#1232)
- *(minibf)* Implement `/pools/{pool_id}/updates` endpoint (#1237)
- *(minibf)* Add `/epochs/{number}/blocks/{pool_id}` endpoint (#1233)
- *(minibf)* Add `/scripts/{script_hash}/utxos` endpoint (#1207)
- *(stelae)* Make an unknown layer kind skippable at restore, strict at publish (#1242)
- *(snapshot)* Retain state dumps at configured epochs (#1246)
- *(cardano)* [**breaking**] Merge the four per-account epoch logs into one entity (#1249)
- Introduce fjall archive backend as the default (#1257)
- *(snapshot)* Add the `snapshot backfill` publisher driver (#1259)

### 🐛 Bug Fixes

- *(cardano)* Move the pots when a treasury withdrawal enacts (#1227)
- *(deps)* Update `h2` for `RUSTSEC-2026-0258` (#1229)
- *(cardano)* Align the boundary DRep distribution with the ledger's post-enactment snapshot (#1228)
- *(snapshot)* Align the stele's sequence and tag with the epoch it names (#1240)
- *(archive)* Support EBBs persistence via multi-block per slot (#1239)
- *(grpc)* Don't panic on an unservable FollowTip/WatchTx intersect (#1226)
- *(cli)* Advance the state rebuild by block, not by slot (#1244)
- *(cardano)* Delete the unredeemed AVVM utxos the Allegra boundary reclaims (#1250)
- *(stelae)* Retry a read the OS interrupted instead of failing the layer (#1254)

### 🚜 Refactor

- *(snapshot)* Split the logs layer into per-namespace kinds (#1241)
- *(snapshot)* Split the state layer into per-namespace kinds (#1243)

### 🔧 Continuous Integration

- Improve workflow performance by removing steps from the critical path (#1251)

### ⚡ Performance

- *(redb3)* Avoid empty space in leaf pages by randomizing insert order (#1253)
- *(snapshot)* Band the index traversal across a run of epochs (#1255)
- *(stelae)* Move a publish's layer round trips concurrently (#1260)

### 🧪 Testing

- *(snapshot)* Pin every namespace's field table against the compatibility contract (#1245)

### ⚙️ Miscellaneous Tasks

- Increase surface of dolos-devs code ownership (#1231)

### Release

- V1.7.0-alpha.1

## [1.7.0-alpha.0] - 2026-08-16

### 🚀 Features

- *(stelae)* Setup a walking skeleton for the Stelae protocol (#1147)
- *(storage)* Add the export/restore seam Stelae index layers need (#1150)
- *(storage)* Introduce true memory built-in store for state and indexes (#1158)
- *(storage)* Make the archive dimension registry enforce itself (#1161)
- *(stelae)* Read layers as a stream, bounded by one record (#1148)
- *(snapshot)* Define our custom stelae profile (#1166)
- *(stelae)* Write layers as a stream, bounded by one record (#1167)
- *(minibf)* Implement `/accounts/{stake_address}/transactions` endpoint (#1127)
- *(snapshot)* Export a stele from a live node (#1168)
- *(snapshot)* Support restoring a node from a stele (#1169)
- *(stelae)* Push and pull a stele to an OCI registry (#1170)
- *(minibf)* Implement the `/pools` endpoint (#1137)
- *(minibf)* Implement `/accounts/{stake_address}/addresses/assets` endpoint (#1126)
- *(snapshot)* Publish a stele into a registry, and only what is new (#1173)
- *(snapshot)* Restore from a registry, and survive being interrupted (#1176)
- *(stelae)* Authenticate a registry with basic credentials (#1184)
- *(stelae)* Name the official registry's published read-only pair (#1186)
- *(stelae)* Give a publisher digest, verify and inspect capabilities (#1188)
- *(stelae)* Introduce scratch dir for publish operations (#1191)
- *(stelae)* Run preflight rules before publish / restore (#1192)
- *(cardano)* Introduce log per-account epoch stake distribution (#1151)
- *(minibf)* Implement `/epochs/{number}/stakes` endpoint (#1152)
- *(minibf)* Implement `/epochs/{number}/stakes/{pool_id}` endpoint (#1154)
- *(minibf)* Implement `/assets/policy/{policy_id}` endpoint (#1189)
- *(cardano)* Introduce committee, constitution and governance singleton state (#1130)
- *(core)* Introduce cbor::U128 newtype (#1208)
- *(minibf)* Implement `/epochs/{number}`, `/next` and `/previous` (#1171)
- *(stelae)* Support resume of interrupted publish (#1200)
- *(stelae)* Introduce restore / publish progress reports (#1210)
- *(cardano)* Enact the four missing governance action variants (#1211)
- *(cli)* Introduce `data check` command to review semantic consistency (#1213)
- *(cardano)* Compute DRep and SPO stake distributions in the EWRAP pass (#1212)
- *(cardano)* Governance "ratify" engine in shadow mode (#1214)
- *(cardano)* Compute proposal outcomes instead of using hacks (#1215)

### 🐛 Bug Fixes

- *(minibf)* Render relay IPv6 addresses in network byte order (#1136)
- *(minibf)* Return affected addresses from `/blocks/{hash_or_number}/addresses` (#1124)
- *(stelae)* A registry client should not need a C toolchain (#1180)
- *(stelae)* Hold a publisher to the record ceiling its reader enforces (#1182)
- *(stelae)* Specify the manifest third parties read, and prove it in CI (#1183)
- *(stelae)* A blob that already exists is a dedup, not a rename (#1187)
- *(minibf)* Return a JSON error body for a malformed stake address (#1177)
- *(stelae)* Improve error messages on staging dir preflight checks (#1202)
- *(cardano)* Clear a vote delegation only when the ledger cleared it (#1216)

### 🚜 Refactor

- *(storage)* Let append_prehashed take an iterator (#1159)
- *(storage)* Factor the archive record prefix walk (#1160)
- *(storage)* Source the stored tag key from one definition (#1162)
- *(storage)* Refuse unsupported store methods instead of panicking (#1164)
- *(minibf)* Share input resolution across scan endpoints (#1134)
- *(minibf)* Share input resolution for the account transactions scan (#1174)
- *(minibf)* Share input resolution for the block addresses endpoint (#1190)
- *(stelae)* Improve DRY on layer opening prologue (#1201)

### 📚 Documentation

- Restore lost changelog
- *(adr)* Rename the snapshot mechanism Stelae (#1146)
- Fix the Tx3 Rust SDK signer import path (#1181)

### 🔧 Continuous Integration

- Pin git-cliff workdir so changelog survives submodule
- Ban cmake in deny.toml instead of documenting the refusal (#1196)
- Don't publish prereleases to homebrew and npm

### ⚡ Performance

- *(storage)* Hold exact record keys inline (#1163)

### 🎨 Styling

- *(snapshot)* Normalize the preflight test comments (#1209)

### 🧪 Testing

- Improve CI coverage with comprehensive all-features workspace testing (#1157)
- *(storage)* One seeder for the index roundtrip and cost fixtures (#1165)
- *(stelae)* Point the oci harness at a deployed registry (#1185)
- *(stelae)* Avoid preflight disk space margins race (#1203)

### ⚙️ Miscellaneous Tasks

- *(storage)* Formalize supported vs deprecated backends (#1155)
- Add dolos-devs as code owner for `crates/minibf` (#1194)

### Release

- V1.7.0-alpha.0

## [1.6.0] - 2026-07-27

### 🚀 Features

- *(minibf)* Implement `/pools/retiring` endpoint (#1122)
- *(cardano)* Capture governance votes and full proposal state (#1129)
- *(minibf)* Add block txs CBOR endpoints (#1123)
- *(minibf)* Implement `/addresses/{address}/total` endpoint (#1133)
- *(minibf)* Implement `/pools/{pool_id}/relays` endpoint (#1132)

### 🐛 Bug Fixes

- *(cardano)* Stop dropping StakeVoteDeleg drep leg and drep anchors (#1128)
- *(bootstrap)* Install rustls crypto provider for mithril client (#1131)

### ⚙️ Miscellaneous Tasks

- *(minibf)* Bump openapi crate to 1.90 (#1125)

### Release

- V1.6.0

## [1.5.0] - 2026-07-16

### 🚀 Features

- *(bootstrap)* Migrate mithril client to v2 Cardano Database format (#1063)

### 🐛 Bug Fixes

- *(examples)* Update blaze-client deps to clear security advisories (#1054)
- *(deps)* Bump vulnerable dependencies to clear security advisories (#1057)
- *(cardano)* Adjust fn visibility to satisfy private interface lint (#1060)
- *(deps)* Upgrade opentelemetry family to 0.32 (#1058)
- *(minibf)* Return 404 for valid but non-existent asset in `/assets/{asset}/addresses` (#1055)
- *(cardano)* Update gov proposal mappings and update skill (#1065)

### 📚 Documentation

- Add ADR for deterministic OCI snapshots (#1056)
- *(examples)* Introduce UTxO RPC WatchTx examples (#1061)
- Incorporate ADR 004 review feedback (#1062)

### 🔧 Continuous Integration

- Add cargo-deny advisories gate (#1059)

### Release

- V1.5.0

## [1.4.0] - 2026-07-08

### 🐛 Bug Fixes

- *(wal)* Correct slot-range endianness so pruning and lookups work (#1045)
- *(cli)* Make wal-integrity tool robust to synthetic entries and discontinuities (#1046)
- *(core)* Unify store catch-up policy after crash (#1048)
- *(cardano)* Add missing enacted governance proposals to hacks.rs (#1052)
- *(cardano)* Inject Plutus POSIXTime in milliseconds for mempool validation (#1028)

### 🚜 Refactor

- *(sync)* Add apply-side block-sequence continuity guard (#1049)

### 📚 Documentation

- Document issue label structure in CONTRIBUTING (#1030)
- Add architecture section describing internals (#1044)

### 🔧 Continuous Integration

- Harden Rust CI (#1051)

### 🧪 Testing

- Harden housekeeping test coverage (#1047)

### ⚙️ Miscellaneous Tasks

- Link high-stakes TODOs to tracking issues (#1043)

### Release

- V1.4.0

## [1.3.2] - 2026-06-23

### ⚙️ Miscellaneous Tasks

- *(trp)* Bump tx3-resolver/tx3-cardano to 0.23.0 (#1027)

### Release

- V1.3.2

## [1.3.1] - 2026-06-20

### 🐛 Bug Fixes

- *(cli)* Download snapshots via ranged ring buffer (#1025)
- *(cli)* Keep snapshot progress bar live during ranged download (#1026)

### Release

- V1.3.1

## [1.3.0] - 2026-06-19

### 🚀 Features

- *(utxorpc)* Expose configurable HTTP/2 transport tuning (#1011)
- *(utxorpc)* Honor field_mask in Sync service to skip unused block representations (#1012)

### 🐛 Bug Fixes

- *(cardano)* Surface lagging pool snapshots in RUPD instead of obscure panic (#1016)
- *(cardano)* Fail loud on lagging pool snapshots and unfinished epoch boundaries (#1017)
- *(minibf)* Source address-utxo tx_hash from TxoRef, not archive block_data (#1009)

### 📚 Documentation

- Re-generate wiped-out changelog

### ⚙️ Miscellaneous Tasks

- *(cardano)* Add more gov proposal mappings (#1013)
- Switch snapshot source to R2 (#1014)
- *(cardano)* Update gov proposal mappings (#1023)
- *(trp)* Bump tx3-cardano and tx3-resolver to 0.22.0 (#1022)
- Bump pallas to 1.1.1 (#1024)

### Release

- V1.3.0

## [1.2.0] - 2026-06-01

### 🚀 Features

- *(utxorpc)* Support certificate pattern matching in the `watch` service (#994)
- *(utxorpc)* Honor height & hash params in dump history calls (#993)
- *(trp)* Populate reference script expression in TRP UTxO mapping (#1007)

### 🐛 Bug Fixes

- *(utxorpc)* Honor search of utxo by policy id and asset name (#992)
- *(u5c)* Populate block_ref in AnyUtxoData via block_by_tx_hash (#1000)
- *(u5c)* Convert ChainPoint/BlockRef timestamp from seconds to ms (#1003)
- *(testing)* Use Waker::noop() to satisfy clippy::manual_noop_waker
- Use Waker::noop() to satisfy clippy::manual_noop_waker (#1004)
- *(cli)* Shutdown databases before export (#1005)

### 🚜 Refactor

- Extend BlockRefMeta and use on the other drivers (#1006)

### 📚 Documentation

- Add performance subsection (#1001)

### ⚙️ Miscellaneous Tasks

- Update preview enactments (#996)
- *(minibf)* Upgrade blockfrost crate to 0.1.88 (#1002)
- *(trp)* Bump tx3-cardano and tx3-resolver to 0.21.0 (#1008)

### Release

- V1.2.0

## [1.1.1] - 2026-05-05

### 🚀 Features

- *(u5c)* Serve v1alpha and v1beta gRPC side-by-side (#987)
- *(cardano)* Stage van Rossem fork - protocol v11 (#990)

### 🐛 Bug Fixes

- *(minibf)* Add v11 new cost model mapping names (#988)
- *(cardano)* Short-circuit ewrap replay between boundary phases (#991)

### Release

- V1.1.1

## [1.1.0] - 2026-04-30

### 🚀 Features

- *(minibf)* Introduce `/accounts/{account}/withdrawals` (#974)
- *(minibf)* Introduce `/pools/{pool_id}/metadata` endpoint (#975)
- *(minibf)* Introduce `/pools/{pool_id}` endpoint (#976)
- *(utxorpc)* Implement read_genesis / read_era_summary queries (#964)
- *(wal)* Version on-disk schema and force-reset on incompatibility (#984)
- *(u5c)* Allow LedgerContext to query archive for spent UTxOs (#962)

### 🐛 Bug Fixes

- *(sync)* Apply deltas before WAL commit so rollback undo works (#985)

### 🚜 Refactor

- *(cardano)* Move delta structs alongside entities (#968)
- *(cardano)* Enforce undo axiom via proptest (#971)
- *(cardano)* Shard mem-hungry work units (#978)

### 📚 Documentation

- Reconcile schema and minibf coverage with code (#986)

### 🧪 Testing

- *(minikupo)* Add comprehensive unit test coverage (#939)

### ⚙️ Miscellaneous Tasks

- Update tx3 to v0.17.0 (#965)
- *(minibf)* Upgrade minibf to version 0.1.87 (#973)
- *(cardano)* Update proposal outcomes (#979)
- *(cardano)* Update proposal outcomes (#983)

### Release

- V1.1.0

## [1.0.3] - 2026-04-04

### 🚀 Features

- *(cli)* Allow skipping sanitization during export (#952)

### 🐛 Bug Fixes

- *(sync)* Restart intersect on header violation (#954)
- *(minibf)* Avoid using timing-dependant logs for active stake data (#956)

### ⚙️ Miscellaneous Tasks

- Update cargo dist version and scripts (#955)
- Remove sneaky mac files
- Update tx3 to v0.16.4 (#957)

### Release

- V1.0.3

## [1.0.2] - 2026-03-19

### 🐛 Bug Fixes

- *(cli)* Reset wal after any kind of bootstrap (#950)
- *(cli)* Generate deterministic snapshots by removing file metadata (#951)

### Release

- V1.0.2

## [1.0.1] - 2026-03-18

### 🐛 Bug Fixes

- Catch-up stores during init (#947)
- Avoid slot-only chain intersection (#948)
- *(cli)* Avoid bootstrap checks when trying to reset-wal (#949)

### 📚 Documentation

- Fix broken doc links (#945)

### Release

- V1.0.1

## [1.0.0] - 2026-03-13

### 🚀 Features

- Introduce mini-kupo api (#898)
- *(cli)* Introduce doctor check command
- *(cli)* Improve init flow with better UX (#942)

### 🐛 Bug Fixes

- *(minikupo)* Nest endpoints under `/v1` prefix for compatibility (#937)
- Don't fail trying to remove non-existing socket file
- *(storage)* Set quick repair for redb state store (#943)

### 🚜 Refactor

- *(cardano)* Remove track config in favor of simplicity (#933)
- Remove explicit defaults from storage configs (#935)
- Use custom tracing target for axum requests (#936)
- *(storage)* Split index tags keyspace to follow access pattern (#908)
- *(config)* Turn pull_batch_size into implicit default
- Turn max_history into a sync config
- Turn WAL max rollback into a sync config
- Skip serializing default storage config
- Move magic field to the chain config
- Use implicit defaults for logging config
- Use implicit defaults for telemetry config
- Treat many defaults as a config concern
- Skip serializing sync and serve configs if default

### 📚 Documentation

- Improve readme ahead of v1 (#932)
- Overhaul documentation ahead of v1 (#944)
- Improve readme header

### 🧪 Testing

- Improve e2e test with latest config changes

### ⚙️ Miscellaneous Tasks

- *(trp)* Update tx3 deps to v0.16.2 (#931)
- *(fjall)* Bump to fjall 3.1.0 and remove patch (#934)
- Remove data folder from example
- *(trp)* Bump tx3 to v0.16.3 (#938)
- Apply config changes review feedback
- Improve log messages severity across the board (#941)

### Release

- V1.0.0

## [1.0.0-rc.12] - 2026-03-05

### 🚀 Features

- *(minibf)* Implement `/addresses/{address}` endpoint (#925)
- Introduce script indexes and related minibf endpoints (#899)

### 🐛 Bug Fixes

- *(trp)* Avoid overriding existing tx witnesses (#926)
- *(minibf)* Avoid 500 error on `/txs/{hash}` for current epoch txs (#918)
- Avoid TOCTOU race on submit utxos (#929)
- *(fjall)* Use patched version of fjall with deadlock fix (#930)
- *(u5c)* Fix `outputs_match_asset` to not require asset_name (#833)

### 🧪 Testing

- *(trp)* Improve coverage of submit method (#927)

### ⚙️ Miscellaneous Tasks

- *(trp)* Update tx3 deps to v0.16.1 (#928)

### Release

- V1.0.0-rc.12

## [1.0.0-rc.11] - 2026-02-27

### 🚀 Features

- *(o7s)* Implement missing queries to support ogmios connection (#913)

### 🐛 Bug Fixes

- Adjust inconsistencies in mempool tracking (#915)
- *(trp)* Fix mapping of plutus maps (#921)

### 📚 Documentation

- Update config and minibf sections to match latest changes (#914)
- Add relevant maintenance / debugging skills

### ⚙️ Miscellaneous Tasks

- Bump tx3 to v0.15.0 (#916)
- Bump tx3 to v0.15.1 (#917)
- *(minibf)* Upgrade openapi spec to 0.1.85 (#922)
- Update latest hacks

### Release

- V1.0.0-rc.11

## [1.0.0-rc.10] - 2026-02-17

### 🚀 Features

- Introduce OTLP integration with focus on mempool (#891)
- Introduce comprehensive mempool support (#901)
- *(cli)* Improve flags for the bootstrap command (#909)
- *(minibf)* Adjust max scan limit via config (#911)

### 🐛 Bug Fixes

- Unregistered dreps should not figure as expired (#887)
- *(minibf)* Follow redirects for pool offchain metadata (#888)
- *(minbf)* Compute Byron tx fees during mapping (#886)
- *(cardano)* Add strict metadata checks for CIP25 (#882)
- *(minibf)* Improve handling of tx metadata on AssetState (#881)
- *(index)* Fine-tune fjall default for expected write throughput (#896)
- Adapt data export command to new storage structure (#892)
- *(cardano)* Apply mainnet-specific historical edge-cases (#895)
- *(cli)* Remove wal from snapshot (#897)
- Hook mempool confirm in sync pipeline (#904)
- Avoid excessive mem increase during boundary (#905)
- *(trp)* Adjust interface to match available mempool data (#910)

### 🚜 Refactor

- Formalize `strict` feature flag (ex `relaxed`) (#902)
- Move rollback logic to core (#907)

### 🧪 Testing

- Treat epoch tests as best-effort (#903)

### ⚙️ Miscellaneous Tasks

- *(minibf)* Fallback to genesis tx if hash not found (#884)
- Bump fjall to v3.0.2 (#906)

### Release

- V1.0.0-rc.10

## [1.0.0-rc.9] - 2026-02-09

### 🐛 Bug Fixes

- *(minibf)* Add guard for inconsistent range params (#879)

### 🔧 Continuous Integration

- Enable automatic publishing to npm (#873)

### 🧪 Testing

- *(minibf)* Implement endpoint unit test full coverage via synthetic data (#877)
- *(minibf)* Include pagination, range and order in unit tests (#878)
- Relax constraints of smoke test to avoid flaky results (#880)

### ⚙️ Miscellaneous Tasks

- Remove toy-domain feature flag (#885)

### Release

- V1.0.0-rc.9

## [1.0.0-rc.8] - 2026-02-07

### 🐛 Bug Fixes

- *(minibf)* Support parsing of byron addresses when required (#863)
- *(minibf)* Add memory cache for heavy network query (#865)
- *(minibf)* Improve active epoch logic on pool_updates mapping (#862)
- *(minibf)* Add mainnet genesis block (#867)
- *(minibf)* Parse complex onchain metadata for assets (#860)
- *(minibf)* Track drep drop on account when retires (#868)
- *(minibf)* Skip invalid plutus-v1 proposal preview (#871)
- *(cardano)* Separate reward log types into different namespaces (#875)

### 🚜 Refactor

- Introduce block streaming query extensions (#864)
- *(minibf)* Improve mem of first-by-tag type of queries (#866)
- *(minibf)* Improve memory usage on utxo queries (#869)
- *(minibf)* Relax max pagination limit (#876)

### ⚙️ Miscellaneous Tasks

- *(minibf)* Make better use of from/to pagination filters (#870)

### Release

- V1.0.0-rc.8

## [1.0.0-rc.7] - 2026-02-04

### 🚀 Features

- *(o7s)* Implement many missing LocalState queries to support cardano cli (#858)

### 🐛 Bug Fixes

- Introduce async query facade to avoid starving the runtime
- *(minibf)* Improve conformance tests coverage (#847)
- *(cardano)* Take into account drained rewards in epoch state (#855)
- *(cardano)* Use mark protocol version for babbage pot calc (#857)
- *(cardano)* Bootstrap nonce in genesis if chain starts on Shelley
- Improve conformance tests coverage (#853)
- *(minibf)* Format special dreps on accounts (#861)

### 🧪 Testing

- Add comprehensive flow for cardano integration tests (#852)
- Fix nonce ground-truth checks
- Revisit cardano integration tests to include stake, delegation and rewards (#854)
- *(cardano)* Remove many false positives from ground-truth checks

### ⚙️ Miscellaneous Tasks

- Fix clippy warnings across the board (#851)
- Tidy up xtask for shared use (#856)
- Update pallas v1.0.0-alpha.4 (#859)

### Release

- V1.0.0-rc.7

## [1.0.0-rc.6] - 2026-01-28

### 🚀 Features

- Track and ref-count witness-set datum values (#807)
- *(grpc)* Upgrade u5c server to 0.18.1 specs (#813)
- *(redb3)* Use bucketed keys for indexes (#836)
- *(redb3)* Turn tx index into single value table (#837)
- *(bin)* Add cardinality stats command
- Introduce LSM engine just for indexes (#843)
- Introduce LSM engine for state store
- Support storage backend switch via config
- Support fjall-specific storage configs
- *(fjall)* Expose engine settings via config (#845)
- Support per-store location paths
- *(cli)* Add command to import archive from immutable db
- Support no-op stores for index and archive
- *(redb)* Off-load block data to flat files (#848)
- *(bin)* Force warn+ logs on mithril bootstrap

### 🐛 Bug Fixes

- *(minibf)* Map metadata numeric values to json numbers
- *(fjall)* Use snapshot for reads instead of direct keyspace access
- *(cardano)* Avoid write-during-iter in estart and ewrap
- Add graceful domain shutdown procedure
- Make sure fjall logs show up on tracing output
- *(fjall)* Reduce number of keyspaces by using prefixed keys
- *(cardano)* Use whole chain summary for epoch calculation (#844)
- *(cli)* Skip starting point when resuming Mithril import
- *(cardano)* Use entity streaming for mem-heavy work units (#846)
- *(minibf)* Show logs on internal 500 errors
- *(cli)* Avoid scanning the skipped epoch during archive import
- *(minibf)* Add max scan limit for paginated endpoints (#849)
- Reduce level of excesive traces

### 🚜 Refactor

- Isolate indexes into its own storage (#841)
- Formalize work unit concept (#842)
- Make index interface chain-agnostic
- Turn datum state trackig into an entity
- Use persistent state for sharing reward data
- Remove async from domain trait
- Simplify adapter boilerplate

### 📚 Documentation

- Introduce basic AGENTS.md

### 🧪 Testing

- Fix toy domain implementation

### ⚙️ Miscellaneous Tasks

- Fix lint warnings
- Gitignore opencode files

### Release

- V1.0.0-rc.6

## [1.0.0-rc.5] - 2026-01-07

### 🚀 Features

- *(trp)* Update tx3 framework to v0.14 (#824)

### 🐛 Bug Fixes

- *(cardano)* Handle pool re-registrations (#822)

### 🚜 Refactor

- *(trp)* Remove dependency on tx3-sdk (#816)

### 🧪 Testing

- *(trp)* Use tx3 sdk just for testing (#821)

### ⚙️ Miscellaneous Tasks

- Update tx3 deps to v0.14.2

### Release

- V1.0.0-rc.5

## [1.0.0-rc.4] - 2025-12-18

### 🚀 Features

- Introduce mempool-aware utxo store

### 🐛 Bug Fixes

- Remove stage timeout (#811)
- Use correct condition for notifying tip events (#812)
- *(cardano)* Drop delegators for retiring dreps (#817)
- *(cardano)* Avoid dropping delegators on retired pools (#818)
- *(cardano)* Improve speed of RUPD work log (#819)

### 🔧 Continuous Integration

- Include adhoc branches on docker builds
- Remove x86 apple binary build

### ⚙️ Miscellaneous Tasks

- Fix lint warnings (#820)

### Release

- V1.0.0-rc.4

## [1.0.0-rc.3] - 2025-12-04

### 🚀 Features

- *(cardano)* Introduce custom utxos via config (#796)
- *(trp)* Implement utxo temp locking mechanism (#800)
- Introduce relaxed arithmetic option (#809)
- *(minibf)* Introduce metrics endpoint (#810)

### 🐛 Bug Fixes

- *(minibf)* Fetch pool metadata in parallel (#790)
- Enable relaxed mode for Pallas (#792)
- Honor until-tip sync option (#545)
- *(cardano)* Burn fees for Byron epoch pots (#804)
- *(cardano)* Track MIRs as part of epoch rewards (#806)
- Apply pending refactors to windows code

### 🚜 Refactor

- Adjust Mithril features to simplify build process (#795)
- Unify all configs into single core location (#799)
- Turn on relaxed mode by default

### 📚 Documentation

- Update chain config schema reference (#794)

### ⚙️ Miscellaneous Tasks

- *(cardano)* Update missing hack for preview network (#791)
- Update Pallas to v1.0.0-alpha.3
- Remove legacy test-vectors crate (#797)
- Unify both redb crates (#798)
- *(minibf)* Update blockfrost-openapi to v0.1.83 (#801)

### Release

- V1.0.0-rc.3

## [1.0.0-rc.2] - 2025-11-13

### 🚀 Features

- *(cardano)* Support genesis bootstrap of custom staking (#788)

### 🐛 Bug Fixes

- *(cli)* Remove bad force protocol in preprod init (#782)
- *(cardano)* Make sure sync can continue after stop epoch (#786)
- *(cardano)* Make genesis nonce aware of protocol (#783)
- *(minibf)* Adjust conformance of pools and rewards endpoints (#784)
- *(minibf)* Map circulating supply to BF semantics (#789)

### ⚙️ Miscellaneous Tasks

- Fix broken tests (#781)
- *(trp)* Update tx3 deps to v0.12 (#785)
- *(cardano)* Reduce excessive log levels (#787)

### Release

- V1.0.0-rc.2

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

### Release

- V1.0.0-rc.1

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
