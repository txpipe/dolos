# Preprod Conformance Evaluation

## Executive Summary

| Category | In Scope | Passing | Failing | Won't Fix |
|----------|----------|---------|---------|-----------|
| accounts | 28 | 24 | 4 | 0 |
| addresses | 73 | 65 | 8 | 0 |
| assets | 24 | 20 | 4 | 0 |
| blocks | 70 | 70 | 0 | 0 |
| epochs | 4 | 4 | 0 | 0 |
| genesis | 1 | 1 | 0 | 0 |
| governance | 4 | 4 | 0 | 0 |
| metadata | 18 | 18 | 0 | 0 |
| network | 2 | 2 | 0 | 0 |
| pools | 15 | 9 | 6 | 0 |
| txs | 36 | 35 | 1 | 0 |
| **Total** | **275** | **252** | **23** | **0** |

**Passing Rate:** 91.6% (252/275)
## Won't Fix

*No endpoints currently marked as Won't Fix.*

To add Won't Fix items, edit the `preprod_wontfix.json` file.

## Outstanding Failures (Needs Fix)

### ACCOUNTS

#### /accounts/{stake_address} (2 failures)

**URL:** `accounts/stake_test1upvjras0sny422fesgr9yhq0cjnqjmzk8as08qsjvlr37ng796phq`
- **Test:** accounts/:stake_address retire and register drep after voting. should have their drep_id cleared.
- **Issue:** DRep clearing logic not implemented - drep_id should be null for retired DReps

**URL:** `accounts/stake_test1urmus498k7r299azjvhh50c9044zwqxgqfuqqrj3m46y8ucef0hex`
- **Test:** accounts/:stake_address - BF account
- **Issue:** Test expectation mismatch

#### /accounts/{stake_address}/rewards (2 failures)

**URL:** `accounts/stake_test1uz55sf04mkd29tehvf4pu95vjhd6e72a50tcycje88jgcysxnh7d8/rewards?count=5&page=1`
- **Test:** accounts/:stake_address?queryparams generic stake address rewards with multiple types
- **Issue:** Test expectation mismatch

**URL:** `accounts/stake_test1uz55sf04mkd29tehvf4pu95vjhd6e72a50tcycje88jgcysxnh7d8/rewards?count=5&page=1&order=asc`
- **Test:** accounts/:stake_address?queryparams generic stake address rewards with multiple types
- **Issue:** Test expectation mismatch

---

### ADDRESSES

#### /addresses/{address}/transactions (8 failures)

**URL:** `addresses/addr_test1wrrgep77m0v8uv5unauluwgyr7pmdr2827wgye3sx5aw7yg7z2dsu/transactions?page=1011&count=6`
- **Test:** addresses/:address/transactions generic dormant shelley address
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_test1wrrgep77m0v8uv5unauluwgyr7pmdr2827wgye3sx5aw7yg7z2dsu/transactions?page=1011&count=6&order=asc`
- **Test:** addresses/:address/transactions generic dormant shelley address
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1c6xg0hkmmplr98yl08lrjpqlswmg636hnjpxvvp48th3zsq296f/transactions?page=1011&count=6`
- **Test:** addresses/:address/transactions generic dormant shelley address
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1c6xg0hkmmplr98yl08lrjpqlswmg636hnjpxvvp48th3zsq296f/transactions?page=1011&count=6&order=asc`
- **Test:** addresses/:address/transactions generic dormant shelley address
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_test1wrrgep77m0v8uv5unauluwgyr7pmdr2827wgye3sx5aw7yg7z2dsu/transactions?page=1011&count=99&order=desc`
- **Test:** addresses/:address/transactions generic dormant shelley address desc
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1c6xg0hkmmplr98yl08lrjpqlswmg636hnjpxvvp48th3zsq296f/transactions?page=1011&count=99&order=desc`
- **Test:** addresses/:address/transactions generic dormant shelley address desc
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1pjggm5nyjkll8amnrsh4hvjz6zsdvr9knmnlsgtgczls7x9qy3y/transactions?order=asc&page=420`
- **Test:** addresses/:address/transactions generic payment_cred 1
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1pjggm5nyjkll8amnrsh4hvjz6zsdvr9knmnlsgtgczls7x9qy3y/transactions?order=asc&page=42000&count=1`
- **Test:** addresses/:address/transactions generic payment_cred 2
- **Issue:** Test expectation mismatch

---

### ASSETS

#### /assets/{asset} (4 failures)

**URL:** `assets/d207b461ecbdd1756277bd99a232558f077ed6e3cdc2712dad9a44fb000de140426c6f636b66726f737423303136`
- **Test:** assets/:asset - Blockfrost CIP68v2 NFT asset with unknown fields (utf8 and non-utf8) (+multiple metadata updates)
- **Issue:** Metadata parsing/formatting issue

**URL:** `assets/7badd4d46fea0f78c6ab10268083023c665c7119f7b8a6da08366264000de140426c6f636b66726f737423303139`
- **Test:** assets/:asset - Blockfrost CIP68v2 NFT asset non-utf8 prop (0xdeadbeef) and utf8 prop ("deadbeef")
- **Issue:** Metadata parsing/formatting issue

**URL:** `assets/d207b461ecbdd1756277bd99a232558f077ed6e3cdc2712dad9a44fb000de140426c6f636b66726f737423303136`
- **Test:** assets/:asset - Blockfrost CIP68v2 NFT asset with unknown fields (utf8 and non-utf8) (+multiple metadata updates)
- **Issue:** Metadata parsing/formatting issue

**URL:** `assets/7badd4d46fea0f78c6ab10268083023c665c7119f7b8a6da08366264000de140426c6f636b66726f737423303139`
- **Test:** assets/:asset - Blockfrost CIP68v2 NFT asset non-utf8 prop (0xdeadbeef) and utf8 prop ("deadbeef")
- **Issue:** Metadata parsing/formatting issue

---

### POOLS

#### /pools/extended (6 failures)

**URL:** `pools/extended?count=1&page=1`
- **Test:** pools/extended output matches data returned from /pools/:pool_id and /pools/:pool_id/metadata
- **Issue:** Extended pool data endpoint issue

**URL:** `pools/extended?count=1&page=2`
- **Test:** pools/extended output matches data returned from /pools/:pool_id and /pools/:pool_id/metadata
- **Issue:** Extended pool data endpoint issue

**URL:** `pools/extended?count=3&page=3`
- **Test:** pools/extended output matches data returned from /pools/:pool_id and /pools/:pool_id/metadata
- **Issue:** Extended pool data endpoint issue

**URL:** `pools/extended?count=3&page=4`
- **Test:** pools/extended output matches data returned from /pools/:pool_id and /pools/:pool_id/metadata
- **Issue:** Extended pool data endpoint issue

**URL:** `pools/extended?count=5&page=3`
- **Test:** pools/extended?queryparams
- **Issue:** Test expectation mismatch

**URL:** `pools/extended?count=5&page=3&order=asc`
- **Test:** pools/extended?queryparams
- **Issue:** Test expectation mismatch

---

### TXS

#### /txs/{hash}/pool_updates (1 failures)

**URL:** `txs/6d8f5d067eb0d4cb0569514551ac83ae9c7a532e4b8e2d23126252cf443ab19e/pool_updates`
- **Test:** txs/:tx/pool_updates - generic shelley with pool certs
- **Issue:** Pool update data format issue

---
