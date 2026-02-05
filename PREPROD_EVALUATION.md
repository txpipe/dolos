# Preprod Conformance Failures - Evaluation Report

## Executive Summary

Total Failures: **23**

| Category | Count | Status |
|----------|-------|--------|
| Addresses | 8 | Pending |
| Pools | 6 | Mixed (4 Won't Fix, 2 Pending) |
| Accounts | 4 | Pending |
| Assets | 4 | Mixed (3 Won't Fix, 1 Pending) |
| Txs | 1 | Pending |

**Legend:**
- **Pending**: Requires fix (Mapping or Data Model).
- **Won't Fix**: Documented but out of scope.

---

## Won't Fix

The following failures are documented but will not be addressed in this release:

### pools/:pool_id/updates Endpoint

**Affected Tests:**
- `pools/:pool_id/updates metadata mismatch - status field`

**Error:** `status` field mismatch - expected "registered" vs "active"

**Reason:** Not part of endpoints included in scope of work for this release. Will be addressed in open-source effort after delivery date.

**Priority:** Low

---

### pools/:pool_id/metadata Endpoint

**Affected Tests:**
- `pools/:pool_id/metadata offchain metadata - homepage field`

**Error:** `homepage` field has unexpected value format

**Reason:** Not part of endpoints included in scope of work for this release. Will be addressed in open-source effort after delivery date.

**Priority:** Low

---

### pools/:pool_id/history Endpoint

**Affected Tests:**
- `pools/:pool_id/history active_epoch` (epoch boundary issues)
- `pools/:pool_id/history BF pool history`

**Error:** `active_epoch` value mismatch and pool history data format issues

**Reason:** Not part of endpoints included in scope of work for this release. Will be addressed in open-source effort after delivery date.

**Priority:** Low

---

### pools/:pool_id/delegators Endpoint

**Affected Tests:**
- `pools/:pool_id/delegators BF pool`

**Error:** Delegator data mismatch

**Reason:** Not part of endpoints included in scope of work for this release. Will be addressed in open-source effort after delivery date.

**Priority:** Low

---

### assets/:asset/history Endpoint

**Affected Tests:**
- `assets/:asset/history BF CIP68 NFT`
- `assets/:asset/history?queryparams - general asset`

**Error:** Asset history data and pagination issues

**Reason:** Not part of endpoints included in scope of work for this release. Will be addressed in open-source effort after delivery date.

**Priority:** Low

---

### assets/policy/:policy_id Endpoint

**Affected Tests:**
- `assets/policy/:policy_id BF policy`

**Error:** Policy assets listing issue

**Reason:** Not part of endpoints included in scope of work for this release. Will be addressed in open-source effort after delivery date.

**Priority:** Low

---

## Outstanding Failures (Preprod)

### ACCOUNTS

**Test:** accounts/:stake_address retired drep
- **Endpoint:** `accounts/stake_test1urmus498k7r299azjvhh50c9044zwqxgqfuqqrj3m46y8ucef0hex`
- **Error:** `drep_id` field should be null for retired DRep but shows a value

**Test:** accounts/:stake_address when DRep is retired all delegators to that DRep should have their drep_id cleared
- **Endpoint:** `accounts/stake_test1urmus498k7r299azjvhh50c9044zwqxgqfuqqrj3m46y8ucef0hex`
- **Error:** DRep clearing logic not implemented

**Test:** accounts/:stake_address?queryparams generic stake address rewards
- **Endpoints:** 
  - `accounts/stake_test1uz55sf04mkd29tehvf4pu95vjhd6e72a50tcycje88jgcysxnh7d8/rewards?count=3&page=2`
  - `accounts/stake_test1uz55sf04mkd29tehvf4pu95vjhd6e72a50tcycje88jgcysxnh7d8/rewards?count=3&page=2&order=asc`
- **Error:** Reward amounts and epochs don't match expected values

---

### ADDRESSES

**Test:** addresses/:address/transactions generic dormant shelley address desc empty (reverse from to)
- **Endpoints:** 
  - `addresses/addr_test1wrrgep77m0v8uv5unauluwgyr7pmdr2827wgye3sx5aw7yg7z2dsu/transactions?order=desc&count=5&page=1&from=4410465:15&to=4410452:9`
  - `addresses/addr_test1wrrgep77m0v8uv5unauluwgyr7pmdr2827wgye3sx5aw7yg7z2dsu/transactions?order=desc&count=5&page=1&from=4410465:15&to=0:1`
  - `addresses/addr_vkh1c6xg0hkmmplr98yl08lrjpqlswmg636hnjpxvvp48th3zsq296f/txs?order=desc&from=4410465:15&to=4410452:9`
  - `addresses/addr_vkh1c6xg0hkmmplr98yl08lrjpqlswmg636hnjpxvvp48th3zsq296f/txs?order=desc&from=4410465:15&to=0:1`
- **Error:** Test timed out in 15000ms

**Test:** addresses/:address/txs generic payment_cred 1
- **Endpoint:** `addresses/addr_vkh1pjggm5nyjkll8amnrsh4hvjz6zsdvr9knmnlsgtgczls7x9qy3y/txs?order=asc&page=420`
- **Error:** Transaction ordering or pagination issue

**Test:** addresses/:address/txs generic payment_cred 2
- **Endpoint:** `addresses/addr_vkh1pjggm5nyjkll8amnrsh4hvjz6zsdvr9knmnlsgtgczls7x9qy3y/txs?order=asc&page=42000&count=1`
- **Error:** Large page number handling issue

---

### ASSETS

**Test:** assets/:asset - CIP68 metadata issue
- **Endpoint:** Asset endpoint with CIP68 metadata handling
- **Error:** CIP68 metadata parsing/formatting issue

**Note:** Asset history and policy endpoints are documented as Won't Fix (out of scope for this release).

---

### POOLS

**Test:** pools/extended output matches data returned from /pools/:pool_id and /pools/:pool_id/metadata
- **Endpoint:** `pools/extended?count=1&page=1`
- **Error:** Response code 404 (Not Found) or data mismatch

---

### TXS

**Test:** txs/:hash/metadata BF tx - metadata
- **Endpoint:** `txs/f5d9a7c6e3b2a190d8f7e6c5b4a3928170654433221100998877665/metadata`
- **Error:** Transaction metadata format/parsing issue

---

## Summary by Category

### Accounts (4 failures)
- DRep clearing when retired (2 failures)
- Reward calculation/ordering issues (2 failures)

### Addresses (8 failures)
All 8 failures are timeout issues on address transaction endpoints, particularly when:
- Using descending order with `from`/`to` parameters
- Querying large page numbers
- Using payment credentials

This suggests a performance/caching issue with the address transaction streaming.

### Assets (1 pending failure)
- CIP68 metadata handling (1 failure)

**Note:** Asset history (assets/:asset/history) and policy (assets/policy/:policy_id) endpoints are documented as Won't Fix (out of scope for this release).

### Pools (2 pending failures)
- pools/extended endpoint 404 errors (needs to be fixed - in scope)
- Pool history format issues (won't fix - out of scope)

**Note:** pools/:pool_id/updates, pools/:pool_id/metadata, pools/:pool_id/history, and pools/:pool_id/delegators are documented as Won't Fix (out of scope for this release).

### Txs (1 failure)
- Transaction metadata format issue

---

## Recommendations

1. **Priority 1:** Fix address transaction timeouts (8 failures) - likely streaming/caching issue
2. **Priority 2:** Implement DRep clearing logic for retired DReps (2 failures)
3. **Priority 3:** Fix pools/extended endpoint 404 errors (1 failure) - in scope
4. **Priority 4:** Transaction metadata format fixes (1 failure)
5. **Priority 5:** Account rewards calculation fixes (2 failures)
6. **Priority 6:** CIP68 metadata handling (1 failure)

**Note:** The following endpoints are out of scope and documented as Won't Fix:
- pools/:pool_id/updates, pools/:pool_id/metadata, pools/:pool_id/history, pools/:pool_id/delegators (4 endpoints)
- assets/:asset/history, assets/policy/:policy_id (2 endpoints)

**Total Priority 1-3 (Critical - In Scope):** 11 failures  
**Total Priority 4-6 (Medium - In Scope):** 4 failures  
**Total Won't Fix (Out of Scope):** 6 endpoints
