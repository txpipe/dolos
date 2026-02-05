# Preview Conformance Evaluation

## Executive Summary

| Category | In Scope | Passing | Failing | Won't Fix |
|----------|----------|---------|---------|-----------|
| accounts | 33 | 31 | 2 | 0 |
| addresses | 82 | 82 | 0 | 0 |
| assets | 24 | 24 | 0 | 0 |
| blocks | 69 | 69 | 0 | 0 |
| epochs | 3 | 2 | 1 | 0 |
| genesis | 1 | 1 | 0 | 0 |
| governance | 6 | 6 | 0 | 0 |
| metadata | 18 | 18 | 0 | 0 |
| network | 2 | 1 | 1 | 0 |
| pools | 15 | 9 | 6 | 0 |
| txs | 33 | 33 | 0 | 0 |
| **Total** | **286** | **276** | **10** | **0** |

**Passing Rate:** 96.5% (276/286)
## Won't Fix

*No endpoints currently marked as Won't Fix.*

To add Won't Fix items, edit the `preview_wontfix.json` file.

## Outstanding Failures (Needs Fix)

### ACCOUNTS

#### /accounts/{stake_address} (2 failures)

**URL:** `accounts/stake_test1uq3f3kt99hu4e3vt7cnx6uya88qjjw52yexh56qcknqkj9qa0awyd`
- **Test:** accounts/:stake_address when DRep is retired all delegators to that DRep should have their drep_id cleared.
- **Issue:** DRep clearing logic not implemented - drep_id should be null for retired DReps

**URL:** `accounts/stake_test1upvjras0sny422fesgr9yhq0cjnqjmzk8as08qsjvlr37ng796phq`
- **Test:** accounts/:stake_address retire and register drep after voting. should have their drep_id cleared.
- **Issue:** DRep clearing logic not implemented - drep_id should be null for retired DReps

---

### EPOCHS

#### /epochs/{number}/parameters (1 failures)

**URL:** `epochs/4/parameters`
- **Test:** epochs/:number/parameters epoch - costModels.PlutusV1
- **Issue:** Test expectation mismatch

---

### NETWORK

#### /network/eras (1 failures)

**URL:** `network/eras`
- **Test:** network eras
- **Issue:** Network eras boundary issue

---

### POOLS

#### /pools/extended (6 failures)

**URL:** `pools/extended?count=1&page=1`
- **Test:** pools/extended output matches data returned from /pools/:pool_id and /pools/:pool_id/metadata
- **Issue:** Extended pool data endpoint issue

**URL:** `pools/extended?count=1&page=2`
- **Test:** pools/extended output matches data returned from /pools/:pool_id and /pools/:pool_id/metadata
- **Issue:** Extended pool data endpoint issue

**URL:** `pools/extended?count=3&page=4`
- **Test:** pools/extended output matches data returned from /pools/:pool_id and /pools/:pool_id/metadata
- **Issue:** Extended pool data endpoint issue

**URL:** `pools/extended?count=3&page=3`
- **Test:** pools/extended output matches data returned from /pools/:pool_id and /pools/:pool_id/metadata
- **Issue:** Extended pool data endpoint issue

**URL:** `pools/extended?count=5&page=3&order=asc`
- **Test:** pools/extended?queryparams
- **Issue:** Test expectation mismatch

**URL:** `pools/extended?count=5&page=3`
- **Test:** pools/extended?queryparams
- **Issue:** Test expectation mismatch

---
