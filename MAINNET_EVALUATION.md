# Mainnet Conformance Evaluation

## Executive Summary

| Category | In Scope | Passing | Failing | Won't Fix |
|----------|----------|---------|---------|-----------|
| accounts | 32 | 24 | 8 | 0 |
| addresses | 109 | 89 | 20 | 0 |
| assets | 14 | 9 | 5 | 0 |
| blocks | 83 | 59 | 24 | 0 |
| epochs | 11 | 1 | 10 | 0 |
| genesis | 1 | 1 | 0 | 0 |
| governance | 10 | 8 | 2 | 0 |
| metadata | 18 | 18 | 0 | 0 |
| network | 2 | 0 | 2 | 0 |
| pools | 16 | 9 | 7 | 0 |
| txs | 62 | 57 | 5 | 0 |
| **Total** | **358** | **275** | **83** | **0** |

**Passing Rate:** 76.8% (275/358)
## Won't Fix

*No endpoints currently marked as Won't Fix.*

To add Won't Fix items, edit the `mainnet_wontfix.json` file.

## Outstanding Failures (Needs Fix)

### ACCOUNTS

#### /accounts/{stake_address} (2 failures)

**URL:** `accounts/stake1ux3dy2p970cv2lsqvl4nqxwj7c878tgs6a6h9yekk3pr27g70l5g4`
- **Test:** accounts/:stake_address retired drep
- **Issue:** DRep clearing logic not implemented - drep_id should be null for retired DReps

**URL:** `accounts/stake1ux3dy2p970cv2lsqvl4nqxwj7c878tgs6a6h9yekk3pr27g70l5g4`
- **Test:** accounts/:stake_address when DRep is retired all delegators to that DRep should have their drep_id cleared.
- **Issue:** DRep clearing logic not implemented - drep_id should be null for retired DReps

#### /accounts/{stake_address}/rewards (6 failures)

**URL:** `accounts/stake1u9fzg77vrgfqlplkjqe9hntdcvsurpvxd60yp2fhn73002qsv9pdk/rewards?count=3&page=2`
- **Test:** accounts/:stake_address?queryparams generic stake address rewards
- **Issue:** Test expectation mismatch

**URL:** `accounts/stake1u9fzg77vrgfqlplkjqe9hntdcvsurpvxd60yp2fhn73002qsv9pdk/rewards?count=3&page=2&order=asc`
- **Test:** accounts/:stake_address?queryparams generic stake address rewards
- **Issue:** Test expectation mismatch

**URL:** `accounts/stake1uxa6lm0x9ezhywczl8rs048mmvn396qtk0w4z2tzu2cytuqs0e38d/rewards?count=4&page=38`
- **Test:** accounts/:stake_address?queryparams generic stake address rewards with multiple types
- **Issue:** Test expectation mismatch

**URL:** `accounts/stake1uxa6lm0x9ezhywczl8rs048mmvn396qtk0w4z2tzu2cytuqs0e38d/rewards?count=4&page=38&order=asc`
- **Test:** accounts/:stake_address?queryparams generic stake address rewards with multiple types
- **Issue:** Test expectation mismatch

**URL:** `accounts/stake1uyr7kdys3kmruysratwqzjpx0ya8rjsh8t68d2573yp3g0cr05y2r/rewards?count=1&page=11`
- **Test:** accounts/:stake_address?queryparams generic stake address rewards with multiple types
- **Issue:** Test expectation mismatch

**URL:** `accounts/stake1uyr7kdys3kmruysratwqzjpx0ya8rjsh8t68d2573yp3g0cr05y2r/rewards?count=1&page=11&order=asc`
- **Test:** accounts/:stake_address?queryparams generic stake address rewards with multiple types
- **Issue:** Test expectation mismatch

---

### ADDRESSES

#### /addresses/{address}/transactions (20 failures)

**URL:** `addresses/addr1zxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tvpw288a4x0xf8pxgcntelxmyclq83s0ykeehchz2wtspks905plm/transactions?page=923`
- **Test:** addresses/addr1zxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tvpw288a4x0xf8pxgcntelxmyclq83s0ykeehchz2wtspks905plm/transactions precached response
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr1w999n67e86jn6xal07pzxtrmqynspgx0fwmcmpua4wc6yzsxpljz3/transactions?page=423`
- **Test:** addresses/addr1w999n67e86jn6xal07pzxtrmqynspgx0fwmcmpua4wc6yzsxpljz3/transactions precached response
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr1w999n67e86jn6xal07pzxtrmqynspgx0fwmcmpua4wc6yzsxpljz3/transactions?page=423&from=0&to=6666034`
- **Test:** addresses/addr1w999n67e86jn6xal07pzxtrmqynspgx0fwmcmpua4wc6yzsxpljz3/transactions precached response
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr1w999n67e86jn6xal07pzxtrmqynspgx0fwmcmpua4wc6yzsxpljz3/transactions?page=423&from=0:0&to=6666034:9`
- **Test:** addresses/addr1w999n67e86jn6xal07pzxtrmqynspgx0fwmcmpua4wc6yzsxpljz3/transactions precached response
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr1w999n67e86jn6xal07pzxtrmqynspgx0fwmcmpua4wc6yzsxpljz3/transactions?page=423&from=0:128&to=6666034:10`
- **Test:** addresses/addr1w999n67e86jn6xal07pzxtrmqynspgx0fwmcmpua4wc6yzsxpljz3/transactions precached response
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions?page=42000`
- **Test:** addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions precached response 1
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions?page=42000&order=asc`
- **Test:** addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions precached response 1
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions?page=42000&from=0:0&to=9001129:4`
- **Test:** addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions precached response 1
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions?page=42000&from=0:777&to=9001129:444`
- **Test:** addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions precached response 1
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions?order=asc&page=69000&count=1`
- **Test:** addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions precached response 2
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions?order=asc&page=69000&count=1&from=0:0&to=7861897:0`
- **Test:** addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions precached response 2
- **Issue:** Test expectation mismatch

**URL:** `addresses/DdzFFzCqrhstmqBkaU98vdHu6PdqjqotmgudToWYEeRmQKDrn4cAgGv9EZKtu1DevLrMA1pdVazufUCK4zhFkUcQZ5Gm88mVHnrwmXvT/transactions?order=desc&count=5&page=1&from=4377130:10&to=4376980:1`
- **Test:** addresses/:address/transactions generic dormant exchange byron address desc empty (reverse from to)
- **Issue:** Test expectation mismatch

**URL:** `addresses/DdzFFzCqrhstmqBkaU98vdHu6PdqjqotmgudToWYEeRmQKDrn4cAgGv9EZKtu1DevLrMA1pdVazufUCK4zhFkUcQZ5Gm88mVHnrwmXvT/transactions?order=desc&count=5&page=1&from=4377130:10&to=0:1`
- **Test:** addresses/:address/transactions generic dormant exchange byron address desc empty (reverse from to)
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions?order=asc&page=1&count=1&from=7861897&to=7861897`
- **Test:** addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions precached response 2
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh15ew2tzjwn364l2pszu7j5h9w63v2crrnl97m074w9elrk6zy2tc/transactions?page=337669&count=5`
- **Test:** addresses/:address/transactions generic payment_cred 3 - page with self tx
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh15ew2tzjwn364l2pszu7j5h9w63v2crrnl97m074w9elrk6zy2tc/transactions?order=asc&page=337669&count=5`
- **Test:** addresses/:address/transactions generic payment_cred 3 - page with self tx
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions?order=asc&page=1&count=1&from=7861897:0&to=7861897:0`
- **Test:** addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions precached response 2
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions?order=asc&page=1&count=1&from=7861896&to=7861897:1`
- **Test:** addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions precached response 2
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions?page=1&from=9001102:7&to=9001129:444`
- **Test:** addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions precached response 1
- **Issue:** Test expectation mismatch

**URL:** `addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions?count=100&from=9001102:7&to=9001129:444`
- **Test:** addresses/addr_vkh1jp520glspzqrakkg0tckrxrq7txumeqvy6v8xfdvuyu26sf89v5/transactions precached response 1
- **Issue:** Test expectation mismatch

---

### ASSETS

#### /assets/{asset} (5 failures)

**URL:** `assets/00000002df633853f6a47465c9496721d2d5b1291b8398016c0e87ae6e7574636f696e`
- **Test:** assets/:asset - all hail nutcoin!
- **Issue:** Test expectation mismatch

**URL:** `assets/d5e6bf0500378d4f0da4e8dde6becec7621cd8cbf5cbb9b87013d4cc537061636542756433343132`
- **Test:** assets/:asset - space bud with metadata update which is not the latest tx_mint
- **Issue:** Test expectation mismatch

**URL:** `assets/0e14267a8020229adc0184dd25fa3174c3f7d6caadcb4425c70e7c04756e7369673033323839`
- **Test:** assets/:asset - non-valid according to https://cips.cardano.org/cips/cip25/
- **Issue:** Metadata parsing/formatting issue

**URL:** `assets/026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570390dbfc3f92cdaebc581ec7c28e35da8b92c87cabf981511698df52d7ea61c70`
- **Test:** assets/:asset - asset with metadata
- **Issue:** Test expectation mismatch

**URL:** `assets/fca746f58adf9f3da13b7227e5e2c6052f376447473f4d49f8004195000de140436974697a656e202330313333`
- **Test:** assets/:asset - Asset with CIP68v1 metadata with string encoded as array(*) 9F tag (introduced in CIP68v3, but we tolerate it in previous version of the standard)
- **Issue:** Metadata parsing/formatting issue

---

### BLOCKS

#### /blocks/{hash_or_number} (4 failures)

**URL:** `blocks/5f20df933584822601f9e3f8c024eb5eb252fe8cefb24d1317dc3d432e940ebb`
- **Test:** blocks/:hash_or_number - hash - genesis
- **Issue:** Test expectation mismatch

**URL:** `blocks/89d9b5a5b8ddc8d7e5a6795e9774d97faf1efea59b2caf7eaf9f8c5b32059df4`
- **Test:** blocks/:hash_or_number - hash - first boundary
- **Issue:** Test expectation mismatch

**URL:** `blocks/471ee59bee5cadc22ec85c4519acd4ee6f843eb30b34793db6ba1a9eb0426afb`
- **Test:** blocks/:hash_or_number - hash - generic boundary
- **Issue:** Test expectation mismatch

**URL:** `blocks/1`
- **Test:** blocks/:hash_or_number - first blocks 1/4
- **Issue:** Test expectation mismatch

#### /blocks/{hash_or_number}/next (9 failures)

**URL:** `blocks/471ee59bee5cadc22ec85c4519acd4ee6f843eb30b34793db6ba1a9eb0426afb/next?count=2`
- **Test:** blocks/:hash_or_number/next?queryparams - generic boundary
- **Issue:** Test expectation mismatch

**URL:** `blocks/21579/next?count=10`
- **Test:** blocks/:hash_or_number/next?queryparams - generic boundary inside
- **Issue:** Test expectation mismatch

**URL:** `blocks/d8bbdc12fc56d7b3fa9de6de325f8f139beaf5962ee147a0107bd19b931b1eba/next?count=10`
- **Test:** blocks/:hash_or_number/next?queryparams - generic boundary inside
- **Issue:** Test expectation mismatch

**URL:** `blocks/21586/next?count=5`
- **Test:** blocks/:hash_or_number/next?queryparams - generic boundary edge
- **Issue:** Test expectation mismatch

**URL:** `blocks/3bd04916b6bc2ad849d519cfae4ffe3b1a1660c098dbcd3e884073dd54bc8911/next?count=5`
- **Test:** blocks/:hash_or_number/next?queryparams - generic boundary edge
- **Issue:** Test expectation mismatch

**URL:** `blocks/8d9309b0aa8faf0f4df797efb4e3af9b88072bef3f8177bd159befc186944649/next`
- **Test:** blocks/:hash_or_number/next - generic shelley
- **Issue:** Test expectation mismatch

**URL:** `blocks/5058570/next`
- **Test:** blocks/:hash_or_number/next - generic shelley
- **Issue:** Test expectation mismatch

**URL:** `blocks/8d9309b0aa8faf0f4df797efb4e3af9b88072bef3f8177bd159befc186944649/next?count=2&page=2`
- **Test:** blocks/:hash_or_number/next-previous?queryparams - generic shelley
- **Issue:** Test expectation mismatch

**URL:** `blocks/5058570/next?count=2&page=2`
- **Test:** blocks/:hash_or_number/next-previous?queryparams - generic shelley
- **Issue:** Test expectation mismatch

#### /blocks/{hash_or_number}/previous (11 failures)

**URL:** `blocks/5058577/previous?count=2&page=2`
- **Test:** blocks/:hash_or_number/next-previous?queryparams - generic shelley
- **Issue:** Test expectation mismatch

**URL:** `blocks/d323bf6587d92da6d61e6a3adcfef81f7598fbf5fe52ab7de19d09a933e30c1a/previous?count=2&page=2`
- **Test:** blocks/:hash_or_number/next-previous?queryparams - generic shelley
- **Issue:** Test expectation mismatch

**URL:** `blocks/518336/previous?count=3`
- **Test:** blocks/:hash_or_number/previous?queryparams - generic boundary inside 1
- **Issue:** Test expectation mismatch

**URL:** `blocks/232b18231c885fd09ff7d643fd2536a64b07d4566860535f5bea7034d30cb630/previous?count=3`
- **Test:** blocks/:hash_or_number/previous?queryparams - generic boundary inside 1
- **Issue:** Test expectation mismatch

**URL:** `blocks/f74fe59538d1c3a68842d0072e3aa68818e681d0ba45fd993a8bc9051fc4af3a/previous`
- **Test:** blocks/:hash_or_number/previous - generic shelley
- **Issue:** Test expectation mismatch

**URL:** `blocks/5058671/previous`
- **Test:** blocks/:hash_or_number/previous - generic shelley
- **Issue:** Test expectation mismatch

**URL:** `blocks/fd509e014462437d1786934ec6b622a705aab62318c87107a0f245b4cb404a83/previous?page=298020&count=5`
- **Test:** blocks/:hash_or_number/previous?queryparams - generic boundary
- **Issue:** Test expectation mismatch

**URL:** `blocks/4009725/previous?page=398832&count=10`
- **Test:** blocks/:hash_or_number/previous?queryparams - generic boundary inside 2
- **Issue:** Test expectation mismatch

**URL:** `blocks/23dae10eef453b6978b94f3dcc58dfea8f8a39fc720bd0f4c84c87c73f488ca8/previous?page=398832&count=10`
- **Test:** blocks/:hash_or_number/previous?queryparams - generic boundary inside 2
- **Issue:** Test expectation mismatch

**URL:** `blocks/4009732/previous?page=797664&count=5`
- **Test:** blocks/:hash_or_number/previous?queryparams - generic boundary edge
- **Issue:** Test expectation mismatch

**URL:** `blocks/476c64be0d3dea51073beaf4efe02a355ebac1c484aed54ceeb662b4696e8b63/previous?page=797664&count=5`
- **Test:** blocks/:hash_or_number/previous?queryparams - generic boundary edge
- **Issue:** Test expectation mismatch

---

### EPOCHS

#### /epochs/{number}/parameters (10 failures)

**URL:** `epochs/211/parameters`
- **Test:** epochs/:number/parameters generic shelley epoch
- **Issue:** Test expectation mismatch

**URL:** `epochs/208/parameters`
- **Test:** epochs/:number/parameters epoch - shelley - 1.HF
- **Issue:** Test expectation mismatch

**URL:** `epochs/236/parameters`
- **Test:** epochs/:number/parameters - allegra - 2.HF
- **Issue:** Test expectation mismatch

**URL:** `epochs/251/parameters`
- **Test:** epochs/:number/parameters - mary - 3.HF
- **Issue:** Test expectation mismatch

**URL:** `epochs/290/parameters`
- **Test:** epochs/:number/parameters - alonzo - 4.HF
- **Issue:** Test expectation mismatch

**URL:** `epochs/298/parameters`
- **Test:** epochs/:number/parameters - vasil - 5.HF
- **Issue:** Test expectation mismatch

**URL:** `epochs/306/parameters`
- **Test:** epochs/:number/parameters - vasil - 5.HF + 1. protocol update
- **Issue:** Test expectation mismatch

**URL:** `epochs/394/parameters`
- **Test:** epochs/:number/parameters - valentine (SECP) - 7.HF
- **Issue:** Test expectation mismatch

**URL:** `epochs/365/parameters`
- **Test:** epochs/:number/parameters - vasil - 6.HF
- **Issue:** Test expectation mismatch

**URL:** `epochs/444/parameters`
- **Test:** epochs/444/parameters
- **Issue:** Test expectation mismatch

---

### GOVERNANCE

#### /governance/dreps/{drep_id} (2 failures)

**URL:** `governance/dreps/drep1ygkfv2u7aazrfhqgh0qssramjd6av09rsh3ejd0rd95cmusezderl`
- **Test:** governance drep with multiple registrations and deregistrations (active)
- **Issue:** Governance DRep status issue

**URL:** `governance/dreps/drep1wmjtmutl6xrjud6g9ycyn25nww2g8p4xw2qgrqyevpevuv3p4jf`
- **Test:** governance drep with multiple registrations and deregistrations (inactive)
- **Issue:** Governance DRep status issue

---

### NETWORK

#### /network (1 failures)

**URL:** `network`
- **Test:** network test
- **Issue:** Test expectation mismatch

#### /network/eras (1 failures)

**URL:** `network/eras`
- **Test:** network eras
- **Issue:** Network eras boundary issue

---

### POOLS

#### /pools/extended (6 failures)

**URL:** `pools/extended?count=1&page=2`
- **Test:** pools/extended output matches data returned from /pools/:pool_id and /pools/:pool_id/metadata
- **Issue:** Extended pool data endpoint issue

**URL:** `pools/extended?count=1&page=1`
- **Test:** pools/extended output matches data returned from /pools/:pool_id and /pools/:pool_id/metadata
- **Issue:** Extended pool data endpoint issue

**URL:** `pools/extended?count=3&page=3`
- **Test:** pools/extended output matches data returned from /pools/:pool_id and /pools/:pool_id/metadata
- **Issue:** Extended pool data endpoint issue

**URL:** `pools/extended?count=3&page=4`
- **Test:** pools/extended output matches data returned from /pools/:pool_id and /pools/:pool_id/metadata
- **Issue:** Extended pool data endpoint issue

**URL:** `pools/extended?count=5&page=3&order=asc`
- **Test:** pools/extended?queryparams
- **Issue:** Test expectation mismatch

**URL:** `pools/extended?count=5&page=3`
- **Test:** pools/extended?queryparams
- **Issue:** Test expectation mismatch

#### /pools/{pool_id}/delegators (1 failures)

**URL:** `pools/pool1n2yl5u5ycyp07aj6np7clwhwdh7v734swrrpy2hcvmhhj953awm/delegators`
- **Test:** pools delegators of retired pools with MIRs
- **Issue:** Test expectation mismatch

---

### TXS

#### /txs/{hash} (2 failures)

**URL:** `txs/313223c9f0d09ee287148874f938ecf37fea7bf3c10a5d649522d0f40db1cf71`
- **Test:** txs/:tx - byron dust tx with hacky address and huge size
- **Issue:** Test expectation mismatch

**URL:** `txs/313223c9f0d09ee287148874f938ecf37fea7bf3c10a5d649522d0f40db1cf71`
- **Test:** txs/:tx - byron dust tx with hacky address and huge size
- **Issue:** Test expectation mismatch

#### /txs/{hash}/pool_updates (2 failures)

**URL:** `txs/28bd5e8c342ab89d6642e446cb299058ea36256af1718e4af9326898ce4192d7/pool_updates`
- **Test:** txs/:tx/pool_updates - generic shelley with pool certs
- **Issue:** Pool update data format issue

**URL:** `txs/6299278d563d92bc10cf77562a0437ae600d2b52941fdef45efcefec2f921160/pool_updates`
- **Test:** txs/:tx/pool_updates - shelley with pool update which does not have onchain reward address
- **Issue:** Pool update data format issue

#### /txs/{hash}/utxos (1 failures)

**URL:** `txs/927edb96f3386ab91b5f5d85d84cb4253c65b1c2f65fa7df25f81fab1d62987a/utxos`
- **Test:** txs/:tx/utxos - byron block 1 tx without inputs
- **Issue:** Test expectation mismatch

---
