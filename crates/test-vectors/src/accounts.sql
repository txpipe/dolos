WITH

withdrawals AS (
  SELECT addr_id, SUM(amount) AS "amount"
  FROM withdrawal
  JOIN tx ON tx.id = withdrawal.tx_id
  JOIN block ON block.id = tx.block_id
  WHERE block.epoch_no <= {{ epoch }}
  GROUP BY addr_id
),

reserves AS (
  SELECT addr_id, SUM(amount) AS "amount"
  FROM reserve
  JOIN tx ON tx.id = reserve.tx_id
  JOIN block ON block.id = tx.block_id
  WHERE block.epoch_no <= {{ epoch }}
  GROUP BY addr_id
),

treasuries AS (
  SELECT addr_id, SUM(amount) AS "amount"
  FROM treasury
  JOIN tx ON tx.id = treasury.tx_id
  JOIN block ON block.id = tx.block_id
  WHERE block.epoch_no <= {{ epoch }}
  GROUP BY addr_id
),

instant_rewards AS (
  SELECT addr_id, SUM(amount) AS "amount"
  FROM reward_rest
  WHERE spendable_epoch <= {{ epoch }}
  GROUP BY addr_id
),

rewards AS (
  SELECT addr_id, SUM(amount) AS "amount"
  FROM reward
  WHERE spendable_epoch <= {{ epoch }}
  AND type <> 'refund'
  GROUP BY addr_id
),

refunds AS (
  SELECT addr_id, SUM(amount) AS "amount"
  FROM reward
  WHERE spendable_epoch <= {{ epoch }}
  AND type = 'refund'
  GROUP BY addr_id
),

all_utxos AS (
  SELECT sa.id AS addr_id, COALESCE(SUM(txo.value), 0) AS amount
  FROM stake_address sa
  JOIN tx_out txo ON (txo.stake_address_id = sa.id)
  LEFT JOIN tx_in txi ON (txo.tx_id = txi.tx_out_id)
    AND (txo.index = txi.tx_out_index)
  WHERE txi IS NULL
    AND txo.stake_address_id = sa.id
  GROUP BY 1
),

registered_at AS (
  SELECT addr_id, MIN(block.slot_no) AS slot 
  FROM stake_registration sa 
  JOIN tx ON tx.id = sa.tx_id 
  JOIN block ON tx.block_id = block.id
  WHERE block.epoch_no <= {{ epoch }}
  GROUP BY addr_id
),

pool AS (
  SELECT addr_id, ph."view" AS pool
  FROM (
    SELECT
    addr_id,
    pool_hash_id AS phid
    FROM (
      SELECT
        d.addr_id,
        d.tx_id,
        d.pool_hash_id,
        ROW_NUMBER() OVER (
          PARTITION BY d.addr_id
          ORDER BY d.pool_hash_id DESC
        ) AS rn
      FROM delegation AS d
      JOIN tx ON d.tx_id = tx.id
      JOIN block ON tx.block_id = block.id
      WHERE block.epoch_no <= {{ epoch }}
    ) AS subquery
    WHERE rn = 1
  ) addr_to_ph 
  JOIN pool_hash ph ON addr_to_ph.phid = ph.id
),

active_slots AS (
  SELECT addr_id, json_agg(slot ORDER BY slot) AS active_slots
  FROM (
    SELECT addr_id, block.slot_no AS slot
    FROM stake_registration sr
    JOIN tx ON sr.tx_id = tx.id
    JOIN block ON tx.block_id = block.id
    WHERE block.epoch_no <= {{ epoch }}

    UNION ALL

    SELECT addr_id, block.slot_no AS slot
    FROM stake_deregistration sd
    JOIN tx ON sd.tx_id = tx.id
    JOIN block ON tx.block_id = block.id
    WHERE block.epoch_no <= {{ epoch }}

    UNION ALL

    SELECT addr_id, block.slot_no AS slot
    FROM delegation d
    JOIN tx ON d.tx_id = tx.id
    JOIN block ON tx.block_id = block.id
    WHERE block.epoch_no <= {{ epoch }}
  ) slots
  GROUP BY 1
),

seen_addresses AS (
  SELECT sa.id AS addr_id, json_agg(DISTINCT txo.address ORDER BY txo.address) AS seen_addresses 
  FROM stake_address sa
  JOIN tx_out txo ON (txo.stake_address_id = sa.id)
  JOIN tx ON txo.tx_id = tx.id
  JOIN block ON tx.block_id = block.id
  LEFT JOIN tx_in txi ON (txo.tx_id = txi.tx_out_id)
    AND (txo.index = txi.tx_out_index)
  WHERE txi IS NULL
    AND txo.stake_address_id = sa.id
    AND block.epoch_no <= {{ epoch }}
  GROUP BY 1
),

reward_log AS (
  SELECT 
    sa.id AS addr_id,
    json_agg(json_build_object(
      'epoch', r.earned_epoch,
      'amount', r.amount::TEXT,
      'pool_id', ph.view,
      'as_leader', r.type = 'leader'
    )) AS reward_log
  FROM stake_address sa
  JOIN reward r ON (sa.id = r.addr_id)
  JOIN pool_hash ph ON (ph.id = r.pool_id)
  WHERE r.earned_epoch <= {{ epoch }}
  GROUP BY 1
)

SELECT
  sa.view AS "key",

  registered_at.slot AS registered_at,
  (
    COALESCE(all_utxos.amount) 
    + COALESCE(rewards.amount, 0) 
    + COALESCE(instant_rewards.amount, 0) 
    + COALESCE(refunds.amount, 0) 
    - COALESCE(withdrawals.amount, 0)
  )::TEXT AS "controlled_amount",
  (
    COALESCE(rewards.amount, 0) + COALESCE(instant_rewards.amount, 0)
  )::TEXT AS "rewards_sum",
  COALESCE(withdrawals.amount, 0)::TEXT AS "withdrawals_sum",
  COALESCE(reserves.amount, 0)::TEXT AS "reserves_sum",
  COALESCE(treasuries.amount, 0)::TEXT AS "treasury_sum",
  (
    COALESCE(rewards.amount, 0) 
    + COALESCE(instant_rewards.amount, 0) 
    + COALESCE(refunds.amount, 0) 
    - COALESCE(withdrawals.amount, 0)
  )::TEXT AS "withdrawable_amount",
  pool.pool AS pool_id,
  active_slots.active_slots,
  seen_addresses.seen_addresses,
  reward_log.reward_log

FROM stake_address sa
LEFT JOIN reward_log ON sa.id = reward_log.addr_id
LEFT JOIN seen_addresses ON sa.id = seen_addresses.addr_id
LEFT JOIN active_slots ON sa.id = active_slots.addr_id
LEFT JOIN pool ON sa.id = pool.addr_id
LEFT JOIN registered_at ON sa.id = registered_at.addr_id
LEFT JOIN all_utxos ON sa.id = all_utxos.addr_id
LEFT JOIN rewards ON sa.id = rewards.addr_id
LEFT JOIN withdrawals ON sa.id = withdrawals.addr_id
LEFT JOIN reserves ON sa.id = reserves.addr_id
LEFT JOIN treasuries ON sa.id = treasuries.addr_id
LEFT JOIN instant_rewards ON sa.id = instant_rewards.addr_id
LEFT JOIN refunds ON sa.id = refunds.addr_id

{{ limit }}
