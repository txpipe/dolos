WITH 

total_supply AS (
  SELECT 45000000000000000 - reserves
  FROM ada_pots
  WHERE epoch_no = {{ epoch }}
  LIMIT 1
), 

queried_pools AS (
  SELECT ph.id AS "pool_hash_id",
    ph.view AS "pool_id",
    encode(ph.hash_raw, 'hex') AS "hex",
    encode(pu.vrf_key_hash, 'hex') AS "vrf_keyhash",
    sa.view AS "reward_account",
    pu.margin,
    pu.fixed_cost,
    pu.declared_pledge,
    (
      SELECT COUNT(*)
      FROM block b
        JOIN slot_leader sl ON (sl.id = b.slot_leader_id)
      WHERE sl.pool_hash_id = ph.id
      AND b.epoch_no <= {{ epoch }}
    ) AS "blocks_minted",
     (
      SELECT json_build_object(
        'hash', encode(pmr.hash, 'hex'),
        'url', pmr.url,
        'ticker', pod.ticker_name,
        'name', COALESCE(pod.json->>'name', NULL),
        'description', COALESCE(pod.json->>'description', NULL),
        'homepage', COALESCE(pod.json->>'homepage', NULL)
      )
      FROM pool_metadata_ref pmr
      -- Note: Simple LEFT JOIN LEFT JOIN off_chain_pool_data pod ON pod.pmr_id = pmr.id
      -- Is not enough as the pmr record may reference failed fetch.
      -- By joining on pod.hash = pmr.hash we reuse previously fetched off-chain data (with the same hash)
      -- LATERAL join is needed because they may be multiple pod/pmr records matching the hash
      LEFT JOIN LATERAL (
        SELECT *
        FROM off_chain_pool_data pod
        JOIN tx ON tx.id = pmr.registered_tx_id
        JOIN block ON tx.block_id = block.id
        WHERE pod.hash = pmr.hash
        AND block.epoch_no <= {{ epoch }}
        ORDER BY pmr.registered_tx_id DESC
        LIMIT 1
      ) pod ON TRUE
      WHERE pmr.id = pu.meta_id
    ) AS "metadata"
  FROM pool_hash ph
    JOIN (
      SELECT pu.hash_id,
        pu.vrf_key_hash,
        pu.reward_addr_id,
        pu.registered_tx_id AS "max_registered_tx_id",
        pu.cert_index AS "update_cert_index",
        pu.margin as "margin",
        pu.fixed_cost::TEXT AS "fixed_cost",
        pu.pledge::TEXT AS "declared_pledge",
        pu.meta_id
      FROM pool_update pu
        JOIN (
          SELECT hash_id,
            MAX(registered_tx_id) AS tempmax
          FROM pool_update
          JOIN tx ON tx.id = pool_update.registered_tx_id
          JOIN block ON tx.block_id = block.id
          WHERE block.epoch_no <= {{ epoch }}
          GROUP BY hash_id
        ) pumax ON (pumax.hash_id = pu.hash_id)
        AND (pumax.tempmax = pu.registered_tx_id)
    ) pu ON (pu.hash_id = ph.id)
    LEFT JOIN (
      SELECT pr.hash_id,
        pr.announced_tx_id AS "max_announced_tx_id",
        pr.retiring_epoch AS "retiring_epoch",
        pr.cert_index AS "retire_cert_index"
      FROM pool_retire pr
        JOIN (
          SELECT hash_id,
            MAX(announced_tx_id) AS tempmax
          FROM pool_retire
          JOIN tx ON tx.id = pool_retire.announced_tx_id
          JOIN block ON tx.block_id = block.id
          WHERE block.epoch_no <= {{ epoch }}
          GROUP BY hash_id
        ) prmax ON (prmax.hash_id = pr.hash_id)
        AND (prmax.tempmax = pr.announced_tx_id)
    ) pr ON (pr.hash_id = ph.id)
    LEFT JOIN stake_address sa ON (sa.id = pu.reward_addr_id)
  WHERE (
      retiring_epoch IS NULL
      OR (
        max_announced_tx_id IS NOT NULL
        AND (
          max_registered_tx_id > max_announced_tx_id
          OR (
            max_registered_tx_id < max_announced_tx_id
            AND retiring_epoch > {{ epoch }}
          )
        )
      )
      OR (max_announced_tx_id IS NULL)
      OR (
        max_registered_tx_id = max_announced_tx_id
        AND update_cert_index > retire_cert_index
      )
    )
  GROUP BY ph.id,
    pu.margin,
    pu.fixed_cost,
    pu.declared_pledge,
    pu.meta_id,
    ph.hash_raw,
    pu.vrf_key_hash,
    sa.view
),

live_stake_accounts AS (
  SELECT d.addr_id AS "stake_address_id",
    d.pool_hash_id AS "pool_hash_id"
  FROM delegation d
    JOIN stake_registration sr ON (sr.addr_id = d.addr_id)
    LEFT JOIN (
      SELECT addr_id,
        MAX(tx_id) AS tempmax
      FROM stake_deregistration
      JOIN tx ON tx.id = stake_deregistration.tx_id
      JOIN block ON tx.block_id = block.id
      WHERE block.epoch_no <= {{ epoch }}
      GROUP BY addr_id
    ) deregmax ON (deregmax.addr_id = d.addr_id)
    JOIN queried_pools qp ON (qp.pool_hash_id = d.pool_hash_id)
  WHERE d.id = (
      SELECT MAX(delegation.id)
      FROM delegation
      JOIN tx ON tx.id = delegation.tx_id
      JOIN block ON tx.block_id = block.id
      WHERE block.epoch_no <= {{ epoch }}
      AND addr_id = d.addr_id
    )
    AND sr.tx_id = (
      SELECT MAX(stake_registration.tx_id)
      FROM stake_registration
      JOIN tx ON tx.id = stake_registration.tx_id
      JOIN block ON tx.block_id = block.id
      WHERE block.epoch_no <= {{ epoch }}
      AND addr_id = d.addr_id
    )
    AND (
      (
        deregmax.tempmax IS NOT NULL
        AND sr.tx_id > (
          SELECT MAX(stake_deregistration.tx_id)
          FROM stake_deregistration
          JOIN tx ON tx.id = stake_deregistration.tx_id
          JOIN block ON tx.block_id = block.id
          WHERE block.epoch_no <= {{ epoch }}
          AND addr_id = d.addr_id
        )
      )
      OR (deregmax.tempmax IS NULL)
    )
),

live_stake_accounts_amounts AS (
  SELECT pool_hash_id,
    COALESCE(SUM(txo.value), 0) AS "amounts_pool"
  FROM live_stake_accounts lsa
    JOIN tx_out txo USING (stake_address_id)
    JOIN tx ON txo.tx_id = tx.id
    JOIN block ON tx.block_id = block.id
    LEFT JOIN (
      SELECT tx_out_id, tx_out_index
      FROM tx_in
      JOIN tx ON tx.id = tx_in.tx_in_id
      JOIN block ON block.id = tx.block_id
      WHERE block.epoch_no < {{ epoch }}
    ) txi ON (txo.tx_id = txi.tx_out_id)
    AND (txo.index = txi.tx_out_index)
  WHERE txi IS NULL
  AND block.epoch_no < {{ epoch }}
  GROUP BY pool_hash_id
),

live_stake_accounts_rewards AS (
  SELECT lsa.pool_hash_id,
    COALESCE(SUM(amount), 0) AS "amount_rewards_pool"
  FROM live_stake_accounts lsa
    JOIN reward r ON (lsa.stake_address_id = r.addr_id)
  WHERE type <> 'refund'
    AND spendable_epoch <= {{ epoch }}
  GROUP BY lsa.pool_hash_id
),

live_stake_accounts_instant_rewards AS (
  SELECT lsa.pool_hash_id,
    COALESCE(SUM(amount), 0) AS "amount_instant_rewards_pool"
  FROM live_stake_accounts lsa
    JOIN reward_rest rr ON (lsa.stake_address_id = rr.addr_id)
  WHERE spendable_epoch <= {{ epoch }}
  GROUP BY lsa.pool_hash_id
),

live_stake_accounts_refunds AS (
  SELECT lsa.pool_hash_id,
    COALESCE(SUM(amount), 0) AS "amount_refunds_pool"
  FROM live_stake_accounts lsa
    JOIN reward r ON (lsa.stake_address_id = r.addr_id)
  WHERE type = 'refund'
    AND spendable_epoch <= {{ epoch }}
  GROUP BY lsa.pool_hash_id
),

live_stake_accounts_withdrawal AS (
  SELECT pool_hash_id,
    COALESCE(SUM(amount), 0) AS "amount_withdrawals_pool"
  FROM live_stake_accounts lsa
    JOIN withdrawal w ON (lsa.stake_address_id = w.addr_id)
  GROUP BY pool_hash_id
),

live_stake_queried_pools_sum AS (
  SELECT qp.pool_hash_id AS "pool_hash_id",
    (
      (COALESCE(amounts_pool, 0)) + (COALESCE(amount_rewards_pool, 0)) + (COALESCE(amount_instant_rewards_pool, 0)) + (COALESCE(amount_refunds_pool, 0)) - (COALESCE(amount_withdrawals_pool, 0))
    ) AS "live_stake_pool"
  FROM queried_pools qp
    LEFT JOIN live_stake_accounts_amounts USING (pool_hash_id)
    LEFT JOIN live_stake_accounts_rewards USING (pool_hash_id)
    LEFT JOIN live_stake_accounts_instant_rewards USING (pool_hash_id)
    LEFT JOIN live_stake_accounts_refunds USING (pool_hash_id)
    LEFT JOIN live_stake_accounts_withdrawal USING (pool_hash_id)
  GROUP BY pool_hash_id,
    amounts_pool,
    amount_rewards_pool,
    amount_instant_rewards_pool,
    amount_refunds_pool,
    amount_withdrawals_pool
),

owners AS (
  SELECT pu.hash_id AS pool_hash_id, json_agg(sa.view ORDER BY sa.view) AS owners
  FROM pool_owner po
  JOIN pool_update pu ON (pu.id = po.pool_update_id)
  JOIN stake_address sa ON (sa.id = po.addr_id)
  JOIN pool_hash ph ON (ph.id = pu.hash_id)
  JOIN tx ON pu.registered_tx_id = tx.id
  JOIN block ON block.id = tx.block_id
  WHERE block.epoch_no <= {{ epoch }}
  GROUP BY 1
),

relays AS (
  SELECT 
    pu.hash_id AS pool_hash_id,
    JSON_AGG(JSON_BUILD_OBJECT(
      'ipv4', pr.ipv4,
      'ipv6', pr.ipv6,
      'dns', pr.dns_name,
      'dns_srv_name', pr.dns_srv_name,
      'port', pr.port
    )) AS relays
  FROM tx
  JOIN pool_update pu ON (tx.id = pu.registered_tx_id)
  JOIN pool_relay pr ON (pu.id = pr.update_id)
  JOIN block ON block.id = tx.block_id
  WHERE block.epoch_no <= {{ epoch }}
  GROUP BY 1
)

SELECT 
  qp.vrf_keyhash AS "key",

  qp.vrf_keyhash AS "vrf_keyhash",
  qp.reward_account AS "reward_account",

  COALESCE(
    (
      SELECT COALESCE(SUM(es.amount), 0)
      FROM epoch_stake es
      WHERE es.pool_id = qp.pool_hash_id
        AND es.epoch_no = {{ epoch }}
    ),
    0
  )::TEXT AS "active_stake",

  COALESCE(
    (
      SELECT COALESCE(SUM(es.amount), 0)
      FROM epoch_stake es
      WHERE es.pool_id = qp.pool_hash_id
        AND es.epoch_no = {{ epoch }} + 1
    ),
    0
  )::TEXT AS "wait_stake",
  -- cast to TEXT to avoid number overflow
  (
    COALESCE(
      (
        SELECT live_stake_pool
        FROM live_stake_queried_pools_sum lsqps
        WHERE qp.pool_hash_id = lsqps.pool_hash_id
      ),
      0
    )
  )::TEXT AS "live_stake", -- cast to TEXT to avoid number overflow
  (
    COALESCE(
      (
        SELECT live_stake_pool
        FROM live_stake_queried_pools_sum lsqps
        WHERE qp.pool_hash_id = lsqps.pool_hash_id
      ) / (
        (
          SELECT *
          FROM total_supply
        ) / (
          SELECT optimal_pool_count
          FROM epoch_param
          WHERE epoch_no = {{ epoch }}
          LIMIT 1
        )
      ), 0
    )
  )::FLOAT AS "live_saturation",
  qp.margin as "margin_cost",
  qp.blocks_minted,
  qp.fixed_cost,
  qp.declared_pledge,

  MAX(qp.metadata::TEXT)::JSON AS metadata,
  MAX(owners.owners::TEXT)::JSON AS owners,
  MAX(relays.relays::TEXT)::JSON AS relays

FROM queried_pools qp
LEFT JOIN owners ON owners.pool_hash_id = qp.pool_hash_id
LEFT JOIN relays ON relays.pool_hash_id = qp.pool_hash_id
GROUP BY qp.pool_hash_id,
  qp.pool_id,
  qp.hex,
  qp.margin,
  qp.fixed_cost,
  qp.declared_pledge,
  qp.vrf_keyhash,
  qp.reward_account,
  qp.blocks_minted

{{ limit }}
