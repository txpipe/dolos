SELECT 
  encode(ma.policy, 'hex') || encode(ma.name, 'hex') AS "key",
  SUM(mtm.quantity)::TEXT AS "quantity",
  (
    SELECT encode(tx.hash, 'hex')
    FROM tx
    WHERE tx.id = MIN(mtm.tx_id)
  ) AS "initial_tx",
  (
    SELECT block.slot_no
    FROM tx
    JOIN block ON tx.block_id = block.id
    WHERE tx.id = MIN(mtm.tx_id)
  ) AS "initial_slot",
  COUNT(*) AS mint_tx_count
FROM ma_tx_mint mtm
JOIN multi_asset ma ON (mtm.ident = ma.id)
JOIN tx on tx.id = mtm.tx_id
JOIN block on block.id = tx.block_id
WHERE block.epoch_no <= {{ epoch }}
GROUP BY policy, name

{{ limit }}
