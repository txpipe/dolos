WITH queried_epoch AS (
  SELECT 
    no AS "epoch_no",
    drep_activity
  FROM epoch e
  JOIN epoch_param ep ON (ep.epoch_no = e.no)
  WHERE e.no = {{ epoch }}
  ORDER BY e.no DESC
),

calculated_active AS (
  SELECT
    dh.id AS drep_hash_id,
    (
      CASE
        WHEN (
          SELECT COALESCE(MAX(dr.tx_id), 1)
          FROM drep_registration dr
          JOIN tx ON tx.id = dr.tx_id
	        JOIN block b ON b.id = tx.block_id
          WHERE b.epoch_no <= {{ epoch }} AND dr.drep_hash_id = dh.id AND dr.deposit > 0
        ) > (
          SELECT COALESCE(MAX(dr.tx_id), -1)
          FROM drep_registration dr
          JOIN tx ON tx.id = dr.tx_id
	        JOIN block b ON b.id = tx.block_id
          WHERE b.epoch_no <= {{ epoch }} AND dr.drep_hash_id = dh.id AND dr.deposit < 0
        ) THEN true
        ELSE false
      END
    ) AS registered,
    
    (
      SELECT MIN(b.slot_no)
      FROM drep_registration dr
      JOIN tx ON tx.id = dr.tx_id
      JOIN block b ON b.id = tx.block_id
      WHERE b.epoch_no <= {{ epoch }} AND dr.drep_hash_id = dh.id AND dr.deposit > 0
    ) AS initial_slot,
    
    (
      SELECT MAX(combined_epochs.slot_no)
      FROM (
        -- From drep_registration
        SELECT b.slot_no
        FROM drep_registration dr
        JOIN tx ON tx.id = dr.tx_id
        JOIN block b ON b.id = tx.block_id
        WHERE b.epoch_no <= {{ epoch }} AND dr.drep_hash_id = dh.id
        
        UNION

        -- From voting_procedure
        SELECT b.slot_no
        FROM voting_procedure vp
        JOIN tx ON vp.tx_id = tx.id
        JOIN block b ON b.id = tx.block_id
        WHERE b.epoch_no <= {{ epoch }} AND vp.drep_voter = dh.id
      ) combined_epochs
    ) AS last_active_slot
  FROM drep_hash dh
)

SELECT 
  dh.view AS "drep_id",
  COALESCE(dd.amount, 0)::TEXT AS "voting_power",
  NOT ca.registered AS "retired",
  ca.initial_slot,
  ca.last_active_slot
FROM drep_hash dh
  LEFT JOIN calculated_active ca ON dh.id = ca.drep_hash_id
  LEFT JOIN drep_distr dd ON (
    dh.id = dd.hash_id
    AND dd.epoch_no = (
      SELECT epoch_no
      FROM queried_epoch where epoch_no <= {{ epoch }}
    )
  )

{{ limit }}
