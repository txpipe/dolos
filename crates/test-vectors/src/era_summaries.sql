WITH 

slot AS (
  SELECT epoch_no AS epoch, MAX(slot_no) AS slot
  FROM block
  GROUP BY 1
),

time AS (
  SELECT start_time, end_time, no AS epoch
  FROM epoch
)


SELECT 
  DISTINCT protocol_major as "protocol_major", 
  epoch_no as "epoch", 
  slot.slot,
  time.start_time,
  time.end_time
FROM param_proposal
JOIN slot ON param_proposal.epoch_no = slot.epoch
JOIN time ON param_proposal.epoch_no = time.epoch
WHERE protocol_major IS NOT NULL
AND epoch_no <= {{ epoch }}
ORDER BY epoch_no
