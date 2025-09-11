SELECT MAX(block.slot_no) AS slot
FROM block
WHERE block.epoch_no <= {{ epoch }}
