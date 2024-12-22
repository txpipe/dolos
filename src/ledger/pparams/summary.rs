use pallas::applying::MultiEraProtocolParameters;

#[derive(Clone, Debug)]
pub struct EraBoundary {
    pub epoch: u64,
    pub slot: u64,
    pub timestamp: chrono::DateTime<chrono::FixedOffset>,
}

#[derive(Clone, Debug)]
pub struct EraSummary {
    pub start: EraBoundary,
    pub end: Option<EraBoundary>,
    pub protocol_version: usize,
    pub epoch_length: u64,
    pub slot_length: u64,
}

#[derive(Debug)]
pub struct ChainSummary {
    past: Vec<EraSummary>,
    current: Option<EraSummary>,
}

impl ChainSummary {
    pub fn start(initial_pparams: &MultiEraProtocolParameters) -> Self {
        Self {
            past: vec![],
            current: Some(EraSummary {
                start: EraBoundary {
                    epoch: 0,
                    slot: 0,
                    timestamp: initial_pparams.system_start(),
                },
                end: None,
                protocol_version: initial_pparams.protocol_version(),
                epoch_length: initial_pparams.epoch_length(),
                slot_length: initial_pparams.slot_length(),
            }),
        }
    }

    pub fn advance(&mut self, end_epoch: u64, pparams: &MultiEraProtocolParameters) {
        let mut current = self.current.take().unwrap();
        let epoch_delta = end_epoch - current.start.epoch;

        let slot_delta = epoch_delta * current.epoch_length;
        let end_slot = current.start.slot + slot_delta;
        let second_delta = slot_delta * current.slot_length;
        let end_timestamp =
            current.start.timestamp + chrono::Duration::seconds(second_delta as i64);

        let boundary = EraBoundary {
            epoch: end_epoch,
            slot: end_slot,
            timestamp: end_timestamp,
        };

        current.end = Some(boundary.clone());
        self.past.push(current);

        self.current = Some(EraSummary {
            start: boundary.clone(),
            end: None,
            protocol_version: pparams.protocol_version(),
            epoch_length: pparams.epoch_length(),
            slot_length: pparams.slot_length(),
        });
    }
}
