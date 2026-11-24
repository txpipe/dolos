use bytes::Bytes;
use dolos_core::DomainError;
use pallas::crypto::hash::Hash;
use tonic::Status;

/// The status of a failed chain stream. The chain may catch up with a cursor
/// that has no overlap, or fill an empty WAL, so those two tell a client to
/// retry.
pub fn chain_stream_status(context: &str, err: &DomainError) -> Status {
    let message = format!("{context}: {err}");

    match err {
        DomainError::NoOverlap(_) | DomainError::WalIsEmpty => Status::unavailable(message),
        _ => Status::internal(message),
    }
}

pub fn bytes_to_hash32(data: &Bytes) -> Result<Hash<32>, Status> {
    let array: [u8; 32] = data
        .as_ref()
        .try_into()
        .map_err(|_| Status::invalid_argument("invalid hash value, needs to be 32-bytes long"))?;

    Ok(Hash::<32>::new(array))
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;

    #[test]
    fn only_a_recoverable_stream_failure_tells_the_client_to_retry() {
        let code = |err| chain_stream_status("failed", &err).code();

        assert_eq!(code(DomainError::NoOverlap(10)), Code::Unavailable);
        assert_eq!(code(DomainError::WalIsEmpty), Code::Unavailable);
        assert_eq!(code(DomainError::Internal("x".into())), Code::Internal);
    }
}
