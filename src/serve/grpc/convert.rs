use bytes::Bytes;
use dolos_core::hash::Hash as CoreHash;
use tonic::Status;

pub fn bytes_to_hash32(data: &Bytes) -> Result<CoreHash<32>, Status> {
    let array: [u8; 32] = data
        .as_ref()
        .try_into()
        .map_err(|_| Status::invalid_argument("invalid hash value, needs to be 32-bytes long"))?;

    Ok(CoreHash::<32>::new(array))
}
