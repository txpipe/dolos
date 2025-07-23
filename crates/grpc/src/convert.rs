use bytes::Bytes;
use pallas::crypto::hash::Hash;
use tonic::Status;

pub fn bytes_to_hash32(data: &Bytes) -> Result<Hash<32>, Status> {
    let array: [u8; 32] = data
        .as_ref()
        .try_into()
        .map_err(|_| Status::invalid_argument("invalid hash value, needs to be 32-bytes long"))?;

    Ok(Hash::<32>::new(array))
}
