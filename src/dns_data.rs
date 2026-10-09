pub mod dns;
use domain::base::name::ToName;
use ed25519_dalek::VerifyingKey;

use crate::dns_data::VerifyingStoreError::KeyDecodeError;

pub trait DnsData {
    async fn get_dns_vk(&self, name: impl ToName) -> Result<VerifyingKeyStore, ()>;
}

pub enum VerifyingKeyStore {
    V1(VerifyingKey),
}

pub enum VerifyingStoreError {
    UnableToSplitRecord,
    UnknownVersion,
    HexDecodeError(hex::FromHexError),
    KeyDecodeError(),
}
impl From<hex::FromHexError> for VerifyingStoreError {
    fn from(value: hex::FromHexError) -> Self {
        VerifyingStoreError::HexDecodeError(value)
    }
}

pub fn verifying_store_from_str(str: &str) -> Result<VerifyingKeyStore, VerifyingStoreError> {
    let (v, k) = str
        .split_once(";")
        .ok_or(VerifyingStoreError::UnableToSplitRecord)?;
    let k = k.trim();

    match v.strip_prefix("v=") {
        Some("1") => Ok(VerifyingKeyStore::V1(
            // TODO this unwrap
            VerifyingKey::from_bytes(hex::decode(k)?.as_array().unwrap())
                .map_err(|_| KeyDecodeError())?,
        )),
        _ => return Err(VerifyingStoreError::UnknownVersion),
    }
}
