pub mod dns;
use domain::base::name::ToName;
use ed25519_dalek::{PUBLIC_KEY_LENGTH, VerifyingKey};

pub trait DnsData {
    async fn get_dns_vk(&self, name: impl ToName) -> Option<VerifyingKey>;
}

/// Parses TXT record text holding a hex-encoded ed25519 verifying key.
pub fn parse_vk(txt: &[u8]) -> Option<VerifyingKey> {
    let mut bytes = [0u8; PUBLIC_KEY_LENGTH];
    // errors on wrong length as well as on non-hex input
    hex::decode_to_slice(txt.trim_ascii(), &mut bytes).ok()?;
    VerifyingKey::from_bytes(&bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn test_vk() -> VerifyingKey {
        SigningKey::from_bytes(&[7; 32]).verifying_key()
    }

    #[test]
    fn parses_hex_key() {
        let txt = hex::encode(test_vk().to_bytes());

        assert_eq!(parse_vk(txt.as_bytes()), Some(test_vk()));
    }

    #[test]
    fn ignores_surrounding_whitespace() {
        let txt = format!("  {}\n", hex::encode(test_vk().to_bytes()));

        assert_eq!(parse_vk(txt.as_bytes()), Some(test_vk()));
    }

    #[test]
    fn rejects_bad_records() {
        let hex_key = hex::encode(test_vk().to_bytes());

        assert_eq!(parse_vk(b""), None);
        assert_eq!(parse_vk(b"v=spf1 -all"), None);
        assert_eq!(parse_vk(hex_key[..62].as_bytes()), None);
        assert_eq!(parse_vk(format!("{hex_key}00").as_bytes()), None);
    }
}
