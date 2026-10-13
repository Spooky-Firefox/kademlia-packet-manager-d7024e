pub mod dns;
pub mod fake_dns;
use ed25519_dalek::{PUBLIC_KEY_LENGTH, VerifyingKey};

pub trait DnsData: Send + Sync {
    // The domain is a plain string because that is how records carry it.
    // A string that is not a valid domain name gives `None`, like a domain
    // with no key.
    //
    // The future is `Send`, and the resolver `Send + Sync`, so a STORE
    // handler generic over it can still run inside `tokio::spawn`; an
    // `async fn` in the impl satisfies it.
    fn get_dns_vk(&self, domain: &str) -> impl Future<Output = Option<VerifyingKey>> + Send;
}

pub enum DnsBackend {
    Real(dns::Dns),
    Fake(fake_dns::FakeDns),
}

impl DnsData for DnsBackend {
    async fn get_dns_vk(&self, domain: &str) -> Option<VerifyingKey> {
        match self {
            DnsBackend::Real(dns) => dns.get_dns_vk(domain).await,
            DnsBackend::Fake(dns) => dns.get_dns_vk(domain).await,
        }
    }
}

impl<T: DnsData> DnsData for std::sync::Arc<T> {
    fn get_dns_vk(&self, domain: &str) -> impl Future<Output = Option<VerifyingKey>> + Send {
        self.as_ref().get_dns_vk(domain)
    }
}

pub const TXT_PREFIX: &str = "d7024ePK=";

pub fn format_vk(vk: &VerifyingKey) -> String {
    format!("{TXT_PREFIX}{}", hex::encode(vk.to_bytes()))
}

pub fn parse_vk(txt: &[u8]) -> Option<VerifyingKey> {
    let hex_key = txt.trim_ascii().strip_prefix(TXT_PREFIX.as_bytes())?;
    let mut bytes = [0u8; PUBLIC_KEY_LENGTH];
    hex::decode_to_slice(hex_key, &mut bytes).ok()?;
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
    fn format_then_parse_round_trips() {
        let txt = format_vk(&test_vk());

        assert!(txt.starts_with(TXT_PREFIX));
        assert_eq!(parse_vk(txt.as_bytes()), Some(test_vk()));
    }

    #[test]
    fn ignores_surrounding_whitespace() {
        let txt = format!("  {}\n", format_vk(&test_vk()));

        assert_eq!(parse_vk(txt.as_bytes()), Some(test_vk()));
    }

    #[test]
    fn rejects_bad_records() {
        let hex_key = hex::encode(test_vk().to_bytes());

        assert_eq!(parse_vk(b""), None);
        assert_eq!(parse_vk(b"v=spf1 -all"), None);
        assert_eq!(parse_vk(hex_key.as_bytes()), None, "missing prefix");
        assert_eq!(
            parse_vk(format!("{TXT_PREFIX}{}", &hex_key[..62]).as_bytes()),
            None
        );
        assert_eq!(
            parse_vk(format!("{TXT_PREFIX}{hex_key}00").as_bytes()),
            None
        );
    }
}
