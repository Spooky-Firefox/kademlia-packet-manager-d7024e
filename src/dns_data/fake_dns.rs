//! In-memory stand-in for DNS, preloaded with [`FAKE_KEYS`]. Lookups go
//! through the same [`parse_vk`] as the real resolver.

use std::str::FromStr;

use crate::dns_data::{DnsData, format_vk, parse_vk};
use domain::base::{Name, ToName};
use ed25519_dalek::{PUBLIC_KEY_LENGTH, SECRET_KEY_LENGTH, SigningKey, VerifyingKey};

/// A hardcoded test identity: the domain it is published under and its key pair (hex).
pub struct FakeKey {
    pub domain: &'static str,
    pub secret: &'static str,
    pub public: &'static str,
}

impl FakeKey {
    pub fn signing_key(&self) -> Option<SigningKey> {
        let mut bytes = [0u8; SECRET_KEY_LENGTH];
        hex::decode_to_slice(self.secret, &mut bytes).ok()?;
        Some(SigningKey::from_bytes(&bytes))
    }

    pub fn verifying_key(&self) -> Option<VerifyingKey> {
        let mut bytes = [0u8; PUBLIC_KEY_LENGTH];
        hex::decode_to_slice(self.public, &mut bytes).ok()?;
        VerifyingKey::from_bytes(&bytes).ok()
    }
}

pub const FAKE_KEYS: [FakeKey; 5] = [
    FakeKey {
        domain: "node0.d7024e.test",
        secret: "db0e232ecbf4392f6024d7d60f2a607e76806386eef28f49ce7c545199f225df",
        public: "5156ad53f8a8149e5e3b563c242f529fc868696a2a3f10d28f38dea5566d399d",
    },
    FakeKey {
        domain: "node1.d7024e.test",
        secret: "9bac258b10589885bfd4f1e66f568e13d55d397e30bd32bf03bee22ef0afb647",
        public: "8bbb1a371a13f8b249611a97b87db20eb78c88807450e928d564683945f07d86",
    },
    FakeKey {
        domain: "node2.d7024e.test",
        secret: "347a651033fd5862f274252ba5cbaff0acdb0f0c80e00cc4ec9a1178ce6c5b17",
        public: "64444f23d6b8558aff9db73edc5aa009e53fd15fc317a015432e6faa7ec56afa",
    },
    FakeKey {
        domain: "node3.d7024e.test",
        secret: "185b7ba881d096e99a11d53025753f9bf5329b39f233dc4eefd19e394272ce91",
        public: "6bde32011a06049181c4650096ab5482bbad9a27dca4f8283847aa3f8af7f92a",
    },
    FakeKey {
        domain: "node4.d7024e.test",
        secret: "473b42df5527cac51674af7ed40cecc3b060245a03c17cae6e36b71f5c0a5998",
        public: "87fbdb8b3f9e64330ece6f18830b0826f9872a96bacba9a7870078e2b96d1faf",
    },
];

/// TXT records as (domain, text), looked up the way a resolver would.
pub struct FakeDns {
    pub records: Vec<(Name<Vec<u8>>, String)>,
}

impl FakeDns {
    /// One `d7024ePK=` TXT record per entry in [`FAKE_KEYS`].
    pub fn new() -> Self {
        let records = FAKE_KEYS
            .iter()
            .filter_map(|key| {
                Some((
                    Name::from_str(key.domain).ok()?,
                    format_vk(&key.verifying_key()?),
                ))
            })
            .collect();
        FakeDns { records }
    }
}

impl DnsData for FakeDns {
    async fn get_dns_vk(&self, name: impl ToName) -> Option<VerifyingKey> {
        self.records
            .iter()
            .filter(|(domain, _)| domain.name_eq(&name))
            .find_map(|(_, txt)| parse_vk(txt.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, Verifier};

    fn name(s: &str) -> Name<Vec<u8>> {
        Name::from_str(s).unwrap()
    }

    #[test]
    fn hardcoded_pairs_match() {
        for key in &FAKE_KEYS {
            let signing = key.signing_key().unwrap();

            assert_eq!(
                Some(signing.verifying_key()),
                key.verifying_key(),
                "{}",
                key.domain
            );
        }
    }

    #[test]
    fn has_one_record_per_key() {
        assert_eq!(FakeDns::new().records.len(), FAKE_KEYS.len());
    }

    #[tokio::test]
    async fn lookup_returns_published_key() {
        let dns = FakeDns::new();

        for key in &FAKE_KEYS {
            assert_eq!(dns.get_dns_vk(name(key.domain)).await, key.verifying_key());
        }
    }

    #[tokio::test]
    async fn looked_up_key_verifies_signature() {
        let dns = FakeDns::new();
        let key = &FAKE_KEYS[2];
        let signature = key.signing_key().unwrap().sign(b"hello");

        let vk = dns.get_dns_vk(name(key.domain)).await.unwrap();

        assert!(vk.verify(b"hello", &signature).is_ok());
    }

    #[tokio::test]
    async fn unknown_domain_is_none() {
        assert_eq!(
            FakeDns::new().get_dns_vk(name("nobody.d7024e.test")).await,
            None
        );
    }

    #[tokio::test]
    async fn skips_unrelated_txt_records() {
        let mut dns = FakeDns::new();
        let key = &FAKE_KEYS[0];
        dns.records
            .insert(0, (name(key.domain), "v=spf1 -all".to_string()));

        assert_eq!(dns.get_dns_vk(name(key.domain)).await, key.verifying_key());
    }
}
