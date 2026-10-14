use std::str::FromStr;

use crate::dns_data::{DnsData, parse_vk};
use domain::base::{Name, Rtype};
use domain::rdata::Txt;
use domain::resolv::StubResolver;
use ed25519_dalek::VerifyingKey;

pub struct Dns {
    pub resolver: StubResolver,
}

impl Dns {
    /// A resolver using the system's configuration (`/etc/resolv.conf`).
    pub fn new() -> Self {
        Dns {
            resolver: StubResolver::new(),
        }
    }
}

impl DnsData for Dns {
    /// Returns the first TXT record on `domain` that parses as a verifying key.
    async fn get_dns_vk(&self, domain: &str) -> Option<VerifyingKey> {
        let name: Name<Vec<u8>> = Name::from_str(domain).ok()?;
        //let response = self.resolver.query((name, Rtype::TXT)).await.ok()?;
        let response = StubResolver::new().query((name, Rtype::TXT)).await.ok()?;
        response
            .answer()
            .ok()?
            .limit_to::<Txt<_>>()
            .flatten()
            // text() joins multi-string records, unlike as_flat_slice()
            .find_map(|record| parse_vk(&record.data().text::<Vec<u8>>()))
    }
}
