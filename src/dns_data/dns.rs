use crate::dns_data::{DnsData, parse_vk};
use domain::base::{Rtype, ToName};
use domain::rdata::Txt;
use domain::resolv::StubResolver;
use ed25519_dalek::VerifyingKey;

pub struct DNS {
    pub resolver: StubResolver,
}

impl DnsData for DNS {
    /// Returns the first TXT record on `name` that parses as a verifying key.
    async fn get_dns_vk(&self, name: impl ToName) -> Option<VerifyingKey> {
        let response = self
            .resolver
            .query((name.to_name::<Vec<u8>>(), Rtype::TXT))
            .await
            .ok()?;

        response
            .answer()
            .ok()?
            .limit_to::<Txt<_>>()
            .flatten()
            // text() joins multi-string records, unlike as_flat_slice()
            .find_map(|record| parse_vk(&record.data().text::<Vec<u8>>()))
    }
}
