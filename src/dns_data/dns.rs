use crate::dns_data::{self, DnsData, VerifyingKeyStore, verifying_store_from_str};
use domain::base::Rtype;
use domain::rdata::Txt;
use domain::resolv::StubResolver;

pub struct DNS {
    pub resolver: StubResolver,
}

impl DnsData for DNS {
    async fn get_dns_vk(&self, name: impl domain::base::ToName) -> Result<VerifyingKeyStore, ()> {
        if let Ok(r) = self
            .resolver
            .query((name.to_name::<Vec<u8>>(), Rtype::TXT))
            .await
        {
            r.answer()
                .iter()
                .flat_map(|x| x.limit_to::<Txt<_>>())
                .find_map(|x| {
                    if let Ok(x) = x {
                        verifying_store_from_str(
                            str::from_utf8(x.data().as_flat_slice().unwrap()).unwrap(),
                        )
                        .ok()
                    } else {
                        None
                    }
                })
                .ok_or(())
        } else {
            Err(())
        }
    }
}
