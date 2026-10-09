use std::{net::IpAddr, str::FromStr};

use domain::base::Rtype;
use domain::base::name::Name;
use domain::rdata::Txt;
use domain::resolv::StubResolver;

use tokio;
#[tokio::main]
async fn main() {
    let res = StubResolver::new();
    let name: Name<Vec<u8>> = Name::from_str("ronstad.se.ronstad.se").unwrap();
    let res = res.query((name, Rtype::TXT)).await.unwrap();

    for x in res.answer().iter().flat_map(|x| x.limit_to::<Txt<_>>()) {
        println!("{:?}", x.unwrap().data())
    }
}
