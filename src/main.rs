use std::error::Error;
use std::net::SocketAddr;

mod bootstrap;
mod cli;
mod close_nodes;
mod handle_rpc;
mod hashing;
mod lookup;
mod node;
mod pending;
mod rpc;
mod rpc_transport;

use node::RealNode;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let mut args = std::env::args().skip(1);

    let Some(bind) = args.next() else {
        eprintln!("usage: cargo run -- IP:PORT [BOOTSTRAP_IP:PORT]");
        return Ok(());
    };

    let bind_address: SocketAddr = bind.parse()?;

    let bootstrap_address = match args.next() {
        Some(address) => Some(address.parse::<SocketAddr>()?),
        None => None,
    };

    if args.next().is_some() {
        eprintln!("usage: cargo run -- IP:PORT [BOOTSTRAP_IP:PORT]");
        return Ok(());
    }

    let node = RealNode::bind(bind_address).await?;

    println!("node address: {}", node.address());
    println!("node id:      {}", hex::encode(node.id()));

    if let Some(seed) = bootstrap_address {
        println!("bootstrapping through {seed}...");

        let neighbours = match bootstrap::bootstrap(node.rpc(), seed).await {
            Ok(n) => n,
            Err(e) => {
                eprintln!("bootstrap failed: {e}"); // uses your Display
                std::process::exit(1);
            }
        };
        println!("bootstrap complete; lookup returned {} contacts", neighbours.len());
    } else {
        println!("starting a new network");
    }

    cli::run(&node).await?;

    Ok(())
}
