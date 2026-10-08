use crate::close_nodes::{CloseNodes, Contact, ID_BYTES, Key};
use crate::lookup;
use crate::node::Node;
use crate::rpc_transport::RpcTransport;

use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::Instant;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader, Lines};

// vibecoded for better visual clarity
fn short_id(id: &[u8]) -> String {
    let encoded = hex::encode(id);

    if encoded.len() <= 8 {
        return encoded;
    }

    format!("{}…{}", &encoded[..4], &encoded[encoded.len() - 4..])
}

fn parse_key(input: &str) -> Result<Key, String> {
    let bytes = hex::decode(input).map_err(|_| "key must be hexadecimal".to_string())?;

    if bytes.len() != ID_BYTES {
        return Err(format!(
            "key must contain exactly {} hexadecimal characters",
            ID_BYTES * 2
        ));
    }

    let mut key = [0u8; ID_BYTES];
    key.copy_from_slice(&bytes);

    Ok(key)
}

fn show_routing_table<T: RpcTransport, U: RpcTransport>(node: &Node<T, U>) {
    let (siblings, buckets) = node.routing_snapshot();

    println!("siblings:");

    if siblings.is_empty() {
        println!("  <empty>");
    } else {
        for contact in siblings {
            println!("  {}  {}", short_id(&contact.id), contact.address);
        }
    }

    println!("buckets:");

    if buckets.is_empty() {
        println!("  <empty>");
    } else {
        for (index, contacts) in buckets {
            println!("  bucket {index}:");

            for contact in contacts {
                println!("    {}  {}", short_id(&contact.id), contact.address);
            }
        }
    }
}

fn show_datastore<T: RpcTransport, U: RpcTransport>(node: &Node<T, U>) {
    let values = node.datastore_snapshot();

    if values.is_empty() {
        println!("<empty>");
        return;
    }

    for (key, size) in values {
        println!("{}  {} bytes", short_id(&key), size);
    }
}

fn print_help() {
    println!("commands:");
    println!("  ping IP:PORT");
    println!("  put FILENAME");
    println!("  get KEY [FILENAME]");
    println!("  show rt");
    println!("  show ds");
    println!("  help");
    println!("  exit");
}

/// Read commands from stdin and run them as `node`, until `exit` or EOF.
///
/// Generic over the transports, so the same prompt drives a real node or one
/// on the in-process fake network.
pub async fn run<T: RpcTransport, U: RpcTransport>(node: &Node<T, U>) -> io::Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    run_with(node, &mut lines).await
}

/// [`run`], reading commands from `lines` instead of a reader of its own.
///
/// For a caller that also reads stdin between sessions: two buffered readers
/// on one stdin would each read ahead and swallow the other's lines.
pub async fn run_with<T, U, R>(node: &Node<T, U>, lines: &mut Lines<R>) -> io::Result<()>
where
    T: RpcTransport,
    U: RpcTransport,
    R: AsyncBufRead + Unpin,
{
    print_help();

    loop {
        print!("kademlia> ");
        io::stdout().flush()?;

        let Some(line) = lines.next_line().await? else {
            break;
        };

        let parts: Vec<&str> = line.split_whitespace().collect();

        if parts.is_empty() {
            continue;
        }

        match parts.as_slice() {
            ["ping", address] => {
                let address: SocketAddr = match address.parse() {
                    Ok(address) => address,
                    Err(_) => {
                        println!("invalid address");
                        continue;
                    }
                };

                let start = Instant::now();

                match node.rpc().ping(address).await {
                    Some(id) => {
                        let elapsed = start.elapsed();

                        // A successful manual ping is useful routing knowledge.
                        node.rpc()
                            .close_nodes()
                            .maybe_add_contact(Contact { id, address });

                        println!(
                            "pong from {} ({}) in {:.2?}",
                            address,
                            short_id(&id),
                            elapsed
                        );
                    }

                    None => {
                        println!("no response from {address}");
                    }
                }
            }

            ["put", filename] => {
                let value = match tokio::fs::read(filename).await {
                    Ok(value) => value,
                    Err(e) => {
                        println!("could not read {filename}: {e}");
                        continue;
                    }
                };

                let size = value.len();
                let key = lookup::store_value(node.rpc(), value).await;

                // PUT must print the complete key, not the abbreviated
                // debugging representation.
                println!("stored {size} bytes as {}", hex::encode(key));
            }

            ["get", key] => {
                let key = match parse_key(key) {
                    Ok(key) => key,
                    Err(e) => {
                        println!("{e}");
                        continue;
                    }
                };

                match lookup::lookup_value(node.rpc(), key).await {
                    Some((source, value)) => {
                        println!(
                            "received {} bytes from {} ({})",
                            value.len(),
                            source.address,
                            short_id(&source.id)
                        );

                        match std::str::from_utf8(&value) {
                            Ok(text) => println!("{text}"),
                            Err(_) => {
                                println!("binary value (hex): {}", hex::encode(value));
                            }
                        }
                    }

                    None => {
                        println!("value not found");
                    }
                }
            }

            ["get", key, filename] => {
                let key = match parse_key(key) {
                    Ok(key) => key,
                    Err(e) => {
                        println!("{e}");
                        continue;
                    }
                };

                match lookup::lookup_value(node.rpc(), key).await {
                    Some((source, value)) => {
                        if let Err(e) = tokio::fs::write(filename, &value).await {
                            println!("could not write {filename}: {e}");
                            continue;
                        }

                        println!(
                            "received {} bytes from {} ({}) and wrote {}",
                            value.len(),
                            source.address,
                            short_id(&source.id),
                            filename
                        );
                    }

                    None => {
                        println!("value not found");
                    }
                }
            }

            ["show", "rt"] => {
                show_routing_table(node);
            }

            ["show", "ds"] => {
                show_datastore(node);
            }

            ["help"] => {
                print_help();
            }

            ["exit"] => {
                break;
            }

            _ => {
                println!("unknown command; type 'help'");
            }
        }
    }

    Ok(())
}
