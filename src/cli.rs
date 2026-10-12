use crate::close_nodes::{CloseNodes, Contact, ID_BYTES, Key};
use crate::dns_data::DnsData;
use crate::dns_data::{DnsBackend, fake_dns::FAKE_KEYS};
use crate::lookup;
use crate::node::RealNode;
use crate::package::Version;
use crate::package::{Signed, VERSION_RECORD_TAG, VersionRecord, record_key};
use crate::value::Value;

use bincode::Options;
use ed25519_dalek::{SECRET_KEY_LENGTH, SigningKey};
use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::Instant;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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
/// Parse a package identifier in DOMAIN:PACKAGE format.
fn parse_package_name(input: &str) -> Result<(&str, &str), &'static str> {
    let (domain, package) = input.split_once(':').ok_or("expected DOMAIN:PACKAGE")?;

    if domain.is_empty() || package.is_empty() || package.contains(':') {
        return Err("expected DOMAIN:PACKAGE");
    }

    Ok((domain, package))
}

fn parse_install_name(input: &str) -> Result<(&str, &str, Option<Version>), &'static str> {
    // Split DOMAIN:PACKAGE:VERSION at the final colon.
    let (name, version) = input
        .rsplit_once(':')
        .ok_or("expected DOMAIN:PACKAGE:VERSION")?;

    // Reuse our existing DOMAIN:PACKAGE parser.
    let (domain, package) =
        parse_package_name(name).map_err(|_| "expected DOMAIN:PACKAGE:VERSION")?;

    // 'latest' means the caller wants the newest version.
    let requested_version = if version == "latest" {
        None
    } else {
        Some(
            version
                .parse::<Version>()
                .map_err(|_| "invalid version; expected major.minor.patch or latest")?,
        )
    };

    Ok((domain, package, requested_version))
}

async fn load_publisher_key(node: &RealNode) -> Result<SigningKey, String> {
    match node.dns().as_ref() {
        DnsBackend::Fake(_) => {
            // Each node uses one configured test publisher identity.
            // Default to node0 if no identity was specified.
            let identity = std::env::var("D7024E_FAKE_PUBLISHER")
                .unwrap_or_else(|_| FAKE_KEYS[0].domain.to_string());

            FAKE_KEYS
                .iter()
                .find(|key| key.domain.eq_ignore_ascii_case(&identity))
                .ok_or_else(|| format!("unknown fake publisher identity: {identity}"))?
                .signing_key()
                .ok_or_else(|| format!("invalid fake publisher key for {identity}"))
        }

        DnsBackend::Real(_) => {
            let path = std::env::var("D7024E_SIGNING_KEY_FILE")
                .map_err(|_| "set D7024E_SIGNING_KEY_FILE to your private key file".to_string())?;

            let contents = tokio::fs::read_to_string(&path)
                .await
                .map_err(|error| format!("could not read signing key file: {error}"))?;

            let mut bytes = [0u8; SECRET_KEY_LENGTH];

            hex::decode_to_slice(contents.trim(), &mut bytes)
                .map_err(|_| "signing key must be exactly 64 hexadecimal characters".to_string())?;

            Ok(SigningKey::from_bytes(&bytes))
        }
    }
}

struct PublishArgs<'a> {
    domain: &'a str,
    package: &'a str,
    version: Version,
    filename: &'a str,
    previous: Option<Version>,
    force: bool,
}

fn parse_publish_args<'a>(parts: &[&'a str]) -> Result<PublishArgs<'a>, &'static str> {
    let mut force = false;
    let mut previous = None;
    let mut positional = Vec::new();

    for &part in parts {
        if part == "--force" {
            if force {
                return Err("duplicate --force");
            }
            force = true;
        } else if let Some(value) = part.strip_prefix("--prev=") {
            if previous.is_some() {
                return Err("duplicate --prev");
            }

            previous = Some(
                value
                    .parse::<Version>()
                    .map_err(|_| "invalid previous version")?,
            );
        } else if part.starts_with("--") {
            return Err("unknown publish option");
        } else {
            positional.push(part);
        }
    }

    let [name, filename] = positional.as_slice() else {
        return Err("expected DOMAIN:PACKAGE:VERSION FILENAME");
    };

    // Reuse the parser from the install command.
    let (domain, package, requested) = parse_install_name(name)?;

    let version = requested.ok_or("publish requires a numeric version, not latest")?;

    Ok(PublishArgs {
        domain,
        package,
        version,
        filename,
        previous,
        force,
    })
}

fn default_install_filename(domain: &str, package: &str, version: Version) -> String {
    let name = format!("{domain}-{package}-{version}.pkg");

    // Keep generated filenames in the current directory.
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}
fn show_routing_table(node: &RealNode) {
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

/// Format a locally stored DHT value for the datastore listing.
///
/// Signed version records are stored as blobs, so we attempt to
/// decode them. This is for display, not signature verification.
fn describe_datastore_entry(key: &Key, value: &Value) -> String {
    let key_hex = hex::encode(key);

    match value {
        Value::Latest(pointer) => {
            format!(
                "{key_hex}  latest pointer ({} bytes)\n\
                 \x20   package: {}:{}\n\
                 \x20   version: {}\n\
                 \x20   record: {}",
                value.len(),
                pointer.body.domain,
                pointer.body.package,
                pointer.body.version,
                hex::encode(pointer.body.version_record_hash),
            )
        }

        Value::Blob(bytes) => {
            // A signed version record is serialized and stored
            // as an ordinary content-addressed blob.
            //
            // Bound decoding work: arbitrary blobs are not
            // necessarily valid bincode records.
            let decoded = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_limit(64 * 1024)
                .allow_trailing_bytes()
                .deserialize::<Signed<VersionRecord>>(bytes);

            if let Ok(record) = decoded {
                // Do not mistake an arbitrary blob for a record.
                if record.body.tag == VERSION_RECORD_TAG && record_key(&record) == Some(*key) {
                    return format!(
                        "{key_hex}  version record ({} bytes)\n\
                         \x20   package: {}:{}\n\
                         \x20   version: {}\n\
                         \x20   blob: {}\n\
                         \x20   prev: {}",
                        value.len(),
                        record.body.domain,
                        record.body.package,
                        record.body.version,
                        hex::encode(record.body.blob_hash),
                        hex::encode(record.body.prev),
                    );
                }
            }

            // Ordinary package files or other stored data.
            format!("{key_hex}  blob ({} bytes)", value.len())
        }
    }
}

fn show_datastore(node: &RealNode) {
    let values = node.datastore_snapshot();

    if values.is_empty() {
        println!("<empty>");
        return;
    }

    for (key, value) in values {
        println!("{}", describe_datastore_entry(&key, &value));
    }
}

fn print_help() {
    println!("commands:");
    println!("  ping IP:PORT");
    println!("  put FILENAME");
    println!("  get KEY [FILENAME]");
    println!("  install DOMAIN:PACKAGE:VERSION [FILENAME]");
    println!("  publish [--force] [--prev=VERSION] DOMAIN:PACKAGE:VERSION FILENAME");
    println!("  show rt");
    println!("  show ds");
    println!("  show dns DOMAIN");
    println!("  show DOMAIN:PACKAGE");
    println!("  help");
    println!("  exit");
}

pub async fn run(node: &RealNode) -> io::Result<()> {
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();

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
                let key = lookup::store_value(node.rpc(), value.into()).await;

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

            ["show", "dns", domain] => match node.dns().get_dns_vk(domain).await {
                Some(key) => {
                    println!("{}: {}", domain, hex::encode(key.to_bytes()));
                }

                None => {
                    println!("no public key found for {domain}");
                }
            },

            ["show", name] => {
                let (domain, package) = match parse_package_name(name) {
                    Ok(parts) => parts,
                    Err(error) => {
                        println!("{error}");
                        continue;
                    }
                };

                match crate::registry::show_package(
                    node.rpc(),
                    node.dns().as_ref(),
                    domain,
                    package,
                )
                .await
                {
                    Ok(versions) => {
                        println!("{domain}:{package}");

                        for version in versions {
                            if version.is_latest {
                                println!("  {} (latest)", version.version);
                            } else {
                                println!("  {}", version.version);
                            }

                            println!("    blob: {}", hex::encode(version.blob_key));
                        }
                    }

                    Err(error) => {
                        println!("could not show {domain}:{package}: {error}");
                    }
                }
            }

            ["install", identifier, output @ ..] if output.len() <= 1 => {
                let (domain, package, requested_version) = match parse_install_name(identifier) {
                    Ok(parts) => parts,
                    Err(error) => {
                        println!("{error}");
                        continue;
                    }
                };

                // Let the registry backend do the network lookup
                // and all signature/hash verification.
                let installed = match crate::registry::install_package(
                    node.rpc(),
                    node.dns().as_ref(),
                    domain,
                    package,
                    requested_version,
                )
                .await
                {
                    Ok(installed) => installed,
                    Err(error) => {
                        println!("could not install {identifier}: {error}");
                        continue;
                    }
                };

                // Use a user-supplied filename, or generate one.
                let filename = output
                    .first()
                    .map(|name| (*name).to_string())
                    .unwrap_or_else(|| {
                        default_install_filename(domain, package, installed.version)
                    });

                // Refuse to overwrite an existing file.
                let mut file = match tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&filename)
                    .await
                {
                    Ok(file) => file,
                    Err(error) => {
                        println!("could not create {filename}: {error}");
                        continue;
                    }
                };

                // Write the verified package bytes.
                if let Err(error) = file.write_all(&installed.blob).await {
                    drop(file);
                    let _ = tokio::fs::remove_file(&filename).await;

                    println!("could not write {filename}: {error}");
                    continue;
                }

                println!("installed {domain}:{package}:{}", installed.version);
                println!("  file: {filename}");
                println!("  size: {} bytes", installed.blob.len());
                println!("  hash: {}", hex::encode(installed.blob_key));
            }

            ["install", ..] => {
                println!("usage: install DOMAIN:PACKAGE:VERSION [FILENAME]");
            }

            ["publish", args @ ..] => {
                let options = match parse_publish_args(args) {
                    Ok(options) => options,
                    Err(error) => {
                        println!("{error}");
                        println!(
                            "usage: publish [--force] [--prev=VERSION] \
                 DOMAIN:PACKAGE:VERSION FILENAME"
                        );
                        continue;
                    }
                };

                // 1. Read the package file.
                let blob = match tokio::fs::read(options.filename).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        println!("could not read {}: {error}", options.filename);
                        continue;
                    }
                };

                let size = blob.len();

                // 2. Obtain the private key for this domain.
                let signer = match load_publisher_key(node).await {
                    Ok(key) => key,
                    Err(error) => {
                        println!("cannot publish: {error}");
                        continue;
                    }
                };

                // 3. Call the existing registry publishing backend.
                let result = crate::registry::publish_package(
                    node.rpc(),
                    node.dns().as_ref(),
                    options.domain,
                    options.package,
                    options.version,
                    blob,
                    &signer,
                    options.previous,
                    options.force,
                )
                .await;

                match result {
                    Ok(receipt) => {
                        println!(
                            "published {}:{}:{}",
                            options.domain, options.package, options.version
                        );

                        println!("  size: {size} bytes");

                        println!(
                            "  blob: {} ({} acknowledgments)",
                            hex::encode(receipt.blob_key),
                            receipt.blob_accepted
                        );

                        println!(
                            "  record: {} ({} acknowledgments)",
                            hex::encode(receipt.record_key),
                            receipt.record_accepted
                        );

                        println!(
                            "  latest: {} ({} acknowledgments)",
                            hex::encode(receipt.latest_key),
                            receipt.latest_accepted
                        );
                    }

                    Err(error) => {
                        println!("publication failed: {error}");
                    }
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_package_names() {
        assert_eq!(
            parse_package_name("example.com:calculator"),
            Ok(("example.com", "calculator"))
        );

        assert!(parse_package_name("example.com").is_err());
        assert!(parse_package_name("example.com:").is_err());
        assert!(parse_package_name(":calculator").is_err());
        assert!(parse_package_name("example.com:pkg:1.0.0").is_err());
    }

    #[test]
    fn parses_install_names() {
        let (domain, package, version) =
            parse_install_name("example.com:calculator:1.2.3").unwrap();

        assert_eq!(domain, "example.com");
        assert_eq!(package, "calculator");
        assert_eq!(version, Some("1.2.3".parse().unwrap()));

        let (domain, package, version) =
            parse_install_name("example.com:calculator:latest").unwrap();

        assert_eq!(domain, "example.com");
        assert_eq!(package, "calculator");
        assert_eq!(version, None);

        assert!(parse_install_name("example.com:calculator").is_err());
        assert!(parse_install_name("example.com::1.0.0").is_err());
        assert!(parse_install_name("example.com:calculator:abc").is_err());
        assert!(parse_install_name("example.com:calculator:1.0").is_err());
    }

    #[test]
    fn generates_safe_install_filename() {
        let version: Version = "2.0.0".parse().unwrap();

        assert_eq!(
            default_install_filename("example.com", "calculator", version),
            "example.com-calculator-2.0.0.pkg"
        );

        assert!(!default_install_filename("example.com", "../calculator", version,).contains('/'));
    }

    #[test]
    fn parses_publish_command() {
        let args = [
            "--force",
            "--prev=1.0.0",
            "node0.d7024e.test:calculator:3.0.0",
            "calculator.zip",
        ];

        let parsed = parse_publish_args(&args).unwrap();

        assert_eq!(parsed.domain, "node0.d7024e.test");
        assert_eq!(parsed.package, "calculator");
        assert_eq!(parsed.version, "3.0.0".parse().unwrap());
        assert_eq!(parsed.previous, Some("1.0.0".parse().unwrap()));
        assert_eq!(parsed.filename, "calculator.zip");
        assert!(parsed.force);
    }

    #[test]
    fn rejects_invalid_publish_commands() {
        assert!(parse_publish_args(&[]).is_err());

        assert!(parse_publish_args(&["example.com:calculator:latest", "file.zip",]).is_err());

        assert!(
            parse_publish_args(&["--prev=invalid", "example.com:calculator:2.0.0", "file.zip",])
                .is_err()
        );

        assert!(
            parse_publish_args(&[
                "--force",
                "--force",
                "example.com:calculator:1.0.0",
                "file.zip",
            ])
            .is_err()
        );
    }

    #[test]
    fn datastore_displays_plain_blobs() {
        let value = Value::Blob(b"ordinary package bytes".to_vec());

        let display = describe_datastore_entry(&value.key(), &value);

        assert!(display.contains("blob"));
        assert!(display.contains("ordinary") == false);
        assert!(!display.contains("version record"));
    }

    #[test]
    fn datastore_displays_version_records() {
        use crate::package::{VersionRecord, sign};
        use ed25519_dalek::SigningKey;

        let signer = SigningKey::from_bytes(&[7; 32]);

        let record = sign(
            VersionRecord {
                tag: VERSION_RECORD_TAG.to_string(),
                domain: "example.com".to_string(),
                package: "calculator".to_string(),
                version: "1.0.0".parse().unwrap(),
                blob_hash: [7; 32],
                prev: [0; 32],
            },
            &signer,
        )
        .unwrap();

        let value = Value::Blob(bincode::serialize(&record).unwrap());
        let display = describe_datastore_entry(&value.key(), &value);

        assert!(display.contains("version record"));
        assert!(display.contains("example.com:calculator"));
        assert!(display.contains("1.0.0"));
        assert!(display.contains(&hex::encode([7; 32])));
    }

    #[test]
    fn datastore_displays_latest_pointers() {
        use crate::package::{LATEST_POINTER_TAG, LatestPointer, sign};
        use ed25519_dalek::SigningKey;

        let signer = SigningKey::from_bytes(&[7; 32]);

        let pointer = sign(
            LatestPointer {
                tag: LATEST_POINTER_TAG.to_string(),
                domain: "example.com".to_string(),
                package: "calculator".to_string(),
                version: "2.0.0".parse().unwrap(),
                version_record_hash: [9; 32],
            },
            &signer,
        )
        .unwrap();

        let value = Value::Latest(pointer);
        let display = describe_datastore_entry(&value.key(), &value);

        assert!(display.contains("latest pointer"));
        assert!(display.contains("example.com:calculator"));
        assert!(display.contains("2.0.0"));
        assert!(display.contains(&hex::encode([9; 32])));
    }
}
