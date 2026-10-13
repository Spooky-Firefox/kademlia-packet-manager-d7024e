use crate::node_scope;
use std::fmt::Arguments;
use std::path::Path;

/// Format one line as `[LEVEL target] node=ADDR message`, the `node=` part
/// present only when the line was logged from inside a node's scope (see
/// [`node_scope`]).
fn tagged(out: fern::FormatCallback, message: &Arguments, record: &log::Record) {
    match node_scope::current() {
        Some(node) => out.finish(format_args!(
            "[{} {}] node={} {}",
            record.level(),
            record.target(),
            node,
            message
        )),
        None => out.finish(format_args!(
            "[{} {}] {}",
            record.level(),
            record.target(),
            message
        )),
    }
}

/// `metrics` lines, unformatted, to `metrics.log`.
fn metrics() -> Result<fern::Dispatch, fern::InitError> {
    Ok(fern::Dispatch::new()
        .level(log::LevelFilter::Info)
        .filter(|metadata| metadata.target() == "metrics")
        .format(|out, message, _record| out.finish(format_args!("{}", message)))
        .chain(fern::log_file("metrics.log")?))
}

pub fn setup() -> Result<(), fern::InitError> {
    let console = fern::Dispatch::new()
        .level(log::LevelFilter::Info)
        .filter(|metadata| metadata.target() != "metrics")
        .format(tagged)
        .chain(std::io::stdout());

    fern::Dispatch::new()
        .chain(console)
        .chain(metrics()?)
        .apply()?;

    Ok(())
}

/// Logging for a many-node simulation: every non-metrics line at `level` or
/// above goes to the file at `log_path`, tagged with its node, so one node's
/// history is a `grep 'node=ADDR '` away. Only warnings and errors reach
/// stdout, which a thousand nodes would otherwise drown.
pub fn setup_sim(
    log_path: impl AsRef<Path>,
    level: log::LevelFilter,
) -> Result<(), fern::InitError> {
    let file = fern::Dispatch::new()
        .level(level)
        .filter(|metadata| metadata.target() != "metrics")
        .format(tagged)
        .chain(fern::log_file(log_path)?);

    let console = fern::Dispatch::new()
        .level(log::LevelFilter::Info)
        .filter(|metadata| metadata.target() != "metrics")
        .format(tagged)
        .chain(std::io::stdout());

    fern::Dispatch::new()
        .chain(file)
        .chain(console)
        .chain(metrics()?)
        .apply()?;

    Ok(())
}
