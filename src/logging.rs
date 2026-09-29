pub fn setup() -> Result<(), fern::InitError> {
    let console = fern::Dispatch::new()
        .level(log::LevelFilter::Info)
        .filter(|metadata| metadata.target() != "metrics")
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{} {}] {}",
                record.level(),
                record.target(),
                message
            ))
        })
        .chain(std::io::stdout());

    let metrics = fern::Dispatch::new()
        .level(log::LevelFilter::Info)
        .filter(|metadata| metadata.target() == "metrics")
        .format(|out, message, _record| out.finish(format_args!("{}", message)))
        .chain(fern::log_file("metrics.log")?);

    fern::Dispatch::new()
        .chain(console)
        .chain(metrics)
        .apply()?;

    Ok(())
}
