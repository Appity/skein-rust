use env_logger::Builder;
use log::LevelFilter;

const LOG_ENV_VAR : &str = "SKEIN_LOG";

pub fn setup(level: usize) {
    let mut builder = Builder::new();

    builder.filter_level(
        match level {
            0 => LevelFilter::Info,
            1 => LevelFilter::Debug,
            _ => LevelFilter::Trace
        }
    );

    builder.parse_env(LOG_ENV_VAR);
    builder.init();
}
