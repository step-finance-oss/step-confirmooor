use log::info;
use std::path::{Path, PathBuf};

mod dedupe;
mod error;
mod processor;
mod rabbit;
mod runtime;
mod tx_ring_buffer;

/// the main command line entry point to our program
fn main() {
    println!("Begin");

    // load settings from our dotenv files
    for path in [".env.local", ".env"] {
        if dotenv(path).is_err() {
            // Parse errors can contain an entire line, including a broker password.
            eprintln!("Failed to load {path}");
            std::process::exit(1);
        }
    }

    //cofigure logging from env
    env_logger::builder()
        .filter_level(if cfg!(debug_assertions) {
            log::LevelFilter::Debug
        } else {
            log::LevelFilter::Warn
        })
        .parse_default_env()
        .init();

    info!("Environment configured");

    //call runtime's run method, which will setup the runtime and execute our main loop
    //an error results in a log and a return with return code 1
    std::process::exit(match runtime::run() {
        Ok(()) => 0,
        Err(e) => {
            log::error!("{:?}", e);
            1
        },
    });
}

/// loads dotenv from file, ignoring missing files
fn dotenv(name: impl AsRef<Path>) -> Result<Option<PathBuf>, dotenv::Error> {
    match dotenv::from_filename(name) {
        Ok(p) => Ok(Some(p)),
        Err(dotenv::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
