use super::processor::*;
use crate::error::*;
use anyhow::Context;
use clap::Parser;
use log::*;

/// simple method responsible for loading our args, setting up runtime, then blocking on
/// the main processor loops
pub(crate) fn run() -> Result<()> {
    //parse the args for the processor
    let args = ProcessArgs::parse();

    args.validate()?;

    info!("Starting runtime with {:?} threads", args.thread_count);
    let rt = {
        let mut b = tokio::runtime::Builder::new_multi_thread();

        //use the same thread count in runtime as will in processor
        if let Some(thread_count) = args.thread_count {
            b.worker_threads(thread_count);
        }

        b.enable_all()
            .build()
            .context("Failed to initialize async runtime")?
    };

    // hand another to our main runtime
    // method blocks within our tokio runtime, returning status from the processor when method returns
    rt.block_on(super::processor::process(args))
}
