//! Implementation of the CLI for icarust. Simple - just lets a user provide verbosity and a path to a config toml.

use crate::utils::set_up_logging;
use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
/// Struct to capture the command line interface options
pub struct Cli {
    /// Path to the simulation profile toml file to use
    #[clap(short, long)]
    pub profile: std::path::PathBuf,
    /// Output directory, overwrites directory specified in simulation profile
    #[clap(short, long)]
    pub outdir: Option<std::path::PathBuf>,
    /// Determines verbosity of the processing, can be specified multiple times (max -vvv)
    #[structopt(short, long, parse(from_occurrences))]
    pub verbosity: u8,
    /// Data generator thread delay in seconds
    #[clap(short, long, default_value="0")]
    pub data_delay: u64,
    /// Data generator thread run time in minutes
    #[clap(short='r', long, default_value="0")]
    pub data_run_time: u64,
    /// Log the action counts to console
    #[clap(short, long)]
    pub log_actions: bool,
}

impl Cli {
    /// Sets logging level suplied by verbosity count.
    ///
    /// Levels possible are:
    ///
    /// - `Warn` (default)
    /// - `Info`
    /// - `Debug`
    /// - `Trace`
    pub fn set_logging(&self) {
        use log::LevelFilter::*;

        let log_level = match self.verbosity {
            1 => Info,
            2 => Debug,
            level if level > 2 => Trace,
            _ => Warn,
        };

        set_up_logging(log_level);
    }
    /// Check the config file is where specified
    pub fn check_config_exists(&self) {
        assert!(self.profile.exists())
    }
}
