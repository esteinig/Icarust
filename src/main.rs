//! # Icarust
//!
//! A binary for running a mock, read-fish compatible grpc server to test live unblocking read-until experiments.
//!
//! The details for configuring the run can be found in Profile_tomls.
//!
//! Simply `cargo run --config <config.toml>` in the directory to start the server, which hosts the Manager Server on 127.0.0.1:10000
//!
//! Has one position, which is hosted on 127.0.0.1:10001
//!
/// The module pertaining the CLI code


use clap::Parser;

use icarust::cli::Cli;
use icarust::icarust::Icarust;

/// Main function - Runs two asynchronous GRPC servers
/// The first server is the manager server, which here manages available sequencing positions and minknow version information.
/// Once a client connects to the manager it may then connect to the second GRPC server, which manages all the services relating to the
/// sequencing position.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse the arguments from the command line
    let args = Cli::parse();
    args.set_logging();
    args.check_config_exists();

    let icarust = Icarust::from_toml(&args.simulation_profile);
    icarust.run(args.data_delay, args.data_run_time).await?;

    Ok(())
}
