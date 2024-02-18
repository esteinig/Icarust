use clap::Parser;
use icarust::cli::Cli;
use icarust::icarust::Icarust;

/// Main routine
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
