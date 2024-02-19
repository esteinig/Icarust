use clap::Parser;
use icarust::cli::Cli;
use icarust::icarust::Icarust;

/// Main routine
/// 
/// TODO:
/// 
/// 1. Death rate settings and yield parsing from Blow5
/// 2.Sample rate parsing from Blow5
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // Parse the arguments from the command line
    let args = Cli::parse();

    // Some manual checks
    args.set_logging();
    args.check_config_exists();

    let icarust = Icarust::from_toml(&args.profile, args.outdir);
    icarust.run(args.data_delay, args.data_run_time).await?;

    Ok(())
}
