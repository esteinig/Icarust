use clap::Parser;
use icarust::cli::Cli;
use icarust::icarust::Icarust;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // Parse the arguments from the command line
    let args = Cli::parse();

    // Some manual checks
    args.set_logging();
    args.check_config_exists();

    let icarust = Icarust::from_toml(&args.profile, None, None, args.outdir, None, false);
    icarust.run(args.data_delay, args.data_run_time, args.log_actions).await?;

    Ok(())
}
