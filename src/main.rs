#![allow(missing_docs)]
#![deny(missing_doc_code_examples)]
#![recursion_limit = "1024"]
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
pub mod cli;
mod impl_services;
pub mod r10_simulation;
mod reacquisition_distribution;
pub mod utils;
#[macro_use]
extern crate log;
extern crate lazy_static;

mod read_length_distribution;
/// Import all our definied services
mod services;

use chrono::prelude::*;
use clap::Parser;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use uuid::Uuid;

use crate::impl_services::acquisition::Acquisition;
use crate::impl_services::analysis_configuration::Analysis;
use crate::impl_services::data::DataServiceServicer;
use crate::impl_services::device::Device;
use crate::impl_services::instance::Instance;
use crate::impl_services::log::Log;
use crate::impl_services::manager::Manager;
use crate::impl_services::protocol::ProtocolServiceServicer;

use crate::services::minknow_api::acquisition::acquisition_service_server::AcquisitionServiceServer;
use crate::services::minknow_api::analysis_configuration::analysis_configuration_service_server::AnalysisConfigurationServiceServer;
use crate::services::minknow_api::data::data_service_server::DataServiceServer;
use crate::services::minknow_api::device::device_service_server::DeviceServiceServer;
use crate::services::minknow_api::instance::instance_service_server::InstanceServiceServer;
use crate::services::minknow_api::log::log_service_server::LogServiceServer;
use crate::services::minknow_api::manager::flow_cell_position::{RpcPorts, SharedHardwareGroup};
use crate::services::minknow_api::manager::manager_service_server::ManagerServiceServer;
use crate::services::minknow_api::manager::FlowCellPosition;
use crate::services::minknow_api::protocol::protocol_service_server::ProtocolServiceServer;

use crate::reacquisition_distribution::{DeathChance, _calculate_death_chance};
use crate::read_length_distribution::ReadLengthDist;

/// Holds the  type of the pore we are simulating
#[derive(Clone)]
pub enum PoreType {
    /// R10 pore
    R10,
    /// R9 pore
    R9,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    parameters: Parameters,
    sample: Vec<Sample>,
    output_path: std::path::PathBuf,
    global_mean_read_length: Option<f64>,
    random_seed: Option<u64>,
    target_yield: f64,
    working_pore_percent: Option<usize>,
    pore_type: Option<String>,
    
}

impl Config {
    pub fn get_working_pore_precent(&self) -> usize {
        self.working_pore_percent.unwrap_or(90)
    }

    /// Check that we have a valid pore type or return the default R10 pore.
    pub fn check_pore_type(&self) -> PoreType {
        match &self.pore_type {
            Some(pore_type) => match pore_type.as_str() {
                "R10" => PoreType::R10,
                "R9" => PoreType::R9,
                _ => {
                    panic!("Invalid pore type specified")
                }
            },
            None => PoreType::R9,
        }
    }

    /// Calculate the chance a pore will die.
    pub fn calculate_death_chance(&self, starting_channels: usize) -> HashMap<String, DeathChance> {
        let target_yield = &self.target_yield;
        let mut deaths = HashMap::new();
        for sample in &self.sample {
            let mean_read_len = match sample.mean_read_length {
                Some(rl) => rl,
                None => match self.global_mean_read_length {
                    Some(rl) => rl,
                    None => {
                        panic!("Sample {} does not have a mean read length and no global read length is set.", sample.input_genome.display());
                    }
                },
            };
            let name = &sample.name;
            let death = DeathChance {
                base_chance: _calculate_death_chance(
                    starting_channels as f64,
                    *target_yield,
                    mean_read_len,
                ),
                mean_read_length: mean_read_len,
            };
            deaths.insert(name.clone(), death);
        }
        deaths
    }

    /// Get the usize version of the run duration so we can stop running if we exceed it.
    /// If not set a default value of 4800 is returned
    pub fn get_experiment_duration_set(&self) -> usize {
        self.parameters.experiment_duration_set.unwrap_or({
            // wasn't set so default 4800
            4800
        })
    }
    // Get the User set random seed. If not found provide one as a random usize
    pub fn get_rand_seed(&self) -> u64 {
        match self.random_seed {
            Some(seed) => seed,
            None => rand::random::<u64>(),
        }
    }

    // Check config fields and error out if there's a problem
    pub fn check_fields(&self) {
        let _pore_type = &self.check_pore_type();
        for sample in &self.sample {
            match sample.mean_read_length {
                Some(_) => {}
                None => match self.global_mean_read_length {
                    Some(_) => {}
                    None => {
                        panic!("Sample {} does not have a mean read length and no global read length is set.", sample.input_genome.display());
                    }
                },
            }
            if sample.is_amplicon() && sample.is_barcoded() {
                if let Some(barcodes) = &sample.barcodes {
                    if let Some(read_files) = &sample.weights_files {
                        if barcodes.len() != read_files.len() {
                            panic!("If providing amplicon weights, it is necessary to provide as many as there are barcodes.")
                        }
                    }
                }
            }
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Parameters {
    cert_dir: PathBuf,
    manager_port: u32,
    position_port: u32,
    channels: usize,
    sample_name: String,
    experiment_name: String,
    flowcell_name: String,
    experiment_duration_set: Option<usize>,
    device_id: String,
    position: String,
    break_read_ms: Option<u64>,
}

impl Parameters {
    pub fn get_chunk_size_ms(&self) -> u64 {
        self.break_read_ms.unwrap_or(400)
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Sample {
    name: String,
    input_genome: std::path::PathBuf,
    mean_read_length: Option<f64>,
    weight: usize,
    weights_files: Option<Vec<std::path::PathBuf>>,
    amplicon: Option<bool>,
    barcodes: Option<Vec<String>>,
    barcode_weights: Option<Vec<usize>>,
    uneven: Option<bool>,
}

impl Sample {
    pub fn get_read_len_dist(&self, global_read_len: Option<f64>) -> ReadLengthDist {
        match self.mean_read_length {
            Some(mrl) => ReadLengthDist::new(mrl / 450.0 * 4000.0),
            None => ReadLengthDist::new(global_read_len.unwrap() / 450.0 * 4000.0),
        }
    }
    pub fn is_amplicon(&self) -> bool {
        self.amplicon.unwrap_or(false)
    }
    pub fn is_barcoded(&self) -> bool {
        self.barcodes.is_some()
    }
}

/// Loads our config TOML to get the sample name, experiment name and flowcell name, which is returned as a Config struct.
fn _load_toml(file_path: &std::path::PathBuf) -> Config {
    let contents = fs::read_to_string(file_path).expect("Something went wrong with reading the config file");
    let config: Config = toml::from_str(&contents).unwrap();
    config
}

/// Icarust main runner
struct Icarust {
    pub config: Config,
    pub run_id: String,
    pub output_path: PathBuf
}
impl Icarust {
    pub fn from_toml(file_path: &PathBuf) -> Self {
        
        let config = _load_toml(file_path);
        config.check_fields();

        let (run_id, output_path) = Icarust::get_run_params(&config);
        Self { config, run_id, output_path }
    }
    // Delay and runtime in seconds
    pub async fn run(&self, data_delay: u64, data_runtime: u64) -> Result<(), Box<dyn std::error::Error>>  {

        // Manager service
        let tls_manager = self.get_tls_config();
        let addr_manager = format!("[::0]:{}", self.config.parameters.manager_port).parse().unwrap();
        let manager_service_server = self.get_manager_service_server();

        // Spawn an Async thread and send it off somewhere
        tokio::spawn(async move {
            Server::builder()
            .tls_config(tls_manager)
            .unwrap()
            .concurrency_limit_per_connection(256)
            .add_service(manager_service_server)
            .serve(addr_manager)
            .await
            .unwrap();
        });

        // Graceful shutdown
        let graceful_shutdown = Arc::new(Mutex::new(false));
        let graceful_shutdown_clone = Arc::clone(&graceful_shutdown);

        // Create the position server for our one position.
        let log_svc = LogServiceServer::new(Log {});
        let instance_svc = InstanceServiceServer::new(Instance {});
        let analysis_svc = AnalysisConfigurationServiceServer::new(Analysis {});
        let device_svc = DeviceServiceServer::new(Device::new(self.config.parameters.channels));
        let acquisition_svc = AcquisitionServiceServer::new(Acquisition {
            run_id: self.run_id.clone(),
        });
        let protocol_svc = ProtocolServiceServer::new(ProtocolServiceServicer::new(
            self.run_id.clone(), self.output_path.clone(),
        ));


        let data_service_server = DataServiceServer::new(DataServiceServicer::new(
            self.run_id.clone(),
            &self.config,
            self.output_path.clone(),
            self.config.parameters.channels,
            graceful_shutdown_clone,
            data_delay,
            data_runtime
        ));
        
        ctrlc::set_handler(move || {
            {
                let mut x = graceful_shutdown.lock().unwrap();
                *x = true;
            }
            std::thread::sleep(Duration::from_millis(2000));
            std::process::exit(0);
        })
            .expect("FAILED TO CATCH SIGNAL SOMWHOW");

            
        let tls_position = self.get_tls_config();
        let addr_position: SocketAddr = format!("[::0]:{}", self.config.parameters.position_port).parse().unwrap();

        Server::builder()
            .tls_config(tls_position)
            .unwrap()
            .concurrency_limit_per_connection(256)
            .add_service(log_svc)
            .add_service(device_svc)
            .add_service(instance_svc)
            .add_service(analysis_svc)
            .add_service(acquisition_svc)
            .add_service(protocol_svc)
            .add_service(data_service_server)
            .serve(addr_position)
            .await?;

        Ok(())
    }
    fn get_manager_service_server(&self) -> ManagerServiceServer<Manager> {

        // Create the manager server and add the service to it
        let manager_init = Manager {
            positions: vec![FlowCellPosition {
                name: self.config.parameters.device_id.clone(),
                state: 1,
                rpc_ports: Some(RpcPorts {
                    secure: 10001,
                    secure_grpc_web: 420,
                }),
                protocol_state: 1,
                error_info: "Help me I'm trapped in the computer".to_string(),
                shared_hardware_group: Some(SharedHardwareGroup { group_id: 1 }),
                is_integrated: true,
                can_sequence_offline: true,
                location: None,
            }],
        };
        ManagerServiceServer::new(manager_init)
    }
    fn get_tls_config(&self) -> ServerTlsConfig {
        let cert = std::fs::read(self.config.parameters.cert_dir.join("localhost.crt")).expect("No TLS cert found");
        let key = std::fs::read(self.config.parameters.cert_dir.join("localhost.key")).expect("No TLS key found");
        let server_identity = Identity::from_pem(cert, key);
        ServerTlsConfig::new().identity(server_identity)
    }
    fn get_run_params(config: &Config) -> (String, PathBuf) {

        let run_id = Uuid::new_v4().to_string().replace('-', "");
        let sample_id = config.parameters.sample_name.clone();
        let experiment_id = config.parameters.experiment_name.clone();
        let output_dir = config.output_path.clone();
        let start_time_string: String = format!("{}", Utc::now().format("%Y%m%d_%H%M"));
        let flowcell_id = config.parameters.flowcell_name.clone();
        let mut output_path = output_dir.clone();
        output_path.push(experiment_id);
        output_path.push(sample_id);
        output_path.push(format!(
            "{}_XIII_{}_{}",
            start_time_string,
            flowcell_id,
            &run_id[0..9],
        ));
        (run_id, output_path)
    }
}

/// Main function - Runs two asynchronous GRPC servers
/// The first server is the manager server, which here manages available sequencing positions and minknow version information.
/// Once a client connects to the manager it may then connect to the second GRPC server, which manages all the services relating to the
/// sequencing position.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse the arguments from the command line
    let args = cli::Cli::parse();
    args.set_logging();
    args.check_config_exists();

    let icarust = Icarust::from_toml(&args.simulation_profile);
    icarust.run(args.data_delay, args.data_run_time).await?;

    Ok(())
}
