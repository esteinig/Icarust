use std::fs::create_dir_all;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tokio::signal;
use slow5::FileReader;

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

use crate::config::{Config, load_toml};

const SERVER_CERT: &[u8] = include_bytes!("../static/server.crt");
const SERVER_KEY: &[u8] = include_bytes!("../static/server.key");

// Icarust main runner
pub struct Icarust {
    pub config: Config,
}
impl Icarust {
    pub fn from_toml(file_path: &PathBuf, output_path: Option<PathBuf>) -> Self {

        log::debug!("Reading configuration file: {}", file_path.display());
        let mut config = load_toml(file_path);

        if config.simulation.target_yield.is_none() && !config.simulation.deplete {
            log::error!("When continously sampling from community (deplete = false) target yield must be set in configuration file!");
            process::exit(1);
        }

        (   
            config.simulation.sampling_rate, 
            config.simulation.mean_read_length, 
            config.simulation.target_yield,
            config.simulation.run_id
        ) = Icarust::parse_simulation_header(
            &config.simulation.community, 
            &config.simulation.target_yield
        );

        // If the experiment, flowcell and sample name are provided a traditional run 
        // output directory path is created, otherwise the outdir from the configuration is used
        let output_path_from_config = Icarust::get_run_params(&config);

        config.outdir = match output_path { 
            // If an output path is directly provided to the function
            Some(path) => path, 
            None => output_path_from_config
        };

        if !config.outdir.exists() {
            log::info!("Creating output directory: {}", config.outdir.display());
            create_dir_all(&config.outdir).unwrap();
        }

        log::info!("{:#?}", config);

        Self { config }

    }
    // Delay and runtime in seconds
    pub async fn run(&self, data_delay: u64, data_runtime: u64) -> Result<(), Box<dyn std::error::Error>>  {

        // Manager service
        let tls_manager = self.get_tls_config();
        let addr_manager = format!("[::0]:{}", self.config.server.manager_port).parse().unwrap();
        let manager_service_server = self.get_manager_service_server();

        // Spawn an Async thread and send it off somewhere
        let manager_handle = tokio::spawn(async move {
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
        let graceful_shutdown_main= Arc::clone(&graceful_shutdown);

        // Create the device server and services
        let log_svc = LogServiceServer::new(Log {});
        let instance_svc = InstanceServiceServer::new(Instance {});
        let analysis_svc = AnalysisConfigurationServiceServer::new(Analysis {});
        
        let device_svc = DeviceServiceServer::new(Device::new(
            self.config.parameters.channels
        ));
        let acquisition_svc = AcquisitionServiceServer::new(Acquisition { 
            run_id: self.config.simulation.run_id.clone() 
        });
        let protocol_svc = ProtocolServiceServer::new(ProtocolServiceServicer::new(
            self.config.simulation.run_id.clone(), 
            self.config.outdir.clone(),
        ));
        let data_service_server = DataServiceServer::new(DataServiceServicer::new(
            self.config.simulation.run_id.clone(),
            &self.config,
            self.config.parameters.channels, // total channel count for device
            graceful_shutdown_clone,
            data_delay,
            data_runtime
        ));

        let tls_position = self.get_tls_config();
        let addr_position: SocketAddr = format!("[::0]:{}", self.config.server.position_port).parse().unwrap();

        // Send off the main server as well - this allows us to check for the
        // graceful shutdown Mutex and shutdown the main run routine as well
        let data_handle = tokio::spawn(async move {
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
                .await.unwrap();
        });


        tokio::spawn(async move {
            // Shutdown signal on manual termination
            // replaces the previous implementation
            // due to multiple handler errors when 
            // running multiple setups
            tokio::select! {
                _ = signal::ctrl_c() => {
                    log::warn!("Received manual shutdown signal");
                    {
                        let mut x = graceful_shutdown.lock().unwrap();
                        *x = true;
                    }
                },
            }
        });


        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3));

        loop {
            interval.tick().await;
            if *graceful_shutdown_main.lock().unwrap() {
                log::info!("Received graceful shutdown signal in main routine");
                // Abort calls since we may want to reuse the same struct 
                // `run` method in a loop for benchmarks
                data_handle.abort();
                manager_handle.abort();
                std::thread::sleep(Duration::from_secs(10));
                break;
            }
        }

        Ok(())
    }
    fn get_manager_service_server(&self) -> ManagerServiceServer<Manager> {

        // Create the manager server and add the service to it
        let manager_init = Manager {
            positions: vec![FlowCellPosition {
                // Icarust uses `device_id` for this - not sure if it should be `position`? 
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
        let server_identity = Identity::from_pem(SERVER_CERT, SERVER_KEY);
        ServerTlsConfig::new().identity(server_identity)
    }
    fn parse_simulation_header(community: &PathBuf, config_target_yield: &Option<f64>) -> (u64, f64, Option<f64>, String) {

        let reader = FileReader::open(&community).unwrap();

        // Community files created with Cipher have the required information in the header
        let header = reader.header();

        let run_id = from_utf8(
            header.get_attribute("run_id", 0).unwrap()
        ).unwrap().parse::<String>().unwrap();

        let sampling_rate = from_utf8(
            header.get_attribute("sampling_rate", 0).unwrap()
        ).unwrap().parse::<u64>().unwrap();
        

        let mean_read_length = from_utf8(
            header.get_attribute("mean_read_length", 0).unwrap()
        ).unwrap().parse::<f64>().unwrap();

        let target_yield = match config_target_yield {
            // If a target yield was specifically set in the configuration
            // use this value instead of the community header value -
            // this should always be done with continuous sampling 
            // without depletion!
            Some(target_yield) => Some(target_yield.to_owned()),
            None => {
                 Some(from_utf8(
                    header.get_attribute("target_yield", 0).unwrap()
                ).unwrap().parse::<f64>().unwrap())
            }
        };
        

        (sampling_rate, mean_read_length, target_yield, run_id)

    }
    fn get_run_params(config: &Config) -> PathBuf {

        let run_start: String = format!("{}", chrono::Utc::now().format("%Y%m%d_%H%M"));

        // Traditional path of real run or direct output directory
        let outdir = match (
            &config.parameters.experiment_name, 
            &config.parameters.flowcell_name, 
            &config.parameters.sample_name
        ) {
            (
                Some(experiment_name), 
                Some(flowcell_name), 
                Some(sample_name)
            ) => {
                config.outdir.join(experiment_name).join(sample_name).join(
                    format!("{run_start}_XIII_{flowcell_name}_{}", &config.simulation.run_id[0..9])
                ).join("fast5_pass")
            },
            _ => config.outdir.to_owned()
        };   
        
        outdir
    }
}
