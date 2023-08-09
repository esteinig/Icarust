
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

use crate::config::{Config, load_toml};

/// Icarust main runner
pub struct Icarust {
    pub config: Config,
    pub run_id: String,
    pub output_path: PathBuf
}
impl Icarust {
    pub fn from_toml(file_path: &PathBuf) -> Self {
        
        let config = load_toml(file_path);
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
            
        let tls_position = self.get_tls_config();
        let addr_position: SocketAddr = format!("[::0]:{}", self.config.parameters.position_port).parse().unwrap();

        // Send of the main server as well - this allows us to check for the
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
            .await
            .unwrap();
        });


        ctrlc::set_handler(move || {
            {
                let mut x = graceful_shutdown.lock().unwrap();
                *x = true;
            }
            std::thread::sleep(Duration::from_millis(2000));
        }).expect("FAILED TO CATCH SIGNAL SOMEWHOW");


        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3));
    
        loop {
            interval.tick().await;
            let x = graceful_shutdown_main.lock().unwrap();
            if *x {
                log::info!("Received graceful shutdown signal in main routine");
                // Added this since we may want to reuse the same struct and run method
                data_handle.abort();
                manager_handle.abort();
                std::thread::sleep(Duration::from_secs(2));
                break;
            }
        }

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
        let start_time_string: String = format!("{}", chrono::Utc::now().format("%Y%m%d_%H%M"));
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