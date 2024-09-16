
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, process};
use std::collections::HashMap;
use std::fs;

use crate::reacquisition::{DeathChance, calculate_death_chance};


#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct Config {
    pub name: String,
    pub outdir: PathBuf,
    pub server: ServerConfig,
    pub simulation: SimulationConfig,
    pub parameters: ParameterConfig,
    pub seed: Option<u64>,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct ServerConfig {
    pub manager_port: u32,
    pub position_port: u32,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct ParameterConfig {
    pub channels: usize,
    pub break_read_ms: u64,
    pub data_generator_sleep_ms: Option<u64>,
    pub data_service_sleep_ms: Option<u64>,
    pub working_pore_percent: usize,
    pub pore_death_multiplier: Option<f64>,
    pub device_id: String,
    pub position: String,
    pub sample_name: Option<String>,
    pub experiment_name: Option<String>,
    pub flowcell_name: Option<String>,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct SimulationConfig {
    pub community: PathBuf,
    pub deplete: bool,
    pub target_yield: Option<f64>,
    
    #[serde(skip_deserializing)]
    pub run_id: String,
    #[serde(skip_deserializing)]
    pub mean_read_length: f64,
    #[serde(skip_deserializing)]
    pub sampling_rate: u64,
}

impl Config {

    pub fn to_json(&self, file: &PathBuf) {
        serde_json::to_writer(
            &std::fs::File::create(&file).expect("Faile to create Icarust configuration file"), &self
        ).expect("Failed to write Icarust configuration to file")
    }

    /// Calculate the base chance a pore will die
    pub fn calculate_death_chance(&self, starting_channels: usize) -> HashMap<String, DeathChance> {

        let mut deaths = HashMap::new();
        let death = DeathChance {
            base_chance: calculate_death_chance(
                starting_channels as f64,
                match self.simulation.target_yield {
                    Some(target_yield) => target_yield,
                    None => {
                        log::error!("Target yield for simulation must be specified"); // usually set in config parser
                        process::exit(1)
                    }
                },
                self.simulation.mean_read_length,
                match self.parameters.pore_death_multiplier { Some(multiplier) => multiplier, None => 1.0 }
            ),
            mean_read_length: self.simulation.mean_read_length,
            multiplier: match self.parameters.pore_death_multiplier { Some(multiplier) => multiplier, None => 1.0 }
        };
        deaths.insert("0".to_string(), death);

        deaths
    }

    // Get the User set random seed. If not found provide one as a random usize
    pub fn get_rand_seed(&self) -> u64 {
        match self.seed {
            Some(seed) => seed,
            None => rand::random::<u64>(),
        }
    }
}


/// Loads our config TOML to get the sample name, experiment name and flowcell name, which is returned as a Config struct.
pub fn load_toml(file_path: &std::path::PathBuf) -> Config {
    let contents = fs::read_to_string(file_path).expect("Something went wrong with reading the config file");
    let config: Config = toml::from_str(&contents).unwrap();
    config
}