
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

use crate::reacquisition_distribution::{DeathChance, _calculate_death_chance};

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct Config {
    pub output_path: PathBuf,
    pub target_yield: f64,
    pub mean_read_length: f64,
    pub community: PathBuf,
    pub parameters: Parameters,
    pub working_pore_percent: Option<usize>,
    pub random_seed: Option<u64>,
}

impl Config {

    pub fn to_json(&self, file: &PathBuf) {
        serde_json::to_writer(
            &std::fs::File::create(&file).expect("Faile to create Icarust configuration file"), &self
        ).expect("Failed to write Icarust configuration to file")
    }

    pub fn get_working_pore_precent(&self) -> usize {
        self.working_pore_percent.unwrap_or(90)
    }

    /// Calculate the chance a pore will die - TODO: modify simulation with per member mean read length and use in configuration 
    pub fn calculate_death_chance(&self, starting_channels: usize) -> HashMap<String, DeathChance> {

        let mut deaths = HashMap::new();
        let death = DeathChance {
            base_chance: _calculate_death_chance(
                starting_channels as f64,
                self.target_yield.to_owned(),
                self.mean_read_length,
            ),
            mean_read_length: self.mean_read_length,
        };
        deaths.insert("0".to_string(), death);

        deaths
    }

    // Get the User set random seed. If not found provide one as a random usize
    pub fn get_rand_seed(&self) -> u64 {
        match self.random_seed {
            Some(seed) => seed,
            None => rand::random::<u64>(),
        }
    }
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct Parameters {
    pub sample_name: String,
    pub experiment_name: String,
    pub flowcell_name: String,
    pub experiment_duration_set: Option<usize>,
    pub device_id: String,
    pub position: String,
    pub break_read_ms: Option<u64>,
    /// The sample rate in Hz - one of 4000 or 5000. Only used for RNA R9 and DNA R10
    pub sample_rate: Option<u64>,
    // ES: added from original fork - configuration for the data service added 
    // to parameter configuraton file rather than separate .ini file
    pub cert_dir: PathBuf,
    pub manager_port: u32,
    pub position_port: u32,
    pub channels: usize,
}

impl Parameters {
    /// Return the chunk size of sample measured in ms. If not specified, defaults to 400ms.
    pub fn get_chunk_size_ms(&self) -> u64 {
        self.break_read_ms.unwrap_or(400)
    }
}

impl Parameters {
    /// Return the sample rate (hz) of the signal in pores. If not specified, returns 4000.
    pub fn get_sample_rate(&self) -> u64 {
        self.sample_rate.unwrap_or(4000)
    }
}



/// Loads our config TOML to get the sample name, experiment name and flowcell name, which is returned as a Config struct.
pub fn load_toml(file_path: &std::path::PathBuf) -> Config {
    let contents = fs::read_to_string(file_path).expect("Something went wrong with reading the config file");
    let config: Config = toml::from_str(&contents).unwrap();
    config
}
