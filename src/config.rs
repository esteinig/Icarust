
use std::path::PathBuf;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;

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
    pub parameters: Parameters,
    pub sample: Vec<Sample>,
    pub output_path: std::path::PathBuf,
    pub global_mean_read_length: Option<f64>,
    pub random_seed: Option<u64>,
    pub target_yield: f64,
    pub working_pore_percent: Option<usize>,
    pub pore_type: Option<String>,
    
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
    pub cert_dir: PathBuf,
    pub manager_port: u32,
    pub position_port: u32,
    pub channels: usize,
    pub sample_name: String,
    pub experiment_name: String,
    pub flowcell_name: String,
    pub experiment_duration_set: Option<usize>,
    pub device_id: String,
    pub position: String,
    pub break_read_ms: Option<u64>,
}

impl Parameters {
    pub fn get_chunk_size_ms(&self) -> u64 {
        self.break_read_ms.unwrap_or(400)
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Sample {
    pub name: String,
    pub input_genome: std::path::PathBuf,
    pub mean_read_length: Option<f64>,
    pub weight: usize,
    pub weights_files: Option<Vec<std::path::PathBuf>>,
    pub amplicon: Option<bool>,
    pub barcodes: Option<Vec<String>>,
    pub barcode_weights: Option<Vec<usize>>,
    pub uneven: Option<bool>,
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
pub fn load_toml(file_path: &std::path::PathBuf) -> Config {
    let contents = fs::read_to_string(file_path).expect("Something went wrong with reading the config file");
    let config: Config = toml::from_str(&contents).unwrap();
    config
}