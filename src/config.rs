
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

use crate::reacquisition_distribution::{DeathChance, _calculate_death_chance};
use crate::read_length_distribution::ReadLengthDist;

/// Holds the type of the pore we are simulating
#[derive(Clone, Copy, Serialize)]
pub enum PoreType {
    /// R10 model
    R10,
    /// R9 model
    R9,
}

/// Type of simulation - RNA or DNA? :hmmmm:
#[derive(Clone, Copy, Serialize)]
pub enum NucleotideType {
    /// DNA -ACGT baby
    DNA,
    /// RNA - boooo
    RNA,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct Config {
    pub parameters: Parameters,
    pub sample: Vec<Sample>,
    pub output_path: std::path::PathBuf,
    pub global_mean_read_length: Option<f64>,
    pub random_seed: Option<u64>,
    pub target_yield: f64,
    pub working_pore_percent: Option<usize>,
    pub nucleotide_type: Option<String>,
    pub pore_type: Option<String>,
    pub pod5: bool,
    // ES: Added plugin path to configuration file for Frust5 integration
    pub vbz_plugin: PathBuf,
    // ES: Added k-mer model data as configurable path
    pub kmer_model: PathBuf,
    // ES: Added prefix squiggle as configurable path
    pub prefix_squiggle: PathBuf
    // My apologies Rory, it probably didn't have source code published for a reason, 
    // but needed to modify the plugin path for config to use Icarust as a library
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

    /// Check that we have a valid pore type if None specified, return R10.
    pub fn check_pore_type(&self) -> PoreType {
        match &self.pore_type {
            Some(pore_type) => match pore_type.as_str() {
                "R10" => PoreType::R10,
                "R9" => PoreType::R9,
                _ => {
                    panic!("Invalid model specified - must be one of R10 or R9")
                }
            },
            None => PoreType::R10,
        }
    }

    /// Check whether we are simulating RNA or DNA - if not specified in config - DNA
    pub fn check_dna_or_rna(&self) -> NucleotideType {
        match &self.nucleotide_type {
            Some(nucleotide_type) => match nucleotide_type.as_str() {
                "DNA" => NucleotideType::DNA,
                "RNA" => NucleotideType::RNA,
                _ => {
                    panic!("Invalid Nucleotide specified - must be one of DNA or RNA")
                }
            },
            None => NucleotideType::DNA,
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
        let _ = &self.check_pore_type();
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


#[derive(Deserialize, Debug, Clone, Serialize)]
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
    pub fn get_read_len_dist(
        &self,
        global_read_len: Option<f64>,
        sample_rate: u64,
    ) -> ReadLengthDist {
        match self.mean_read_length {
            Some(mrl) => ReadLengthDist::new(mrl / 400.0 * sample_rate as f64),
            None => ReadLengthDist::new(global_read_len.unwrap() / 400.0 * sample_rate as f64),
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
