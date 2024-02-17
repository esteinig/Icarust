#![allow(missing_docs)]
//!This crate wraps the most basic functionality of the [`ONT-FAST5-API`] for python, but in Rust!
//!# Warning
//!Very much in alpha and a WIP, I worte this for one specific use case that I had.
//!
//! Currently it is only possible to read and write FAST5. It uses the HDF5 crate to deal with HDF5 files.
//! It does apply the VBZ plugin to the files.
//! 
//!  [`ONT-FAST5_API`]: https://github.com/nanoporetech/ont_fast5_api

use hdf5::types::VarLenAscii;
use hdf5::{Error, File, Group, Result};
use std::collections::HashMap;
use std::ffi::OsStr;

use crate::frust5_api::utils;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

///Provides the Attributes for the Raw Group - which in turn conatins the Signal dataset.
///This allows us to match for each attribute field, and provide the correct type to HDF5.
pub enum RawAttrsOpts<'a> {
    /// The duration of the read in seconds.
    Duration(u32),
    /// The reason that the read stopped.
    EndReason(u8),
    /// No idea lol
    MedianBefore(f64),
    /// 37 character UUID-4 identifer for the read.
    ReadId(&'a str),
    /// Read number - the number the read is in the run.
    ReadNumber(i32),
    /// Also not sure
    StartMux(u8),
    /// The start time of the read in milliseconds (I guess)
    StartTime(u64),
}

/// Open a fast5 and return a Vec containing the groups.
/// 
/// # Panics
/// Will panic if no Groups are found, or there is an issue reading the FAST5 file.
/// 
/// # Example
/// ```rust
/// use frust5_api::read_fast5;

/// ```
pub fn read_fast5(file_name: &str) -> Result<Vec<Group>, hdf5::Error> {
    let file = File::open(file_name)?; // open for reading
    let gs = file.groups()?; // open the dataset;
    Ok(gs.clone())
}

/// Struct representing a "Multi" Fast5 file. 
pub struct MultiFast5File {
    /// The filename of the MultiFast5 opened.
    filename: String,
    /// The mode that the file was opened as.
    mode: OpenMode,
    /// The handle to the HDF5 file.
    handle: File,
    /// A hashmap of run_id to the read_id of the first read - used to hardlink all the attributes of read groups together.
    _run_id_map: HashMap<String, String>,
}

/// Stuct to represent the channel info attributes for each read.
pub struct ChannelInfo {
    pub digitisation: f64,
    pub offset: f64,
    pub range: f64,
    pub sampling_rate: f64,
    pub channel_number: String,
}

#[doc(hidden)]
impl IntoIterator for ChannelInfo {
    type Item = (String, f64);
    type IntoIter = std::array::IntoIter<(String, f64), 4>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIterator::into_iter([
            ("digitisation".to_string(), self.digitisation),
            ("offset".to_string(), self.offset),
            ("range".to_string(), self.range),
            ("sampling_rate".to_string(), self.sampling_rate),
        ])
    }
}

impl ChannelInfo {
    /// Return a new Channel Info struct.
    pub fn new(
        digitisation: f64,
        offset: f64,
        range: f64,
        sampling_rate: f64,
        channel_number: String,
    ) -> ChannelInfo {
        ChannelInfo {
            digitisation,
            offset,
            range,
            sampling_rate,
            channel_number,
        }
    }
}

/// The mode to open a file with.
pub enum OpenMode {
    /// Open a fast5 to write to the end of.
    Append,
    /// Open a fast5 for read only.
    Read,
}
const HARDLINK_GROUPS: [&str; 2] = ["context_tags", "tracking_id"];

impl MultiFast5File {
    /// Create a new MultiFast5 file - for either reading or writing.
    /// 
    /// # Panics
    /// 
    /// - Currently if opening for writing and the file already exists, as tries to write attributes that already exist
    /// - If opening for reading and the file doesn't already exist.
    /// 
    /// # Examples
    /// ```
    /// ```
    pub fn new(filename: String, mode: OpenMode) -> MultiFast5File {
        let file = match mode {
            OpenMode::Append => {
                let file = File::with_options()
                    .with_fapl(|p| p.core().core_filebacked(true))
                    .append(&filename)
                    .unwrap();
                // default attributes for now
                let file_type = VarLenAscii::from_ascii("multi-read").unwrap();
                let file_version = VarLenAscii::from_ascii("2.2").unwrap();
                file.new_attr::<VarLenAscii>()
                    .create("file_type")
                    .unwrap()
                    .write_scalar(&file_type)
                    .unwrap();
                file.new_attr::<VarLenAscii>()
                    .create("file_version")
                    .unwrap()
                    .write_scalar(&file_version)
                    .unwrap();
                file
            }
            OpenMode::Read => {
                File::open("FAL37440_pass_5e83140e_100.fast5").unwrap() // open for reading
            }
        };
        MultiFast5File {
            filename: filename.clone(),
            mode,
            handle: file,
            _run_id_map: HashMap::new(),
        }
    }
    /// Diverged from ONT come back and straight up rework
    /// Create and empty read group, and populate all the fields for it. MISNOMER - doesn't return an empty read, returns a populated read.
    pub fn create_empty_read(
        &mut self,
        read_id: String,
        run_id: String,
        tracking_id: &HashMap<&str, &str>,
        context_tags: &HashMap<&str, &str>,
        channel_info: ChannelInfo,
        raw_attrs: &HashMap<&str, RawAttrsOpts>,
        signal: Vec<i16>,
        vbz_plugin: &OsStr
    ) -> Result<Group, Error> {
        // plz work
        std::env::set_var("HDF5_PLUGIN_PATH", vbz_plugin);
        let group_name = format!("read_{}", read_id);
        let group = self.handle.create_group(&group_name).unwrap();
        let s = VarLenAscii::from_ascii(run_id.as_str()).unwrap();
        group
            .new_attr::<VarLenAscii>()
            .create("run_id")?
            .write_scalar(&s).expect(format!("{} group is {:#?}", &s, group).as_str());
        // set the shared groups for every read - namely the contstant Dict attributes
        if self._run_id_map.contains_key(&run_id) {
            for shared_group in HARDLINK_GROUPS {
                self.handle
                    .link_hard(
                        format!("read_{}/{}", self._run_id_map[&run_id], shared_group).as_str(),
                        format!("{}/{}", group_name, shared_group).as_str(),
                    )
                    .expect(format!("{}/{}", self._run_id_map[&run_id], shared_group).as_str());
            }
            // populate all the fields on the read.
        } else {
            self._run_id_map.insert(run_id, read_id);
            let context_group = group.create_group("context_tags")?;
            let tracking_group = group.create_group("tracking_id")?;
            utils::add_tracking_info(tracking_group, &tracking_id)?;
            utils::add_context_tags(context_group, context_tags)?;
        }
        let channel_group = group.create_group("channel_id")?;
        let raw_data_group = group.create_group("Raw")?;
        utils::add_channel_info(channel_group, channel_info)?;
        utils::add_raw_data(raw_data_group, signal, raw_attrs)?;
        Ok(group)
    }
}