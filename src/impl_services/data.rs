#![allow(missing_docs)]
#![deny(missing_doc_code_examples)]
#![allow(clippy::too_many_arguments)]

use futures::lock::Mutex as AsyncMutex;

use futures::{Stream, StreamExt};
use std::cmp::min;
use std::collections::HashMap;
use std::fmt;
use std::fs::create_dir_all;
use std::mem;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::from_utf8;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;
use std::{thread, u8};

use byteorder::{ByteOrder, LittleEndian};
use chrono::prelude::*;
use fnv::FnvHashSet;

use rand::prelude::*;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::reacquisition_distribution::{ReacquisitionPoisson, SampleDist};
use crate::services::minknow_api::data::data_service_server::DataService;
use crate::services::minknow_api::data::get_data_types_response::DataType;
use crate::services::minknow_api::data::get_live_reads_request::action;
use crate::services::minknow_api::data::get_live_reads_response::ReadData;
use crate::services::minknow_api::data::{
    get_live_reads_request, get_live_reads_response, GetDataTypesRequest, GetDataTypesResponse,
    GetLiveReadsRequest, GetLiveReadsResponse,
};
use crate::config::Config;

/// unused
#[derive(Debug)]
struct RunSetup {
    setup: bool,
    first: u32,
    last: u32,
    dtype: i32,
}

impl RunSetup {
    pub fn new() -> RunSetup {
        RunSetup {
            setup: false,
            first: 0,
            last: 0,
            dtype: 0,
        }
    }
}

#[derive(Debug)]
pub struct DataServiceServicer {
    read_data: Arc<Mutex<Vec<ReadInfo>>>,
    // to be implemented
    action_responses: Arc<Mutex<Vec<get_live_reads_response::ActionResponse>>>,
    setup: Arc<Mutex<RunSetup>>,
    break_chunks_ms: u64,
    channel_size: usize,
    sample_rate: u64,
}

/// Internal to the data generation thread
#[derive(Clone)]
struct ReadInfo {
    read_id: String,
    read: Vec<i16>,
    channel: usize,
    stop_receiving: bool,
    read_number: u32,
    was_unblocked: bool,
    write_out: bool,
    start_time: u64,
    start_time_seconds: usize,
    start_time_utc: DateTime<Utc>,
    start_mux: u8,
    end_reason: u8,
    channel_number: String,
    prev_chunk_start: usize,
    duration: usize,
    time_accessed: DateTime<Utc>,
    time_unblocked: DateTime<Utc>,
    dead: bool,
    last_read_len: u64,
    pause: f64,
    // Which sample is this read from - so we can get the chance it kills the pore
    read_sample_name: String,
}

impl fmt::Debug for ReadInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{{\n
        Read Id: {}
        Stop receiving: {}
        Data len: {}
        Read number: {}
        Was Unblocked: {}
        Duration: {}
        Time Started: {}
        Time Accessed: {}
        Prev Chunk End: {}
        Dead: {}
        Read from: {}
        Pause: {}
        WriteOut: {}
        }}",
            self.read_id,
            self.stop_receiving,
            self.read.len(),
            self.read_number,
            self.was_unblocked,
            self.duration,
            self.start_time_utc,
            self.time_accessed,
            self.prev_chunk_start,
            self.dead,
            self.read_sample_name,
            self.pause,
            self.write_out
        )
    }
}

/// Convert our vec of i16 signal to a vec of bytes to be transferred to the read until API
fn convert_to_u8(raw_data: Vec<i16>) -> Vec<u8> {
    let mut dst: Vec<u8> = vec![0; raw_data.len() * 2];
    LittleEndian::write_i16_into(&raw_data, &mut dst);
    dst
}

/// Create the output dir to write fast5 too, if it doesn't already exist
fn create_ouput_dir(output_dir: &std::path::PathBuf) -> std::io::Result<()> {
    create_dir_all(output_dir)?;
    Ok(())
}


use slow5::{FieldType, FileWriter, Record, RecordCompression, SignalCompression};

/// Start the thread that will handle writing out the signal file
fn start_write_out_thread(
    run_id: String,
    config: &Config,
    output_path: PathBuf,
    write_out_gracefully: Arc<Mutex<bool>>,
) -> SyncSender<ReadInfo> {
    let (complete_read_tx, complete_read_rx) = sync_channel(8000);
    let config = config.clone();

    thread::spawn(move || {
        
        let mut read_infos: Vec<ReadInfo> = Vec::with_capacity(8000);

        let exp_start_time = Utc::now();
        let iso_time = exp_start_time.to_rfc3339_opts(
            SecondsFormat::Millis, false
        );

        let sample_rate = config.parameters.get_sample_rate() as f64;


        let output_dir = PathBuf::from(
            format!("{}/fast5_pass", output_path.display())
        );
        if !output_dir.exists() {
            create_ouput_dir(&output_dir).unwrap();
        }

        let mut read_numbers_seen = FnvHashSet::with_capacity_and_hasher(
            4000, Default::default()
        );
        let mut file_counter = 0;

        loop {

            // Loop to collect reads and write out files
            for finished_read_info in complete_read_rx.try_iter() {
                read_infos.push(finished_read_info);
            }

            // Check if the graceful termination for 
            // the write out thread has been triggered
            let terminate_thread = { 
                *write_out_gracefully.lock().unwrap() 
            };

            if read_infos.len() >= 4000 || terminate_thread {

                // Output file path for Blow5
                let output_blow5 = output_dir.join(
                    format!(
                        "{}_pass_{}_{}.blow5",
                        config.parameters.flowcell_name,
                        &run_id[0..6],
                        file_counter,
                    )
                );

                // Drain 4000 reads and write them into Blow5
                let mut writer = FileWriter::options()
                    .record_compression(RecordCompression::Zlib)
                    .signal_compression(SignalCompression::StreamVByte)
                    .attr("run_id", "run_0", 0)
                    .attr("asic_id", "asic_id_0", 0)
                    .aux("median", FieldType::Float)
                    .create(&output_blow5)
                    .unwrap();
                
                log::info!("Writing out file to: {}", output_blow5.display());

                let range_end = min(4000, read_infos.len());
                for read_info in read_infos.drain(..range_end) {

                    // Skip this read if we are trying to write it out twice
                    if !read_numbers_seen.insert(read_info.read_id.clone()) {
                        log::warn!("Read seen twice?");
                        continue;
                    }

                    let mut new_end = read_info.read.len();

                    if read_info.was_unblocked {
                        // We calculate the actual unblocked signal array length using
                        // time of unblock and start time of read on write-out
                        let elapsed_time: chrono::TimeDelta = read_info.time_unblocked.time() - read_info.start_time_utc.time();
                        let stop = convert_milliseconds_to_samples(
                            elapsed_time.num_milliseconds(),
                            config.parameters.get_sample_rate(),
                        );
                        new_end = min(stop, read_info.read.len());
                    }

                    let signal = read_info.read[0..new_end].to_vec();
                    log::debug!("{read_info:#?}");

                    if signal.is_empty() {
                        log::error!("Attempted to write empty signal");
                        continue;
                    };

                    let mut rec = Record::builder()
                        .read_id(read_info.read_id)
                        .read_group(0)
                        .range(12.0)
                        .digitisation(4096.)
                        .offset(3.0)
                        .sampling_rate(sample_rate)
                        .raw_signal(&read_info.read)
                        .build()
                        .unwrap();

                    rec.set_aux_field(&mut writer, "median", 1.2f32).unwrap();
                    writer.add_record(&rec).unwrap();
                    
                
                

                    //     match out_file {
                    //         OutputFileType::Fast5(ref mut multi) => {
                    //             let raw_attrs: HashMap<&str, RawAttrsOpts> = HashMap::from([
                    //                 ("duration", RawAttrsOpts::Duration(signal.len() as u32)),
                    //                 (
                    //                     "end_reason",
                    //                     RawAttrsOpts::EndReason(to_write_info.end_reason),
                    //                 ),
                    //                 ("median_before", RawAttrsOpts::MedianBefore(100.0)),
                    //                 (
                    //                     "read_id",
                    //                     RawAttrsOpts::ReadId(to_write_info.read_id.as_str()),
                    //                 ),
                    //                 (
                    //                     "read_number",
                    //                     RawAttrsOpts::ReadNumber(to_write_info.read_number as i32),
                    //                 ),
                    //                 ("start_mux", RawAttrsOpts::StartMux(to_write_info.start_mux)),
                    //                 (
                    //                     "start_time",
                    //                     RawAttrsOpts::StartTime(to_write_info.start_time),
                    //                 ),
                    //             ]);
                    //             let channel_info = ChannelInfo::new(
                    //                 2048_f64,
                    //                 0.0,
                    //                 200.0,
                    //                 config.parameters.get_sample_rate() as f64,
                    //                 to_write_info.channel_number.clone(),
                    //             );
                    //            if let Err(_) = multi
                    //                 .create_populated_read(
                    //                     to_write_info.read_id.clone(),
                    //                     run_id.clone(),
                    //                     &tracking_id,
                    //                     &context_tags,
                    //                     channel_info,
                    //                     &raw_attrs,
                    //                     signal,
                    //                     config.vbz_plugin.as_os_str()
                    //                 )
                    //                 {   
                    //                     log::debug!("Read creation during Fast5 file write-out failed! Nothing to see here, citizen...");
                    //                     continue; // handle error when writing the Fast5 file
                    //                 }; 
                    //         }
                    //         OutputFileType::Pod5(ref mut pod5) => {
                    //             let end_reason = if to_write_info.was_unblocked {
                    //                 EndReason::DATA_SERVICE_UNBLOCK_MUX_CHANGE
                    //             } else {
                    //                 EndReason::SIGNAL_POSITIVE
                    //             };
                    //             let pt = match ic_pt {
                    //                 PoreType::R9 => PodPoreType::R9,
                    //                 PoreType::R10 => PodPoreType::R10,
                    //             };
                    //             let read: PodReadInfo = PodReadInfo {
                    //                 read_id: Uuid::parse_str(to_write_info.read_id.as_str()).unwrap(),
                    //                 pore_type: pt,
                    //                 signal_: signal,
                    //                 channel: to_write_info.channel as u16,
                    //                 well: 1,
                    //                 calibration_offset: -264.0,
                    //                 calibration_scale: 0.187_069_85,
                    //                 read_number: to_write_info.read_number,
                    //                 start: 1,
                    //                 median_before: 100.0,
                    //                 tracked_scaling_scale: 1.0,
                    //                 tracked_scaling_shift: 0.1,
                    //                 predicted_scaling_scale: 1.5,
                    //                 predicted_scaling_shift: 0.15,
                    //                 num_reads_since_mux_change: 10,
                    //                 time_since_mux_change: 5.0,
                    //                 num_minknow_events: 1000,
                    //                 end_reason,
                    //                 end_reason_forced: false,
                    //                 run_info: run_id.clone(),
                    //                 num_samples,
                    //             };
                    //             pod5.push_read(read);
                    //         }
                    //     }
                    // }
                    // if let OutputFileType::Pod5(ref mut pod5) = out_file {
                    //     pod5.write_reads_to_ipc();
                    //     // println!("{:#?}", pod5._signal);
                    //     pod5.write_signal_to_ipc();
                    //     pod5.write_footer();
                    // };
                }
                file_counter += 1;
                read_numbers_seen.clear();

                writer.close();
            }

            {
                if *write_out_gracefully.lock().unwrap() {
                    break;
                }
            }
            
            thread::sleep(Duration::from_millis(1));
        }
        log::info!("Exiting write out thread...");
    });
    complete_read_tx
}

fn start_unblock_thread(
    channel_read_info: Arc<Mutex<Vec<ReadInfo>>>,
    run_setup: Arc<Mutex<RunSetup>>,
) -> SyncSender<GetLiveReadsRequest> {

    let (tx, rx): (
        SyncSender<GetLiveReadsRequest>,
        Receiver<GetLiveReadsRequest>,
    ) = sync_channel(6000);

    thread::spawn(move || {
        
        // We have like some actions to adress before we do anything
        let mut read_numbers_actioned = [0; 3000];
        let mut total_unblocks = 0;
        let mut total_sr = 0;

        for get_live_req in rx.iter() {
            let request_type = get_live_req.request.unwrap();
            // Match whether we have actions or a setup
            let (_setup_proc, unblock_proc, stop_rec_proc) = match request_type {
                // Setup Request
                get_live_reads_request::Request::Setup(_) => setup(request_type, run_setup.clone()),
                // List of actions, pass through to take actions
                get_live_reads_request::Request::Actions(_) => {
                    take_actions(request_type, &channel_read_info, &mut read_numbers_actioned)
                }
            };
            
            total_unblocks += unblock_proc;
            total_sr += stop_rec_proc;

            log::info!(
                "Unblocked: {}, Stop receiving: {}, Total unblocks {}, total sr {}",
                unblock_proc, stop_rec_proc, total_unblocks, total_sr
            );
        }
    });

    tx
}

/// Process a get_live_reads_request StreamSetup, setting all the fields on the Threads RunSetup struct. This actually has no
/// effect on the run itself, but could be implemented to do so in the future if required.
fn setup(
    setup_request: get_live_reads_request::Request,
    setup_arc: Arc<Mutex<RunSetup>>,
) -> (usize, usize, usize) {
    
    let mut setup = setup_arc.lock().unwrap();
    log::info!("Received stream run setup, setting up...");

    if let get_live_reads_request::Request::Setup(_h) = setup_request {
        setup.first = _h.first_channel;
        setup.last = _h.last_channel;
        setup.dtype = _h.raw_data_type;
        setup.setup = true;
    }
    // We have processed the first action
    (1, 0, 0)
}

/// Iterate through a given set of received actions and match the type of action to take
/// Unblock a read by emptying the ReadChunk held in channel readinfo dict
/// Stop receving a read sets the stop_receiving field on a ReadInfo struct to True, so we don't send it back.
/// Action Responses are appendable to a Vec which can be shared between threads, so can be accessed by the GRPC, which drains the Vec and sends back all responses.
/// Returns the number of actions processed.
fn take_actions(
    action_request: get_live_reads_request::Request,
    channel_read_info: &Arc<Mutex<Vec<ReadInfo>>>,
    read_numbers_actioned: &mut [u32; 3000],
) -> (usize, usize, usize) {
    // Check that we have an action type and not a setup, which should be impossible
    log::debug!("Processing non setup actions");

    let (unblocks_processed, stop_rec_processed) = match action_request {
        get_live_reads_request::Request::Actions(actions) => {
            
            // let mut add_response = response_carrier.lock().unwrap();

            let mut unblocks_processed: usize = 0;
            let mut stop_rec_processed: usize = 0;

            let mut read_infos = channel_read_info.lock().unwrap();

            for action in actions.actions {
                
                // Action can be optional (none action) in RPC specification. Added a continue statement.
                if let None = action.action {
                    continue;
                }

                let action_type = action.action.unwrap();
                let zero_index_channel = action.channel as usize - 1;
                let (_action_response, unblock_count, stopped_count) = match action_type {
                    action::Action::Unblock(unblock) => unblock_reads(
                        unblock,
                        action.action_id,
                        zero_index_channel,
                        action.read.unwrap(),
                        read_numbers_actioned,
                        read_infos.get_mut(zero_index_channel).unwrap_or_else(|| {
                            panic!("failed to unblock on channel {}", action.channel)
                        }),
                    ),
                    action::Action::StopFurtherData(stop) => stop_sending_read(
                        stop,
                        action.action_id,
                        zero_index_channel,
                        read_infos.get_mut(zero_index_channel).unwrap_or_else(|| {
                            panic!("failed to stop receiving on channel {}", action.channel)
                        }),
                    ),
                };
                // add_response.push(action_response);
                unblocks_processed += unblock_count;
                stop_rec_processed += stopped_count;
            }
            (unblocks_processed, stop_rec_processed)
        }
        _ => panic!(),
    };
    (0, unblocks_processed, stop_rec_processed)
}

/// Unblocks reads by clearing the channels (Represented by the index in a Vec) read vec.
fn unblock_reads(
    _action: get_live_reads_request::UnblockAction,
    action_id: String,
    channel_number: usize,
    read_number: action::Read,
    channel_num_to_read_num: &mut [u32; 3000],
    channel_read_info: &mut ReadInfo,
) -> (
    Option<get_live_reads_response::ActionResponse>,
    usize,
    usize,
) {
    let value = channel_read_info;
    // Destructure read number from action request
    if let action::Read::Number(read_num) = read_number {
        // Check if the last read_num we performed an action on isn't this one, on this channel
        if channel_num_to_read_num[channel_number] == read_num {
            // log::debug!("Ignoring second unblock! on read {}", read_num);
            return (None, 0, 0);
        }
        if read_num != value.read_number {
            // log::debug!("Ignoring unblock for old read");
            return (None, 0, 0);
        }
        // If we are dealing with a new read, set the new read num as the last dealt with read num at this channel number
        channel_num_to_read_num[channel_number] = read_num;
    };
    // Set the was_unblocked field for writing out
    value.was_unblocked = true;
    value.write_out = true;
    // Set the time unblocked so we can work out the length of the read to serve
    value.time_unblocked = Utc::now();
    // End reason of unblock
    value.end_reason = 4;
    (
        Some(get_live_reads_response::ActionResponse {
            action_id,
            response: 0,
        }),
        1,
        0,
    )
}

/// Stop sending read data, sets Stop receiving to True.
fn stop_sending_read(
    _action: get_live_reads_request::StopFurtherData,
    action_id: String,
    _channel_number: usize,
    value: &mut ReadInfo,
) -> (
    Option<get_live_reads_response::ActionResponse>,
    usize,
    usize,
) {
    // need a way of picking out channel by channel number or read ID

    value.stop_receiving = true;
    (
        Some(get_live_reads_response::ActionResponse {
            action_id,
            response: 0,
        }),
        0,
        1,
    )
}

use slow5::{FileReader, RecordExt};

/// Read the pre-computed simulation Blow5 from Cipher
fn process_simulated_community(config: &Config) -> (FileReader, Vec<String>) {
    let mut reader = FileReader::open(&config.community).unwrap();
    
    let mut read_index = Vec::new();
    while let Some(Ok(rec)) = reader.records().next() {
        read_index.push(
            from_utf8(rec.read_id()).unwrap().to_string()
        );
    }

    // Reader is consumed in above iteration, so we return another open handle
    let read_reader: FileReader = FileReader::open(&config.community).unwrap();
    (read_reader, read_index)
}

/// Convert an elapased period of time in milliseconds into samples
fn convert_milliseconds_to_samples(milliseconds: i64, sampling: u64) -> usize {
    (milliseconds as f64 * (sampling / 1000) as f64) as usize
}

/// Create and return a vector that stores the internal data generate thread state to be shared between the server and the threads.
///
/// - the vector is the length of the set number of channels with each element representing a "channel"
/// - these are accessed by index, with channel 1 represented by element at index 0
/// - the created vector is populated by ReadInfo structs, which are used to track the ongoing state of a channel during a run
///
/// This vector already exists and is shared around, so is not returned by this function.
/// 
/// Returns the number of pores that are alive.
fn setup_channel_vec(
    size: usize,
    thread_safe: &Arc<Mutex<Vec<ReadInfo>>>,
    rng: &mut StdRng,
    wpp: usize,
) -> usize {

    // Create channel Arc<Mutex> vector - we hold a vector of chunks to be served each iteration below
    let thread_safe_chunks = Arc::clone(thread_safe);
    let mut channel_reads = thread_safe_chunks.lock().unwrap();

    let percent_pore = wpp as f64 / 100.0;
    let mut alive = 0;

    for channel_number in 1..size+1 {
        let read_info = ReadInfo {
            read_id: Uuid::nil().to_string(),
            read: vec![],
            channel: channel_number,
            stop_receiving: false,
            read_number: 0,
            was_unblocked: false,
            write_out: false,
            start_time: 0,
            start_time_seconds: 0,
            start_time_utc: Utc::now(),
            channel_number: channel_number.to_string(),
            end_reason: 0,
            start_mux: 1,
            prev_chunk_start: 0,
            duration: 0,
            time_accessed: Utc::now(),
            time_unblocked: Utc::now(),
            dead: !(rng.gen_bool(percent_pore)),
            last_read_len: 0,
            pause: 0.0,
            read_sample_name: String::from(""),
        };
        if !read_info.dead {
            alive += 1
        }
        channel_reads.push(read_info);
    }
    alive
}

/// Generate a read, which is stored as a `ReadInfo` in the channel_read_info vec. 
/// This is mutated in place for each new read created in the data service loop.
fn generate_read(
    read_reader: &mut FileReader,
    read_index: &Vec<String>,
    read_info: &mut ReadInfo,
    rng: &mut StdRng,
    read_number: &mut u32,
    experiment_start_time: &u64,
    sample_rate: u64,
) {
    // Set stop receieivng to false so we don't accidentally not send the read
    read_info.stop_receiving = false;
    // Update as this read hasn't yet been unblocked
    read_info.was_unblocked = false;
    // Signal positive end_reason
    read_info.end_reason = 1;
    // We want to write this out at the end (when it has completed or is unblocked)
    read_info.write_out = true;

    let now = Utc::now();

    // Read start time in samples (seconds since start of experiment * sample rate)
    read_info.start_time = (now.timestamp() as u64 - experiment_start_time) * sample_rate;
    read_info.start_time_seconds = (now.timestamp() as u64 - experiment_start_time) as usize;
    read_info.start_time_utc = now;

    read_info.read_number = *read_number;

    // We replace the entire read generation by a random sample 
    // of a signal read from a pre-computed community simulation
    // which can be any nucleic acid, pore version, read prefix
    // and is simulated at a particular depth and abundance for
    // each member

    // TODO: Check if we should give option deplete the community signal...

    // Get a random read from the simulated community
    let sample_index = rng.gen_range(0..read_index.len());
    let read_id = read_index[sample_index].as_bytes();

    // Random access through the Blow5 reader
    let record = read_reader.get_record(read_id).unwrap();

    // Iterate the view to get our full read
    read_info.read = record.raw_signal_iter().collect();

    // Set estimated duration in seconds
    read_info.duration = read_info.read.len() / sample_rate as usize;

    // Set the read length for channel death chance
    read_info.last_read_len = read_info.read.len() as u64;

    // Assign read identifier from sampled read only if sampling from the community with depletion 
    read_info.read_id = uuid::Uuid::new_v4().to_string(); // TODO: from_utf8(record.read_id()).unwrap().to_string();

    log::debug!("Sampled index {sample_index} with read identifier: {} [signal array length: {}]", read_info.read_id, read_info.read.len());

    // Reset these time based metrics
    read_info.time_accessed = Utc::now();

    // Previous chunk start has to be zero as there are now no previous chunks on a new read
    read_info.prev_chunk_start = 0;

}

impl DataServiceServicer {
    /// Configure a new read generator and spawn a thread 
    /// that produces the reads for each channel and sends
    /// them into the write out queue if a read completed 
    /// or was unblocked
    pub fn new(
        run_id: String,
        config: &Config,
        output_path: PathBuf,
        channel_size: usize,
        graceful_shutdown: Arc<Mutex<bool>>,
        data_delay: u64,   // seconds
        data_run_time: u64 // seconds
    ) -> DataServiceServicer {

        let now = Instant::now();

        // Setting up the runtime parameters
        let working_pore_percent = config.get_working_pore_precent();
        let break_chunks_ms: u64 = config.parameters.get_chunk_size_ms();
        let sample_rate: u64 = config.parameters.get_sample_rate();
        let start_time: u64 = Utc::now().timestamp() as u64;
        
        // Creates a thread safe vector of channels holding `ReadInfo`
        let channel_vec_safe: Arc<Mutex<Vec<ReadInfo>>> = Arc::new(
            Mutex::new(Vec::with_capacity(channel_size))
        );
        let channel_vec_safe_clone = Arc::clone(&channel_vec_safe);

        // Creates a thread safe vector of action response enums from the MinKNOW RPC
        let action_response_safe: Arc<Mutex<Vec<get_live_reads_response::ActionResponse>>> = Arc::new(
            Mutex::new(Vec::with_capacity(channel_size))
        );
        let _action_response_safe_clone = Arc::clone(&action_response_safe);
        
        // Creates a new thread safe run setup
        let run_setup = RunSetup::new();
        let is_setup = Arc::new(Mutex::new(run_setup));
        let is_safe_setup = Arc::clone(&is_setup);

        // Looks like this prepares the samples from the configuration...
        let (mut read_reader, read_index) = process_simulated_community(&config);
        
        // Graceful termination signals handled in main routine
        let write_out_gracefully = Arc::clone(&graceful_shutdown);
        let end_run_time_gracefully: Arc<Mutex<bool>> = Arc::clone(&write_out_gracefully);

        // Write out thread queue sender
        let complete_read_tx = start_write_out_thread(
            run_id, 
            config, 
            output_path, 
            write_out_gracefully
        );

        // Pore configurations: starting counts and death chances
        let mut rng: StdRng = rand::SeedableRng::seed_from_u64(1234567);

        // Sets up the mutable thread safe channel vector with initial data
        // Channel with initial configuration is not returned since we are using the thread safe Arc<Mutex>> wrapper so that
        // the channel vector can be modified across threads, we do however get the initial function pore count back

        let starting_functional_pore_count = setup_channel_vec(
            channel_size, 
            &channel_vec_safe,
            &mut rng, 
            working_pore_percent
        );

        // Use initial functional pore count to calculate death chances
        let death_chance = config.calculate_death_chance(starting_functional_pore_count);
        log::info!("Death chances {:#?}", death_chance);

        // See where this belongs...
        let mut time_logged_at: f64 = 0.0;

        if data_run_time > 0 {
            log::warn!("Maximum run time for data generation set to {} seconds", data_run_time)
        }

        // Start the thread to generate data
        thread::spawn(move || {

            // Delay data generation, set from command line or configuration
            if data_delay > 0 {
                log::info!("Delay data generation by {} seconds...", &data_delay);
                thread::sleep(std::time::Duration::from_secs(data_delay.clone()));
            }

            // Setup reacquisition poisson distribution
            let reacquisition_poisson = ReacquisitionPoisson::new(1.0, 0.0, 0.0001, 0.05);

            // Read number for adding to unblock
            let mut read_number: u32 = 0;
            let mut completed_reads: u32 = 0;

            // Infinite loop for data generation
            loop {
                let read_process = Instant::now();
                log::debug!("Sequencer mock loop start");

                let mut new_reads = 0;
                let mut dead_pores = 0;
                let mut empty_pores = 0;
                let mut awaiting_reacquisition = 0;
                let mut occupied = 0;

                // Not sure what's going on here?

                // Sleep the length of the milliseconds chunk size
                // Don't sleep the thread just reacquire reads
                thread::sleep(Duration::from_millis(10));

                let _channels_with_reads = 0;

                // Access the channel vector with a lock
                let mut channels = channel_vec_safe_clone.lock().unwrap();

                // Iterate over configured channel indices and get some basic stats 
                // about what is going on at each channel; ceck if a read has finished 
                // or has been unblocked => send it to write queue and generate a new read 
                // for this channel, which can be acquired at a certain probability (80%)
                for i in 0..channel_size {
                    
                    let time_taken = read_process.elapsed().as_secs_f64();

                    // Safely get the current `ReadInfo` for this channel
                    let read_info = channels.get_mut(i).unwrap();

                    // If this channel read is marked as dead, increase the
                    // current dead pore count and check the next channel read
                    if read_info.dead {
                        dead_pores += 1;
                        continue;
                    }

                    // If this channel does not currently have a read we mark this 
                    // channel as empty otherwise we mark this channel as occupied
                    if read_info.read.is_empty() {
                        empty_pores += 1;
                        // If the channel pause is positive (i.e. after a read has completed, the pause
                        // period is drawn from a poisson distribution and marks the time before another 
                        // fragment can be acquired) subtract the time taken since the start if this data 
                        // generation loop iteration and mark this channel as awaiting reacquisition 
                        if read_info.pause > 0.0 {
                            read_info.pause -= time_taken;
                            awaiting_reacquisition += 1;
                            continue;
                        }
                    } else {
                        occupied += 1;
                    }

                    // I think this is actually quite important for downstream basecalling and evaluation of 
                    // experiment results over time - check if the commented out sleep statement affects this
                    let read_estimated_finish_time = read_info.start_time_seconds + read_info.duration;

                    // Time since the experiment started until now - used to check if the read has finished
                    let experiment_time = Utc::now().timestamp() as u64 - start_time;

                    log::debug!(
                        "exp time: {}, read_finish_time: {}, is exp greater {}", 
                        experiment_time, 
                        read_estimated_finish_time, 
                        experiment_time as usize > read_estimated_finish_time
                    );
                    
                    // Read has finished or was unblocked
                    if experiment_time as usize > read_estimated_finish_time || read_info.was_unblocked

                    {   
                        // Attribute is set when a new read is created in `generate_read` 
                        // and should therefore always be true?

                        // If the write out attribute of `ReadInfo` for this read is true
                        // several things happen... 
                        if read_info.write_out {

                            // Read is counted as complete
                            completed_reads += 1;

                            // Read is sent to the completed reads queue for write out
                            complete_read_tx.send(read_info.clone()).unwrap();

                            // Pause is set using a random sample of the reacquisition distribution
                            // not entirely sure what's happening in this case...
                            read_info.pause = reacquisition_poisson.sample(&mut rng);

                            // Death chance is computed based on the configured death chance for the sample
                            let yolo = death_chance.get("0").unwrap();

                            // All our death chances are altered by yield, so we need to change the 
                            // chance of death  if a read was unblocked due to resulting lowered yield
                            let prev_chance_multiplier = match read_info.was_unblocked {
                                true => {
                                    // We unblocked the read and now we need to alter the chance of death
                                    // for this pore to be lower as the yield was lowered
                                    let unblock_time = read_info.time_unblocked;
                                    let read_start_time = read_info.start_time_utc;
                                    let elapsed_time = (unblock_time - read_start_time).num_milliseconds();
                                    // Convert the elapsed time into a very rough amount of bases
                                    (elapsed_time as f64 * 0.45) / yolo.mean_read_length
                                }
                                false => 1.0, // No change in death chance as read was not unblocked
                            };

                            // Compute whether the channel is now dead
                            read_info.dead = rng.gen_bool(
                                yolo.base_chance * prev_chance_multiplier,
                            );
                        }

                        // Clear the read in this channel
                        read_info.read.clear();
                        // Shrink the read allocation to new empty status
                        read_info.read.shrink_to_fit();
                        // Set was unblocked status to false if this 
                        read_info.was_unblocked = false;

                        // Not sure this comment is clear to me right now
                        read_info.write_out = false; // Could be a slow problem here? 

                        // Our pore died, so sad
                        if read_info.dead {
                            dead_pores += 1;
                            continue;
                        }

                        // Chance to acquire a new read - we might
                        // want to make this configurable
                        if rng.gen_bool(0.8) {

                            new_reads += 1;
                            read_number += 1;

                            // Create new read for this channel - this also sets the read start
                            // time which is later used in the signal request to compute the
                            // signal chunk size...

                            generate_read(
                                &mut read_reader,
                                &read_index,
                                read_info,
                                &mut rng,
                                &mut read_number,
                                &start_time,
                                sample_rate
                            )
                        }
                    }
                }
                
                // Logging every second
                let _end = now.elapsed().as_secs_f64();
                if _end.ceil() > time_logged_at {
                    log::info!(
                        "New reads: {}, Occupied: {}, Empty pores: {}, Dead pores: {}, Sequenced reads: {}, Awaiting: {}",
                        new_reads, occupied, empty_pores, dead_pores, completed_reads, awaiting_reacquisition
                    );
                    time_logged_at = _end.ceil();
                }

                // Graceful shutdown check at this iteration
                {
                    if *graceful_shutdown.lock().unwrap() {
                        break;
                    }
                }

                // If most pores are dead, end the experiment
                if dead_pores >= (0.99 * channel_size as f64) as usize {
                    *graceful_shutdown.lock().unwrap() = true;
                    break;
                }

                // Maximum run time of data generation, graceful shutdown without exiting process so that
                // the data generation routine can be used as library import and does not shutdown main runtime
                if data_run_time > 0 {
                    if now.elapsed().as_secs() >= data_run_time+data_delay {
                        log::warn!("Maximum run time for exceeded, ceased data generation and shutting down...");
                        {
                            let mut x = end_run_time_gracefully.lock().unwrap();
                            *x = true;
                        }
                        break;
                    }
                }
            }
        });

        // Return our newly initialised DataServiceServicer to add onto the GRPC server
        DataServiceServicer {
            read_data: channel_vec_safe, // Arc<Mutex> that links to the clone used in data generation loop
            action_responses: action_response_safe,
            setup: is_safe_setup,
            break_chunks_ms,
            channel_size,
            sample_rate,
        }
    }
}


#[derive(Debug, Clone)]
pub struct ChannelRange {
    pub start: usize,
    pub end: usize
}
impl ChannelRange {
    pub fn new(channel_size: &usize) -> Self {
        Self {
            start: 0,
            end: channel_size.clone()
        }
    }
}

#[tonic::async_trait]
impl DataService for DataServiceServicer {
    type get_live_readsStream = Pin<Box<dyn Stream<Item = Result<GetLiveReadsResponse, Status>> + Send + 'static>>;

    async fn get_live_reads(
        &self,
        _request: Request<tonic::Streaming<GetLiveReadsRequest>>,
    ) -> Result<Response<Self::get_live_readsStream>, Status> {
        // Incoming stream setup
        let mut stream = _request.into_inner();

        // Get a reference to the channel vector
        let data_lock = Arc::clone(&self.read_data);
        let data_lock_unblock = Arc::clone(&self.read_data);

        // ArcMutex of RunSetup
        let setup = Arc::clone(&self.setup.clone());

        // Start the unblock thread on setup
        let tx_unblocks = { start_unblock_thread(data_lock_unblock, setup) };
        
        // LiveReadsRequest counter
        let mut stream_counter = 1;

        let channel_size = self.channel_size;
        let break_chunk_ms = self.break_chunks_ms;
        let sample_rate = self.sample_rate;

        // Chunk size determined by break_chunk_ms and sample rate
        let chunk_size = break_chunk_ms as f64 / 1000.0 * sample_rate as f64;

        // AsyncMutex for channel range implementation
        let channel_range = Arc::new(AsyncMutex::new(ChannelRange::new(&channel_size)));
        let channel_range_clone = Arc::clone(&channel_range);

        // Stream the responses back
        let output = async_stream::try_stream! {

            // ES: Really clever to have the threads inside the stream generator and 
            // push data back through a bounded queue that yields into the stream...

            // Async channel that will await when it has one element, pushes the read response back immediately
            let (tx_get_live_reads_response, mut rx_get_live_reads_response) = tokio::sync::mpsc::channel(1);

            // Spawn an async thread that handles the incoming GetLiveReadsRequests - spawned after we receive our first connection
            tokio::spawn(async move {
                while let Some(live_reads_request) = stream.next().await {
                    let now2 = Instant::now();
                    let live_reads_request = live_reads_request.unwrap();

                    // On initiation request get the channel range indices for first and last channel
                    if let Some(get_live_reads_request::Request::Setup(setup_request)) = &live_reads_request.request {
                        let mut range = channel_range.lock().await;
                        range.start = setup_request.first_channel as usize - 1;  // channel range indices for iteratio below
                        range.end = setup_request.last_channel as usize - 1;
                        log::debug!("Channel range configured: {:#?}", &range);
                    }
                    // Send all the actions we wish to take to unblock thread
                    tx_unblocks.send(live_reads_request).unwrap();
                    stream_counter += 1
                }
            });

            // Spawn an async thread that will get the read data from the data generation thread and return it
            tokio::spawn(async move {
                loop{

                    let now2 = Instant::now();
                    let mut container: Vec<(usize, ReadData)> = Vec::with_capacity(channel_size);

                    // Number of chunks that we will send back on data generation
                    let size = (channel_size as f64 / 24 as f64).ceil() as usize;

                    // Current channel number
                    let mut channel: u32 = 1;

                    // Counters
                    let mut num_reads_stop_receiving: usize = 0;
                    let mut num_channels_empty: usize = 0;

                    // Maximum read length in samples that we will consider sending samples for -
                    // this is interesting - it should probably should be dependent on sample rate
                    // and pore speed:
                    
                    // - DNA R10.4.1 V14 at 400 bps and 5Khz => 5000/400 = 12.5 ADC per base => 30000/12.5 ~ 2.5kbp
                    // - DNA R9.4.1 at 450 bps and 4Khz => 4000/460 = 8.8 ADC per base => 30000/8.8 ~ 3.5kbp 

                    let max_read_len_samples: usize = 30000;

                    // The below code block allows us to send the responses across an await
                    {   
                        // Access the Arc<AsyncMutex> for the configured range from initiation request above
                        let range = channel_range_clone.lock().await; 

                        // Get the channel data from the service data, unlock the syncronous 
                        // Arc<Mutex> across an asyncronous await - may be able to change this
                        // to an Arc<AsyncMutex>
                        let mut read_data_vec = {
                            // log::debug!("Getting GRPC lock {:#?}", now2.elapsed().as_millis());
                            let mut z1 = data_lock.lock().unwrap();
                            // log::debug!("Got GRPC lock {:#?}", now2.elapsed().as_millis());
                            z1
                        };

                        // Iterate over each channel
                        for i in range.start..range.end {
                            
                            let mut read_info = read_data_vec.get_mut(i).unwrap();
                            log::debug!("Elapsed at start of drain {}", now2.elapsed().as_millis());

                            if !read_info.stop_receiving && !read_info.was_unblocked && read_info.read.len() > 0 {

                                // Work out where to start and stop our slice of signal
                                let mut start = read_info.prev_chunk_start;
                                let now_time = Utc::now();
                                
                                // Elapsed time since start of read generation
                                let read_start_time = read_info.start_time_utc;
                                let elapsed_time = now_time.time() - read_start_time.time();

                                // How far through the read we are in total samples
                                let mut stop = convert_milliseconds_to_samples(elapsed_time.num_milliseconds(), sample_rate);

                                // Slice of signal is too short
                                if start > stop || (stop - start) < chunk_size as usize {
                                    continue
                                }

                                // Read through pore is too long
                                if stop > max_read_len_samples {
                                    continue
                                }

                                // Only send last chunks worth of data
                                if  (stop - start) > (chunk_size as f64 * 1.1_f64) as usize {

                                    // Work out where a break_reads size finishes i.e if we have gotten 1.5 chunks worth since last time, 
                                    // that is not actually possible on a real sequencer. So we need to calculate where the 1 chunk finishes 
                                    // and set that as the `prev_chunk_stop`` and serve it

                                    let full_width = stop - start;
                                    let chunks_in_width = full_width.div_euclid(chunk_size as usize);

                                    stop = chunk_size as usize * chunks_in_width;
                                    start = stop - chunk_size as usize;

                                    if start > read_info.read.len() {
                                        start = read_info.read.len() - 1000;
                                    }

                                }

                                // Check start is not past end
                                if start > read_info.read.len() {
                                    continue
                                }

                                // Only send back one chunks worth of data -
                                // don't overslice the read by going off the end
                                let stop = min(stop, read_info.read.len());
                                read_info.time_accessed = now_time;
                                read_info.prev_chunk_start = stop;
                                let read_chunk = read_info.read[start..stop].to_vec();

                                // Chunk is too short
                                if read_chunk.len() < 300 {
                                    continue
                                }

                                container.push(
                                    (read_info.channel, ReadData{
                                        id: read_info.read_id.clone(),
                                        number: read_info.read_number.clone(),
                                        start_sample: 0,
                                        chunk_start_sample: 0,
                                        chunk_length:  read_chunk.len() as u64,
                                        chunk_classifications: vec![83],
                                        raw_data: convert_to_u8(read_chunk),
                                        median_before: 225.0,
                                        median: 110.0,
                                    })
                                );

                            }
                        }
                        // Drop the channel read vector to free the lock on it
                        mem::drop(read_data_vec);
                    }
                    // Reset channel so we don't over total number of channels whilst spinning for data
                    let mut channel_data = HashMap::with_capacity(24);

                    for chunk in container.chunks(24) {
                        for (channel,read_data) in chunk {
                            channel_data.insert(channel.clone() as u32, read_data.clone());
                        }
                        tx_get_live_reads_response.send(GetLiveReadsResponse{
                            samples_since_start: 0,
                            seconds_since_start: 0.0,
                            channels: channel_data.clone(),
                            action_responses: vec![]
                        }).await.unwrap_or_else(|_| {
                            panic!(
                                "Failed to send read chunks - has the adaptive samplign client disconnected?"
                            )
                        });
                        channel_data.clear();
                    }
                    container.clear();

                    // ES: curiously we observed an `unblock-all` increase in latency 
                    // when compared to playback runs in MinKNOW. It is around 180 bp
                    // for R9.4.1 (460 bps, 4Khz) so maybe this sleep contributes when 
                    // the break chunk parameter is set at 400 ms? I think this is it...

                    // I assume this delay is not the case when MinKNOW samples from 
                    // real ADC streams where the data does not have to be generated.

                    // I think this sleep call is conceptually wrong:

                    // MinKNOW (break_reads_after_seconds) partitions the raw data streams
                    // (hyperstreams in the remote call documentation) but since we have generated
                    // these chunks here already (equivalent to sampling a continous chunk from 
                    // a raw data stream) this sleep adds an extra duration to the read we observe
                    // when we compare to playback runs in MinKNOW. Test with Readfish.

                    // I think it is ok to comment out - the mean read lengths produced without
                    // this sleep are as expected, but might have affected delay in the unblock
                    // request?

                    // thread::sleep(Duration::from_millis(break_chunk_ms));
                }

            });
            while let Some(message) = rx_get_live_reads_response.recv().await {
                yield message
            }

        };
        Ok(Response::new(Box::pin(output) as Self::get_live_readsStream))
    }

    async fn get_data_types(
        &self,
        _request: Request<GetDataTypesRequest>,
    ) -> Result<Response<GetDataTypesResponse>, Status> {
        Ok(Response::new(GetDataTypesResponse {
            uncalibrated_signal: Some(DataType {
                r#type: 0,
                big_endian: false,
                size: 2,
            }),
            calibrated_signal: Some(DataType {
                r#type: 0,
                big_endian: false,
                size: 2,
            }),
            bias_voltages: Some(DataType {
                r#type: 0,
                big_endian: false,
                size: 2,
            }),
        }))
    }
}
