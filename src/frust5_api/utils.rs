use std::collections:: HashMap;

use hdf5;
use hdf5::types::{FixedAscii, VarLenAscii};

use crate::frust5_api::frust5::{ChannelInfo, RawAttrsOpts};

fn _set_string_attributes(
    group: hdf5::Group,
    data: &HashMap<&str, &str>,
) -> Result<(), hdf5::Error> {
    for (key, value) in data.into_iter() {
        let attr = VarLenAscii::from_ascii(value).unwrap();
        group
            .new_attr::<VarLenAscii>()
            .create(*key)?
            .write_scalar(&attr)?;
    }
    Ok(())
}

fn _set_float_attributes(group: hdf5::Group, data: ChannelInfo) -> Result<(), hdf5::Error> {
    for (key, value) in data {
        group
            .new_attr::<f64>()
            .create(key.as_str())?
            .write_scalar(&value)?;
    }
    Ok(())
}

pub fn add_tracking_info(
    group: hdf5::Group,
    data: &HashMap<&str, &str>,
) -> Result<(), hdf5::Error> {
    _set_string_attributes(group, data)?;
    Ok(())
}

pub fn add_context_tags(group: hdf5::Group, data: &HashMap<&str, &str>) -> Result<(), hdf5::Error> {
    _set_string_attributes(group, data)?;
    Ok(())
}

pub fn add_channel_info(group: hdf5::Group, data: ChannelInfo) -> Result<(), hdf5::Error> {
    let attr = VarLenAscii::from_ascii(&data.channel_number).unwrap();
    group
        .new_attr::<VarLenAscii>()
        .create("channel_number")?
        .write_scalar(&attr)?;
    _set_float_attributes(group, data)?;
    Ok(())
}

pub fn add_raw_data(
    group: hdf5::Group,
    data: Vec<i16>,
    attrs: &HashMap<&str, RawAttrsOpts>,
) -> Result<(), hdf5::Error> {
    // set attributes on the group
    for (key, value) in attrs.into_iter() {
        match value {
            // reference and not reference not important on match - just testing that out!
            &RawAttrsOpts::ReadId(read_id) => {
                let attr = FixedAscii::<37>::from_ascii(read_id).unwrap();
                group
                    .new_attr::<FixedAscii<37>>()
                    .create(*key)?
                    .write_scalar(&attr)?;
            }
            &RawAttrsOpts::Duration(duration) => {
                group
                    .new_attr::<u32>()
                    .create(*key)?
                    .write_scalar(&duration)?;
            }
            &RawAttrsOpts::EndReason(end_reason) => {
                group
                    .new_attr::<u8>()
                    .create(*key)?
                    .write_scalar(&end_reason)?;
            }
            &RawAttrsOpts::MedianBefore(median_before) => {
                group
                    .new_attr::<f64>()
                    .create(*key)?
                    .write_scalar(&median_before)?;
            }
            RawAttrsOpts::ReadNumber(read_number) => {
                group
                    .new_attr::<i32>()
                    .create(*key)?
                    .write_scalar(read_number)?;
            }
            RawAttrsOpts::StartMux(start_mux) => {
                group
                    .new_attr::<u8>()
                    .create(*key)?
                    .write_scalar(start_mux)?;
            }
            RawAttrsOpts::StartTime(start_time) => {
                group
                    .new_attr::<u64>()
                    .create(*key)?
                    .write_scalar(start_time)?;
            }
        }
    }
    #[cfg(feature = "blosc")]
    blosc_set_nthreads(2); // set number of blosc threads
    let builder = group.new_dataset_builder();
    builder
        .add_filter(32020, &[0, 2, 1, 1])
        .with_data(&data)
        .create("Signal")?;
    Ok(())
}