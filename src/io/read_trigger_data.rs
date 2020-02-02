/*
 * ARIADNE Experiment, Department of Physics, University of Liverpool
 *
 * timepix-spidr-data-parser/src/io/read_trigger_data.rs
 *
 * Authors: Jared Vann
 */

use std::fs;
use std::io;
use std::path::PathBuf;

use csv;

use crate::Trigger;

pub fn read_trigger_data(data_file: &PathBuf) -> io::Result<Vec<Trigger>> {
    let file = fs::File::open(&data_file)?;
    let mut rdr = csv::Reader::from_reader(file);
    let mut triggers = Vec::new();

    for result in rdr.deserialize() {
        triggers.push(result?);
    }

    Ok(triggers)
}
