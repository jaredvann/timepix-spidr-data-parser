/*
 * ARIADNE Experiment, Department of Physics, University of Liverpool
 *
 * timepix-spidr-data-parser/src/io/write_trigger_data.rs
 *
 * Authors: Jared Vann
 */

use std::fs::File;
use std::io;

use csv;

use crate::Trigger;

pub fn write_triggers_to_csv(file: &mut File, triggers: &[Trigger]) -> io::Result<()> {
    let mut csv_writer = csv::Writer::from_writer(file);

    for trigger in triggers {
        csv_writer.serialize(trigger)?;
    }

    csv_writer.flush()?;

    Ok(())
}
