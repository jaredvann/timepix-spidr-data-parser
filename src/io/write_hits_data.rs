/*
 * ARIADNE Experiment, Department of Physics, University of Liverpool
 *
 * timepix-spidr-data-parser/src/io/write_hits_data.rs
 *
 * Authors: Jared Vann
 */

use std::fs::File;
use std::io;
use std::io::Write as _;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::Hit;

pub fn write_hits_to_file(file: &mut File, hits: &[Hit]) -> io::Result<()> {
    let mut buf = Vec::with_capacity(hits.len() * 16);

    for hit in hits {
        buf.write_u16::<LittleEndian>(hit.col)?;
        buf.write_u16::<LittleEndian>(hit.row)?;
        buf.write_u64::<LittleEndian>(hit.toa)?;
        buf.write_u32::<LittleEndian>(hit.tot)?;
    }

    file.write_all(&buf)?;

    Ok(())
}
