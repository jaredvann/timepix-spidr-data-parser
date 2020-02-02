/*
 * ARIADNE Experiment, Department of Physics, University of Liverpool
 *
 * timepix-spidr-data-parser/src/io/write_cluster_data.rs
 *
 * Authors: Jared Vann
 */

use std::fs::File;
use std::io;
use std::io::Write as _;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::Hit;

pub fn write_cluster_to_file(file: &mut File, cluster: &[Hit], toa_adjustment: i64) -> io::Result<()> {
    let mut buf = Vec::with_capacity((cluster.len() + 1) * 16);

    for hit in cluster {
        let toa = hit.toa as i64 + toa_adjustment;

        if toa < 0 {
            dbg!(hit.toa);
            dbg!(toa_adjustment);
            panic!("Attempting to write ToA with negative value");
        }

        buf.write_u16::<LittleEndian>(hit.col)?;
        buf.write_u16::<LittleEndian>(hit.row)?;
        buf.write_u64::<LittleEndian>(toa as u64)?;
        buf.write_u32::<LittleEndian>(hit.tot)?;
    }

    // Terminating zeroed hit
    buf.write_u128::<LittleEndian>(0_u128)?;

    file.write_all(&buf)?;

    Ok(())
}
