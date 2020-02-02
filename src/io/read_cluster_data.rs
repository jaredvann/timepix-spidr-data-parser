/*
 * ARIADNE Experiment, Department of Physics, University of Liverpool
 *
 * timepix-spidr-data-parser/src/io/read_cluster_data.rs
 *
 * Authors: Jared Vann
 */

use std::fs;
use std::io;
use std::io::prelude::*;
use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};

use crate::{Hit, BUFFER_SIZE};

pub fn read_cluster_data(data_file: &str) -> io::Result<Vec<Vec<Hit>>> {
    let mut file = fs::File::open(&data_file)?;

    let mut clusters = Vec::new();
    let mut current_cluster = Vec::new();

    let mut buf: [u8; BUFFER_SIZE * 16] = [0; BUFFER_SIZE * 16];

    loop {
        let bytes_read = file.read(&mut buf)?;

        if bytes_read == 0 {
            break;
        }

        // Check that number of bytes is exactly divisible by the size of a hit
        assert!(bytes_read % 16 == 0);

        let mut cur = Cursor::new(&buf[..]);

        for _ in 0..(bytes_read / 16) {
            let col = cur.read_u16::<LittleEndian>()?;
            let row = cur.read_u16::<LittleEndian>()?;
            let toa = cur.read_u64::<LittleEndian>()?;
            let tot = cur.read_u32::<LittleEndian>()?;

            if col == 0 && row == 0 && toa == 0 && tot == 0 {
                clusters.push(current_cluster);
                current_cluster = Vec::new();
            } else {
                current_cluster.push(Hit { col, row, toa, tot });
            }
        }
    }

    Ok(clusters)
}

pub struct ReadClusterIterator {
    file: fs::File,
    buf: [u8; BUFFER_SIZE * 16],
    bytes_read_from_buf: usize,
    bytes_left_to_read_from_buf: usize,
}

impl ReadClusterIterator {
    pub fn new(data_file: &str) -> ReadClusterIterator {
        ReadClusterIterator {
            file: fs::File::open(&data_file).unwrap(),
            buf: [0; BUFFER_SIZE * 16],
            bytes_read_from_buf: 0,
            bytes_left_to_read_from_buf: 0,
        }
    }
}

impl Iterator for ReadClusterIterator {
    type Item = Vec<Hit>;

    fn next(&mut self) -> Option<Vec<Hit>> {
        let mut current_cluster = Vec::new();

        loop {
            if self.bytes_left_to_read_from_buf == 0 {
                let bytes_read_into_buf = self.file.read(&mut self.buf).unwrap();

                if bytes_read_into_buf == 0 {
                    break;
                }

                // Check that number of bytes is exactly divisible by the size of a hit
                assert!(bytes_read_into_buf % 16 == 0);

                self.bytes_left_to_read_from_buf = bytes_read_into_buf;
                self.bytes_read_from_buf = 0;
            }

            let mut cur = Cursor::new(&self.buf[..]);
            cur.set_position(self.bytes_read_from_buf as u64);

            for _ in 0..(self.bytes_left_to_read_from_buf / 16) {
                let col = cur.read_u16::<LittleEndian>().unwrap();
                let row = cur.read_u16::<LittleEndian>().unwrap();
                let toa = cur.read_u64::<LittleEndian>().unwrap();
                let tot = cur.read_u32::<LittleEndian>().unwrap();

                self.bytes_left_to_read_from_buf -= 16;
                self.bytes_read_from_buf += 16;

                if col == 0 && row == 0 && toa == 0 && tot == 0 {
                    return Some(current_cluster);
                } else {
                    current_cluster.push(Hit { col, row, toa, tot });
                }
            }
        }

        None
    }
}
