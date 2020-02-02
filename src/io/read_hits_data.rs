/*
 * ARIADNE Experiment, Department of Physics, University of Liverpool
 *
 * timepix-spidr-data-parser/src/io/read_hits_data.rs
 *
 * Authors: Jared Vann
 */

use std::fs;
use std::io;
use std::io::prelude::*;
use std::io::Cursor;
use std::path::PathBuf;

use byteorder::{LittleEndian, ReadBytesExt};
use separator::Separatable as _;

use crate::{Hit, BUFFER_SIZE};

pub fn read_hits_data(data_file: &PathBuf, max_hits: Option<usize>) -> io::Result<Vec<Hit>> {
    let mut file = fs::File::open(&data_file)?;
    let mut hits = Vec::new();
    let mut buf: [u8; BUFFER_SIZE * 16] = [0; BUFFER_SIZE * 16];

    loop {
        let bytes_read = file.read(&mut buf)?;

        if bytes_read == 0 {
            break;
        }

        if hits.len() % 1000 == 0 {
            print!("\rLoaded {} hits", hits.len().separated_string());
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
                panic!("Unexpected null hit")
            } else {
                hits.push(Hit { col, row, toa, tot });

                if let Some(max) = max_hits {
                    if hits.len() == max {
                        print!("\rLoaded {} hits", hits.len().separated_string());
                        return Ok(hits);
                    }
                }
            }
        }
    }

    print!("\rLoaded {} hits", hits.len().separated_string());

    Ok(hits)
}

pub struct ReadHitsIterator {
    file: fs::File,
    buf: [u8; BUFFER_SIZE * 16],
    bytes_read_from_buf: usize,
    bytes_left_to_read_from_buf: usize,
}

impl ReadHitsIterator {
    pub fn new(data_file: &PathBuf) -> ReadHitsIterator {
        ReadHitsIterator {
            file: fs::File::open(&data_file).unwrap(),
            buf: [0; BUFFER_SIZE * 16],
            bytes_read_from_buf: 0,
            bytes_left_to_read_from_buf: 0,
        }
    }
}

impl Iterator for ReadHitsIterator {
    type Item = Hit;

    fn next(&mut self) -> Option<Hit> {
        if self.bytes_left_to_read_from_buf == 0 {
            let bytes_read_into_buf = self.file.read(&mut self.buf).unwrap();

            if bytes_read_into_buf == 0 {
                return None;
            }

            // Check that number of bytes is exactly divisible by the size of a hit
            assert!(bytes_read_into_buf % 16 == 0);

            self.bytes_left_to_read_from_buf = bytes_read_into_buf;
            self.bytes_read_from_buf = 0;
        }

        let mut cur = Cursor::new(&self.buf[..]);
        cur.set_position(self.bytes_read_from_buf as u64);

        let col = cur.read_u16::<LittleEndian>().unwrap();
        let row = cur.read_u16::<LittleEndian>().unwrap();
        let toa = cur.read_u64::<LittleEndian>().unwrap();
        let tot = cur.read_u32::<LittleEndian>().unwrap();

        self.bytes_left_to_read_from_buf -= 16;
        self.bytes_read_from_buf += 16;

        Some(Hit { col, row, toa, tot })
    }
}
