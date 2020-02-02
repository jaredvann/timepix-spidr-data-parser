/*
 * ARIADNE Experiment, Department of Physics, University of Liverpool
 *
 * timepix-spidr-data-parser/src/io/read_raw_data.rs
 *
 * Authors: Jared Vann
 */
use std::cmp;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::mem;

use byteorder::{LittleEndian, ReadBytesExt};
use colored::Colorize;
use separator::Separatable as _;

use crate::{Hit, Trigger, BUFFER_SIZE};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadRawDataMode {
    HitsOnly,
    TriggersOnly,
    Both,
}

pub fn read_raw_data(
    data_files: &[std::path::PathBuf],
    mode: ReadRawDataMode,
    max_packets: Option<usize>,
    hot_pixels: &[(u16, u16)],
) -> io::Result<(usize, Vec<Hit>, Vec<Trigger>)> {
    let mut buffer: [u8; BUFFER_SIZE * 8] = [0; BUFFER_SIZE * 8];

    let mut prev_trigtime_coarse: u64 = 0;
    let mut trigtime_global_ext: u64 = 0;

    let mut long_time: u64 = 0;
    let mut longtime_lsb: u64 = 0;

    let mut hits = Vec::new();
    let mut triggers = Vec::new();

    let mut trigger_overflows = 0;

    let mut packets_parsed = 0;

    for (i, data_file) in data_files.iter().enumerate() {
        println!("Reading data file {}/{}", i + 1, data_files.len());

        let mut file = fs::File::open(&data_file)?;

        // First bytes in file are spidr ID and subsequent header size
        let _spidr_id = file.read_u32::<LittleEndian>()?;
        let spidr_header_size = file.read_u32::<LittleEndian>()?;
        let spidr_header_size = cmp::min(spidr_header_size, 66304);

        // Skip header
        file.seek(SeekFrom::Current(i64::from(spidr_header_size)))?;

        loop {
            let bytes_read = file.read(&mut buffer)?;

            // Check there are still some bytes to process
            if bytes_read == 0 {
                break;
            }

            // Check that number of bytes is exactly divisible by the size of a hit
            if bytes_read % 8 != 0 {
                panic!("Bytes read from {:#?} is not divisible by 8 (the size of a hit)", data_file);
            }

            let hit_data = unsafe { mem::transmute::<[u8; BUFFER_SIZE * 8], [u64; BUFFER_SIZE]>(buffer) };

            for data in hit_data.iter().take(bytes_read / 8) {
                packets_parsed += 1;

                if packets_parsed % 10_000_000 == 0 {
                    println!(
                        "Data file {}/{}; Processing packet {}",
                        i + 1,
                        data_files.len(),
                        packets_parsed.separated_string()
                    );
                }

                let header = ((data & 0xF000_0000_0000_0000) >> 60) & 0xF;

                if (header == 0xA || header == 0xB) && (mode != ReadRawDataMode::TriggersOnly) {
                    // Calculate col and row
                    let dcol = (data & 0x0FE0_0000_0000_0000) >> 52; //(16+28+9-1)
                    let spix = (data & 0x001F_8000_0000_0000) >> 45; //(16+28+3-2)
                    let pix = (data & 0x0000_7000_0000_0000) >> 44; //(16+28)
                    let col = (dcol + pix / 4) as u16;
                    let row = (spix + (pix & 0x3)) as u16;

                    // Calculate ToT
                    let tot = (((data & 0x3FF0_0000) >> 20) * 25) as u32;

                    // ToA and ToT measurement using 40 MHz counters
                    // ToA: 14 bits (resolution 25 ns, range 409 µs)
                    // ToT: 10 bits (resolution 25 ns, range 25 µs)
                    // Fast ToA measurement (640 MHz counter)
                    // 4 bits (resolution 1.56 ns, range 25 ns)
                    // common stop TDC -> subtract fast TDC value from time measurement

                    // Extract timing information
                    let spidr_time = data & 0xFFFF;
                    let temp_toa = (data & 0xFFF_C000_0000) >> 30;
                    let temp_toa_fast = (data & 0xF_0000) >> 16;
                    let temp_toa_coarse = (spidr_time << 14) | temp_toa;

                    // Calculate the global time
                    let pixel_bits = ((temp_toa_coarse >> 28) & 0x3) as i32; // units 25 ns
                    let long_time_bits = ((long_time >> 28) & 0x3) as i32; // units 25 ns;
                    let diff = long_time_bits - pixel_bits;

                    let global_time = match diff {
                        1 | -3 => ((long_time - 0x1000_0000) & 0xFFFF_C000_0000) | (temp_toa_coarse & 0x3FFF_FFFF),
                        3 | -1 => ((long_time + 0x1000_0000) & 0xFFFF_C000_0000) | (temp_toa_coarse & 0x3FFF_FFFF),
                        _ => (long_time & 0xFFFF_C000_0000) | (temp_toa_coarse & 0x3FFF_FFFF),
                    };

                    if (global_time >> 28) & 0x3 != pixel_bits as u64 {
                        println!("{}", "WARNING: checking bits should match!".yellow());
                    }

                    // Subtract fast toa (ftoa count until the first clock edge, so less counts means later arrival of the hit)
                    let mut toa = (global_time << 12) - (temp_toa_fast << 8);
                    // let mut toa = global_time - (temp_toa_fast << 4);

                    // Now correct for the column to column phase shift (todo: check header for number of clock phases)
                    toa += ((u64::from(col) / 2) % 16) << 8;
                    if ((col / 2) % 16) == 0 {
                        toa += 16 << 8
                    };

                    toa >>= 12;

                    // Finally convert to ns
                    toa *= 25;

                    if hot_pixels.iter().find(|(hcol, hrow)| *hcol == col && *hrow == row).is_none() {
                        hits.push(Hit { col, row, toa, tot });
                    }
                } else if header == 0x4 || header == 0x6 {
                    let subheader = (data & 0x0F00_0000_0000_0000) >> 56;

                    // Finding subheader type (F for trigger or 4,5 for time)
                    if subheader == 0xF && mode != ReadRawDataMode::HitsOnly
                    // Trigger information
                    {
                        let raw_count = ((data & 0x00FF_F000_0000_0000) >> 44) as u32;
                        let trigtime_coarse = (data & 0x0000_0FFF_FFFF_F000) >> 12;
                        let trigtime_fine = (data >> 5) & 0xF as u64; // phases of 320 MHz clock in bits 5 to 8
                        let trigtime_fine = ((trigtime_fine - 1 as u64) << 9) / 12 as u64;
                        let trigtime_fine = (data & 0x0000_0000_0000_0E00) | (trigtime_fine & 0x0000_0000_0000_01FF as u64);

                        // Check if the first trigger number is 1
                        if triggers.is_empty() && raw_count != 1 {
                            println!("{}", format!("WARNING: First trigger number in file is not 1! ({})", raw_count).yellow());
                        }

                        if trigtime_coarse < prev_trigtime_coarse
                        // 32 time counter wrapped
                        {
                            if trigtime_coarse < (prev_trigtime_coarse - 1000) {
                                trigtime_global_ext += 0x1_0000_0000;
                                println!("{}", "WARNING: Coarse trigger time counter wrapped!".yellow());
                            } else {
                                println!("{}", "WARNING: Small backward time jump in trigger packet!".yellow());
                            }
                        }

                        // let time = ((trigtime_global_ext + trigtime_coarse) << 12) | trigtime_fine; // save in ns
                        let time = (trigtime_global_ext + trigtime_coarse) | trigtime_fine; // save in ns
                        let time = time * 25 as u64;

                        prev_trigtime_coarse = trigtime_coarse;

                        let event = raw_count + 4096 * trigger_overflows;

                        if raw_count == 4095 {
                            trigger_overflows += 1;
                        }

                        triggers.push(Trigger { event, time });
                    } else if subheader == 0x4 {
                        // 32 lsb of timestamp
                        longtime_lsb = (data & 0x0000_FFFF_FFFF_0000) >> 16;
                    } else if subheader == 0x5 {
                        // 32 msb of timestamp
                        let longtime_msb = (data & 0x0000_0000_FFFF_0000) << 16;
                        let tmplongtime = longtime_msb | longtime_lsb;

                        // Now check for large forward jumps in time;
                        // 0x10000000 corresponds to about 6 seconds
                        if tmplongtime > (long_time + 0x1000_0000) && long_time > 0 {
                            println!("{}", "WARNING: Large forward time jump!".yellow());
                            long_time = (longtime_msb - 0x1000_0000) | longtime_lsb;
                        } else {
                            long_time = tmplongtime;
                        }
                    }
                }

                if let Some(m) = max_packets {
                    if packets_parsed >= m {
                        println!(
                            "{}",
                            format!("Reached maximum requested number of packets ({})", packets_parsed.separated_string()).bold()
                        );
                        return Ok((packets_parsed, hits, triggers));
                    }
                }
            }
        }
    }

    Ok((packets_parsed, hits, triggers))
}
