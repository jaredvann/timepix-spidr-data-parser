/*
 * ARIADNE Experiment, Department of Physics, University of Liverpool
 *
 * -----------------------------
 * Timepix Hot Pixel Search Tool
 * -----------------------------
 *
 * timepix-spidr-data-parser/src/heatmap_generator.rs
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
use clap;
use colored::Colorize;
use glob::glob;
use separator::Separatable as _;

use timepix_spidr_data_parser::*;

fn main() -> io::Result<()> {
    println!(
        "-----------------------------\n{}\n-----------------------------",
        "Timepix Hot Pixel Search Tool".bold()
    );

    //
    // Generate command line option parser
    //
    let matches = clap::App::new("")
        // General options
        .arg(clap::Arg::with_name("INPUT").help("Sets the input file pattern").required(true).index(1))
        .get_matches();

    //
    // Read command line options
    //
    let input_glob_str = matches.value_of("INPUT").unwrap();

    //
    // Parse input file list
    //
    let input_files: Vec<_> = glob(input_glob_str)
        .expect("Failed to read input file glob pattern")
        .filter_map(|x| x.ok()) // Check glob worked
        .filter(|x| x.is_file()) // Check is file
        .filter(|x| x.extension().is_some()) // Check has file extension
        .filter(|x| x.extension().unwrap() == "dat") // Check extension is .dat
        .collect();

    if input_files.is_empty() {
        println!("No input files matched!");
        return Ok(());
    }

    println!("Matched {} input files", input_files.len());

    let mut buffer: [u8; BUFFER_SIZE * 8] = [0; BUFFER_SIZE * 8];

    let mut packets_parsed: u64 = 0;

    let mut pixel_grid: [(u16, u16, u64); 256*256] = [(0,0,0); 256*256];

    for y in 0..256 {
        for x in 0..256 {
            pixel_grid[y*256+x].0 = x as u16;
            pixel_grid[y*256+x].1 = y as u16;
        }
    }

    for (i, data_file) in input_files.iter().enumerate() {
        println!("Reading data file {}/{}", i + 1, input_files.len());

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
                        "Data file {}/{}; Processed {} packets",
                        i + 1,
                        input_files.len(),
                        packets_parsed.separated_string()
                    );
                }

                let header = ((data & 0xF000_0000_0000_0000) >> 60) & 0xF;

                if header == 0xA || header == 0xB {
                    // Calculate col and row
                    let dcol = (data & 0x0FE0_0000_0000_0000) >> 52; //(16+28+9-1)
                    let spix = (data & 0x001F_8000_0000_0000) >> 45; //(16+28+3-2)
                    let pix = (data & 0x0000_7000_0000_0000) >> 44; //(16+28)
                    let col = (dcol + pix / 4) as usize;
                    let row = (spix + (pix & 0x3)) as usize;

                    pixel_grid[row*256 + col].2 += 1;
                }
            }
        }
    }

    pixel_grid.sort_by(|a, b| b.2.cmp(&a.2));

    let mut file = std::fs::File::create("hot_pixels.csv")?;

    writeln!(file, "pos,x,y,hits")?;

    for i in 0..10 {
        println!("{}: x: {}, y: {}, hits: {}", i+1, pixel_grid[i].0, pixel_grid[i].1, pixel_grid[i].2);
    }

    for i in 0..100 {
        writeln!(file, "{},{},{},{}", i+1, pixel_grid[i].0, pixel_grid[i].1, pixel_grid[i].2)?;
    }

    Ok(())
}
