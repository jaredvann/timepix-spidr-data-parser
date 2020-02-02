/*
 * ARIADNE Experiment, Department of Physics, University of Liverpool
 *
 * -------------------------
 * Timepix Heatmap Generator
 * -------------------------
 *
 * timepix-spidr-data-parser/src/heatmap_generator.rs
 *
 * Authors: Jared Vann
 */

use std::fs;
use std::io;
use std::io::Write;
use std::path::Path;

use clap;
use colored::Colorize;
use glob::glob;
use itertools::Itertools;
use separator::Separatable as _;

use timepix_spidr_data_parser::*;

fn main() -> io::Result<()> {
    println!(
        "-------------------------\n{}\n-------------------------",
        "Timepix Heatmap Generator".bold()
    );

    //
    // Generate command line option parser
    //
    let matches = clap::App::new("")
        // General options
        .arg(clap::Arg::with_name("INPUT").help("Sets the input file pattern").required(true).index(1))
        .arg(clap::Arg::with_name("OUTPUT").help("Sets the output file to use").required(true).index(2))
        .arg(
            clap::Arg::with_name("sum-hits")
                .help("Sum the hits per pixel")
                .long("sum-hits")
                .conflicts_with("sum-tot"),
        )
        .arg(
            clap::Arg::with_name("sum-tot")
                .help("Sum the ToT per pixel")
                .long("sum-tot")
                .conflicts_with("sum-hits"),
        )
        .arg(
            clap::Arg::with_name("packets")
                .help("Number of packets to process (default is all)")
                .short("n")
                .long("packets")
                .takes_value(true),
        )
        .get_matches();

    //
    // Read command line options
    //
    let input_glob_str = matches.value_of("INPUT").unwrap();
    let output_file_str = matches.value_of("OUTPUT").unwrap();

    if !matches.is_present("sum-tot") && !matches.is_present("sum-hits") {
        println!("{}", "One usage mode must be selected: either '--sum-tot' or '--sum-hits'".red());
        return Ok(());
    }

    let max_packets = matches.value_of("packets").and_then(parse_human_readable_number::<usize>);
    // .and_then(|x| usize::try_from(x).ok());

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

    //
    // Check output file does not already exist
    //
    let output_file_path = Path::new(output_file_str);

    if output_file_path.exists() {
        println!("{}", format!("Given output file '{}' already exists!", output_file_str).red());
        return Ok(());
    }

    let (packets_parsed, hits, _) = read_raw_data(&input_files, ReadRawDataMode::HitsOnly, max_packets, &HOT_PIXELS)?;

    println!("Parsed {} packets", packets_parsed.separated_string());
    println!("Loaded {} hits", hits.len().separated_string());

    let mut heatmap: [u64; 256 * 256] = [0; 256 * 256];

    if matches.is_present("sum-tot") {
        for hit in hits.iter() {
            heatmap[hit.row as usize * 256 + hit.col as usize] += u64::from(hit.tot);
        }
    } else {
        for hit in hits.iter() {
            heatmap[hit.row as usize * 256 + hit.col as usize] += 1;
        }
    }

    let mut output_file = fs::File::create(output_file_path)?;

    for row in 0..256 {
        let row_string = heatmap[row * 256..(row + 1) * 256].iter().map(|x| x.to_string()).join(",");
        writeln!(&mut output_file, "{}", row_string)?;
    }

    println!(
        "{}",
        format!(
            "\nSummed {} hits and output to {}\n",
            hits.len().separated_string(),
            output_file_path.to_str().unwrap()
        )
        .bold()
    );

    Ok(())
}
