/*
 * ARIADNE Experiment, Department of Physics, University of Liverpool
 *
 * -----------------------
 * Timepix Raw Data Parser
 * -----------------------
 *
 * timepix-spidr-data-parser/src/bin/raw_data_parser.rs
 *
 * Authors: Jared Vann
 */

#[macro_use]
extern crate lazy_static;

use std::cmp;
use std::collections::VecDeque;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::mem;
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt};
use chrono::prelude::*;
use clap;
use colored::Colorize;
use glob::glob;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon;
use regex::Regex;
use separator::Separatable as _;

use timepix_spidr_data_parser::*;

const BATCH_SIZE: usize = 1_000_000;
const SKIM_OFF: usize = 800_000;

#[derive(Clone, Debug)]
struct FileInfo {
    run_name: Option<String>,
    device: String,
    start_time: DateTime<Utc>, // TODO: check is UTC
    file_in_run: u32,
    path: PathBuf,
}

lazy_static! {
    static ref RE: Regex = Regex::new(r"(\w*?)_*(\w\d{4}_\w\d{2})-(\d{2})(\d{2})(\d{2})-(\d{2})(\d{2})(\d{2})-(\d*).dat").unwrap();
}

fn main() -> io::Result<()> {
    println!(
        "\n-----------------------\n{}\n-----------------------\n",
        "Timepix Raw Data Parser".bold()
    );

    //
    // Generate command line option parser
    //
    let matches = clap::App::new("")
        // General options
        .arg(clap::Arg::with_name("INPUT").help("Sets the input file pattern").required(true).index(1))
        .arg(
            clap::Arg::with_name("OUTPUT")
                .help("Sets the output directory to use")
                .required(true)
                .index(2),
        )
        .arg(clap::Arg::with_name("disable-mt").help("Disables multi-threading").long("disable-mt"))
        .arg(
            clap::Arg::with_name("dry-run")
                .help("Shows output of what runs will be processed without changing any data")
                .long("dry-run"),
        )
        .arg(clap::Arg::with_name("overwrite").help("Overwrites any previous data").long("overwrite"))
        .get_matches();

    //
    // Read command line options
    //
    let input_glob_str = matches.value_of("INPUT").unwrap();
    let output_dir_str = matches.value_of("OUTPUT").unwrap();

    let output_dir = Path::new(output_dir_str);

    let dry_run = matches.is_present("dry-run");
    let disable_mt = matches.is_present("disable-mt");

    if !dry_run && matches.is_present("overwrite") {
        println!("Removing existing contents of output directory");
        fs::remove_dir_all(output_dir).unwrap();
    }

    if !dry_run && !output_dir.exists() {
        if let Err(err) = fs::create_dir_all(output_dir) {
            println!("{}", format!("Could not create output directory: '{}'!", err).red());
            return Ok(());
        }
    }

    let file_infos: Vec<_> = glob(input_glob_str)
        .expect("Failed to read input file glob pattern")
        .filter_map(|x| x.ok()) // Check glob worked
        .filter(|x| x.is_file()) // Check is file
        .filter(|x| x.extension().is_some()) // Check has file extension
        .filter(|x| x.extension().unwrap() == "dat") // Check extension is .dat
        .filter_map(parse_file_name)
        .collect();

    if file_infos.is_empty() {
        println!("No input files matched!");
        return Ok(());
    }

    println!("Matched {} input files", file_infos.len());

    let mut grouped_file_infos: Vec<(Vec<&FileInfo>, PathBuf)> = Vec::new();

    let mut i = 0;
    while i < file_infos.len() {
        let info = &file_infos[i];

        if info.file_in_run != 1 {
            println!("{}", format!("Unexpected first file in run: '{}'", info.path.display()).yellow());
            i += 1;
            continue;
        }

        let mut run_file_infos = vec![info];

        let datetime_str = file_infos[i].start_time.format("%Y-%m-%d_%H-%M-%S");

        if i < file_infos.len() - 1 {
            for j in 2.. {
                if i == file_infos.len() - 1 {
                    break;
                }

                let next_info = &file_infos[i + 1];

                if next_info.file_in_run == j && next_info.run_name == info.run_name {
                    run_file_infos.push(next_info);
                    i += 1;
                } else {
                    break;
                }
            }
        }

        let run_output_dir = output_dir.join(match &info.run_name {
            Some(run_name) => format!("{}_{}", datetime_str, run_name),
            None => format!("{}", datetime_str),
        });

        if !run_output_dir.exists() {
            grouped_file_infos.push((run_file_infos, run_output_dir));
        }

        i += 1;
    }

    let n_runs = grouped_file_infos.len();

    println!("Grouped files into {} runs", n_runs);

    if dry_run {
        for (run_file_infos, run_output_dir) in grouped_file_infos {
            println!("\nOutput dir: {}\nFiles:", run_output_dir.to_str().unwrap());

            for file_info in run_file_infos {
                println!("  - {}", file_info.path.to_str().unwrap());
            }
        }
    } else {
        let sty = ProgressStyle::default_bar()
            .template(PROGRESS_BAR_TEMPLATE)
            .progress_chars(PROGRESS_BAR_CHARS);

        let disable_mt = disable_mt || grouped_file_infos.len() == 1;

        if disable_mt {
            for (run_file_infos, run_output_dir) in grouped_file_infos {
                let n_bytes: u64 = run_file_infos.iter().map(|x| x.path.metadata().unwrap().len()).sum();
                let n_packets = (n_bytes - 66304 * run_file_infos.len() as u64) / 8;

                let progress_bar = ProgressBar::new(n_packets);
                progress_bar.set_style(sty.clone());

                process_run(run_file_infos, run_output_dir, progress_bar).unwrap();
            }
        } else {
            let multi_progress = MultiProgress::new();

            rayon::scope(|s| {
                for (run_file_infos, run_output_dir) in grouped_file_infos {
                    let n_bytes: u64 = run_file_infos.iter().map(|x| x.path.metadata().unwrap().len()).sum();
                    let n_packets = (n_bytes - 66304 * run_file_infos.len() as u64) / 8;

                    let progress_bar = multi_progress.add(ProgressBar::new(n_packets));
                    progress_bar.set_style(sty.clone());

                    s.spawn(move |_| process_run(run_file_infos, run_output_dir, progress_bar).unwrap());
                }

                multi_progress.join().unwrap();
            });
        }
    }

    Ok(())
}

fn parse_file_name(path: PathBuf) -> Option<FileInfo> {
    let path = path.to_owned();

    let path_str = match path.to_str() {
        Some(s) => s,
        None => return None,
    };

    let caps = match RE.captures(path_str) {
        Some(caps) => caps,
        None => return None,
    };

    let run_name = match caps.get(1).unwrap().as_str() {
        "" => None,
        x => Some(x.to_string()),
    };
    let device = caps.get(2).unwrap().as_str().to_string();
    let year = 2000 + caps.get(3).unwrap().as_str().parse::<i32>().unwrap();
    let month = caps.get(4).unwrap().as_str().parse::<u32>().unwrap();
    let day = caps.get(5).unwrap().as_str().parse::<u32>().unwrap();
    let hour = caps.get(6).unwrap().as_str().parse::<u32>().unwrap();
    let minute = caps.get(7).unwrap().as_str().parse::<u32>().unwrap();
    let second = caps.get(8).unwrap().as_str().parse::<u32>().unwrap();
    let file_in_run = caps.get(9).unwrap().as_str().parse::<u32>().unwrap();

    let start_time = Utc.ymd(year, month, day).and_hms(hour, minute, second);

    Some(FileInfo {
        run_name,
        device,
        start_time,
        file_in_run,
        path,
    })
}

fn vecdeque_insertion_sort<T: Ord>(list: &mut VecDeque<T>) {
    for i in 1..list.len() {
        for j in (1..=i).rev() {
            if list[j - 1] <= list[j] {
                break;
            }
            list.swap(j - 1, j);
        }
    }
}

fn process_run(file_infos: Vec<&FileInfo>, run_output_dir: PathBuf, progress_bar: ProgressBar) -> io::Result<()> {
    let run_name = file_infos[0].path.file_stem().unwrap().to_str().unwrap().split("W00").nth(0).unwrap();

    progress_bar.set_message(&format!("| 0 Hits Parsed | 0 Triggers Parsed | 0 Hot Pixels Removed | {}", run_name));

    let data_files: Vec<_> = file_infos.iter().map(|x| x.path.to_owned()).collect();

    let mut hit_conveyor: VecDeque<Hit> = VecDeque::with_capacity(BATCH_SIZE);
    let mut triggers = Vec::new();

    let mut buffer: [u8; BUFFER_SIZE * 8] = [0; BUFFER_SIZE * 8];

    let mut prev_trigtime_coarse: u64 = 0;
    let mut trigtime_global_ext: u64 = 0;

    let mut long_time: u64 = 0;
    let mut longtime_lsb: u64 = 0;

    let mut hits_parsed: usize = 0;
    let mut packets_parsed: usize = 0;
    let mut triggers_parsed: usize = 0;
    let mut hot_pixels_removed: usize = 0;

    fs::create_dir(&run_output_dir)?;
    let mut output_file = fs::File::create(run_output_dir.join("hits.bin"))?;

    for data_file in data_files {
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

            let packet_data = unsafe { mem::transmute::<[u8; BUFFER_SIZE * 8], [u64; BUFFER_SIZE]>(buffer) };
            let packet_data: Vec<u64> = packet_data.to_owned().to_vec();

            for packet in packet_data.iter().take(bytes_read / 8) {
                packets_parsed += 1;

                let header = ((packet & 0xF000_0000_0000_0000) >> 60) & 0xF;

                if header == 0xA || header == 0xB {
                    hits_parsed += 1;

                    // Calculate col and row
                    let dcol = (packet & 0x0FE0_0000_0000_0000) >> 52; //(16+28+9-1)
                    let spix = (packet & 0x001F_8000_0000_0000) >> 45; //(16+28+3-2)
                    let pix = (packet & 0x0000_7000_0000_0000) >> 44; //(16+28)
                    let col = (dcol + pix / 4) as u16;
                    let row = (spix + (pix & 0x3)) as u16;

                    if HOT_PIXELS.iter().any(|(hcol, hrow)| *hcol == col && *hrow == row) {
                        hot_pixels_removed += 1;
                        continue;
                    }

                    let data = (packet & 0x0000_0FFF_FFFF_0000) >> 16;

                    // Calculate ToT
                    let tot = ((data & 0x0000_3FF0) >> 4) as u32 * 25;

                    // ToA and ToT measurement using 40 MHz counters
                    // ToA: 14 bits (resolution 25 ns, range 409 µs)
                    // ToT: 10 bits (resolution 25 ns, range 25 µs)
                    // Fast ToA measurement (640 MHz counter)
                    // 4 bits (resolution 1.56 ns, range 25 ns)
                    // common stop TDC -> subtract fast TDC value from time measurement

                    // Extract timing information
                    let spidr_time = packet & 0x0000_0000_0000_FFFF;
                    let temp_toa = (data & 0x0FFF_C000) >> 14;
                    let temp_toa_fast = data & 0xF;
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

                    // if (global_time >> 28) & 0x3 != pixel_bits as u64 {
                    //     warn!("{} Checking bits should match!", output_prefix);
                    // }

                    // Subtract fast toa (ftoa count until the first clock edge, so less counts means later arrival of the hit)
                    let mut toa = (global_time << 4) - temp_toa_fast;

                    // Now correct for the column to column phase shift (todo: check header for number of clock phases)
                    toa += (u64::from(col) / 2) % 16;
                    if (col / 2) % 16 == 0 {
                        toa += 16;
                    }

                    hit_conveyor.push_back(Hit { col, row, toa, tot });

                    if hit_conveyor.len() >= BATCH_SIZE {
                        vecdeque_insertion_sort(&mut hit_conveyor);

                        let temp = hit_conveyor.split_off(SKIM_OFF);

                        write_hits_to_file(&mut output_file, &hit_conveyor.as_slices().0)?;

                        progress_bar.set_position(packets_parsed as u64);
                        progress_bar.set_message(&format!(
                            "| {} Hits Parsed | {} Triggers Parsed | {} Hot Pixels Removed | {}",
                            hits_parsed.separated_string(),
                            triggers_parsed.separated_string(),
                            hot_pixels_removed.separated_string(),
                            run_name
                        ));

                        hit_conveyor = temp;
                        hit_conveyor.reserve(BATCH_SIZE);
                    }
                } else if header == 0x4 || header == 0x6 {
                    let subheader = (packet & 0x0F00_0000_0000_0000) >> 56;

                    // Finding subheader type (F for trigger or 4,5 for time)
                    if subheader == 0xF
                    // Trigger information
                    {
                        triggers_parsed += 1;

                        let trigtime_coarse = (packet & 0x0000_0FFF_FFFF_F000) >> 12;
                        let trigtime_fine = (packet >> 5) & 0xF as u64; // phases of 320 MHz clock in bits 5 to 8
                        let trigtime_fine = ((trigtime_fine - 1 as u64) << 9) / 12 as u64;
                        let trigtime_fine = (packet & 0x0000_0000_0000_0E00) | (trigtime_fine & 0x0000_0000_0000_01FF as u64);

                        if trigtime_coarse < prev_trigtime_coarse
                        // 32 time counter wrapped
                        {
                            if trigtime_coarse < (prev_trigtime_coarse - 1000) {
                                trigtime_global_ext += 0x1_0000_0000;
                            // warn!("{} Coarse trigger time counter wrapped!", output_prefix);
                            } else {
                                // warn!("{} Small backward time jump in trigger packet!", output_prefix);
                            }
                        }

                        let time = (trigtime_global_ext + trigtime_coarse) | trigtime_fine;
                        let time = time * 25 as u64;

                        prev_trigtime_coarse = trigtime_coarse;

                        triggers.push(Trigger {
                            event: triggers_parsed as u32,
                            time,
                        });

                        progress_bar.set_message(&format!(
                            "| {} Hits Parsed | {} Triggers Parsed | {} Hot Pixels Removed | {}",
                            hits_parsed.separated_string(),
                            triggers_parsed.separated_string(),
                            hot_pixels_removed.separated_string(),
                            run_name
                        ));
                    } else if subheader == 0x4 {
                        // 32 lsb of timestamp
                        longtime_lsb = (packet & 0x0000_FFFF_FFFF_0000) >> 16;
                    } else if subheader == 0x5 {
                        // 32 msb of timestamp
                        let longtime_msb = (packet & 0x0000_0000_FFFF_0000) << 16;
                        let tmplongtime = longtime_msb | longtime_lsb;

                        // Now check for large forward jumps in time;
                        // 0x10000000 corresponds to about 6 seconds
                        if tmplongtime > (long_time + 0x1000_0000) && long_time > 0 {
                            long_time = (longtime_msb - 0x1000_0000) | longtime_lsb;
                        // warn!("{} Large forward time jump!", output_prefix);
                        } else {
                            long_time = tmplongtime;
                        }
                    }
                }
            }
        }
    }

    // Sort and save remaining hits
    vecdeque_insertion_sort(&mut hit_conveyor);
    write_hits_to_file(&mut output_file, &hit_conveyor.as_slices().0)?;

    if !triggers.is_empty() {
        triggers.sort();

        let mut file = fs::File::create(run_output_dir.join("triggers.csv"))?;

        write_triggers_to_csv(&mut file, &triggers)?;
    }

    progress_bar.finish_with_message(&format!(
        "| Done | {} Hits Parsed | {} Triggers Parsed | {}",
        hits_parsed.separated_string(),
        triggers_parsed.separated_string(),
        run_name
    ));

    Ok(())
}
