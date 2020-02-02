/*
 * ARIADNE Experiment, Department of Physics, University of Liverpool
 *
 * -------------------------------
 * Timepix Trigger Extraction Tool
 * -------------------------------
 *
 * timepix-spidr-data-parser/src/bin/trigger_extraction_tool.rs
 *
 * Authors: Jared Vann
 */

use std::collections::VecDeque;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path::Path;

use clap;
use colored::Colorize;
use glob::glob;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon;
use separator::Separatable as _;
use serde::Serialize;
use toml;

use timepix_spidr_data_parser::*;

const HIT_BUFFER_SIZE: usize = 1_000_000;

#[derive(Clone, Serialize)]
struct Settings {
    output_filename: String,
    max_hits: Option<usize>,
    max_triggers: Option<usize>,
    min_event_hits: usize,
    window_look_behind: u64,
    window_look_ahead: u64,
    relative_toa: bool,
    write_all: bool,
    prevent_overlap: bool,
}

trait HasChild {
    fn has_child(&self, file_name: &str) -> io::Result<bool>;
}

impl HasChild for Path {
    fn has_child(&self, file_name: &str) -> io::Result<bool> {
        if !self.is_dir() {
            return Err(io::Error::new(io::ErrorKind::Other, "Not directory"));
        }

        for entry in self.read_dir()? {
            if entry?.file_name() == file_name {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

fn get_line_count(file_name: &str) -> io::Result<usize> {
    let file = std::fs::File::open(file_name)?;
    let f = std::io::BufReader::new(file);
    Ok(f.lines().count())
}

fn main() -> io::Result<()> {
    println!(
        "\n-------------------------------\n{}\n-------------------------------\n",
        "Timepix Trigger Extraction Tool".bold()
    );

    //
    // Generate command line option parser
    //
    let matches = clap::App::new("")
        // General options
        .arg(
            clap::Arg::with_name("INPUT")
                .help("Sets the input directory pattern")
                .required(true)
                .index(1),
        )
        .arg(
            clap::Arg::with_name("filename")
                .help("Sets the output filename (without extension!) to use (default is 'trigger_events.bin/trigger_events.csv')")
                .long("filename")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("max-hits")
                .help("Maximum number of hits to process (default is all)")
                .long("max-hits")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("max-triggers")
                .help("Maximum number of triggers to process (default is all)")
                .long("max-triggers")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("min-event-hits")
                .help("Minimum number of hits in an event to save (default is 0)")
                .long("min-event-hits")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("window-size")
                .help("Acquisition window in us after each trigger")
                .long("window-size")
                .takes_value(true)
                .required(true),
        )
        .arg(
            clap::Arg::with_name("post-trigger-percent")
                .help("Set the percentage of the acquistion window to place after the trigger (default is 100)")
                .long("post-trigger-percent")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("relative-toa")
                .help("Set ToA values relative to the start of acquisition window (default is false)")
                .long("relative-toa"),
        )
        .arg(
            clap::Arg::with_name("write-all")
                .help("Write all triggers to file event if the window contains no hits")
                .long("write-all"),
        )
        .arg(
            clap::Arg::with_name("prevent-overlap")
                .help("Ignores any triggers that overlap with a previous trigger")
                .long("prevent-overlap"),
        )
        .arg(
            clap::Arg::with_name("dry-run")
                .help("Shows output of what runs will be processed without changing any data")
                .long("dry-run"),
        )
        .arg(clap::Arg::with_name("disable-mt").help("Disables multi-threading").long("disable-mt"))
        .get_matches();

    //
    // Read command line options
    //
    let input_glob_str = matches.value_of("INPUT").unwrap();

    let settings = {
        let output_filename = matches.value_of("filename").unwrap_or("trigger_events").to_owned();

        let max_hits = matches.value_of("max-hits").and_then(parse_human_readable_number);
        let max_triggers = matches.value_of("max-triggers").and_then(parse_human_readable_number);
        let min_event_hits = matches.value_of("min-event-hits").and_then(parse_human_readable_number).unwrap_or(0);

        let window_size = matches.value_of("window-size").unwrap().parse::<u64>().ok().unwrap() as u64;

        let post_trigger_percent = matches
            .value_of("post-trigger-percent")
            .and_then(|x| x.parse::<f64>().ok())
            .unwrap_or(100.0);

        assert!(post_trigger_percent <= 100.0);
        assert!(post_trigger_percent > 0.0);

        let window_look_behind = (window_size as f64 * (100.0 - post_trigger_percent) * 10.0) as u64;
        let window_look_ahead = (window_size as f64 * post_trigger_percent * 10.0) as u64;

        let relative_toa = matches.is_present("relative-toa");
        let write_all = matches.is_present("write-all");
        let prevent_overlap = matches.is_present("prevent-overlap");

        Settings {
            output_filename,
            max_hits,
            max_triggers,
            min_event_hits,
            window_look_behind,
            window_look_ahead,
            relative_toa,
            write_all,
            prevent_overlap,
        }
    };

    let dry_run = matches.is_present("dry-run");
    let disable_mt = matches.is_present("disable-mt");

    //
    // Parse input file list
    //
    let input_dirs: Vec<_> = glob(input_glob_str)
        .expect("Failed to read input file glob pattern")
        .filter_map(|x| x.ok()) // Check glob worked
        .filter(|x| x.is_dir()) // Check is directory
        .filter(|x| x.has_child("hits.bin").unwrap())
        .filter(|x| x.has_child("triggers.csv").unwrap())
        // Check doesnt have existing output files
        .filter(|x| !x.has_child(&format!("{}.bin", settings.output_filename)).unwrap())
        .collect();

    if input_dirs.is_empty() {
        println!("No input directories matched!");
        return Ok(());
    }

    println!("Matched {} input directories", input_dirs.len());

    if dry_run {
        println!("");

        for input_dir in input_dirs {
            println!("{}", input_dir.to_str().unwrap());
        }
    } else {
        let multi_progress = MultiProgress::new();
        let sty = ProgressStyle::default_bar()
            .template(PROGRESS_BAR_TEMPLATE)
            .progress_chars(PROGRESS_BAR_CHARS);

        let disable_mt = disable_mt || input_dirs.len() == 1;

        for input_dir in input_dirs {
            let n_triggers = get_line_count(input_dir.join("triggers.csv").to_str().unwrap())? - 1;

            let progress_bar = ProgressBar::new(n_triggers as u64);
            progress_bar.set_style(sty.clone());

            if disable_mt {
                process_run(&input_dir, settings.clone(), progress_bar).unwrap();
            } else {
                let progress_bar = multi_progress.add(progress_bar);
                let s = settings.clone();
                rayon::spawn(move || process_run(&input_dir, s, progress_bar).unwrap());
            }
        }

        multi_progress.join().unwrap();
    }

    Ok(())
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

fn process_run(run_dir: &Path, settings: Settings, progress_bar: ProgressBar) -> io::Result<()> {
    let run_name = run_dir.file_stem().unwrap().to_str().unwrap();

    progress_bar.set_message(&format!("| 0 Events Written | 0 Overlapping Triggers Ignored | {}", run_name));

    let mut hit_iterator = ReadHitsIterator::new(&run_dir.join("hits.bin"));
    let triggers = read_trigger_data(&run_dir.join("triggers.csv"))?;

    let output_data_file_path = run_dir.join(format!("{}{}", settings.output_filename, ".bin"));
    let output_csv_file_path = run_dir.join(format!("{}{}", settings.output_filename, ".csv"));
    let output_toml_file_path = run_dir.join(format!("{}{}", settings.output_filename, ".toml"));

    let mut output_data_file = fs::File::create(&output_data_file_path)?;

    // Setup CSV file
    let output_csv_file = fs::File::create(&output_csv_file_path)?;
    let mut csv_writer = csv::Writer::from_writer(output_csv_file);

    // Write metadata to TOML file
    fs::write(&output_toml_file_path, toml::to_string(&settings).unwrap())?;

    let mut events_written = 0;
    let mut overlapping_triggers_ignored = 0;
    let mut accumulated_file_size = 0;

    let mut last_start: usize = 0;

    let mut hit_buffer = VecDeque::with_capacity(HIT_BUFFER_SIZE);

    for _ in 0..HIT_BUFFER_SIZE {
        if let Some(hit) = hit_iterator.next() {
            hit_buffer.push_back(hit);
        }
    }

    vecdeque_insertion_sort(&mut hit_buffer);

    let mut i = 0;
    while i < triggers.len() {
        let trigger = triggers[i];
        
        progress_bar.inc(1);

        if let Some(max) = settings.max_triggers {
            if i == max {
                break;
            }
        }

        let start_time = trigger.time - settings.window_look_behind;
        let end_time = trigger.time + settings.window_look_ahead;

        let start_time_clks = (start_time as f64 / TOA_CLOCK_TO_NS) as u64;
        let end_time_clks = (end_time as f64 / TOA_CLOCK_TO_NS) as u64;

        while hit_buffer.len() > 0
           && hit_buffer.front().unwrap().toa < start_time_clks
           && hit_buffer.back().unwrap().toa < end_time_clks {
            hit_buffer = hit_buffer.split_off(std::cmp::min(100000, hit_buffer.len()));

            // Replenish hits buffer
            for _ in 0..100000 {
                if let Some(hit) = hit_iterator.next() {
                    hit_buffer.push_back(hit);
                }
            }
        }

        vecdeque_insertion_sort(&mut hit_buffer);

        // Check trigger does not overlap with previous and next triggers
        if settings.prevent_overlap {
            let mut skip = 1;
            
            for j in i+1..triggers.len() {
                if triggers[j].time < end_time {
                    skip += 1;
                }
                else {
                    break;
                }
            }
            
            if skip > 1 {
                i += skip;
                
                overlapping_triggers_ignored += skip;
                
                progress_bar.set_message(&format!(
                    "| {} Events Written | {} Overlapping Triggers Ignored | {}",
                    events_written.separated_string(),
                    overlapping_triggers_ignored.separated_string(),
                    run_name
                ));
                
                continue;
            }
        }

        let mut start_hit: usize = 0;
        let mut end_hit: usize = 0;
        let mut start_set = false;
        let mut end_set = false;

        for (j, hit) in hit_buffer.iter().enumerate().skip(last_start) {
            if !start_set {
                if hit.toa > start_time_clks {
                    start_hit = j;
                    start_set = true;
                }
            } else if hit.toa <= end_time_clks {
                end_hit = j;
                end_set = true;
            } else if hit.toa > end_time_clks {
                break;
            }
        }

        if let Some(max) = settings.max_hits {
            if start_hit >= max || end_hit >= max {
                break;
            }
        }

        if settings.write_all || (end_set && (end_hit - start_hit) > settings.min_event_hits) {
            let toa_adjustment = if settings.relative_toa { -(start_time_clks as i64) } else { 0 };

            if !end_set {
                write_cluster_to_file(&mut output_data_file, &[], toa_adjustment)?;
            } else {
                write_cluster_to_file(&mut output_data_file, &hit_buffer.as_slices().0[start_hit..end_hit], toa_adjustment)?;
            }

            csv_writer.serialize(ClusterMetadata {
                event: i + 1 as usize,
                time: start_time as f64,
                duration: if end_set {
                    (end_time - start_time) as f64 * TOA_CLOCK_TO_NS
                } else {
                    0.0
                },
                hits: if end_set { end_hit - start_hit } else { 0 },
                sum_tot: if end_set {
                    hit_buffer.as_slices().0[start_hit..end_hit].iter().map(|hit| hit.tot).sum()
                } else {
                    0
                },
                offset: accumulated_file_size,
            })?;

            accumulated_file_size += if end_set { (end_hit - start_hit + 1) * 16 } else { 16 };

            events_written += 1;
            progress_bar.set_message(&format!(
                "| {} Events Written | {} Overlapping Triggers Ignored | {}",
                events_written.separated_string(),
                overlapping_triggers_ignored.separated_string(),
                run_name
            ));
        }

        last_start = last_start.saturating_sub(start_hit);
        
        i += 1;
    }

    csv_writer.flush()?;

    progress_bar.finish_with_message(&format!(
        "| Done | {} Events Written | {} Overlapping Triggers Ignored | {}",
        events_written.separated_string(),
        overlapping_triggers_ignored.separated_string(),
        run_name
    ));

    Ok(())
}
