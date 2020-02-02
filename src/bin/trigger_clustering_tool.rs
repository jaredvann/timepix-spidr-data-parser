/*
 * ARIADNE Experiment, Department of Physics, University of Liverpool
 *
 * ------------------------------------
 * Timepix Trigger Data Clustering Tool
 * ------------------------------------
 *
 * timepix-spidr-data-parser/src/bin/trigger_clustering_tool.rs
 *
 * Authors: Jared Vann
 */

extern crate serde_derive;

use std::collections::VecDeque;
use std::convert::TryFrom;
use std::fs;
use std::io;
use std::io::BufRead;
use std::path::Path;

use bit_vec::BitVec;
use clap;
use colored::Colorize;
use glob::glob;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon;
use separator::Separatable as _;
use serde::Serialize;
use toml;

use timepix_spidr_data_parser::*;

#[derive(Clone, Serialize)]
struct Settings {
    input_filename: String,
    output_filename: String,
    min_cluster_hits: usize,
    min_cluster_tot: u32,
    max_pixel_gap: u32,
    max_toa_gap: u32,
    min_hit_tot: u32,
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

fn main() -> io::Result<()> {
    println!(
        "\n------------------------------------\n{}\n------------------------------------\n",
        "Timepix Trigger Data Clustering Tool".bold()
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
            clap::Arg::with_name("input-filename")
                .help("Sets the input filename (without extension!) to use")
                .long("input-filename")
                .required(true)
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("output-filename")
                .help("Sets the output filename (without extension!) to use")
                .long("output-filename")
                .required(true)
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("min-cluster-hits")
                .help("Minimum cluster size in hits")
                .long("min-cluster-hits")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("min-cluster-tot")
                .help("Minimum cluster size in ToT (ns)")
                .long("min-cluster-tot")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("max-pixel-gap")
                .help("Maximum gap in pixels to include in cluster")
                .long("max-pixel-gap")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("min-hit-tot")
                .help("Minimum ToT for hits to be used in clustering (ns) (default is 0)")
                .long("min-hit-tot")
                .takes_value(true),
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
        let input_filename = matches.value_of("input-filename").unwrap().to_owned();
        let output_filename = matches.value_of("output-filename").unwrap().to_owned();

        let min_cluster_hits = matches.value_of("min-cluster-hits").and_then(parse_human_readable_number).unwrap_or(1); // 1 hit
        let min_cluster_tot = matches.value_of("min-cluster-tot").and_then(parse_human_readable_number).unwrap_or(1); // 25 ns

        assert!(min_cluster_hits > 0);
        assert!(min_cluster_tot > 0);

        let max_pixel_gap = matches.value_of("max-pixel-gap").and_then(parse_human_readable_number).unwrap_or(3);
        let max_toa_gap = (matches.value_of("max-toa-gap").and_then(parse_human_readable_number).unwrap_or(5_000) as f64 / TOA_CLOCK_TO_NS) as u32; // 5µs
        let min_hit_tot = matches.value_of("min-hit-tot").and_then(parse_human_readable_number).unwrap_or(0); // 0 ns

        Settings {
            input_filename,
            output_filename,
            min_cluster_hits,
            min_cluster_tot,
            max_pixel_gap,
            max_toa_gap,
            min_hit_tot,
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
        .filter(|x| x.has_child(&format!("{}.bin", settings.input_filename)).unwrap())
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
            let file = std::fs::File::open(input_dir.join(format!("{}.csv", settings.input_filename)))?;

            let n_events = std::io::BufReader::new(file).lines().count() - 1;

            let progress_bar = ProgressBar::new(n_events as u64);
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

fn process_run(run_dir: &Path, settings: Settings, progress_bar: ProgressBar) -> io::Result<()> {
    let run_name = run_dir.file_stem().unwrap().to_str().unwrap();

    let event_iterator = ReadClusterIterator::new(&run_dir.join(format!("{}.bin", settings.input_filename)).to_str().unwrap());

    let input_csv_file_path = run_dir.join(format!("{}.csv", settings.input_filename));
    
    let mut rdr = csv::Reader::from_path(input_csv_file_path)?;
    let input_csv_metadata: Vec<ClusterMetadata> = rdr.deserialize().map(|x| x.unwrap()).collect();

    let output_data_file_path = run_dir.join(format!("{}.bin", settings.output_filename));
    let output_csv_file_path = run_dir.join(format!("{}.csv", settings.output_filename));
    let output_toml_file_path = run_dir.join(format!("{}.toml", settings.output_filename));

    let mut output_data_file = fs::File::create(&output_data_file_path)?;

    // Setup CSV file
    let output_csv_file = fs::File::create(&output_csv_file_path)?;
    let mut csv_writer = csv::Writer::from_writer(output_csv_file);

    // Write metadata to TOML file
    fs::write(&output_toml_file_path, toml::to_string(&settings).unwrap())?;

    let mut clusters_written = 0;
    let mut accumulated_file_size = 0;

    progress_bar.set_message(&format!("| 0 Clusters Saved | {}", run_name));

    for (i, trigger_window_hits) in event_iterator.enumerate() {
        progress_bar.inc(1);

        let clusters = find_cluster(&trigger_window_hits, &settings);

        if clusters.len() == 1 {
            let cluster = &clusters[0];

            let start_time = cluster[0].toa;
            let end_time = cluster[cluster.len() - 1].toa;

            write_cluster_to_file(&mut output_data_file, &cluster, 0)?;
            
            csv_writer.serialize(ClusterMetadata {
                event: input_csv_metadata[i].event,
                time: start_time as f64 * TOA_CLOCK_TO_NS,
                duration: (end_time - start_time) as f64 * TOA_CLOCK_TO_NS,
                hits: cluster.len(),
                sum_tot: cluster.iter().map(|hit| hit.tot).sum(),
                offset: accumulated_file_size,
            })?;

            csv_writer.flush()?;

            clusters_written += 1;
            accumulated_file_size += (cluster.len() + 1) * 16;

            progress_bar.set_message(&format!("| {} Clusters Saved | {}", clusters_written.separated_string(), run_name));
        }
    }

    progress_bar.finish_with_message(&format!("| {} Clusters Saved | {}", clusters_written.separated_string(), run_name));

    Ok(())
}


fn find_cluster(hits: &[Hit], settings: &Settings) -> Vec<Vec<Hit>> {
    let mut clusters = Vec::new();
    
    let mut hit_is_processed = BitVec::from_elem(hits.len(), false);
    
    let mut hits_stack = VecDeque::<usize>::with_capacity(1000);

    let mut cluster = Vec::with_capacity(settings.min_cluster_hits as usize);

    // Iterate over all hits
    let mut i = 0;
    while i < hits.len() {
        // Clear data structures
        cluster.clear();
        hits_stack.clear();  
        let mut hit_is_in_stack = BitVec::from_elem(hits.len(), false);
        
        // Add initial hit to the stack
        hits_stack.push_back(i);
        hit_is_in_stack.set(i, true);

        // Build a stack of neighbouring hits and iterate until all hits in
        // the stack have been processed.
        while !hits_stack.is_empty() {
            let j = hits_stack.pop_front().unwrap();
            hit_is_in_stack.set(j, false);

            let hit1 = hits[j];

            cluster.push(hit1);
            hit_is_processed.set(j, true);

            // Iterate over all hits (need to start from i and not j to allow
            // algorithm to look back in time for complex geometries)
            for k in i..hits.len() {
                if hit_is_processed[k] || hit_is_in_stack[k] {
                    continue;
                }

                let hit2 = hits[k];

                let toa_diff = (i64::try_from(hit2.toa).unwrap() - i64::try_from(hit1.toa).unwrap()).abs();
                let col_diff = (i64::from(hit1.col) - i64::from(hit2.col)).abs();
                let row_diff = (i64::from(hit1.row) - i64::from(hit2.row)).abs();

                // Check hit is spatially nearby
                if toa_diff < i64::from(settings.max_toa_gap) && col_diff as u32 <= settings.max_pixel_gap && row_diff as u32 <= settings.max_pixel_gap {
                    hits_stack.push_back(k);
                    hit_is_in_stack.set(k, true);
                }
            }
        }

        i += 1;

        if cluster.len() < settings.min_cluster_hits as usize {
            continue;
        }

        let sum_tot: u32 = cluster.iter().map(|hit| hit.tot).sum();

        if sum_tot < settings.min_cluster_tot {
            continue;
        }

        cluster.sort();
        clusters.push(cluster);
        cluster = Vec::new();
    }

    clusters
}
