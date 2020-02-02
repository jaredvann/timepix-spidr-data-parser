/*
 * ARIADNE Experiment, Department of Physics, University of Liverpool
 *
 * -----------------------
 * Timepix Clustering Tool
 * -----------------------
 *
 * timepix-spidr-data-parser/src/bin/clustering_tool.rs
 *
 * Authors: Jared Vann
 */

use std::collections::VecDeque;
use std::convert::TryFrom;
use std::fs;
use std::io;
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

const HITS_BUFFER_SIZE: usize = 1_000_000;

#[derive(Clone, Serialize)]
struct Settings {
    output_filename: String,
    max_clusters: Option<usize>,
    min_cluster_hits: usize,
    min_cluster_tot: u32,
    max_pixel_gap: u32,
    max_toa_gap: u32,
    min_hit_tot: u32,
    toa_window: u32,
    relative_toa: bool,
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
        "\n-----------------------\n{}\n-----------------------\n",
        "Timepix Clustering Tool".bold()
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
                .help("Sets the output filename (without extension!) to use (default is 'clusters.bin/clusters.csv'")
                .long("filename")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("max-clusters")
                .help("Maximum number of clusters to find (default is all)")
                .long("max-clusters")
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
            clap::Arg::with_name("max-toa-gap")
                .help("Maximum gap in ToA to include in cluster (ns)")
                .long("max-toa-gap")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("min-hit-tot")
                .help("Minimum ToT for hits to be used in clustering (ns) (default is 0)")
                .long("min-hit-tot")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("toa-window")
                .help("Maximum distance in time to look when clustering (ns)")
                .long("toa-window")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("relative-toa")
                .help("Set ToA values relative to the start of acquisition window (default is false)")
                .long("relative-toa"),
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
        let output_filename = matches.value_of("filename").unwrap_or("clusters").to_owned();

        let max_clusters = matches.value_of("max-clusters").and_then(parse_human_readable_number);
        let min_cluster_hits = matches.value_of("min-cluster-hits").and_then(parse_human_readable_number).unwrap_or(1); // 1 hit
        let min_cluster_tot = matches.value_of("min-cluster-tot").and_then(parse_human_readable_number).unwrap_or(1); // 25 ns

        assert!(min_cluster_hits > 0);
        assert!(min_cluster_tot > 0);

        let max_pixel_gap = matches.value_of("max-pixel-gap").and_then(parse_human_readable_number).unwrap_or(3);
        let max_toa_gap = (matches.value_of("max-toa-gap").and_then(parse_human_readable_number).unwrap_or(5_000) as f64 / TOA_CLOCK_TO_NS) as u32; // 5µs
        let min_hit_tot = matches.value_of("min-hit-tot").and_then(parse_human_readable_number).unwrap_or(0); // 0 ns
        let toa_window = (matches.value_of("toa-window").and_then(parse_human_readable_number).unwrap_or(1_000_000) as f64 / TOA_CLOCK_TO_NS) as u32; // 500µs

        let relative_toa = matches.is_present("relative-toa");

        Settings {
            output_filename,
            max_clusters,
            min_cluster_hits,
            min_cluster_tot,
            max_pixel_gap,
            max_toa_gap,
            min_hit_tot,
            toa_window,
            relative_toa,
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
            let n_hits = input_dir.join("hits.bin").metadata().unwrap().len() / 16;

            let progress_bar = ProgressBar::new(n_hits);
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

    let mut hits_iterator = ReadHitsIterator::new(&run_dir.join("hits.bin"));

    let output_data_file_path = run_dir.join(format!("{}{}", settings.output_filename, ".bin"));
    let output_csv_file_path = run_dir.join(format!("{}{}", settings.output_filename, ".csv"));
    let output_toml_file_path = run_dir.join(format!("{}{}", settings.output_filename, ".toml"));

    let mut output_data_file = fs::File::create(&output_data_file_path)?;

    // Setup CSV file
    let output_csv_file = fs::File::create(&output_csv_file_path)?;
    let mut csv_writer = csv::Writer::from_writer(output_csv_file);

    // Write metadata to TOML file
    fs::write(&output_toml_file_path, toml::to_string(&settings).unwrap())?;

    let mut clusters_written = 0;
    let mut accumulated_file_size = 0;

    let find_cluster_iterator = FindClusterIterator::new(
        &run_name,
        &mut hits_iterator,
        &progress_bar,
        settings.min_cluster_hits as u32,
        settings.min_cluster_tot,
        settings.min_hit_tot,
        settings.max_pixel_gap,
        settings.max_toa_gap,
        settings.toa_window,
    );

    progress_bar.set_message(&format!("| 0 Clusters Found | {}", run_name));

    for cluster in find_cluster_iterator {
        let start_time = cluster[0].toa;
        let end_time = cluster[cluster.len() - 1].toa;

        let toa_adjustment = if settings.relative_toa { -(start_time as i64) } else { 0 };

        write_cluster_to_file(&mut output_data_file, &cluster, toa_adjustment)?;
        clusters_written += 1;

        csv_writer.serialize(ClusterMetadata {
            event: clusters_written,
            time: start_time as f64 * TOA_CLOCK_TO_NS,
            duration: (end_time - start_time) as f64 * TOA_CLOCK_TO_NS,
            hits: cluster.len(),
            sum_tot: cluster.iter().map(|hit| hit.tot).sum(),
            offset: accumulated_file_size,
        })?;

        csv_writer.flush()?;

        accumulated_file_size += (cluster.len() + 1) * 16;

        if let Some(max) = settings.max_clusters {
            if clusters_written == max {
                break;
            }
        }
    }

    progress_bar.finish_with_message(&format!("| Done | {} Clusters Found | {}", clusters_written.separated_string(), run_name));

    Ok(())
}

pub struct FindClusterIterator<'a> {
    run_name: &'a str,
    hits_iterator: &'a mut ReadHitsIterator,
    hits_buffer: VecDeque<Hit>,
    hits_processed: VecDeque<bool>,

    progress_bar: &'a ProgressBar,

    min_cluster_hits: u32,
    min_hit_tot: u32,
    min_sum_tot: u32,
    max_pixel_gap: u32,
    max_toa_gap: u32,
    toa_window: u32,

    n_clusters: usize,
    total_hits_processed: usize,
}

impl<'a> FindClusterIterator<'a> {
    pub fn new(
        run_name: &'a str,
        hits_iterator: &'a mut ReadHitsIterator,
        progress_bar: &'a ProgressBar,
        min_cluster_hits: u32,
        min_sum_tot: u32,
        min_hit_tot: u32,
        max_pixel_gap: u32,
        max_toa_gap: u32,
        toa_window: u32,
    ) -> FindClusterIterator<'a> {
        let mut hits_buffer = VecDeque::with_capacity(HITS_BUFFER_SIZE);
        let mut hits_processed = VecDeque::with_capacity(HITS_BUFFER_SIZE);

        for _ in 0..HITS_BUFFER_SIZE {
            if let Some(hit) = hits_iterator.next() {
                hits_buffer.push_back(hit);
                hits_processed.push_back(false);
            }
        }

        FindClusterIterator {
            run_name,
            hits_iterator,
            hits_buffer,
            hits_processed,
            progress_bar,
            min_cluster_hits,
            min_sum_tot,
            min_hit_tot,
            max_pixel_gap,
            max_toa_gap,
            toa_window,
            n_clusters: 0,
            total_hits_processed: 0,
        }
    }
}

impl<'a> Iterator for FindClusterIterator<'a> {
    type Item = Vec<Hit>;

    fn next(&mut self) -> Option<Vec<Hit>> {
        // Iterate over all hits
        while !self.hits_buffer.is_empty() {
            if self.total_hits_processed % 1_000 == 0 {
                self.progress_bar.set_position(u64::try_from(self.total_hits_processed).unwrap());
            }

            let current_hit = self.hits_buffer.pop_front().unwrap();
            let is_processed = self.hits_processed.pop_front().unwrap();

            while let Some(hit) = self.hits_iterator.next() {
                if hit.tot > self.min_hit_tot {
                    self.hits_buffer.push_back(hit);
                    self.hits_processed.push_back(false);
                    break;
                }
            }

            if self.hits_buffer.is_empty() {
                return None;
            }

            // Skip if this hit has already been processed
            if is_processed {
                continue;
            }

            let start_toa = current_hit.toa;

            let mut cluster = Vec::with_capacity(self.min_cluster_hits as usize);

            let mut hits_stack = VecDeque::<usize>::with_capacity(1000);
            let mut is_in_stack = BitVec::from_elem(self.hits_buffer.len(), false);

            // Add initial hit to the stack
            hits_stack.push_back(0);

            // Build a stack of neighbouring hits and iterate until all hits in
            // the stack have been processed.
            while !hits_stack.is_empty() {
                let j = hits_stack.pop_front().unwrap();
                is_in_stack.set(j, false);

                let hit1 = self.hits_buffer[j];

                cluster.push(hit1);

                self.hits_processed[j] = true;
                self.total_hits_processed += 1;

                // Iterate over all hits (need to start from i and not j to allow
                // algorithm to look back in time for complex geometries)
                for k in 0..self.hits_buffer.len() {
                    if self.hits_processed[k] || is_in_stack[k] {
                        continue;
                    }

                    let hit2 = self.hits_buffer[k];

                    // Check hit is within overall time window (ie. 400us drift)
                    if (hit2.toa - start_toa) > u64::from(self.toa_window) {
                        break;
                    }

                    let toa_diff = (i64::try_from(hit2.toa).unwrap() - i64::try_from(hit1.toa).unwrap()).abs();
                    let col_diff = (i64::from(hit1.col) - i64::from(hit2.col)).abs();
                    let row_diff = (i64::from(hit1.row) - i64::from(hit2.row)).abs();

                    // Check hit is spatially nearby
                    if toa_diff < i64::from(self.max_toa_gap) && col_diff as u32 <= self.max_pixel_gap && row_diff as u32 <= self.max_pixel_gap {
                        hits_stack.push_back(k);
                        is_in_stack.set(k, true);
                    }
                }
            }

            if cluster.len() < self.min_cluster_hits as usize {
                continue;
            }

            let sum_tot: u32 = cluster.iter().map(|hit| hit.tot).sum();

            if sum_tot < self.min_sum_tot {
                continue;
            }

            cluster.sort();

            self.n_clusters += 1;

            self.progress_bar
                .set_message(&format!("| {} Clusters Found | {}", self.n_clusters.separated_string(), self.run_name));

            return Some(cluster);
        }

        None
    }
}
