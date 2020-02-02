# Timepix-Spidr-Data-Parser

A custom parser for raw Spidr Timepix3 data.

Converts data into a more flexible and accesible format as used by the ARIADNE experiment.

The `raw_data_parser` tool works on the raw Spidr data, the rest are tools that work on the output from the `raw_data_parser`.

## Tools

### clustering_tool

Clusters hits geometrically.

### heatmap_generator

Accumulates number of hits/sum of ToT in each pixel for a dataset and exports the results as a CSV file.

### hot_pixel_search

Finds the most active pixels in a dataset and exports the results as a CSV file.

### raw_data_parser

Parses all raw Spidr .dat data files that are given and outputs to a clearer directory and file structure.

### trigger_clustering_tool

Combines the `clustering_tool` with the `trigger_extraction_tool`.

### trigger_extraction_tool

Extracts hits within a set time window around each recorded Spidr trigger, optionally accounting for overlapping trigger windows.


## Requirements

- [Rust](https://rust-lang.org)


## Compilation (Optimised)

```
cargo build --release
```


## Usage

```
./target/release/[clustering_tool|heatmap_generator|hot_pixel_search|raw_data_parser|trigger_clustering_tool|trigger_extraction_tool] ...args...
```

## Authors

* **Jared Vann** - [jaredvann](https://github.com/jaredvann)


## Acknowledgements

The ARIADNE program is proudly supported by the European Research Council Grant No. 677927 and the University of Liverpool.
