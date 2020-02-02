/*
 * ARIADNE Experiment, Department of Physics, University of Liverpool
 *
 * timepix-spidr-data-parser/src/lib.rs
 *
 * Authors: Jared Vann
 */

use std::cmp::Ordering;

use num_traits::Num;
use serde::{Deserialize, Serialize};

mod io;
pub use io::*;

pub const TOA_CLOCK_TO_NS: f64 = 1.5625;
pub const TOT_ADU_TO_NS: u32 = 25;

pub const BUFFER_SIZE: usize = 100_000; // 800KB (this seems fairly optimal - bigger causes stack to fill)

pub const HOT_PIXELS: [(u16, u16); 30] = [
    // (16, 163),
    // (86, 230),
    // (129, 164),
    // (141, 245),
    // (145, 236),
    // (177, 11),
    // (202, 174),
    // (246, 205),
    (177, 245),
    (141, 245),
    (41, 130),
    (81, 205),
    (23, 196),
    (102, 249),
    (44, 114),
    (145, 236),
    (129, 164),
    (218, 103),
    (12, 90),
    (188, 88),
    (87, 148),
    (105, 253),
    (184, 175),
    (235, 142),
    (255, 238),
    (16, 163),
    (168, 203),
    (96, 207),
    (14, 101),
    (140, 164),
    (220, 102),
    (1, 112),
    (237, 174),
    (13, 228),
    (185, 122),
    (163, 120),
    (178, 142),
    (157, 114),
];

pub static PROGRESS_BAR_TEMPLATE: &'static str = "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}";
pub static PROGRESS_BAR_CHARS: &'static str = "##-";

// const XY_CONVERSION_FACTOR: f64 = 0.703_125; // pixels -> mm
// const Z_CONVERSION_FACTOR: f64 = 0.1 * (25.0 / 4096.0); // ns -> mm

#[derive(Clone, Copy, Debug, Eq)]
pub struct Hit {
    pub toa: u64,
    pub tot: u32,
    pub col: u16,
    pub row: u16,
}

impl Ord for Hit {
    fn cmp(&self, other: &Hit) -> Ordering {
        self.toa.cmp(&other.toa)
    }
}

impl PartialOrd for Hit {
    fn partial_cmp(&self, other: &Hit) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Hit {
    fn eq(&self, other: &Hit) -> bool {
        self.toa == other.toa
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq)]
pub struct Trigger {
    pub event: u32,
    pub time: u64,
}

impl Ord for Trigger {
    fn cmp(&self, other: &Trigger) -> Ordering {
        self.time.cmp(&other.time)
    }
}

impl PartialOrd for Trigger {
    fn partial_cmp(&self, other: &Trigger) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Trigger {
    fn eq(&self, other: &Trigger) -> bool {
        self.time == other.time
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct ClusterMetadata {
    pub event: usize,
    pub time: f64,
    pub duration: f64,
    pub hits: usize,
    pub sum_tot: u32,
    pub offset: usize,
}

pub fn parse_human_readable_number<T: Num + std::str::FromStr + std::convert::TryFrom<u64>>(string: &str) -> Option<T> {
    let multiplier: u32 = match string.chars().rev().next()? {
        '0'..='9' => 1,
        'k' | 'K' => 1_000,
        'm' | 'M' => 1_000_000,
        'b' | 'B' => 1_000_000_000,
        _ => {
            return None;
        }
    };

    if multiplier == 1 {
        Some(string.parse::<T>().ok()?)
    } else {
        Some(T::try_from((string[0..string.len() - 1].parse::<f32>().ok()? * multiplier as f32) as u64).ok()?)
    }
}
