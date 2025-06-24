mod mapreduce_core;
mod word_count;

use crate::mapreduce_core::map_reduce;
use crate::word_count::{WordCountMap, WordCountReduce};

use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::time::Instant;

fn read_file(file_path: &str) -> io::Result<Vec<((), String)>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut res: Vec<((), String)> = Vec::new();

    for line_result in reader.lines() {
        let line = line_result?; // Unwrap or handle the Result
        res.push(((), line));
    }

    Ok(res)
}

fn main() {
    let start = Instant::now();

    let input = read_file("shakespeare.txt").unwrap();

    let result = map_reduce(WordCountMap, WordCountReduce, input);

    println!("RESULT: {:?}", result);

    println!("Time elapsed: {} ms", start.elapsed().as_millis());
}
