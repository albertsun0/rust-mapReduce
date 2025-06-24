use crate::mapreduce_core::{Map, Reduce};

#[derive(Clone)]
pub struct WordCountMap;

// Word count Map
impl Map<(), String, String, usize> for WordCountMap {
    fn map(&self, _key: (), value: String) -> Vec<(String, usize)> {
        // () is unit value, think of None/void
        // |word|(...) representes a closure, JS: word => [word, 1]
        // .collect() turns iterator into specified (by type) collection
        value
            .split_whitespace()
            .map(|word| (word.to_string(), 1))
            .collect()
    }
}
#[derive(Clone)]
pub struct WordCountReduce;

// Aggregate count for one word
impl Reduce<String, usize> for WordCountReduce {
    fn reduce(&self, key: String, values: Vec<usize>) -> (String, usize) {
        // println!("REDUCING {:?}", key);
        (key, values.iter().sum())
    }
}
