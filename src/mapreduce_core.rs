use rayon::prelude::*;
use std::collections::HashMap;
// use std::thread::{self, JoinHandle};

// map and reduce interfaces
pub trait Map<K, V, KOut, VOut> {
    fn map(&self, key: K, value: V) -> Vec<(KOut, VOut)>;
}

pub trait Reduce<K, V> {
    fn reduce(&self, key: K, values: Vec<V>) -> (K, V);
}

pub fn map_reduce<M, R, K, V, KOut, VOut>(
    mapper: M,
    reducer: R,
    input: Vec<(K, V)>,
) -> Vec<(KOut, VOut)>
where
    M: Map<K, V, KOut, VOut> + Clone + Send + Sync + 'static,
    R: Reduce<KOut, VOut> + Clone + Send + Sync + 'static,
    K: Send + 'static, // K is safe to move across threads and is static
    V: Send + 'static,
    KOut: Eq + std::hash::Hash + Clone + Send + 'static, //must be hashable + clone?
    VOut: Clone + Send + 'static,
{
    // TODO: what if intermediate gets too big
    // let mut intermediate: Vec<(KOut, VOut)> = Vec::new();

    let intermediate: Vec<_> = input
        .into_par_iter()
        .flat_map(|(k, v)| mapper.clone().map(k, v))
        .collect();
    // for (k, v) in input {
    //     // TODO: use multithreading
    //     intermediate.extend(mapper.map(k, v));
    // }

    let mut group: HashMap<KOut, Vec<VOut>> = HashMap::new();

    for (k, v) in intermediate {
        group.entry(k).or_default().push(v);
    }

    // let handles: Vec<JoinHandle<(KOut, VOut)>> = group
    //     .into_iter()
    //     .map(|(k, v)| {
    //         let reducer_clone = reducer.clone();
    //         thread::spawn(move || reducer_clone.reduce(k, v))
    //     })
    //     .collect();

    // handles.into_iter().map(|h| h.join().unwrap()).collect()
    group
        .into_par_iter() // parallel iterator
        .map(|(k, v)| reducer.clone().reduce(k, v))
        .collect()
}
