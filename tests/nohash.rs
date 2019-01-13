use std::hash::{ Hasher, BuildHasherDefault };
use futures::prelude::*;
use futures::future;
use futures_dagtask::TaskGraph;


type NoHashBuilder = BuildHasherDefault<NoHasher>;

#[derive(Default)]
struct NoHasher(u64);

impl Hasher for NoHasher {
    fn finish(&self) -> u64 {
        self.0 as u64
    }

    fn write(&mut self, _: &[u8]) {
        panic!()
    }

    fn write_u32(&mut self, i: u32) {
        self.0 = i as u64;
    }
}

#[test]
fn test_nohash_graph() {
    let mut graph = TaskGraph::<future::FutureResult<u32, ()>, u32, NoHashBuilder>::default();
    let one = graph.add_task(&[], future::ok(0)).unwrap();
    let two = graph.add_task(&[one], future::ok(1)).unwrap();
    graph.add_task(&[two], future::ok(2)).unwrap();

    let (_, exec) = graph.execute();

    let output = exec.take(3)
        .wait()
        .filter_map(Result::ok)
        .map(|(_, n)| n)
        .collect::<Vec<_>>();
    assert_eq!(output, vec![0, 1, 2]);
}
