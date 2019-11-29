use std::hash::{ Hasher, BuildHasherDefault };
use futures::future;
use futures::prelude::*;
use futures::executor::LocalPool;
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
    let fut = async {
        let mut graph = TaskGraph::<future::Ready<u32>, u32, NoHashBuilder>::default();
        let one = graph.add_task(&[], future::ready(0)).unwrap();
        let two = graph.add_task(&[one], future::ready(1)).unwrap();
        graph.add_task(&[two], future::ready(2)).unwrap();

        let (_, exec) = graph.execute();
        exec.take(3).map(|(_, n)| n).collect::<Vec<_>>().await
    };

    let output = LocalPool::new()
        .run_until(fut);
    assert_eq!(output, vec![0, 1, 2]);
}
