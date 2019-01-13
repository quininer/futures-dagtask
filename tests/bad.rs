use futures::future;
use futures_dagtask::{ TaskGraph, Error };


#[test]
fn test_cycle_dependent() {
    let mut graph = TaskGraph::new();
    let mut graph2 = TaskGraph::new();

    let zero = graph.add_task(&[], future::ok::<u32, ()>(0)).unwrap();
    let err = graph2.add_task(&[zero], future::ok::<u32, ()>(0));

    assert!(match err {
        Err(Error::WouldCycle(_)) => true,
        _ => false
    });
}

#[test]
fn test_overflow_index() {
    let mut graph = TaskGraph::<future::FutureResult<u8, ()>, u8>::default();
    for i in 0..255 {
        graph.add_task(&[], future::ok(i)).unwrap();
    }

    let err = graph.add_task(&[], future::ok(255));

    assert!(match err {
        Err(Error::IndexExhausted(_)) => true,
        _ => false
    });
}
