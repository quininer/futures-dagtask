use futures::future;
use futures_dagtask::{ TaskGraph, Error };


#[test]
fn test_cycle_dependent() {
    let mut graph = TaskGraph::new();
    let mut graph2 = TaskGraph::new();

    let zero = graph.add_task(&[], future::ready::<u32>(0)).unwrap();
    let err = graph2.add_task(&[zero], future::ready::<u32>(0));

    assert!(match err {
        Err(Error::WouldCycle(_)) => true,
        _ => false
    });
}

#[test]
fn test_overflow_index() {
    let mut graph = TaskGraph::<future::Ready<u8>, u8>::default();
    for i in 0..255 {
        graph.add_task(&[], future::ready(i)).unwrap();
    }

    let err = graph.add_task(&[], future::ready(255));

    assert!(match err {
        Err(Error::Exhausted(_)) => true,
        _ => false
    });
}
