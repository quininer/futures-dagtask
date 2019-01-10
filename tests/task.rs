use std::sync::{ Arc, Mutex };
use futures::prelude::*;
use futures::future;
use futures_dagtask::{ TaskGraph, Index };

macro_rules! unwrap_async {
    ( $e:expr ) => {
        match $e {
            Async::Ready(e) => e,
            Async::NotReady => panic!()
        }
    }
}


#[test]
fn test_simple_dependent() {
    let mut graph = TaskGraph::default();
    let zero = graph.add_task(&[], future::ok::<u32, u32>(0));
    let one = graph.add_task(&[zero], future::ok(1));
    let (add, exec) = graph.execute();
    let _two = unwrap_async!(add.add_task(&[one], future::ok(2)));

    let output = exec.take(3)
        .wait()
        .filter_map(Result::ok)
        .map(|(_, n): (Index, _)| n)
        .collect::<Vec<_>>();
    assert_eq!(output, vec![0, 1, 2]);
}

#[test]
fn test_concurrent_dependent() {
    let mut graph = TaskGraph::default();
    let zero0 = graph.add_task(&[], future::ok::<(u32, u32), ()>((0, 0)));
    let zero1 = graph.add_task(&[], future::ok((0, 1)));
    let one0 = graph.add_task(&[zero0], future::ok((1, 0)));
    let _one1 = graph.add_task(&[zero0], future::ok((1, 1)));
    let (add, exec) = graph.execute();
    let _two0 = unwrap_async!(add.add_task(
        &[zero1, one0],
        future::ok((2, 0))
    ));

    let output = exec.take(5)
        .wait()
        .filter_map(Result::ok)
        .map(|(_, n): (Index, _)| n)
        .collect::<Vec<_>>();

    let zero0 = find(&output, (0, 0));
    let zero1 = find(&output, (0, 1));
    let one0 = find(&output, (1, 0));
    let one1 = find(&output, (1, 1));
    let two0 = find(&output, (2, 0));

    assert!(zero0 < one0 && zero1 < two0 && one0 < two0);
    assert!(zero0 < one1);
}

#[test]
fn test_concurrent_dependent2() {
    let mut graph = TaskGraph::default();
    let zero0 = graph.add_task(&[], Box::new(future::ok::<(u32, u32), ()>((0, 0))) as Box<Future<Item=_, Error=_>>);
    let (add, exec) = graph.execute();
    let add = Arc::new(add);
    let add2 = add.clone();
    let _zero1 = unwrap_async!(add.add_task(&[], Box::new(future::lazy(move || {
        let _one0 = unwrap_async!(add2.add_task(&[zero0], Box::new(future::ok((1u32, 0u32))) as Box<_>));
        future::ok((0u32, 1u32))
    })) as Box<_>));

    let output = exec
        .take(3)
        .wait()
        .filter_map(Result::ok)
        .map(|(_, n): (Index, _)| n)
        .collect::<Vec<_>>();

    let zero0 = find(&output, (0, 0));
    let zero1 = find(&output, (0, 1));
    let one0 = find(&output, (1, 0));

    assert!(zero0 < one0 && zero1 < one0);
}

#[test]
fn test_abort() {
    let mut graph = TaskGraph::default();
    let zero = graph.add_task(&[], Box::new(future::ok::<u32, ()>(0)) as Box<Future<Item=_, Error=_>>);
    let (add, exec) = graph.execute();
    let add = Arc::new(Mutex::new(Some(add)));
    let add2 = add.clone();

    {
        unwrap_async!(add.lock().unwrap().as_ref().unwrap().add_task(&[zero], Box::new(future::lazy(move || {
            add2.lock().unwrap().take().unwrap().abort();
            future::ok(1u32)
        })) as Box<_>));
    }

    let output = exec.wait()
        .filter_map(Result::ok)
        .map(|(_, n): (Index, _)| n)
        .collect::<Vec<_>>();

    assert_eq!(output, vec![0u32, 1]);
}

#[test]
fn test_custom_index() {
    let mut graph = TaskGraph::<future::FutureResult<u32, ()>, u64>::default();
    let zero = graph.add_task(&[], future::ok(0));
    let one = graph.add_task(&[zero], future::ok(1));
    let (add, exec) = graph.execute();
    let _two = unwrap_async!(add.add_task(&[one], future::ok(2)));

    let output = exec.take(3)
        .wait()
        .filter_map(Result::ok)
        .map(|(_, n): (Index<u64>, _)| n)
        .collect::<Vec<_>>();
    assert_eq!(output, vec![0, 1, 2]);
}

fn find(output: &[(u32, u32)], target: (u32, u32)) -> usize {
    output.iter()
        .enumerate()
        .find_map(|(i, &n)| if n == target {
            Some(i)
        } else {
            None
        })
        .unwrap()
}
