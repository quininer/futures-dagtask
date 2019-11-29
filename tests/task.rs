use std::sync::{ Arc, Mutex };
use futures::future;
use futures::prelude::*;
use futures::executor::LocalPool;
use futures_dagtask::TaskGraph;


#[test]
fn test_simple_dependent() {
    let fut = async {
        let mut graph = TaskGraph::new();
        let zero = graph.add_task(&[], future::ready::<u32>(0)).unwrap();
        let one = graph.add_task(&[zero], future::ready(1)).unwrap();
        let (add, exec) = graph.execute();
        let _two = add.add_task(&[one], future::ready(2)).await.unwrap();

        exec.take(3)
            .map(|(_, n)| n)
            .collect::<Vec<_>>().await
    };

    let output = LocalPool::new()
        .run_until(fut);

    assert_eq!(output, vec![0, 1, 2]);
}

#[test]
fn test_concurrent_dependent() {
    let fut = async {
        let mut graph = TaskGraph::new();
        let zero0 = graph.add_task(&[], future::ready::<(u32, u32)>((0, 0))).unwrap();
        let zero1 = graph.add_task(&[], future::ready((0, 1))).unwrap();
        let one0 = graph.add_task(&[zero0], future::ready((1, 0))).unwrap();
        let _one1 = graph.add_task(&[zero0], future::ready((1, 1))).unwrap();
        let (add, exec) = graph.execute();
        let _two0 = add.add_task(
            &[zero1, one0],
            future::ready((2, 0))
        ).await.unwrap();

        exec.take(5)
            .map(|(_, n)| n)
            .collect::<Vec<_>>().await
    };

    let output = LocalPool::new()
        .run_until(fut);

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
    let fut = async {
        let mut graph = TaskGraph::new();
        let zero0 = graph.add_task(
            &[],
            future::ready::<(u32, u32)>((0, 0)).boxed()
        ).unwrap();
        let (add, exec) = graph.execute();
        let add = Arc::new(add);
        let add2 = add.clone();

        let _zero1 = add.add_task(
            &[],
            (async move {
                let _one0 = add2.add_task(
                    &[zero0],
                    future::ready((1u32, 0u32)).boxed()
                ).await.unwrap();
                future::ready((0u32, 1u32)).await
            }).boxed()
        ).await.unwrap();

        exec.take(3)
            .map(|(_, n)| n)
            .collect::<Vec<_>>().await
    };

    let output = LocalPool::new()
        .run_until(fut);

    let zero0 = find(&output, (0, 0));
    let zero1 = find(&output, (0, 1));
    let one0 = find(&output, (1, 0));

    assert!(zero0 < one0 && zero1 < one0);
}

#[test]
fn test_cancel() {
    let fut = async {
        let mut graph = TaskGraph::new();
        let zero = graph.add_task(&[], future::ready::<u32>(0)).unwrap();
        let one = graph.add_task(&[zero], future::ready(1)).unwrap();
        let (add, exec) = graph.execute();
        let _two = add.add_task(&[one], future::ready(2)).await.unwrap();

        drop(add);
        exec.map(|(_, n)| n).collect::<Vec<_>>().await
    };

    let output = LocalPool::new()
        .run_until(fut);

    assert_eq!(output, vec![0, 1, 2]);
}

#[test]
fn test_abort() {
    let fut = async {
        let mut graph = TaskGraph::new();
        let zero = graph.add_task(
            &[],
            future::ready::<u32>(0).boxed()
        ).unwrap();
        let (add, exec) = graph.execute();
        let add = Arc::new(Mutex::new(Some(add)));
        let add2 = add.clone();

        add.lock().unwrap().as_ref().unwrap().add_task(
            &[zero],
            (async move {
                add2.lock().unwrap().take().unwrap().abort();
                future::ready(1u32).await
            }).boxed()
        ).await.unwrap();

        exec.map(|(_, n)| n).collect::<Vec<_>>().await
    };

    let output = LocalPool::new()
        .run_until(fut);

    assert_eq!(output, vec![0u32, 1]);
}

#[test]
fn test_custom_index() {
    let fut = async {
        let mut graph = TaskGraph::<future::Ready<u32>, u64>::default();
        let zero = graph.add_task(&[], future::ready(0)).unwrap();
        let one = graph.add_task(&[zero], future::ready(1)).unwrap();
        let (add, exec) = graph.execute();
        let _two = add.add_task(&[one], future::ready(2)).await.unwrap();

        drop(add);
        exec.map(|(_, n)| n).collect::<Vec<_>>().await
    };

    let output = LocalPool::new()
        .run_until(fut);
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
