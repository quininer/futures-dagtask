mod graph;

use std::mem;
use std::vec::IntoIter;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::sync::{oneshot, BiLock};
use futures::prelude::*;
use crate::graph::Graph;
pub use crate::graph::Index;


pub struct TaskGraph<T> {
    dag: Graph<State<T>>,
    root: Index,
}

enum State<T> {
    Pending {
        count: usize,
        task: T
    },
    Running,
}

impl<T> Default for TaskGraph<T> {
    fn default() -> TaskGraph<T> {
        let mut dag = Graph::default();
        let root = dag.add_node(State::Running);
        TaskGraph { dag, root }
    }
}

impl<T: Future> TaskGraph<T> {
    pub fn add_task(&mut self, deps: &[Index], task: T) -> Index {
        self.add_task_with_count(deps.len() | 1, deps, task)
    }

    fn add_task_with_count(&mut self, count: usize, deps: &[Index], task: T) -> Index {
        let index = self.dag.add_node(State::Pending { count, task });
        if deps.is_empty() {
            self.dag.add_edge(self.root, index);
        } else {
            for &parent in deps {
                self.dag.add_edge(parent, index);
            }
        }
        index
    }

    pub fn execute(mut self) -> (AddTask<T>, Execute<T>) {
        let mut queue = FuturesUnordered::new();
        for fut in self.walk(self.root) {
            queue.push(fut);
        }
        let (g1, g2) = BiLock::new((self, Vec::new()));
        let (tx, rx) = oneshot::channel();
        (
            AddTask { inner: g1, tx },
            Execute { inner: g2, done: Vec::new(), queue, rx }
        )
    }

    fn walk<'a>(&'a mut self, index: Index) -> TaskWalker<'a, T> {
        let walker = self.dag.walk(index);
        TaskWalker { dag: &mut self.dag, walker }
    }
}

pub struct AddTask<T> {
    inner: BiLock<(TaskGraph<T>, Vec<Index>)>,
    tx: oneshot::Sender<()>
}

impl<T: Future> AddTask<T> {
    pub fn add_task(&self, deps: &[Index], task: T) -> Async<Index> {
        match self.inner.poll_lock() {
            Async::Ready(mut inner) => {
                let (graph, pending) = &mut *inner;
                let count = deps.iter()
                    .filter(|&&i| graph.dag.contains(i))
                    .count();
                let index = graph.add_task_with_count(count, deps, task);
                if count == 0 {
                    pending.push(index);
                }
                Async::Ready(index)
            },
            Async::NotReady => Async::NotReady
        }
    }

    pub fn abort(self) {
        let _ = self.tx.send(());
    }
}

pub struct Execute<T: Future> {
    inner: BiLock<(TaskGraph<T>, Vec<Index>)>,
    queue: FuturesUnordered<IndexFuture<T>>,
    done: Vec<Index>,
    rx: oneshot::Receiver<()>
}

impl<T: Future> Execute<T> {
    fn enqueue(&mut self) -> Async<()> {
        let mut inner = match self.inner.poll_lock() {
            Async::Ready(inner) => inner,
            Async::NotReady => return Async::NotReady
        };
        let (graph, pending) = &mut *inner;

        while let Some(index) = pending.pop() {
            if let Some(task_state) = graph.dag.get_node_mut(index) {
                if let State::Pending { task, .. } = mem::replace(task_state, State::Running) {
                    self.queue.push(IndexFuture::new(index, task));
                }
            }
        }

        while let Some(index) = self.done.pop() {
            for fut in graph.walk(index) {
                self.queue.push(fut);
            }
            graph.dag.remove_node(index);
        }

        Async::Ready(())
    }
}

impl<T: Future> Stream for Execute<T> {
    type Item = (Index, T::Item);
    type Error = T::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.rx.poll() {
            Ok(Async::NotReady) => (),
            Ok(Async::Ready(())) | Err(_) => return Ok(Async::Ready(None))
        }

        // TODO keep poll ?
        let _ = self.enqueue();

        match self.queue.poll() {
            Ok(Async::Ready(Some((i, item)))) => {
                self.done.push(i);
                Ok(Async::Ready(Some((i, item))))
            },
            Ok(Async::Ready(None)) | Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(err) => Err(err)
        }
    }
}

struct IndexFuture<F: Future> {
    index: Index,
    fut: F
}

impl<F: Future> IndexFuture<F> {
    pub fn new(index: Index, fut: F) -> IndexFuture<F> {
        IndexFuture { index, fut }
    }
}

impl<F: Future> Future for IndexFuture<F> {
    type Item = (Index, F::Item);
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.fut.poll() {
            Ok(Async::Ready(item)) => Ok(Async::Ready((self.index, item))),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(err) => Err(err)
        }
    }
}

struct TaskWalker<'a, T: Future> {
    dag: &'a mut Graph<State<T>>,
    walker: IntoIter<Index>
}

impl<'a, T: Future> Iterator for TaskWalker<'a, T> {
    type Item = IndexFuture<T>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(index) = self.walker.next() {
            let state = match self.dag.get_node_mut(index) {
                Some(node) => node,
                None => continue
            };

            if let State::Pending { count, .. } = state {
                *count -= 1;
            }

            match state {
                State::Pending { count, .. } if *count == 0 => (),
                _ => continue
            }

            if let State::Pending { task, .. } = mem::replace(state, State::Running) {
                return Some(IndexFuture::new(index, task));
            }
        }

        None
    }
}
