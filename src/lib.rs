mod graph;

use std::{ fmt, mem, error };
use std::hash::{ Hash, BuildHasher };
use std::vec::IntoIter;
use std::collections::hash_map::RandomState;
use num_traits::{ CheckedAdd, One };
use futures::stream::futures_unordered::FuturesUnordered;
use futures::sync::{oneshot, BiLock};
use futures::prelude::*;
use crate::graph::Graph;
pub use crate::graph::Index;


pub struct TaskGraph<T, I=u32, S=RandomState> {
    dag: Graph<State<T>, I, S>,
    pending: Vec<IndexFuture<T, I>>
}

enum State<T> {
    Pending {
        count: usize,
        task: T
    },
    Running,
}

impl<T, I, S> Default for TaskGraph<T, I, S>
where
    I: Default + Hash + PartialEq + Eq,
    S: Default + BuildHasher
{
    fn default() -> TaskGraph<T, I, S> {
        TaskGraph { dag: Graph::default(), pending: Vec::new() }
    }
}

impl<T> TaskGraph<T> {
    pub fn new() -> Self {
        TaskGraph::default()
    }
}

impl<T, I, S> TaskGraph<T, I, S>
where
    T: Future,
    I: CheckedAdd + One + Hash + PartialEq + Eq + PartialOrd + Clone,
    S: BuildHasher
{
    pub fn add_task(&mut self, deps: &[Index<I>], task: T) -> Result<Index<I>, Error<T>> {
        let mut count = 0;
        for dep in deps {
            if dep >= &self.dag.last {
                return Err(Error::WouldCycle(task));
            }

            if self.dag.contains(dep) {
                count += 1;
            }
        }

        if count == 0 {
            match self.dag.add_node(State::Running) {
                Ok(index) => {
                    self.pending.push(IndexFuture::new(index.clone(), task));
                    Ok(index)
                },
                Err(_) => Err(Error::IndexExhausted(task))
            }
        } else {
            match self.dag.add_node(State::Pending { count, task }) {
                Ok(index) => {
                    for parent in deps {
                        self.dag.add_edge(parent, index.clone());
                    }
                    Ok(index)
                },
                Err(State::Pending { task, .. }) => Err(Error::IndexExhausted(task)),
                Err(State::Running) => unreachable!()
            }
        }
    }

    pub fn execute(mut self) -> (AddTask<T, I, S>, Execute<T, I, S>) {
        let mut queue = FuturesUnordered::new();
        for fut in self.pending.drain(..) {
            queue.push(fut);
        }
        let (g1, g2) = BiLock::new(self);
        let (tx, rx) = oneshot::channel();
        (
            AddTask { inner: g1, tx },
            Execute { inner: g2, done: Vec::new(), is_canceled: false, queue, rx }
        )
    }

    fn walk(&mut self, index: &Index<I>) -> TaskWalker<'_, T, I, S> {
        let walker = self.dag.walk(index);
        TaskWalker { dag: &mut self.dag, walker }
    }
}

pub struct AddTask<T, I=u32, S=RandomState> {
    inner: BiLock<TaskGraph<T, I, S>>,
    tx: oneshot::Sender<()>
}

impl<T, I, S> AddTask<T, I, S>
where
    T: Future,
    I: CheckedAdd + One + Hash + PartialEq + Eq + PartialOrd + Clone,
    S: BuildHasher
{
    pub fn add_task(&self, deps: &[Index<I>], task: T) -> Async<Result<Index<I>, Error<T>>> {
        match self.inner.poll_lock() {
            Async::Ready(mut graph) => Async::Ready(graph.add_task(deps, task)),
            Async::NotReady => Async::NotReady
        }
    }

    pub fn abort(self) {
        let _ = self.tx.send(());
    }
}

pub struct Execute<T, I=u32, S=RandomState> {
    inner: BiLock<TaskGraph<T, I, S>>,
    queue: FuturesUnordered<IndexFuture<T, I>>,
    done: Vec<Index<I>>,
    rx: oneshot::Receiver<()>,
    is_canceled: bool
}

impl<T, I, S> Execute<T, I, S>
where
    T: Future,
    I: CheckedAdd + One + Hash + PartialEq + Eq + PartialOrd + Clone,
    S: BuildHasher
{
    fn enqueue(&mut self) -> Async<()> {
        let mut graph = match self.inner.poll_lock() {
            Async::Ready(graph) => graph,
            Async::NotReady => return Async::NotReady
        };

        for fut in graph.pending.drain(..) {
            self.queue.push(fut);
        }

        for index in self.done.drain(..) {
            for fut in graph.walk(&index) {
                self.queue.push(fut);
            }
            graph.dag.remove_node(&index);
        }

        Async::Ready(())
    }
}

impl<F, I, S> Stream for Execute<F, I, S>
where
    F: Future,
    I: CheckedAdd + One + Hash + PartialEq + Eq + PartialOrd + Clone,
    S: BuildHasher
{
    type Item = (Index<I>, F::Item);
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.rx.poll() {
            Ok(Async::NotReady) => (),
            Ok(Async::Ready(())) => return Ok(Async::Ready(None)),
            Err(_) => {
                self.is_canceled = true;
            }
        }

        match self.enqueue() {
            Async::Ready(()) => (),
            Async::NotReady => return Ok(Async::NotReady)
        }

        match self.queue.poll() {
            Ok(Async::Ready(Some((i, item)))) => {
                self.done.push(i.clone());
                Ok(Async::Ready(Some((i, item))))
            },
            Ok(Async::Ready(None)) if self.is_canceled => Ok(Async::Ready(None)),
            Ok(Async::Ready(None)) | Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(err) => Err(err)
        }
    }
}

struct IndexFuture<F, I> {
    index: Index<I>,
    fut: F
}

impl<F, I> IndexFuture<F, I> {
    pub fn new(index: Index<I>, fut: F) -> IndexFuture<F, I> {
        IndexFuture { index, fut }
    }
}

impl<F: Future, I: Clone> Future for IndexFuture<F, I> {
    type Item = (Index<I>, F::Item);
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.fut.poll() {
            Ok(Async::Ready(item)) => Ok(Async::Ready((self.index.clone(), item))),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(err) => Err(err)
        }
    }
}

struct TaskWalker<'a, T, I, S> {
    dag: &'a mut Graph<State<T>, I, S>,
    walker: IntoIter<Index<I>>
}

impl<'a, T, I, S> Iterator for TaskWalker<'a, T, I, S>
where
    I: CheckedAdd + One + Hash + PartialEq + Eq + Clone,
    S: BuildHasher
{
    type Item = IndexFuture<T, I>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(index) = self.walker.next() {
            let state = match self.dag.get_node_mut(&index) {
                Some(node) => node,
                None => continue
            };

            if let State::Pending { count, .. } = state {
                *count -= 1;
            }

            match state {
                State::Pending { count: 0, .. } => (),
                _ => continue
            }

            if let State::Pending { task, .. } = mem::replace(state, State::Running) {
                return Some(IndexFuture::new(index, task));
            }
        }

        None
    }
}

pub enum Error<T> {
    WouldCycle(T),
    IndexExhausted(T)
}

impl<T> fmt::Debug for Error<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::WouldCycle(_) => f.debug_struct("WouldCycle").finish(),
            Error::IndexExhausted(_) => f.debug_struct("IndexExhausted").finish()
        }
    }
}

impl<T> fmt::Display for Error<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::WouldCycle(_) => write!(f, "would cycle"),
            Error::IndexExhausted(_) => write!(f, "index exhausted")
        }
    }
}

impl<T> error::Error for Error<T> {
    fn description(&self) -> &str {
        "error"
    }
}
