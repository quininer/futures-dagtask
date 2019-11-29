mod graph;

use std::{ fmt, mem, error };
use std::hash::{ Hash, BuildHasher };
use std::vec::IntoIter;
use std::pin::Pin;
use std::marker::Unpin;
use std::task::{ Context, Poll };
use std::collections::hash_map::RandomState;
use num_traits::{ CheckedAdd, One };
use futures::stream::futures_unordered::FuturesUnordered;
use futures::channel::oneshot;
use futures::lock::BiLock;
use futures::prelude::*;
use crate::graph::Graph;
pub use crate::graph::Index;


pub struct TaskGraph<T, I = u32, S = RandomState> {
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
    S: Default + BuildHasher + Unpin
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
    T: Future + Unpin,
    I: CheckedAdd + One + Hash + PartialEq + Eq + PartialOrd + Clone + Unpin,
    S: BuildHasher + Unpin
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
                Err(_) => Err(Error::Exhausted(task))
            }
        } else {
            match self.dag.add_node(State::Pending { count, task }) {
                Ok(index) => {
                    for parent in deps {
                        self.dag.add_edge(parent, index.clone());
                    }
                    Ok(index)
                },
                Err(State::Pending { task, .. }) => Err(Error::Exhausted(task)),
                Err(State::Running) => unreachable!()
            }
        }
    }

    pub fn execute(mut self) -> (Sender<T, I, S>, Execute<T, I, S>) {
        let queue = FuturesUnordered::new();
        for fut in self.pending.drain(..) {
            queue.push(fut);
        }
        let (g1, g2) = BiLock::new(self);
        let (tx, rx) = oneshot::channel();
        (
            Sender { inner: g1, tx },
            Execute { inner: g2, done: Vec::new(), is_canceled: false, queue, rx }
        )
    }

    fn walk(&mut self, index: &Index<I>) -> TaskWalker<'_, T, I, S> {
        let walker = self.dag.walk(index);
        TaskWalker { dag: &mut self.dag, walker }
    }
}

pub struct Sender<T, I=u32, S=RandomState> {
    inner: BiLock<TaskGraph<T, I, S>>,
    tx: oneshot::Sender<()>
}

impl<T, I, S> Sender<T, I, S>
where
    T: Future + Unpin,
    I: CheckedAdd + One + Hash + PartialEq + Eq + PartialOrd + Clone + Unpin,
    S: BuildHasher + Unpin
{
    #[inline]
    pub fn add_task<'a>(&'a self, deps: &'a [Index<I>], task: T)
        -> impl Future<Output = Result<Index<I>, Error<T>>> + 'a
    {
        async move {
            let mut graph = self.inner.lock().await;
            graph.add_task(deps, task)
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
    T: Future + Unpin,
    I: CheckedAdd + One + Hash + PartialEq + Eq + PartialOrd + Clone + Unpin,
    S: BuildHasher + Unpin
{
    fn enqueue(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let mut graph = match self.inner.poll_lock(cx) {
            Poll::Ready(graph) => graph,
            Poll::Pending => return Poll::Pending
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

        Poll::Ready(())
    }
}

impl<F, I, S> Stream for Execute<F, I, S>
where
    F: Future + Unpin,
    I: CheckedAdd + One + Hash + PartialEq + Eq + PartialOrd + Clone + Unpin,
    S: BuildHasher + Unpin
{
    type Item = (Index<I>, F::Output);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Pending => (),
            Poll::Ready(Ok(())) => return Poll::Ready(None),
            Poll::Ready(Err(_)) => {
                self.is_canceled = true;
            }
        }

        if let Poll::Pending = self.enqueue(cx) {
            return Poll::Pending;
        }

        match Pin::new(&mut self.queue).poll_next(cx) {
            Poll::Ready(Some((i, item))) => {
                self.done.push(i.clone());
                Poll::Ready(Some((i, item)))
            },
            Poll::Ready(None) if self.is_canceled => Poll::Ready(None),
            Poll::Ready(None) | Poll::Pending => Poll::Pending
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

impl<F, I> Future for IndexFuture<F, I>
where
    F: Future + Unpin,
    I: Clone + Unpin
{
    type Output = (Index<I>, F::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let IndexFuture { index, fut } = self.get_mut();

        match Pin::new(fut).poll(cx) {
            Poll::Ready(item) => Poll::Ready((index.clone(), item)),
            Poll::Pending => Poll::Pending,
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
    S: BuildHasher + Unpin
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
    Exhausted(T)
}

impl<T> fmt::Debug for Error<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::WouldCycle(_) => f.debug_struct("WouldCycle").finish(),
            Error::Exhausted(_) => f.debug_struct("Exhausted").finish()
        }
    }
}

impl<T> fmt::Display for Error<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::WouldCycle(_) => write!(f, "would cycle"),
            Error::Exhausted(_) => write!(f, "index exhausted")
        }
    }
}

impl<T> error::Error for Error<T> {}
