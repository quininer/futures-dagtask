use std::mem;
use std::collections::HashMap;
use petgraph::{stable_graph::WalkNeighbors, graph::NodeIndex, stable_graph::StableGraph, Direction};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::sync::{oneshot, BiLock};
use futures::prelude::*;


pub struct TaskGraph<T> {
    dag: StableGraph<(State<T>, TaskIndex), ()>,
    root: NodeIndex,
    last: TaskIndex,
    map: HashMap<TaskIndex, NodeIndex>
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
        let mut dag = StableGraph::new();
        let root = dag.add_node((State::Running, TaskIndex(0)));
        TaskGraph { dag, root, last: TaskIndex(0), map: HashMap::new() }
    }
}

impl<T: Future> TaskGraph<T> {
    pub fn add_task(&mut self, deps: &[TaskIndex], task: T) -> TaskIndex {
        let (task_index, _) = self.add_task_with_count(deps.len() | 1, deps, task);
        task_index
    }

    fn add_task_with_count(&mut self, count: usize, deps: &[TaskIndex], task: T) -> (TaskIndex, NodeIndex) {
        let task_index = self.last.next();
        let graph_index = self.dag.add_node((State::Pending { count, task }, task_index));
        self.map.insert(task_index, graph_index);
        if deps.is_empty() {
            self.dag.update_edge(self.root, graph_index, ());
        } else {
            for parent in deps {
                if let Some(&parent) = self.map.get(parent) {
                    self.dag.update_edge(parent, graph_index, ());
                }
            }
        }
        (task_index, graph_index)
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

    fn walk(&mut self, index: NodeIndex) -> TaskWalker<'_, T> {
        let walker = self.dag.neighbors_directed(index, Direction::Outgoing).detach();
        TaskWalker { dag: &mut self.dag, walker }
    }
}

pub struct AddTask<T> {
    inner: BiLock<(TaskGraph<T>, Vec<NodeIndex>)>,
    tx: oneshot::Sender<()>
}

impl<T: Future> AddTask<T> {
    pub fn add_task(&self, deps: &[TaskIndex], task: T) -> Async<TaskIndex> {
        match self.inner.poll_lock() {
            Async::Ready(mut inner) => {
                let (graph, pending) = &mut *inner;
                let count = deps.iter()
                    .filter(|&i| graph.map.contains_key(i))
                    .count();
                let (task_index, graph_index) = graph.add_task_with_count(count, deps, task);
                if count == 0 {
                    pending.push(graph_index);
                }
                Async::Ready(task_index)
            },
            Async::NotReady => Async::NotReady
        }
    }

    pub fn abort(self) {
        let _ = self.tx.send(());
    }
}

pub struct Execute<T: Future> {
    inner: BiLock<(TaskGraph<T>, Vec<NodeIndex>)>,
    queue: FuturesUnordered<IndexFuture<T>>,
    done: Vec<TaskIndex>,
    rx: oneshot::Receiver<()>
}

impl<T: Future> Execute<T> {
    fn enqueue(&mut self) -> Async<()> {
        let mut inner = match self.inner.poll_lock() {
            Async::Ready(inner) => inner,
            Async::NotReady => return Async::NotReady
        };
        let (graph, pending) = &mut *inner;

        while let Some(graph_index) = pending.pop() {
            if let Some(&mut (ref mut task_state, task_index)) = graph.dag.node_weight_mut(graph_index) {
                if let State::Pending { task, .. } = mem::replace(task_state, State::Running) {
                    self.queue.push(IndexFuture::new(task_index, task));
                }
            }
        }

        while let Some(task_index) = self.done.pop() {
            if let Some(graph_index) = graph.map.remove(&task_index) {
                for fut in graph.walk(graph_index) {
                    self.queue.push(fut);
                }
                graph.dag.remove_node(graph_index);
            }
        }

        Async::Ready(())
    }
}

impl<T: Future> Stream for Execute<T> {
    type Item = (TaskIndex, T::Item);
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
    index: TaskIndex,
    fut: F
}

impl<F: Future> IndexFuture<F> {
    pub fn new(index: TaskIndex, fut: F) -> IndexFuture<F> {
        IndexFuture { index, fut }
    }
}

impl<F: Future> Future for IndexFuture<F> {
    type Item = (TaskIndex, F::Item);
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
    dag: &'a mut StableGraph<(State<T>, TaskIndex), ()>,
    walker: WalkNeighbors<u32>
}

impl<'a, T: Future> Iterator for TaskWalker<'a, T> {
    type Item = IndexFuture<T>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(graph_index) = self.walker.next_node(&self.dag) {
            let &mut (ref mut task_state, task_index) = match self.dag.node_weight_mut(graph_index) {
                Some(node) => node,
                None => continue
            };

            if let State::Pending { count, .. } = task_state {
                *count -= 1;
            }

            match task_state {
                State::Pending { count, .. } if *count == 0 => (),
                _ => continue
            }

            if let State::Pending { task, .. } = mem::replace(task_state, State::Running) {
                return Some(IndexFuture::new(task_index, task));
            }
        }

        None
    }
}

#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug)]
pub struct TaskIndex(u32);

impl TaskIndex {
    fn next(&mut self) -> TaskIndex {
        self.0 += 1;
        TaskIndex(self.0)
    }
}
