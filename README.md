# futures-dagtask

A task queue based on a directed acyclic graph
that allows you to specify dependencies for concurrent tasks.

# Usage

```rust
use futures::future;
use futures_dagtask::TaskGraph;

let mut graph = TaskGraph::new();
let zero = graph.add_task(&[], future::ready::<u32>(0))?;
let one = graph.add_task(&[], future::ready::<u32>(1))?;
let two = graph.add_task(&[one], future::ready::<u32>(2))?;

let (add, exec) = graph.execute();

// spawn(exec.for_each(drop));

let _three = add.add_task(&[two], future::ready(3)).await?;
```

In this example, `zero` and `one` will be executed concurrently,
but `two` will be executed after `one` is completed.

Due to the simplicity of design, we will never have circular dependencies.

![](https://upload.wikimedia.org/wikipedia/commons/c/c6/Topological_Ordering.svg)

# License

This project is licensed under [the MIT license](LICENSE).
