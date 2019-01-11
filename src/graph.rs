use std::mem;
use std::ops::Add;
use std::hash::{ Hash, BuildHasher };
use std::vec::IntoIter;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;


pub struct Graph<N, I=u32, S=RandomState> {
    map: HashMap<Index<I>, (N, Vec<Index<I>>), S>,
    last: Index<I>
}

impl<N, I, S> Default for Graph<N, I, S>
where
    I: Default + Hash + PartialEq + Eq,
    S: Default + BuildHasher
{
    fn default() -> Graph<N, I, S> {
        Graph { map: HashMap::default(), last: Index(Default::default()) }
    }
}

impl<N, I, S> Graph<N, I, S>
where
    for<'a> &'a I: Add<I>,
    for<'a> <&'a I as Add<I>>::Output: Into<I>,
    I: From<u32> + Hash + PartialEq + Eq + Clone,
    S: BuildHasher
{
    pub fn add_node(&mut self, node: N) -> Index<I> {
        let index = self.last.next();
        self.map.insert(index.clone(), (node, Vec::new()));
        index
    }

    pub fn contains(&self, index: &Index<I>) -> bool {
        self.map.contains_key(index)
    }

    pub fn add_edge(&mut self, parent: &Index<I>, next: Index<I>) -> Option<()> {
        self.map.get_mut(parent)
            .map(|(_, sum)| sum.push(next))
    }

    pub fn remove_node(&mut self, index: &Index<I>) -> Option<N> {
        self.map.remove(index)
            .map(|(n, _)| n)
    }

    pub fn get_node_mut(&mut self, index: &Index<I>) -> Option<&mut N> {
        self.map.get_mut(index)
            .map(|(n, _)| n)
    }

    pub fn walk(&mut self, index: &Index<I>) -> IntoIter<Index<I>> {
        self.map.get_mut(index)
            .map(|(_, arr)| mem::replace(arr, Vec::new()).into_iter())
            .unwrap_or_else(|| Vec::new().into_iter())
    }
}

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Debug)]
pub struct Index<I=u32>(I);

impl<I> Index<I>
where
    for<'a> &'a I: Add<I>,
    for<'a> <&'a I as Add<I>>::Output: Into<I>,
    I: From<u32>
{
    fn next(&mut self) -> Index<I> {
        mem::replace(self, Index(self.0.add(I::from(1)).into()))
    }
}
