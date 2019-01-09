use std::mem;
use std::vec::IntoIter;
use indexmap::IndexMap;


pub struct Graph<N> {
    map: IndexMap<Index, (N, Vec<Index>)>,
    last: Index
}

impl<N> Default for Graph<N> {
    fn default() -> Graph<N> {
        Graph { map: IndexMap::new(), last: Index(0) }
    }
}

impl<N> Graph<N> {
    pub fn add_node(&mut self, node: N) -> Index {
        let index = self.last.next();
        self.map.insert(index, (node, Vec::new()));
        index
    }

    pub fn contains(&self, index: Index) -> bool {
        self.map.contains_key(&index)
    }

    pub fn add_edge(&mut self, parent: Index, next: Index) -> Option<()> {
        self.map.get_mut(&parent)
            .map(|(_, sum)| sum.push(next))
    }

    pub fn remove_node(&mut self, index: Index) -> Option<N> {
        self.map.remove(&index)
            .map(|(n, _)| n)
    }

    pub fn get_node_mut(&mut self, index: Index) -> Option<&mut N> {
        self.map.get_mut(&index)
            .map(|(n, _)| n)
    }

    pub fn walk(&mut self, index: Index) -> IntoIter<Index> {
        self.map.get(&index)
                .map(|(_, arr)| arr.clone().into_iter())
                .unwrap_or(Vec::new().into_iter())
    }
}

#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug)]
pub struct Index(u32);

impl Index {
    fn next(&mut self) -> Index {
        mem::replace(self, Index(self.0 + 1))
    }
}
