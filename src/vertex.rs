use crate::db::PrefixSearchIterator;
use crate::{create_vertex_key, TraversalContext};
use rocksdb::{DBWithThreadMode, SingleThreaded};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;

pub const DEFAULT_LABEL: &str = "vertex";
pub const KEY_PREFIX: &str = "vtx_";

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct Vertex {
    id: usize,
    pub label: String,
    pub prop: HashMap<String, String>,
}

impl Vertex {
    pub(crate) fn new<S: ToString>(id: usize, label: S) -> Vertex {
        Vertex {
            id,
            label: label.to_string(),
            prop: HashMap::new(),
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

pub struct VertexTraversal<'a> {
    pub(crate) prefix_search: PrefixSearchIterator<'a, DBWithThreadMode<SingleThreaded>>,
    pub(crate) label: Option<&'a str>,
    pub(crate) _context: TraversalContext<'a>,
}

impl<'a> Iterator for VertexTraversal<'a> {
    type Item = Vertex;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(label) = self.label {
            while let Some((_, value)) = self.prefix_search.next() {
                let value: Vertex = bincode::deserialize(&value).unwrap();
                if value.label == label {
                    return Some(value);
                }
            }
            None
        } else {
            let (_, value) = self.prefix_search.next()?;
            Some(bincode::deserialize(&value).unwrap())
        }
    }
}

pub struct VertexWithIdTraversal<'a> {
    pub(crate) database: &'a DBWithThreadMode<SingleThreaded>,
    pub(crate) id: Option<usize>,
    pub(crate) _context: TraversalContext<'a>,
}

impl<'a> Iterator for VertexWithIdTraversal<'a> {
    type Item = Vertex;

    fn next(&mut self) -> Option<Self::Item> {
        let id = self.id.take()?;
        let key = create_vertex_key(id);
        self.database
            .get(key)
            .unwrap()
            .map(|v| bincode::deserialize(&v).unwrap())
    }
}

pub struct AddVertexTraversal<'a> {
    pub(crate) id: Option<usize>,
    pub(crate) context: TraversalContext<'a>,
}

impl<'a> Iterator for AddVertexTraversal<'a> {
    type Item = Vertex;

    fn next(&mut self) -> Option<Self::Item> {
        let id = self.id.take()?;
        self.context.vertices.get(&id).map(|v| v.entry.clone())
    }
}
