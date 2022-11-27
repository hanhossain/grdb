use crate::db::PrefixSearchIterator;
use rocksdb::{DBWithThreadMode, SingleThreaded};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub const DEFAULT_LABEL: &str = "vertex";

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct Vertex {
    id: usize,
    pub label: String,
}

impl Vertex {
    pub(crate) fn new<S: ToString>(id: usize, label: S) -> Vertex {
        Vertex {
            id,
            label: label.to_string(),
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

pub struct VertexTraversal<'a>(
    pub(crate) PrefixSearchIterator<'a, DBWithThreadMode<SingleThreaded>>,
);

impl<'a> Iterator for VertexTraversal<'a> {
    type Item = Vertex;

    fn next(&mut self) -> Option<Self::Item> {
        let (_, value) = self.0.next()?;
        Some(bincode::deserialize(&value).unwrap())
    }
}
