mod db;
pub mod vertex;

use crate::vertex::{AddVertexTraversal, Vertex, VertexWithIdTraversal};
use db::PrefixSearchIterator;
use rocksdb::{DBWithThreadMode, SingleThreaded, DB};
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use vertex::VertexTraversal;

const KEY_SYS_CONTEXT: &str = "sys_context";

#[derive(Debug)]
pub struct GraphTraversalSource {
    database: DBWithThreadMode<SingleThreaded>,
    context: Cell<GraphContext>,
}

impl GraphTraversalSource {
    pub fn new<P: AsRef<Path>>(path: P) -> GraphTraversalSource {
        let database = DB::open_default(path).unwrap();
        let context = match database.get(KEY_SYS_CONTEXT).unwrap() {
            None => {
                let context = GraphContext { lsn: 0 };
                let bytes = bincode::serialize(&context).unwrap();
                database.put(KEY_SYS_CONTEXT, bytes).unwrap();
                context
            }
            Some(x) => bincode::deserialize(&x).unwrap(),
        };

        GraphTraversalSource {
            database,
            context: Cell::new(context),
        }
    }

    /// Spawns a traversal by adding a vertex with the default label.
    pub fn add_vertex(&self) -> AddVertexTraversal {
        self.add_vertex_with_label(vertex::DEFAULT_LABEL)
    }

    /// Spawns a traversal by adding a vertex with the specified label.
    pub fn add_vertex_with_label<S: ToString>(&self, label: S) -> AddVertexTraversal {
        let id = self.new_id();

        let vertex = Vertex::new(id, label);
        let mut vertices = HashMap::new();
        vertices.insert(id, DirtyEntry::new(vertex));

        AddVertexTraversal {
            id: Some(id),
            context: Rc::new(TraversalContext {
                database: &self.database,
                vertices,
            }),
        }
    }

    /// Spawns a traversal over all vertices.
    pub fn vertices(&self) -> VertexTraversal {
        let prefix_search = PrefixSearchIterator {
            prefix_iterator: self.database.prefix_iterator(vertex::KEY_PREFIX),
            prefix: vertex::KEY_PREFIX.as_bytes(),
        };
        VertexTraversal {
            prefix_search,
            label: None,
            _context: Rc::new(TraversalContext {
                database: &self.database,
                vertices: HashMap::new(),
            }),
        }
    }

    /// Spawns a traversal over the vertices with the specified label.
    pub fn vertices_with_label<'a>(&'a self, label: &'a str) -> VertexTraversal<'a> {
        let prefix_search = PrefixSearchIterator {
            prefix_iterator: self.database.prefix_iterator(vertex::KEY_PREFIX),
            prefix: vertex::KEY_PREFIX.as_bytes(),
        };
        VertexTraversal {
            prefix_search,
            label: Some(label),
            _context: Rc::new(TraversalContext {
                database: &self.database,
                vertices: HashMap::new(),
            }),
        }
    }

    /// Spawns a traversal starting with the vertex with the specified id.
    pub fn vertex_with_id(&self, id: usize) -> VertexWithIdTraversal {
        VertexWithIdTraversal {
            database: &self.database,
            id: Some(id),
            _context: Rc::new(TraversalContext {
                database: &self.database,
                vertices: HashMap::new(),
            }),
        }
    }

    /// Saves the context to the database.
    fn save_context(&self) {
        let bytes = bincode::serialize(&self.context).unwrap();
        self.database.put(KEY_SYS_CONTEXT, bytes).unwrap();
    }

    /// Generate a new id.
    fn new_id(&self) -> usize {
        let mut context = self.context.get();
        context.lsn += 1;
        self.context.set(context);
        self.save_context();
        context.lsn
    }
}

#[derive(Debug)]
pub(crate) struct DirtyEntry<T> {
    pub(crate) dirty: bool,
    pub(crate) entry: T,
}

impl<T> DirtyEntry<T> {
    pub(crate) fn new(entry: T) -> DirtyEntry<T> {
        DirtyEntry { dirty: true, entry }
    }
}

struct TraversalContext<'a> {
    database: &'a DBWithThreadMode<SingleThreaded>,
    vertices: HashMap<usize, DirtyEntry<Vertex>>,
}

impl<'a> Drop for TraversalContext<'a> {
    fn drop(&mut self) {
        eprintln!("dropping traversal context");
        dbg!(&self.vertices);
        for (id, vertex) in &self.vertices {
            if vertex.dirty {
                let key = create_vertex_key(*id);
                let value = bincode::serialize(&vertex.entry).unwrap();
                self.database.put(key, value).unwrap();
            }
        }
    }
}

fn create_vertex_key(id: usize) -> String {
    format!("{}{}", vertex::KEY_PREFIX, id)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct GraphContext {
    /// Last sequence number
    lsn: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use uuid::Uuid;

    #[derive(Debug)]
    struct TestContext {
        filepath: PathBuf,
    }

    impl TestContext {
        fn generate() -> TestContext {
            let file = Uuid::new_v4();
            let mut filepath = std::env::temp_dir();
            filepath.push(file.to_string());

            TestContext { filepath }
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.filepath).unwrap();
        }
    }

    #[test]
    fn create_graph() {
        let config = TestContext::generate();
        let _ = GraphTraversalSource::new(&config.filepath);
    }

    #[test]
    fn add_vertices() {
        let config = TestContext::generate();
        let graph = GraphTraversalSource::new(&config.filepath);

        let v1 = graph.add_vertex().next().unwrap();
        let v2 = graph.add_vertex().next().unwrap();

        let mut expected = HashMap::new();
        expected.insert(v1.id(), v1);
        expected.insert(v2.id(), v2);

        let actual: HashMap<_, _> = graph.vertices().map(|v| (v.id(), v)).collect();

        assert_eq!(actual, expected);
    }

    #[test]
    fn add_vertices_with_label() {
        let config = TestContext::generate();
        let graph = GraphTraversalSource::new(&config.filepath);

        let v1 = graph.add_vertex_with_label("v1").next().unwrap();
        let v2 = graph.add_vertex_with_label("v2").next().unwrap();

        let mut expected = HashMap::new();
        expected.insert(v1.id(), v1);
        expected.insert(v2.id(), v2);

        let actual: HashMap<_, _> = graph.vertices().map(|v| (v.id(), v)).collect();

        assert_eq!(actual, expected);
    }

    #[test]
    fn get_vertices_with_label() {
        let config = TestContext::generate();
        let graph = GraphTraversalSource::new(&config.filepath);

        let _v1 = graph.add_vertex().next().unwrap();
        let v2 = graph.add_vertex_with_label("custom").next().unwrap();
        let _v3 = graph.add_vertex().next().unwrap();
        let v4 = graph.add_vertex_with_label("custom").next().unwrap();

        let mut expected = HashMap::new();
        expected.insert(v2.id(), v2);
        expected.insert(v4.id(), v4);

        let actual: HashMap<_, _> = graph
            .vertices_with_label("custom")
            .map(|v| (v.id(), v))
            .collect();

        assert_eq!(actual, expected);
    }

    #[test]
    fn get_vertex_with_id() {
        let config = TestContext::generate();
        let graph = GraphTraversalSource::new(&config.filepath);

        let v1 = graph.add_vertex().next().unwrap();
        let v2 = graph.add_vertex_with_label("custom").next().unwrap();

        let actual1: Vec<_> = graph.vertex_with_id(v1.id()).collect();
        assert_eq!(actual1, vec![v1]);

        let actual2: Vec<_> = graph.vertex_with_id(v2.id()).collect();
        assert_eq!(actual2, vec![v2]);
    }
}
