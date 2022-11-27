mod db;
pub mod vertex;

use crate::vertex::Vertex;
use db::PrefixSearchIterator;
use rocksdb::{DBWithThreadMode, SingleThreaded, DB};
use serde::{Deserialize, Serialize};
use std::path::Path;
use vertex::VertexTraversal;

const KEY_SYS_CONTEXT: &str = "sys_context";

#[derive(Debug)]
pub struct GraphTraversalSource {
    database: DBWithThreadMode<SingleThreaded>,
    context: GraphContext,
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

        GraphTraversalSource { database, context }
    }

    /// Spawns a traversal by adding a vertex with the default label.
    /// TODO: This needs work to follow the usual traversal pattern.
    pub fn add_vertex(&mut self) -> Vertex {
        self.add_vertex_with_label(vertex::DEFAULT_LABEL)
    }

    /// Spawns a traversal by adding a vertex with the specified label.
    /// TODO: This needs work to follow the usual traversal pattern.
    pub fn add_vertex_with_label<S: ToString>(&mut self, label: S) -> Vertex {
        self.context.lsn += 1;
        self.save_context();

        let vertex = Vertex::new(self.context.lsn, label);
        let bytes = bincode::serialize(&vertex).unwrap();
        let key = create_vertex_key(vertex.id());

        self.database.put(key, bytes).unwrap();

        vertex
    }

    /// Spawns a `VertexTraversal` over all vertices.
    pub fn vertices(&self) -> VertexTraversal {
        let prefix = b"vtx_";
        let prefix_search = PrefixSearchIterator {
            prefix_iterator: self.database.prefix_iterator(prefix),
            prefix,
        };
        VertexTraversal(prefix_search)
    }

    /// Saves the context to the database.
    fn save_context(&mut self) {
        let bytes = bincode::serialize(&self.context).unwrap();
        self.database.put(KEY_SYS_CONTEXT, bytes).unwrap();
    }
}

fn create_vertex_key(id: usize) -> String {
    format!("vtx_{}", id)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        let mut graph = GraphTraversalSource::new(&config.filepath);

        let v1 = graph.add_vertex();
        let v2 = graph.add_vertex();

        let mut expected = HashMap::new();
        expected.insert(v1.id(), v1);
        expected.insert(v2.id(), v2);

        let actual: HashMap<_, _> = graph.vertices().map(|v| (v.id(), v)).collect();

        assert_eq!(actual, expected);
    }

    #[test]
    fn add_vertices_with_label() {
        let config = TestContext::generate();
        let mut graph = GraphTraversalSource::new(&config.filepath);

        let v1 = graph.add_vertex_with_label("v1");
        let v2 = graph.add_vertex_with_label("v2");

        let mut expected = HashMap::new();
        expected.insert(v1.id(), v1);
        expected.insert(v2.id(), v2);

        let actual: HashMap<_, _> = graph.vertices().map(|v| (v.id(), v)).collect();

        assert_eq!(actual, expected);
    }
}
