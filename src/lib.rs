use rocksdb::{DBWithThreadMode, SingleThreaded, DB};
use std::path::Path;

#[derive(Debug)]
pub struct GraphTraversalSource {
    _database: DBWithThreadMode<SingleThreaded>,
}

impl GraphTraversalSource {
    pub fn new<P: AsRef<Path>>(path: P) -> GraphTraversalSource {
        let database = DB::open_default(path).unwrap();
        GraphTraversalSource {
            _database: database,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
