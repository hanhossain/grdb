use rocksdb::{DBAccess, DBIteratorWithThreadMode};

pub(crate) struct PrefixSearchIterator<'a, D: DBAccess> {
    pub(crate) prefix_iterator: DBIteratorWithThreadMode<'a, D>,
    pub(crate) prefix: &'a [u8],
}

impl<'a, D: DBAccess> Iterator for PrefixSearchIterator<'a, D> {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.prefix_iterator.next()?.unwrap();

        // ensure we're still in the right prefix
        if !key.starts_with(self.prefix) {
            return None;
        }

        Some((key, value))
    }
}
