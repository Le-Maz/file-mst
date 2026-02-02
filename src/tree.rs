use blake3::Hash;

use crate::node::{Link, Node};
use crate::store::Store;
use crate::{MerkleKey, MerkleValue, NodeId};
use std::borrow::Borrow;
use std::io;
use std::path::Path;
use std::sync::Arc;

pub struct MerkleSearchTree<K: MerkleKey, V: MerkleValue> {
    pub(crate) root: Link<K, V>,
    pub(crate) store: Arc<Store<K, V>>,
}

impl<K: MerkleKey, V: MerkleValue> MerkleSearchTree<K, V> {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let store = Store::open(path)?;
        if let Some((offset, hash)) = store.read_metadata()? {
            Ok(Self {
                root: Link::Disk { offset, hash },
                store,
            })
        } else {
            Ok(Self {
                root: Link::Loaded(Arc::new(Node::empty(0))),
                store,
            })
        }
    }

    pub fn commit(&mut self) -> io::Result<(u64, Hash)> {
        let (offset, hash) = self.flush()?;
        self.store.write_metadata(offset, hash)?;
        self.store.flush()?;
        Ok((offset, hash))
    }

    /// Creates a new MST backed by a temporary file.
    pub fn new_temporary() -> io::Result<Self> {
        let file = tempfile::tempfile()?;
        let store = Store::new(file);

        Ok(Self {
            root: Link::Loaded(Arc::new(Node::empty(0))),
            store,
        })
    }

    /// Inserts a key-value pair into the tree, modifying it in-place.
    pub fn insert(&mut self, key: K, value: V) -> io::Result<()> {
        let key_arc = Arc::new(key);
        let val_arc = Arc::new(value);

        let root_node = self.resolve_link(&self.root)?;

        let target_level = Node::<K, V>::calc_level(key_arc.as_ref());
        let new_root_node = root_node.put(key_arc, val_arc, target_level, &self.store)?;

        self.root = Link::Loaded(new_root_node);
        Ok(())
    }

    /// Checks if a key exists in the tree.
    pub fn contains<Q>(&self, key: &Q) -> io::Result<bool>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let root = self.resolve_link(&self.root)?;
        root.contains(key, &self.store)
    }

    /// Retrieves a value by key. Returns None if the key does not exist.
    pub fn get<Q>(&self, key: &Q) -> io::Result<Option<Arc<V>>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let root = self.resolve_link(&self.root)?;
        root.get(key, &self.store)
    }

    /// Removes a key from the tree.
    pub fn remove<Q>(&mut self, key: &Q) -> io::Result<()>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let root = self.resolve_link(&self.root)?;

        let (new_root, deleted) = root.delete(key, &self.store)?;

        if !deleted {
            return Ok(());
        }

        if new_root.keys.is_empty() && !new_root.children.is_empty() {
            self.root = new_root.children[0].clone();
        } else {
            self.root = Link::Loaded(new_root);
        }

        Ok(())
    }

    fn flush(&mut self) -> io::Result<(u64, Hash)> {
        let (offset, hash) = self.flush_recursive(&self.root)?;
        self.store.flush()?;
        self.root = Link::Disk { offset, hash };
        Ok((offset, hash))
    }

    pub fn root_hash(&self) -> Hash {
        self.root.hash()
    }

    fn resolve_link(&self, link: &Link<K, V>) -> io::Result<Arc<Node<K, V>>> {
        match link {
            Link::Loaded(node) => Ok(node.clone()),
            Link::Disk { offset, .. } => self.store.load_node(*offset),
        }
    }

    fn flush_recursive(&self, link: &Link<K, V>) -> io::Result<(NodeId, Hash)> {
        match link {
            Link::Disk { offset, hash } => Ok((*offset, *hash)),
            Link::Loaded(node) => {
                let mut dirty_children = false;
                for child in &node.children {
                    if let Link::Loaded(_) = child {
                        dirty_children = true;
                        break;
                    }
                }

                if !dirty_children {
                    let offset = self.store.write_node(node)?;
                    return Ok((offset, node.hash));
                }

                let mut new_children = Vec::new();
                for child in &node.children {
                    let (child_offset, child_hash) = self.flush_recursive(child)?;
                    new_children.push(Link::Disk {
                        offset: child_offset,
                        hash: child_hash,
                    });
                }

                let mut new_node = (**node).clone();
                new_node.children = new_children;
                let offset = self.store.write_node(&new_node)?;
                Ok((offset, new_node.hash))
            }
        }
    }
}
