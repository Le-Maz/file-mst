use blake3::Hash;

use crate::node::{Link, Node};
use crate::store::Store;
use crate::{MerkleKey, MerkleValue, NodeId};
use std::borrow::Borrow;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::sync::Arc;

pub struct MerkleSearchTree<K: MerkleKey, V: MerkleValue> {
    pub(crate) root: Link<K, V>,
    pub(crate) store: Arc<Store<K, V>>,
    last_committed: Option<(u64, Hash)>,
}

impl<K: MerkleKey, V: MerkleValue> MerkleSearchTree<K, V> {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let store = Store::open(path)?;
        if let Some((offset, hash)) = store.read_metadata()? {
            Ok(Self {
                root: Link::Disk { offset, hash },
                store,
                last_committed: Some((offset, hash)),
            })
        } else {
            Ok(Self {
                root: Link::Loaded(Arc::new(Node::empty(0))),
                store,
                last_committed: None,
            })
        }
    }

    pub fn commit(&mut self) -> io::Result<(u64, Hash)> {
        // 1. Flush the nodes (recursive)
        // If no changes, this returns the existing Disk offset/hash instantly.
        let (offset, hash) = self.flush_recursive(&self.root)?;

        // 2. Did anything actually change?
        if let Some((last_off, last_hash)) = self.last_committed
            && last_off == offset
            && last_hash == hash
        {
            // Nothing changed. Return early.
            return Ok((offset, hash));
        }

        // 3. Write metadata and sync
        self.store.write_metadata(offset, hash)?;
        self.store.flush()?;
        self.root = Link::Disk { offset, hash };

        // 4. Update tracker
        self.last_committed = Some((offset, hash));

        Ok((offset, hash))
    }

    /// Creates a new MST backed by a temporary file.
    pub fn new_temporary() -> io::Result<Self> {
        let file = tempfile::tempfile()?;
        let store = Store::new(file);

        Ok(Self {
            root: Link::Loaded(Arc::new(Node::empty(0))),
            store,
            last_committed: None,
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

    /// Compacts the database by copying all reachable nodes to a new file,
    /// eliminating obsolete data and reducing file size.
    ///
    /// This operation effectively "defragments" the storage.
    pub fn compact<P: AsRef<Path>>(&mut self, new_path: P) -> io::Result<()> {
        // 1. Prepare the new file (Truncate ensures it starts empty)
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&new_path)?;

        // Ensure minimum file size for metadata (matching Store::open logic)
        if file.metadata()?.len() == 0 {
            file.set_len(crate::PAGE_SIZE)?;
        }

        let new_store = Store::new(file);

        // 2. Recursively copy the tree from the old store to the new store.
        // This returns the offset of the root in the NEW file.
        let (new_root_offset, new_root_hash) = self.copy_recursive(&self.root, &new_store)?;

        // 3. Write the metadata (Root pointer) to the new store
        new_store.write_metadata(new_root_offset, new_root_hash)?;
        new_store.flush()?;

        // 4. Atomically swap the store in memory
        self.store = new_store;

        // Update the root link to point to the new disk location
        self.root = Link::Disk {
            offset: new_root_offset,
            hash: new_root_hash,
        };

        Ok(())
    }

    /// Helper: Recursively loads a node from the old store and writes it to the new store.
    /// Returns the (Offset, Hash) in the new store.
    fn copy_recursive(
        &self,
        link: &Link<K, V>,
        new_store: &Arc<Store<K, V>>,
    ) -> io::Result<(NodeId, Hash)> {
        // Step A: Resolve the node.
        // If it's on disk, load it from `self.store` (the old store).
        // If it's loaded, use it directly.
        let node = match link {
            Link::Loaded(n) => n.clone(),
            Link::Disk { offset, .. } => self.store.load_node(*offset)?,
        };

        // Step B: Recursively process all children first (Bottom-Up).
        // We need to write children first so we know their NEW offsets to put in the parent.
        let mut new_children_links = Vec::with_capacity(node.children.len());

        for child_link in &node.children {
            let (child_new_offset, child_hash) = self.copy_recursive(child_link, new_store)?;

            // The parent must refer to the child by its NEW disk location.
            new_children_links.push(Link::Disk {
                offset: child_new_offset,
                hash: child_hash,
            });
        }

        // Step C: Construct the new node version.
        // We assume the node content (keys/values) is the same, so the Hash is unchanged.
        // However, we MUST replace the `children` list with the `Link::Disk` variants pointing to the new file.
        let mut new_node = (*node).clone();
        new_node.children = new_children_links;

        // Step D: Write the node to the new store.
        // Since `new_node` now contains only Link::Disk children, `as_disk_ref` inside `write_node` will succeed.
        let new_offset = new_store.write_node(&new_node)?;

        Ok((new_offset, new_node.hash))
    }
}
