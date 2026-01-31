#![doc = include_str!("../README.md")]
#![cfg_attr(test, feature(test))]

#[cfg(test)]
extern crate test;

#[cfg(test)]
mod benches;
#[cfg(test)]
mod tests;

use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

const PAGE_SIZE: u64 = 4096;

/// A trait for types that can serve as keys in a Merkle Search Tree.
pub trait MerkleKey: Ord + Clone + std::fmt::Debug + Serialize + for<'a> Deserialize<'a> {
    fn encode(&self) -> Cow<'_, [u8]>;
}

/// A trait for types that can serve as values. They must be cloneable and serializable.
pub trait MerkleValue: Clone + std::fmt::Debug + Serialize + for<'a> Deserialize<'a> {}
impl<T> MerkleValue for T where T: Clone + std::fmt::Debug + Serialize + for<'a> Deserialize<'a> {}

impl MerkleKey for String {
    #[inline]
    fn encode(&self) -> Cow<'_, [u8]> {
        self.as_bytes().into()
    }
}

impl MerkleKey for Vec<u8> {
    #[inline]
    fn encode(&self) -> Cow<'_, [u8]> {
        self.as_slice().into()
    }
}

pub type Hash = [u8; 32];
type NodeId = u64;

pub struct MerkleSearchTree<K: MerkleKey, V: MerkleValue> {
    root: Link<K, V>,
    store: Arc<Store<K, V>>,
}

impl<K: MerkleKey, V: MerkleValue> MerkleSearchTree<K, V> {
    /// Opens or creates a file-backed Merkle Search Tree at the given path.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let store = Store::open(path)?;
        Ok(Self {
            root: Link::Loaded(Arc::new(Node::empty(0))),
            store,
        })
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

    /// Loads a tree from a known root offset and hash.
    pub fn load_from_root<P: AsRef<Path>>(
        path: P,
        root_offset: u64,
        root_hash: Hash,
    ) -> io::Result<Self> {
        let store = Store::open(path)?;
        Ok(Self {
            root: Link::Disk {
                offset: root_offset,
                hash: root_hash,
            },
            store,
        })
    }

    /// Inserts a key-value pair into the tree, modifying it in-place.
    pub fn insert(&mut self, key: K, value: V) -> io::Result<()> {
        let key_arc = Arc::new(key);
        let root_node = self.resolve_link(&self.root)?;

        let target_level = Node::<K, V>::calc_level(key_arc.as_ref());
        let new_root_node = root_node.put(key_arc, value, target_level, &self.store)?;

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
    pub fn get<Q>(&self, key: &Q) -> io::Result<Option<V>>
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

    /// Persists any dirty nodes to disk and updates the root pointer.
    pub fn flush(&mut self) -> io::Result<(u64, Hash)> {
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

#[derive(Debug, Clone)]
enum Link<K: MerkleKey, V: MerkleValue> {
    Disk { offset: NodeId, hash: Hash },
    Loaded(Arc<Node<K, V>>),
}

impl<K: MerkleKey, V: MerkleValue> Link<K, V> {
    fn hash(&self) -> Hash {
        match self {
            Link::Disk { hash, .. } => *hash,
            Link::Loaded(node) => node.hash,
        }
    }
}

struct Store<K: MerkleKey, V: MerkleValue> {
    file: RwLock<BufWriter<File>>,
    cache: RwLock<HashMap<NodeId, Arc<Node<K, V>>>>,
}

impl<K: MerkleKey, V: MerkleValue> Store<K, V> {
    fn new(file: File) -> Arc<Self> {
        Arc::new(Self {
            file: RwLock::new(BufWriter::with_capacity(64 * 1024, file)),
            cache: RwLock::new(HashMap::new()),
        })
    }

    fn open<P: AsRef<Path>>(path: P) -> io::Result<Arc<Self>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        Ok(Self::new(file))
    }

    fn flush(&self) -> io::Result<()> {
        let mut writer = self.file.write().unwrap();
        writer.flush()
    }

    fn load_node(&self, offset: NodeId) -> io::Result<Arc<Node<K, V>>> {
        {
            let cache = self.cache.read().unwrap();
            if let Some(node) = cache.get(&offset) {
                return Ok(node.clone());
            }
        }

        let mut writer_guard = self.file.write().unwrap();
        writer_guard.seek(SeekFrom::Start(offset))?;
        let file = writer_guard.get_mut();

        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)?;

        let disk_node: DiskNode<K, V> = postcard::from_bytes(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        let node = Arc::new(Node::from_disk(disk_node));
        self.cache.write().unwrap().insert(offset, node.clone());
        Ok(node)
    }

    fn write_node(&self, node: &Node<K, V>) -> io::Result<NodeId> {
        let disk_node = node.to_disk();
        let data = postcard::to_extend(&disk_node, Vec::with_capacity(4096))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        let node_total_len = (data.len() + 4) as u64;
        let mut writer = self.file.write().unwrap();
        let mut current_pos = writer.seek(SeekFrom::End(0))?;

        if node_total_len <= PAGE_SIZE {
            let offset_in_page = current_pos % PAGE_SIZE;
            let space_remaining = PAGE_SIZE - offset_in_page;

            if node_total_len > space_remaining {
                let padding_len = space_remaining as usize;
                let padding = vec![0u8; padding_len];
                writer.write_all(&padding)?;
                current_pos += space_remaining;
            }
        }

        let start_offset = current_pos;
        writer.write_all(&(data.len() as u32).to_le_bytes())?;
        writer.write_all(&data)?;

        Ok(start_offset)
    }
}

#[derive(Serialize, Deserialize)]
struct DiskNode<K, V> {
    level: u32,
    keys: Vec<K>,
    values: Vec<V>,
    children: Vec<(NodeId, Hash)>,
    hash: Hash,
}

#[derive(Debug, Clone)]
struct Node<K: MerkleKey, V: MerkleValue> {
    level: u32,
    keys: Vec<Arc<K>>,
    values: Vec<V>,
    children: Vec<Link<K, V>>,
    hash: Hash,
}

impl<K: MerkleKey, V: MerkleValue> Node<K, V> {
    fn empty(level: u32) -> Self {
        let mut node = Self {
            level,
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            hash: [0u8; 32],
        };
        node.rehash();
        node
    }

    fn to_disk(&self) -> DiskNode<K, V> {
        let children_meta = self
            .children
            .iter()
            .map(|c| match c {
                Link::Disk { offset, hash } => (*offset, *hash),
                Link::Loaded(_) => {
                    panic!("Cannot serialize a node with dirty children! Flush children first.")
                }
            })
            .collect();

        DiskNode {
            level: self.level,
            keys: self.keys.iter().map(|k| k.as_ref().clone()).collect(),
            values: self.values.clone(),
            children: children_meta,
            hash: self.hash,
        }
    }

    fn from_disk(disk: DiskNode<K, V>) -> Self {
        let children = disk
            .children
            .into_iter()
            .map(|(offset, hash)| Link::Disk { offset, hash })
            .collect();

        let keys = disk.keys.into_iter().map(Arc::new).collect();

        Self {
            level: disk.level,
            keys,
            values: disk.values,
            children,
            hash: disk.hash,
        }
    }

    fn calc_level(key: &K) -> u32 {
        let mut h = blake3::Hasher::new();
        h.update(&key.encode());
        let hash = h.finalize();
        let bytes = hash.as_bytes();
        let mut level = 0;
        for byte in bytes {
            if *byte == 0 {
                level += 2;
            } else {
                if *byte & 0xF0 == 0 {
                    level += 1;
                }
                break;
            }
        }
        level
    }

    fn rehash(&mut self) {
        if self.keys.is_empty() && self.children.is_empty() {
            self.hash = [0u8; 32];
            return;
        }

        let mut h = blake3::Hasher::new();
        h.update(&self.level.to_le_bytes());
        h.update(&(self.keys.len() as u64).to_le_bytes());

        for (i, child) in self.children.iter().enumerate() {
            h.update(&child.hash());
            if i < self.keys.len() {
                // Hash Key
                let k_bytes = self.keys[i].encode();
                h.update(&(k_bytes.len() as u64).to_le_bytes());
                h.update(&k_bytes);

                // Hash Value
                let v_bytes = postcard::to_extend(&self.values[i], Vec::with_capacity(4096))
                    .expect("Failed to serialize value for hashing");
                h.update(&(v_bytes.len() as u64).to_le_bytes());
                h.update(&v_bytes);
            }
        }
        self.hash = *h.finalize().as_bytes();
    }

    fn contains<Q>(&self, key: &Q, store: &Store<K, V>) -> io::Result<bool>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match self
            .keys
            .binary_search_by(|probe| probe.as_ref().borrow().cmp(key))
        {
            Ok(_) => Ok(true),
            Err(idx) => {
                if self.children.is_empty() {
                    return Ok(false);
                }
                let child = match &self.children[idx] {
                    Link::Loaded(n) => n.clone(),
                    Link::Disk { offset, .. } => store.load_node(*offset)?,
                };
                child.contains(key, store)
            }
        }
    }

    fn get<Q>(&self, key: &Q, store: &Store<K, V>) -> io::Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match self
            .keys
            .binary_search_by(|probe| probe.as_ref().borrow().cmp(key))
        {
            Ok(idx) => Ok(Some(self.values[idx].clone())),
            Err(idx) => {
                if self.children.is_empty() {
                    return Ok(None);
                }
                let child = match &self.children[idx] {
                    Link::Loaded(n) => n.clone(),
                    Link::Disk { offset, .. } => store.load_node(*offset)?,
                };
                child.get(key, store)
            }
        }
    }

    fn put(
        &self,
        key: Arc<K>,
        value: V,
        key_level: u32,
        store: &Arc<Store<K, V>>,
    ) -> io::Result<Arc<Node<K, V>>> {
        if key_level > self.level {
            let [left_child, right_child] = self.split(&key, store)?;
            let mut new_node = Node {
                level: key_level,
                keys: vec![key],
                values: vec![value],
                children: vec![Link::Loaded(left_child), Link::Loaded(right_child)],
                hash: [0u8; 32],
            };
            new_node.rehash();
            return Ok(Arc::new(new_node));
        }

        if key_level == self.level {
            let mut new_node = self.clone();
            match new_node
                .keys
                .binary_search_by(|probe| probe.as_ref().cmp(&key))
            {
                Ok(idx) => {
                    // Update existing value
                    new_node.values[idx] = value;
                    new_node.rehash();
                    return Ok(Arc::new(new_node));
                }
                Err(idx) => {
                    let child_to_split = if !new_node.children.is_empty() {
                        match &new_node.children[idx] {
                            Link::Loaded(n) => n.clone(),
                            Link::Disk { offset, .. } => store.load_node(*offset)?,
                        }
                    } else {
                        Arc::new(Node::empty(self.level.saturating_sub(1)))
                    };

                    let [left_sub, right_sub] = child_to_split.split(&key, store)?;
                    new_node.keys.insert(idx, key);
                    new_node.values.insert(idx, value);

                    if new_node.children.is_empty() {
                        new_node.children.push(Link::Loaded(left_sub));
                        new_node.children.push(Link::Loaded(right_sub));
                    } else {
                        new_node.children[idx] = Link::Loaded(left_sub);
                        new_node.children.insert(idx + 1, Link::Loaded(right_sub));
                    }
                    new_node.rehash();
                    return Ok(Arc::new(new_node));
                }
            }
        }

        if self.keys.is_empty() && self.children.is_empty() {
            let mut new_node = Node {
                level: key_level,
                keys: vec![key],
                values: vec![value],
                children: vec![
                    Link::Loaded(Arc::new(Node::empty(0))),
                    Link::Loaded(Arc::new(Node::empty(0))),
                ],
                hash: [0u8; 32],
            };
            new_node.rehash();
            return Ok(Arc::new(new_node));
        }

        let mut new_node = self.clone();
        let idx = match new_node
            .keys
            .binary_search_by(|probe| probe.as_ref().cmp(&key))
        {
            Ok(i) => {
                new_node.values[i] = value;
                new_node.rehash();
                return Ok(Arc::new(new_node));
            }
            Err(i) => i,
        };

        let child_node = match &new_node.children[idx] {
            Link::Loaded(n) => n.clone(),
            Link::Disk { offset, .. } => store.load_node(*offset)?,
        };

        let new_child = child_node.put(key, value, key_level, store)?;
        new_node.children[idx] = Link::Loaded(new_child);
        new_node.rehash();
        Ok(Arc::new(new_node))
    }

    fn split(&self, split_key: &K, store: &Arc<Store<K, V>>) -> io::Result<[Arc<Node<K, V>>; 2]> {
        if self.keys.is_empty() && self.children.is_empty() {
            return Ok(std::array::from_fn(|_| Arc::new(Node::empty(self.level))));
        }

        let idx = match self
            .keys
            .binary_search_by(|probe| probe.as_ref().cmp(split_key))
        {
            Ok(i) => i,
            Err(i) => i,
        };

        let left_keys = self.keys[..idx].to_vec();
        let left_values = self.values[..idx].to_vec();

        let right_start = if idx < self.keys.len() && self.keys[idx].as_ref() == split_key {
            idx + 1
        } else {
            idx
        };
        let right_keys = self.keys[right_start..].to_vec();
        let right_values = self.values[right_start..].to_vec();

        let [mid_left, mid_right] = if idx < self.children.len() {
            let child = match &self.children[idx] {
                Link::Loaded(n) => n.clone(),
                Link::Disk { offset, .. } => store.load_node(*offset)?,
            };
            child.split(split_key, store)?
        } else {
            std::array::from_fn(|_| Arc::new(Node::empty(0)))
        };

        let mut left_children = self.children[..idx].to_vec();
        left_children.push(Link::Loaded(mid_left));
        let mut left_node = Node {
            level: self.level,
            keys: left_keys,
            values: left_values,
            children: left_children,
            hash: [0u8; 32],
        };
        left_node.rehash();

        let mut right_children = vec![Link::Loaded(mid_right)];
        if idx + 1 < self.children.len() {
            right_children.extend_from_slice(&self.children[idx + 1..]);
        }
        let mut right_node = Node {
            level: self.level,
            keys: right_keys,
            values: right_values,
            children: right_children,
            hash: [0u8; 32],
        };
        right_node.rehash();

        Ok([left_node, right_node].map(Arc::new))
    }

    fn delete<Q>(&self, key: &Q, store: &Arc<Store<K, V>>) -> io::Result<(Arc<Node<K, V>>, bool)>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match self
            .keys
            .binary_search_by(|probe| probe.as_ref().borrow().cmp(key))
        {
            Ok(idx) => {
                let mut new_node = self.clone();
                new_node.keys.remove(idx);
                new_node.values.remove(idx);

                let left_child = new_node.children.remove(idx);
                let right_child = new_node.children.remove(idx);

                let merged_child = Node::merge(left_child, right_child, store)?;

                new_node.children.insert(idx, merged_child);

                new_node.rehash();
                Ok((Arc::new(new_node), true))
            }
            Err(idx) => {
                if self.children.is_empty() {
                    return Ok((Arc::new(self.clone()), false));
                }

                let child_link = &self.children[idx];
                let child_node = match child_link {
                    Link::Loaded(n) => n.clone(),
                    Link::Disk { offset, .. } => store.load_node(*offset)?,
                };

                let (new_child, deleted) = child_node.delete(key, store)?;

                if !deleted {
                    return Ok((Arc::new(self.clone()), false));
                }

                let mut new_node = self.clone();
                new_node.children[idx] = Link::Loaded(new_child);
                new_node.rehash();
                Ok((Arc::new(new_node), true))
            }
        }
    }

    fn merge(
        left: Link<K, V>,
        right: Link<K, V>,
        store: &Arc<Store<K, V>>,
    ) -> io::Result<Link<K, V>> {
        let left_node = match &left {
            Link::Loaded(n) => n.clone(),
            Link::Disk { offset, .. } => store.load_node(*offset)?,
        };

        let right_node = match &right {
            Link::Loaded(n) => n.clone(),
            Link::Disk { offset, .. } => store.load_node(*offset)?,
        };

        if left_node.keys.is_empty() && left_node.children.is_empty() {
            return Ok(Link::Loaded(right_node));
        }
        if right_node.keys.is_empty() && right_node.children.is_empty() {
            return Ok(Link::Loaded(left_node));
        }

        if left_node.level > right_node.level {
            let mut new_left = (*left_node).clone();
            let last_idx = new_left.children.len() - 1;
            let last_child = new_left.children.remove(last_idx);

            let merged = Node::merge(last_child, right, store)?;
            new_left.children.push(merged);
            new_left.rehash();

            return Ok(Link::Loaded(Arc::new(new_left)));
        }

        if right_node.level > left_node.level {
            let mut new_right = (*right_node).clone();
            let first_child = new_right.children.remove(0);

            let merged = Node::merge(left, first_child, store)?;
            new_right.children.insert(0, merged);
            new_right.rehash();

            return Ok(Link::Loaded(Arc::new(new_right)));
        }

        let mut new_node = (*left_node).clone();
        let mut right_clone = (*right_node).clone();

        let left_boundary_child = new_node.children.pop().expect("Node should have children");
        let right_boundary_child = right_clone.children.remove(0);

        let merged_boundary = Node::merge(left_boundary_child, right_boundary_child, store)?;

        new_node.keys.extend(right_clone.keys);
        new_node.values.extend(right_clone.values);
        new_node.children.push(merged_boundary);
        new_node.children.extend(right_clone.children);
        new_node.rehash();

        Ok(Link::Loaded(Arc::new(new_node)))
    }
}
