use crate::{MerkleKey, MerkleValue, NodeId, store::Store};
use blake3::{Hash, OUT_LEN};
use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, io, sync::Arc};

#[derive(Debug)]
pub enum Link<K: MerkleKey, V: MerkleValue> {
    Disk { offset: NodeId, hash: Hash },
    Loaded(Arc<Node<K, V>>),
}

impl<K: MerkleKey, V: MerkleValue> Clone for Link<K, V> {
    fn clone(&self) -> Self {
        match self {
            Link::Disk { offset, hash } => Link::Disk {
                offset: *offset,
                hash: *hash,
            },
            Link::Loaded(node) => Link::Loaded(node.clone()),
        }
    }
}

impl<K: MerkleKey, V: MerkleValue> Link<K, V> {
    pub fn hash(&self) -> Hash {
        match self {
            Link::Disk { hash, .. } => *hash,
            Link::Loaded(node) => node.hash,
        }
    }
}

#[derive(Debug)]
pub struct Node<K: MerkleKey, V: MerkleValue> {
    pub level: u32,
    pub keys: Vec<Arc<K>>,
    pub values: Vec<Arc<V>>,
    pub children: Vec<Link<K, V>>,
    pub hash: Hash,
}

impl<K: MerkleKey, V: MerkleValue> Clone for Node<K, V> {
    fn clone(&self) -> Self {
        Self {
            level: self.level,
            keys: self.keys.clone(),
            values: self.values.clone(),
            children: self.children.clone(),
            hash: self.hash,
        }
    }
}

#[derive(Deserialize)]
pub struct DiskNode<K, V> {
    pub level: u32,
    pub keys: Vec<K>,
    pub values: Vec<V>,
    pub children: Vec<(NodeId, Hash)>,
    pub hash: Hash,
}

#[derive(Serialize)]
pub struct DiskNodeRef<'a, K, V> {
    pub level: u32,
    pub keys: &'a [Arc<K>],
    pub values: &'a [Arc<V>],
    pub children: Vec<(NodeId, Hash)>,
    pub hash: Hash,
}

impl<K: MerkleKey, V: MerkleValue> Node<K, V> {
    pub(crate) fn empty(level: u32) -> Self {
        let mut node = Self {
            level,
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            hash: Hash::from_bytes([0u8; OUT_LEN]),
        };
        node.rehash();
        node
    }

    pub(crate) fn as_disk_ref(&self) -> DiskNodeRef<'_, K, V> {
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

        DiskNodeRef {
            level: self.level,
            keys: &self.keys,
            values: &self.values,
            children: children_meta,
            hash: self.hash,
        }
    }

    pub(crate) fn from_disk(disk: DiskNode<K, V>) -> Self {
        let children = disk
            .children
            .into_iter()
            .map(|(offset, hash)| Link::Disk { offset, hash })
            .collect();

        let keys = disk.keys.into_iter().map(Arc::new).collect();
        let values = disk.values.into_iter().map(Arc::new).collect();

        Self {
            level: disk.level,
            keys,
            values,
            children,
            hash: disk.hash,
        }
    }

    pub(crate) fn calc_level(key: &K) -> u32 {
        let mut h = blake3::Hasher::new();
        let key_bytes =
            postcard::to_extend(key, Vec::new()).expect("Failed to serialize key for level calc");
        h.update(&key_bytes);
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
            self.hash = Hash::from_bytes([0u8; OUT_LEN]);
            return;
        }

        let mut h = blake3::Hasher::new();
        h.update(&self.level.to_le_bytes());
        h.update(&(self.keys.len() as u64).to_le_bytes());

        for (i, child) in self.children.iter().enumerate() {
            h.update(child.hash().as_bytes());
            if i < self.keys.len() {
                let k_bytes = postcard::to_extend(&self.keys[i], Vec::new())
                    .expect("Failed to serialize key for rehash");
                h.update(&(k_bytes.len() as u64).to_le_bytes());
                h.update(&k_bytes);

                let v_bytes = postcard::to_extend(&self.values[i], Vec::with_capacity(4096))
                    .expect("Failed to serialize value for hashing");
                h.update(&(v_bytes.len() as u64).to_le_bytes());
                h.update(&v_bytes);
            }
        }
        self.hash = h.finalize();
    }

    pub(crate) fn contains<Q>(&self, key: &Q, store: &Store<K, V>) -> io::Result<bool>
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

    pub(crate) fn get<Q>(&self, key: &Q, store: &Store<K, V>) -> io::Result<Option<Arc<V>>>
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

    pub(crate) fn put(
        &self,
        key: Arc<K>,
        value: Arc<V>,
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
                hash: Hash::from_bytes([0u8; OUT_LEN]),
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
                hash: Hash::from_bytes([0u8; OUT_LEN]),
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
            hash: Hash::from_bytes([0u8; OUT_LEN]),
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
            hash: Hash::from_bytes([0u8; OUT_LEN]),
        };
        right_node.rehash();

        Ok([left_node, right_node].map(Arc::new))
    }

    pub(crate) fn delete<Q>(
        &self,
        key: &Q,
        store: &Arc<Store<K, V>>,
    ) -> io::Result<(Arc<Node<K, V>>, bool)>
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
