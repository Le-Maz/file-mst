use blake3::{Hash, OUT_LEN};

use crate::{
    MerkleKey, MerkleValue, NodeId, PAGE_SIZE,
    node::{DiskNode, Node},
};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, RwLock};

pub struct Store<K: MerkleKey, V: MerkleValue> {
    file: RwLock<BufWriter<File>>,
    cache: RwLock<HashMap<NodeId, Arc<Node<K, V>>>>,
}

impl<K: MerkleKey, V: MerkleValue> Store<K, V> {
    pub fn new(file: File) -> Arc<Self> {
        Arc::new(Self {
            file: RwLock::new(BufWriter::with_capacity(64 * 1024, file)),
            cache: RwLock::new(HashMap::new()),
        })
    }
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> io::Result<Arc<Self>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        if file.metadata()?.len() == 0 {
            file.set_len(PAGE_SIZE)?;
        }

        Ok(Self::new(file))
    }

    pub(crate) fn write_metadata(&self, root_offset: u64, root_hash: Hash) -> io::Result<()> {
        let mut writer = self.file.write().unwrap();
        writer.seek(SeekFrom::Start(0))?;

        writer.write_all(&root_offset.to_le_bytes())?;
        writer.write_all(root_hash.as_bytes())?;
        Ok(())
    }

    pub(crate) fn read_metadata(&self) -> io::Result<Option<(u64, Hash)>> {
        let mut writer_guard = self.file.write().unwrap();
        let file = writer_guard.get_mut();
        file.seek(SeekFrom::Start(0))?;

        let mut offset_buf = [0u8; 8];
        file.read_exact(&mut offset_buf)?;
        let offset = u64::from_le_bytes(offset_buf);

        if offset == 0 {
            return Ok(None);
        }

        let mut hash = [0u8; OUT_LEN];
        file.read_exact(&mut hash)?;

        Ok(Some((offset, Hash::from_bytes(hash))))
    }

    pub(crate) fn flush(&self) -> io::Result<()> {
        let mut writer = self.file.write().unwrap();
        writer.flush()?; // Flushes Rust buffer to OS
        writer.get_ref().sync_all() // Flushes OS buffer to Disk
    }

    pub(crate) fn load_node(&self, offset: NodeId) -> io::Result<Arc<Node<K, V>>> {
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

    pub(crate) fn write_node(&self, node: &Node<K, V>) -> io::Result<NodeId> {
        let disk_node = node.as_disk_ref();

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
