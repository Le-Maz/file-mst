use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::thread;
use tokio::sync::oneshot;

use crate::{MerkleKey, MerkleSearchTree, MerkleValue};
use blake3::Hash;

/// Commands sent to the worker thread
enum Command<K, V> {
    Insert {
        key: K,
        value: V,
        resp: oneshot::Sender<io::Result<()>>,
    },
    Remove {
        key: K,
        resp: oneshot::Sender<io::Result<()>>,
    },
    Get {
        key: K,
        resp: oneshot::Sender<io::Result<Option<Arc<V>>>>,
    },
    Contains {
        key: K,
        resp: oneshot::Sender<io::Result<bool>>,
    },
    Commit {
        resp: oneshot::Sender<io::Result<(u64, Hash)>>,
    },
    Compact {
        path: String,
        resp: oneshot::Sender<io::Result<()>>,
    },
}

/// Async wrapper for MerkleSearchTree using a worker thread
#[derive(Debug)]
pub struct AsyncMerkleSearchTree<K, V>
where
    K: MerkleKey + Send + Sync + 'static,
    V: MerkleValue + Send + Sync + 'static,
{
    tx: SyncSender<Command<K, V>>,
}

impl<K, V> Clone for AsyncMerkleSearchTree<K, V>
where
    K: MerkleKey + Send + Sync + 'static,
    V: MerkleValue + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<K, V> From<MerkleSearchTree<K, V>> for AsyncMerkleSearchTree<K, V>
where
    K: MerkleKey + Send + Sync + 'static,
    V: MerkleValue + Send + Sync + 'static,
{
    fn from(mut tree: MerkleSearchTree<K, V>) -> Self {
        let (tx, rx) = mpsc::sync_channel::<Command<K, V>>(512);

        thread::spawn(move || {
            for cmd in rx {
                match cmd {
                    Command::Insert { key, value, resp } => {
                        let _ = resp.send(tree.insert(key, value));
                    }
                    Command::Remove { key, resp } => {
                        let _ = resp.send(tree.remove(&key));
                    }
                    Command::Get { key, resp } => {
                        let _ = resp.send(tree.get(&key));
                    }
                    Command::Contains { key, resp } => {
                        let _ = resp.send(tree.contains(&key));
                    }
                    Command::Commit { resp } => {
                        let _ = resp.send(tree.commit());
                    }
                    Command::Compact { path, resp } => {
                        let _ = resp.send(tree.compact(path));
                    }
                }
            }
        });

        Self { tx }
    }
}

impl<K, V> AsyncMerkleSearchTree<K, V>
where
    K: MerkleKey + Send + Sync + 'static,
    V: MerkleValue + Send + Sync + 'static,
{
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Ok(MerkleSearchTree::open(path)?.into())
    }

    /// Creates a new MST backed by a temporary file.
    pub fn new_temporary() -> io::Result<Self> {
        Ok(MerkleSearchTree::new_temporary()?.into())
    }

    /// Helper to try sending a command to the worker and convert errors to io::Result
    fn try_send(&self, cmd: Command<K, V>) -> io::Result<()> {
        self.tx.try_send(cmd).map_err(|e| match e {
            TrySendError::Full(_) => {
                io::Error::new(io::ErrorKind::ResourceBusy, "Worker queue full")
            }
            TrySendError::Disconnected(_) => {
                io::Error::new(io::ErrorKind::BrokenPipe, "Worker thread disconnected")
            }
        })
    }

    pub async fn insert(&self, key: K, value: V) -> io::Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.try_send(Command::Insert {
            key,
            value,
            resp: resp_tx,
        })?;
        resp_rx.await.map_err(Self::on_oneshot_error).flatten()
    }

    pub async fn remove(&self, key: K) -> io::Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.try_send(Command::Remove { key, resp: resp_tx })?;
        resp_rx.await.map_err(Self::on_oneshot_error).flatten()
    }

    pub async fn get(&self, key: K) -> io::Result<Option<Arc<V>>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.try_send(Command::Get { key, resp: resp_tx })?;
        resp_rx.await.map_err(Self::on_oneshot_error).flatten()
    }

    pub async fn contains(&self, key: K) -> io::Result<bool> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.try_send(Command::Contains { key, resp: resp_tx })?;
        resp_rx.await.map_err(Self::on_oneshot_error).flatten()
    }

    pub async fn commit(&self) -> io::Result<(u64, Hash)> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.try_send(Command::Commit { resp: resp_tx })?;
        resp_rx.await.map_err(Self::on_oneshot_error).flatten()
    }

    pub async fn compact(&self, path: String) -> io::Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.try_send(Command::Compact {
            path,
            resp: resp_tx,
        })?;
        resp_rx.await.map_err(Self::on_oneshot_error).flatten()
    }

    fn on_oneshot_error(recv_error: oneshot::error::RecvError) -> io::Error {
        io::Error::new(io::ErrorKind::BrokenPipe, recv_error)
    }
}
