Here is a comprehensive `README.md` for the Merkle Search Tree project, based on the code provided in `lib.rs`, `benches.rs`, and `tests.rs`.

---

# File-Backed Merkle Search Tree (MST)

A high-performance, persistent, and authenticated dictionary implementation in Rust. This library combines the structural properties of **Merkle Trees** (cryptographic verification) with **Search Trees** (efficient lookups) and creates a flat-file storage engine capable of handling large datasets with efficient I/O.

## Features

* **Disk-Backed Persistence:** Operates directly on a file-backed store with page-aligned writes (4KB).
* **Cryptographic Verification:** Every node maintains a cryptographic hash (`blake3`) of its contents and children, allowing for O(1) root hash retrieval.
* **Efficient Caching:** Implements an in-memory `RwLock`-guarded cache to minimize disk reads for frequently accessed nodes.
* **Lazy Loading:** Nodes are only loaded from disk when traversed. Pointers to children are stored as "Links" which can be either loaded in memory or pointing to a disk offset.
* **Probabilistic Balancing:** Uses the Merkle Search Tree algorithm (hashing keys to determine levels) to maintain balance without complex rotation logic.
* **Serialization:** efficient binary serialization using `postcard`.

## Usage

### 1. Basic In-Memory Operation

For temporary usage (e.g., testing or transient cache), you can create a tree backed by a temporary file that is deleted upon drop.

```rust
use file_mst::{MerkleSearchTree, MerkleKey};
use std::borrow::Cow;
use serde::{Serialize, Deserialize};

fn main() -> std::io::Result<()> {
    // 2. Create a temporary tree
    let mut tree = MerkleSearchTree::new_temporary()?;

    // 3. Insert data
    tree.insert("Alice".to_string())?;
    tree.insert("Bob".to_string())?;

    // 4. Query data
    assert!(tree.contains(&"Alice".to_string())?);
    
    // 5. Get Root Hash
    println!("Root Hash: {:?}", tree.root_hash());

    Ok(())
}

```

### 2. Persistent Storage

To persist data between runs, use `open` and `flush`.

```rust
use file_mst::MerkleSearchTree;
use std::path::Path;

fn run_persistence() -> std::io::Result<()> {
    let path = Path::new("./db.mst");
    
    // Open (or create) the file
    let mut tree = MerkleSearchTree::open(path)?;
    
    tree.insert("Charlie".to_string())?;

    // Flush to disk. This returns the Offset and Hash of the new root.
    // You must save these (e.g., in a separate metadata file) to reload the tree later.
    let (root_offset, root_hash) = tree.flush()?;
    
    println!("Saved root at offset {} with hash {:?}", root_offset, root_hash);
    
    Ok(())
}

```

### 3. Loading from a Previous State

To load a tree, you need the filepath, the root offset, and the root hash returned by the last `flush()`.

```rust
use file_mst::MerkleSearchTree;

fn load_db(offset: u64, hash: [u8; 32]) -> std::io::Result<()> {
    let tree = MerkleSearchTree::load_from_root("./db.mst", offset, hash)?;
    
    if tree.contains(&"Charlie".to_string())? {
        println!("Charlie found!");
    }
    
    Ok(())
}

```

## Architecture

### The `Store`

The `Store` manages a `BufWriter<File>` and a `HashMap` cache.

* **Writing:** Nodes are serialized via `postcard` and written to the end of the file. Writes are padded to ensure alignment with 4KB pages where possible to optimize OS-level paging.
* **Reading:** When a `Link::Disk` is accessed, the store seeks to the offset, reads the length header, and deserializes the node.

### The `Node`

Nodes contain:

* **Level:** Determined by the hash of the keys (probabilistic).
* **Keys:** A sorted vector of user data.
* **Children:** A vector of `Link` enums (either `Loaded(Arc<Node>)` or `Disk { offset, hash }`).
* **Hash:** A `blake3` hash covering the node's level, keys, and children hashes.

### Deterministic Levels

The tree uses a deterministic algorithm to calculate node levels based on the key's hash. This ensures that regardless of insertion order, the resulting tree structure and root hash remain identical for the same set of keys.

## Benchmarks

The project includes a suite of benchmarks using the `test` nightly feature.

To run benchmarks:

```bash
cargo bench

```

## Testing

The library includes comprehensive tests covering:

* Basic CRUD (Create, Read, Update, Delete).
* Idempotency (inserting duplicates).
* Persistence (save/load cycles).
* Fuzzing/Interleaved operations (randomized insert/delete sequences).

To run tests:

```bash
cargo test

```
