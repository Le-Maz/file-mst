# File-Backed Merkle Search Tree (MST) Map

A high-performance, persistent, and authenticated **Key-Value** store implementation in Rust. This library combines the structural properties of **Merkle Trees** (cryptographic verification) with **Search Trees** (efficient lookups) and creates a flat-file storage engine capable of handling large datasets with efficient I/O.

## Features

- **Disk-Backed Persistence:** Operates directly on a file-backed store with page-aligned writes of 4096 bytes.
- **Cryptographic Verification:** Every node maintains a cryptographic hash of its contents (keys and values) and children, allowing for root hash retrieval.
- **Efficient Caching:** Implements an in-memory cache to minimize disk reads for frequently accessed nodes.
- **Lazy Loading:** Nodes are only loaded from disk when traversed.
- **Probabilistic Balancing:** Uses the Merkle Search Tree algorithm (hashing keys to determine levels) to maintain balance without complex rotation logic.
- **Deterministic:** The same set of KV-pairs results in the same root hash, regardless of insertion order.

## Usage

### 1. Basic In-Memory Operation

For temporary usage, you can create a tree backed by a temporary file.

```rust
use file_mst::MerkleSearchTree;

fn main() -> std::io::Result<()> {
    // Create a temporary tree (Key: String, Value: i32)
    let mut tree: MerkleSearchTree<String, i32> = MerkleSearchTree::new_temporary()?;

    // Insert data
    tree.insert("Alice".to_string(), 100)?;
    tree.insert("Bob".to_string(), 200)?;

    // Query data
    if tree.contains("Alice")? {
        println!("Alice exists!");
    }

    // Retrieve value (returns Arc<V>)
    if let Some(val) = tree.get("Bob")? {
        println!("Bob's value is: {}", val);
    }

    // Get Root Hash
    println!("Root Hash: {:?}", tree.root_hash());

    Ok(())
}
```

### 2. Persistent Storage

The `open` method automatically attempts to read metadata (root offset and hash) from the file header. To permanently save changes, use the `commit()` method.

```rust
use file_mst::MerkleSearchTree;
use std::path::Path;

fn run_persistence() -> std::io::Result<()> {
    let path = "db.mst";

    // Open (or create) the file.
    let mut tree: MerkleSearchTree<String, String> = MerkleSearchTree::open(path)?;

    tree.insert("config_key".to_string(), "production_v1".to_string())?;

    // Commit writes all dirty nodes to disk and updates file metadata.
    let (root_offset, root_hash) = tree.commit()?;

    println!("Saved root at offset {} with hash {:?}", root_offset, root_hash);

    Ok(())
}
```

## Architecture

### The `Store`

Manages reading and writing pages. It uses the `postcard` library for efficient binary serialization of nodes.

### The `Node`

Nodes contain:

- **Level:** Determined probabilistically based on the key's hash.
- **Keys & Values:** Sorted vectors of user data.
- **Children:** A vector of `Link` objects, which can be `Loaded` (in RAM) or `Disk` (file offset).

### Operations

- **Insert/Remove:** Operations modify the tree in-place in memory using Copy-on-Write for `Arc` nodes; they become persistent only after calling `commit()`.
- **Get/Contains:** Use `resolve_link` to lazily fetch missing nodes from disk only when required.

## Testing

The library includes comprehensive tests covering:

- Basic CRUD operations.
- Idempotency and hashing determinism.
- Node deletion and boundary cases.
- Large-scale persistence (e.g., 5000+ elements).

```bash
cargo test
```

## Reference

Implementation based on:

> Alex Auvolat, François Taïani. **Merkle Search Trees: Efficient State-Based CRDTs in Open Networks.** SRDS 2019. [hal-02303490](https://hal.inria.fr/hal-02303490)
