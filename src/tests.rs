use std::collections::HashMap;
use std::io;

use super::*;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

// Helper to generate a deterministically random set of keys
fn generate_keys(count: usize, seed: u64) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut keys = Vec::with_capacity(count);
    for _ in 0..count {
        let n: u64 = rng.random();
        keys.push(format!("key-{:016x}", n));
    }
    keys
}

#[test]
fn new_tree_is_empty() {
    let tree: MerkleSearchTree<String, String> = MerkleSearchTree::new_temporary().unwrap();
    assert!(!tree.contains(&String::from("A")).unwrap());
    assert_eq!(tree.root_hash(), [0u8; 32]);
}

#[test]
fn insert_and_contains_basic() {
    let mut tree = MerkleSearchTree::new_temporary().unwrap();
    tree.insert(String::from("A"), "ValA".to_string()).unwrap();
    assert!(tree.contains(&String::from("A")).unwrap());

    assert_eq!(
        tree.get(&String::from("A")).unwrap().as_deref(),
        Some(&"ValA".to_string())
    );
    assert!(!tree.contains(&String::from("B")).unwrap());
}

#[test]
fn insert_update_value() {
    let mut tree = MerkleSearchTree::new_temporary().unwrap();
    tree.insert(String::from("A"), "Val1".to_string()).unwrap();
    assert_eq!(
        tree.get(&String::from("A")).unwrap().as_deref(),
        Some(&"Val1".to_string())
    );

    // Update value
    tree.insert(String::from("A"), "Val2".to_string()).unwrap();
    assert_eq!(
        tree.get(&String::from("A")).unwrap().as_deref(),
        Some(&"Val2".to_string())
    );
}

#[test]
fn insert_duplicate_idempotency() {
    let mut tree = MerkleSearchTree::new_temporary().unwrap();

    tree.insert(String::from("A"), "ValA".to_string()).unwrap();
    let hash1 = tree.root_hash();

    // Inserting identical key-value shouldn't change hash
    tree.insert(String::from("A"), "ValA".to_string()).unwrap();
    let hash2 = tree.root_hash();

    assert_eq!(hash1, hash2, "Tree hash changed after inserting duplicate");

    // Inserting different value SHOULD change hash
    tree.insert(String::from("A"), "ValB".to_string()).unwrap();
    let hash3 = tree.root_hash();
    assert_ne!(hash1, hash3, "Tree hash should change when value changes");
}

#[test]
fn ordering_and_traversal() {
    let mut tree = MerkleSearchTree::new_temporary().unwrap();
    let keys = vec!["B", "A", "C", "E", "D"];

    for &k in &keys {
        tree.insert(String::from(k), format!("v-{}", k)).unwrap();
    }

    for &k in &keys {
        assert!(tree.contains(&String::from(k)).unwrap());
        assert_eq!(
            tree.get(&String::from(k)).unwrap().as_deref(),
            Some(&format!("v-{}", k))
        );
    }
    assert!(!tree.contains(&String::from("Z")).unwrap());
}

#[test]
fn deterministic_hashing_order_independence() {
    let mut rng = StdRng::seed_from_u64(37);
    let mut keys: Vec<String> = (0..100).map(|i| format!("k{}", i)).collect();

    let mut tree1 = MerkleSearchTree::new_temporary().unwrap();
    for k in &keys {
        tree1.insert(k.clone(), k.clone()).unwrap();
    }

    let mut tree2 = MerkleSearchTree::new_temporary().unwrap();
    keys.shuffle(&mut rng);
    for k in &keys {
        tree2.insert(k.clone(), k.clone()).unwrap();
    }

    assert_eq!(
        tree1.root_hash(),
        tree2.root_hash(),
        "Trees with same kv-pairs inserted in different order must have identical hashes"
    );
}

#[test]
fn large_scale_persistence() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let path = file.path().to_owned();

    let count = 5_000;
    let keys = generate_keys(count, 42);

    {
        let mut tree: MerkleSearchTree<String, i32> = MerkleSearchTree::open(&path).unwrap();
        for (i, k) in keys.iter().enumerate() {
            tree.insert(k.clone(), i as i32).unwrap();
        }
        tree.commit().unwrap();
    }

    let loaded_tree: MerkleSearchTree<String, i32> = MerkleSearchTree::open(&path).unwrap();

    for (i, k) in keys.iter().enumerate() {
        let val = loaded_tree.get(k).unwrap();
        assert_eq!(
            val.as_deref(),
            Some(&(i as i32)),
            "Incorrect value for key {}",
            k
        );
    }
    assert!(!loaded_tree.contains(&String::from("non-existent")).unwrap());
}

#[test]
fn exhaustive_deletion() -> io::Result<()> {
    let mut tree = MerkleSearchTree::new_temporary()?;
    let count = 1000;
    let keys: Vec<String> = (0..count).map(|i| format!("key-{:04}", i)).collect();

    for k in &keys {
        tree.insert(k.clone(), k.clone())?;
    }

    // Delete even keys
    for i in (0..count).step_by(2) {
        tree.remove(&keys[i])?;
    }

    #[expect(clippy::needless_range_loop)]
    for i in 0..count {
        let exists = tree.contains(&keys[i])?;
        if i % 2 == 0 {
            assert!(!exists, "Deleted key {} still exists", i);
        } else {
            assert!(exists, "Key {} should still exist", i);
            assert_eq!(tree.get(&keys[i])?.as_deref(), Some(&keys[i]));
        }
    }

    let mut remaining: Vec<String> = keys
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 2 != 0)
        .map(|(_, k)| k.clone())
        .collect();

    let mut rng = StdRng::seed_from_u64(37);
    remaining.shuffle(&mut rng);

    for k in &remaining {
        tree.remove(k)?;
    }

    assert!(!tree.contains("key-0001")?);

    tree.insert(String::from("resurrected"), "alive".to_string())?;
    assert_eq!(
        tree.get("resurrected")?.as_deref(),
        Some(&"alive".to_string())
    );

    Ok(())
}

#[test]
fn interleaved_operations() -> io::Result<()> {
    let mut tree = MerkleSearchTree::new_temporary()?;
    let mut active_keys = HashMap::new();
    let mut rng = StdRng::seed_from_u64(37);

    for i in 0..2000 {
        let key_str = format!("key-{}", rng.random_range(0..500));
        let val = i;

        if active_keys.contains_key(&key_str) {
            if rng.random_bool(0.5) {
                tree.remove(&key_str)?;
                active_keys.remove(&key_str);
                assert!(!tree.contains(&key_str)?);
            } else {
                tree.insert(key_str.clone(), val)?;
                active_keys.insert(key_str.clone(), val);
                assert_eq!(tree.get(&key_str)?.as_deref(), Some(&val));
            }
        } else {
            tree.insert(key_str.clone(), val)?;
            active_keys.insert(key_str.clone(), val);
            assert_eq!(tree.get(&key_str)?.as_deref(), Some(&val));
        }

        if i % 100 == 0 {
            tree.commit()?;
        }
    }

    for (k, v) in active_keys {
        assert_eq!(tree.get(&k)?.as_deref(), Some(&v));
    }

    Ok(())
}

#[test]
fn boundary_deletions() -> io::Result<()> {
    let mut tree = MerkleSearchTree::new_temporary()?;
    let keys = vec!["A", "M", "Z"];

    for &k in &keys {
        tree.insert(String::from(k), 1)?;
    }

    tree.remove("M")?;
    assert!(tree.contains("A")?);
    assert!(tree.contains("Z")?);
    assert!(!tree.contains("M")?);

    tree.remove("A")?;
    assert!(!tree.contains("A")?);
    assert!(tree.contains("Z")?);

    tree.remove("Z")?;
    assert!(!tree.contains("Z")?);

    Ok(())
}

#[test]
fn blobs_and_page_boundaries() {
    use rand::RngCore;

    // 1. Setup
    let file = tempfile::NamedTempFile::new().unwrap();
    let path = file.path().to_owned();
    
    // We use Vec<u8> explicitly to represent "Blobs"
    let mut tree: MerkleSearchTree<String, Vec<u8>> = MerkleSearchTree::open(&path).unwrap();
    let mut rng = StdRng::seed_from_u64(999);

    // 2. Prepare Blob Data
    
    // Blob A: Small (Fits comfortably in one page)
    let mut blob_small = vec![0u8; 100];
    rng.fill_bytes(&mut blob_small);

    // Blob B: Edge Case (Just enough to likely trigger page alignment logic)
    // Node overhead is small, so ~3800 bytes might push the total Node size near 4096.
    let mut blob_boundary = vec![0u8; 3800];
    rng.fill_bytes(&mut blob_boundary);

    // Blob C: Large (Larger than PAGE_SIZE=4096, forces multi-page write)
    // 64KB spans ~16 pages.
    let mut blob_large = vec![0u8; 64 * 1024]; 
    rng.fill_bytes(&mut blob_large);

    // Blob D: Specific Patterns (All zeros, All ones) to catch simple encoding bugs
    let blob_zeros = vec![0u8; 2048];
    let blob_ones = vec![255u8; 2048];

    // 3. Insert Blobs
    tree.insert("small".to_string(), blob_small.clone()).unwrap();
    tree.insert("boundary".to_string(), blob_boundary.clone()).unwrap();
    tree.insert("large".to_string(), blob_large.clone()).unwrap();
    tree.insert("zeros".to_string(), blob_zeros.clone()).unwrap();
    tree.insert("ones".to_string(), blob_ones.clone()).unwrap();

    // 4. Verify In-Memory (Hot Cache)
    assert_eq!(tree.get("small").unwrap().as_deref(), Some(&blob_small));
    assert_eq!(tree.get("boundary").unwrap().as_deref(), Some(&blob_boundary));
    assert_eq!(tree.get("large").unwrap().as_deref(), Some(&blob_large));
    
    // 5. Commit and Re-open (Cold Read from Disk)
    tree.commit().unwrap();
    drop(tree); // Force close file handle

    let tree_loaded: MerkleSearchTree<String, Vec<u8>> = MerkleSearchTree::open(&path).unwrap();

    // Verify Small
    assert_eq!(
        tree_loaded.get("small").unwrap().as_deref(), 
        Some(&blob_small), 
        "Failed to retrieve small blob"
    );

    // Verify Boundary (Did page padding logic corrupt it?)
    assert_eq!(
        tree_loaded.get("boundary").unwrap().as_deref(), 
        Some(&blob_boundary), 
        "Failed to retrieve page-boundary sized blob"
    );

    // Verify Large (Did multi-page read/write work?)
    assert_eq!(
        tree_loaded.get("large").unwrap().as_deref(), 
        Some(&blob_large), 
        "Failed to retrieve large blob > PAGE_SIZE"
    );

    // Verify Binary patterns
    assert_eq!(tree_loaded.get("zeros").unwrap().as_deref(), Some(&blob_zeros));
    assert_eq!(tree_loaded.get("ones").unwrap().as_deref(), Some(&blob_ones));
}

#[test]
fn compaction_reduces_file_size_and_preserves_data() {
    use std::fs;

    // 1. Setup paths using a temporary directory so we can control filenames
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("original.mst");
    let compacted_path = dir.path().join("compacted.mst");

    // 2. Create a persistent tree
    let mut tree = MerkleSearchTree::open(&db_path).unwrap();
    let count = 2000;

    // 3. Fill the tree (Initial State)
    for i in 0..count {
        tree.insert(format!("key-{:04}", i), "original-value".to_string()).unwrap();
    }
    tree.commit().unwrap();
    
    let size_after_insert = fs::metadata(&db_path).unwrap().len();

    // 4. Create Fragmentation (Garbage)
    // Update the first 500 keys (orphans old nodes)
    for i in 0..500 {
        tree.insert(format!("key-{:04}", i), "updated-value".to_string()).unwrap();
    }
    // Delete the next 500 keys (orphans old nodes)
    for i in 500..1000 {
        tree.remove(&format!("key-{:04}", i)).unwrap();
    }
    
    // Commit to flush changes to disk
    tree.commit().unwrap();

    let fragmented_size = fs::metadata(&db_path).unwrap().len();
    assert!(
        fragmented_size > size_after_insert, 
        "File should grow after updates/deletes in append-only store"
    );

    // 5. Perform Compaction
    // This assumes you implemented the `compact` method from the previous step
    tree.compact(&compacted_path).unwrap();

    let compacted_size = fs::metadata(&compacted_path).unwrap().len();

    // 6. Verify File Size Reduction
    println!("Fragmented Size: {} bytes", fragmented_size);
    println!("Compacted Size:  {} bytes", compacted_size);
    
    assert!(
        compacted_size < fragmented_size, 
        "Compaction failed to reduce file size (Fragmented: {}, Compacted: {})",
        fragmented_size, compacted_size
    );

    // 7. Verify Data Integrity
    // Check updated keys
    for i in 0..500 {
        let val = tree.get(&format!("key-{:04}", i)).unwrap();
        assert_eq!(val.as_deref(), Some(&"updated-value".to_string()));
    }

    // Check deleted keys
    for i in 500..1000 {
        assert!(!tree.contains(&format!("key-{:04}", i)).unwrap());
    }

    // Check untouched keys
    for i in 1000..count {
        let val = tree.get(&format!("key-{:04}", i)).unwrap();
        assert_eq!(val.as_deref(), Some(&"original-value".to_string()));
    }
}
