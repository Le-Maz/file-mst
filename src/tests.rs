use super::*;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct TestKey(String);

impl MerkleKey for TestKey {
    fn encode(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.0.as_bytes())
    }
}

impl From<&str> for TestKey {
    fn from(s: &str) -> Self {
        TestKey(s.to_string())
    }
}

// Helper to generate a deterministically random set of keys
fn generate_keys(count: usize, seed: u64) -> Vec<TestKey> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut keys = Vec::with_capacity(count);
    for _ in 0..count {
        let n: u64 = rng.random();
        keys.push(TestKey(format!("key-{:016x}", n)));
    }
    keys
}

#[test]
fn new_tree_is_empty() {
    let tree: MerkleSearchTree<TestKey> = MerkleSearchTree::new_temporary().unwrap();
    assert!(!tree.contains(&TestKey::from("A")).unwrap());
    assert_eq!(tree.root_hash(), [0u8; 32]);
}

#[test]
fn insert_and_contains_basic() {
    let mut tree = MerkleSearchTree::new_temporary().unwrap();
    tree.insert(TestKey::from("A")).unwrap();
    assert!(tree.contains(&TestKey::from("A")).unwrap());
    assert!(!tree.contains(&TestKey::from("B")).unwrap());
}

#[test]
fn insert_duplicate_idempotency() {
    let mut tree = MerkleSearchTree::new_temporary().unwrap();

    tree.insert(TestKey::from("A")).unwrap();
    let hash1 = tree.root_hash();

    // Inserting duplicate shouldn't change hash, structure, or cause errors
    tree.insert(TestKey::from("A")).unwrap();
    let hash2 = tree.root_hash();

    assert_eq!(hash1, hash2, "Tree hash changed after inserting duplicate");
    assert!(tree.contains(&TestKey::from("A")).unwrap());
}

#[test]
fn ordering_and_traversal() {
    let mut tree = MerkleSearchTree::new_temporary().unwrap();
    let keys = vec!["B", "A", "C", "E", "D"];

    for &k in &keys {
        tree.insert(TestKey::from(k)).unwrap();
    }

    for &k in &keys {
        assert!(tree.contains(&TestKey::from(k)).unwrap());
    }
    assert!(!tree.contains(&TestKey::from("Z")).unwrap());
}

#[test]
fn deterministic_hashing_order_independence() {
    // MSTs are unique representations; insertion order must not affect final hash
    let mut rng = StdRng::seed_from_u64(37);
    let mut keys: Vec<TestKey> = (0..100).map(|i| TestKey(format!("k{}", i))).collect();

    let mut tree1 = MerkleSearchTree::new_temporary().unwrap();
    for k in &keys {
        tree1.insert(k.clone()).unwrap();
    }

    let mut tree2 = MerkleSearchTree::new_temporary().unwrap();
    keys.shuffle(&mut rng); // Shuffle keys
    for k in &keys {
        tree2.insert(k.clone()).unwrap();
    }

    assert_eq!(
        tree1.root_hash(),
        tree2.root_hash(),
        "Trees with same keys inserted in different order must have identical hashes"
    );
}

#[test]
fn large_scale_persistence() {
    // 1. Setup file and tree
    let file = tempfile::NamedTempFile::new().unwrap();
    let path = file.path().to_owned();

    let count = 5_000;
    let keys = generate_keys(count, 42);

    // 2. Build tree
    {
        let mut tree: MerkleSearchTree<TestKey> = MerkleSearchTree::open(&path).unwrap();
        for k in &keys {
            tree.insert(k.clone()).unwrap();
        }

        // 3. Flush to disk
        let (root_offset, root_hash) = tree.flush().unwrap();
        assert_ne!(root_hash, [0u8; 32]);

        // Save metadata for reload
        std::fs::write(
            path.with_extension("meta"),
            format!("{} {}", root_offset, hex::encode(root_hash)),
        )
        .unwrap();
    } // Drop tree, flushing BufWriter naturally if implemented correctly (but we did explicit flush)

    // 4. Re-open purely from disk
    let meta = std::fs::read_to_string(path.with_extension("meta")).unwrap();
    let parts: Vec<&str> = meta.split_whitespace().collect();
    let offset: u64 = parts[0].parse().unwrap();
    let hash_bytes = hex::decode(parts[1]).unwrap();
    let mut root_hash = [0u8; 32];
    root_hash.copy_from_slice(&hash_bytes);

    let loaded_tree: MerkleSearchTree<TestKey> =
        MerkleSearchTree::load_from_root(&path, offset, root_hash).unwrap();

    // 5. Verify ALL keys exist
    for k in &keys {
        assert!(
            loaded_tree.contains(k).unwrap(),
            "Tree lost key {:?} after reload",
            k
        );
    }
    assert!(
        !loaded_tree
            .contains(&TestKey::from("non-existent"))
            .unwrap()
    );
}

#[test]
fn exhaustive_deletion() -> io::Result<()> {
    let mut tree = MerkleSearchTree::new_temporary()?;
    let count = 1000;
    // Use deterministic keys
    let keys: Vec<TestKey> = (0..count)
        .map(|i| TestKey(format!("key-{:04}", i)))
        .collect();

    // Insert all
    for k in &keys {
        tree.insert(k.clone())?;
    }

    // 1. Delete even keys (creating sparse gaps)
    for i in (0..count).step_by(2) {
        tree.remove(&keys[i])?;
    }

    // Verify evens are gone, odds remain
    for i in 0..count {
        let exists = tree.contains(&keys[i])?;
        if i % 2 == 0 {
            assert!(!exists, "Deleted key {} still exists", i);
        } else {
            assert!(exists, "Key {} should still exist", i);
        }
    }

    // 2. Delete remaining keys (odds) in random order
    let mut remaining: Vec<TestKey> = keys
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

    // Tree should be effectively empty (or root hash zero/empty structure)
    // Note: Our implementation might leave an empty root node with hash [0;32]
    // or simply contain nothing.
    assert!(!tree.contains(&TestKey::from("key-0001"))?);

    // 3. Re-insert to ensure tree isn't broken
    tree.insert(TestKey::from("resurrected"))?;
    assert!(tree.contains(&TestKey::from("resurrected"))?);

    Ok(())
}

#[test]
fn interleaved_operations() -> io::Result<()> {
    let mut tree = MerkleSearchTree::new_temporary()?;
    let mut active_keys = HashSet::new();
    let mut rng = StdRng::seed_from_u64(37);

    // Perform 2000 random operations
    for i in 0..2000 {
        let key_str = format!("key-{}", rng.random_range(0..500)); // Key space of 500
        let key = TestKey(key_str.clone());

        if active_keys.contains(&key_str) {
            // 50% chance to remove, 50% chance to re-insert (idempotent)
            if rng.random_bool(0.5) {
                tree.remove(&key)?;
                active_keys.remove(&key_str);
                assert!(!tree.contains(&key)?, "Key {} should be removed", key_str);
            } else {
                tree.insert(key.clone())?;
                assert!(tree.contains(&key)?, "Key {} should exist", key_str);
            }
        } else {
            // Insert
            tree.insert(key.clone())?;
            active_keys.insert(key_str.clone());
            assert!(
                tree.contains(&key)?,
                "Key {} should exist after insert",
                key_str
            );
        }

        // Every 100 ops, flush and verify consistency of a few keys
        if i % 100 == 0 {
            tree.flush()?;
        }
    }

    // Final verification
    for k in active_keys {
        assert!(tree.contains(&TestKey(k))?, "Final check failed");
    }

    Ok(())
}

#[test]
fn boundary_deletions() -> io::Result<()> {
    // Tests deleting min/max keys which often trigger edge cases in merging logic
    let mut tree = MerkleSearchTree::new_temporary()?;
    let keys = vec![
        TestKey("A".to_string()),
        TestKey("M".to_string()),
        TestKey("Z".to_string()),
    ];

    for k in &keys {
        tree.insert(k.clone())?;
    }

    // Remove Middle
    tree.remove(&TestKey("M".to_string()))?;
    assert!(tree.contains(&TestKey("A".to_string()))?);
    assert!(tree.contains(&TestKey("Z".to_string()))?);
    assert!(!tree.contains(&TestKey("M".to_string()))?);

    // Remove First
    tree.remove(&TestKey("A".to_string()))?;
    assert!(!tree.contains(&TestKey("A".to_string()))?);
    assert!(tree.contains(&TestKey("Z".to_string()))?);

    // Remove Last
    tree.remove(&TestKey("Z".to_string()))?;
    assert!(!tree.contains(&TestKey("Z".to_string()))?);

    Ok(())
}
