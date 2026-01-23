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
        tree.get(&String::from("A")).unwrap(),
        Some("ValA".to_string())
    );
    assert!(!tree.contains(&String::from("B")).unwrap());
}

#[test]
fn insert_update_value() {
    let mut tree = MerkleSearchTree::new_temporary().unwrap();
    tree.insert(String::from("A"), "Val1".to_string()).unwrap();
    assert_eq!(
        tree.get(&String::from("A")).unwrap(),
        Some("Val1".to_string())
    );

    // Update value
    tree.insert(String::from("A"), "Val2".to_string()).unwrap();
    assert_eq!(
        tree.get(&String::from("A")).unwrap(),
        Some("Val2".to_string())
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
            tree.get(&String::from(k)).unwrap(),
            Some(format!("v-{}", k))
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

        let (root_offset, root_hash) = tree.flush().unwrap();
        assert_ne!(root_hash, [0u8; 32]);

        std::fs::write(
            path.with_extension("meta"),
            format!("{} {}", root_offset, hex::encode(root_hash)),
        )
        .unwrap();
    }

    let meta = std::fs::read_to_string(path.with_extension("meta")).unwrap();
    let parts: Vec<&str> = meta.split_whitespace().collect();
    let offset: u64 = parts[0].parse().unwrap();
    let hash_bytes = hex::decode(parts[1]).unwrap();
    let mut root_hash = [0u8; 32];
    root_hash.copy_from_slice(&hash_bytes);

    let loaded_tree: MerkleSearchTree<String, i32> =
        MerkleSearchTree::load_from_root(&path, offset, root_hash).unwrap();

    for (i, k) in keys.iter().enumerate() {
        let val = loaded_tree.get(k).unwrap();
        assert_eq!(val, Some(i as i32), "Incorrect value for key {}", k);
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

    for i in 0..count {
        let exists = tree.contains(&keys[i])?;
        if i % 2 == 0 {
            assert!(!exists, "Deleted key {} still exists", i);
        } else {
            assert!(exists, "Key {} should still exist", i);
            assert_eq!(tree.get(&keys[i])?, Some(keys[i].clone()));
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

    assert!(!tree.contains(&String::from("key-0001"))?);

    tree.insert(String::from("resurrected"), "alive".to_string())?;
    assert_eq!(
        tree.get(&String::from("resurrected"))?,
        Some("alive".to_string())
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
                assert_eq!(tree.get(&key_str)?, Some(val));
            }
        } else {
            tree.insert(key_str.clone(), val)?;
            active_keys.insert(key_str.clone(), val);
            assert_eq!(tree.get(&key_str)?, Some(val));
        }

        if i % 100 == 0 {
            tree.flush()?;
        }
    }

    for (k, v) in active_keys {
        assert_eq!(tree.get(&k)?, Some(v));
    }

    Ok(())
}

#[test]
fn boundary_deletions() -> io::Result<()> {
    let mut tree = MerkleSearchTree::new_temporary()?;
    let keys = vec!["A".to_string(), "M".to_string(), "Z".to_string()];

    for k in &keys {
        tree.insert(k.clone(), 1)?;
    }

    tree.remove(&"M".to_string())?;
    assert!(tree.contains(&"A".to_string())?);
    assert!(tree.contains(&"Z".to_string())?);
    assert!(!tree.contains(&"M".to_string())?);

    tree.remove(&"A".to_string())?;
    assert!(!tree.contains(&"A".to_string())?);
    assert!(tree.contains(&"Z".to_string())?);

    tree.remove(&"Z".to_string())?;
    assert!(!tree.contains(&"Z".to_string())?);

    Ok(())
}
