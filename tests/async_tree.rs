use blake3::Hash;
use file_mst::AsyncMerkleSearchTree;
use tempfile::tempdir;

#[tokio::test]
async fn insert_and_get() {
    let tree = AsyncMerkleSearchTree::new_temporary().unwrap();

    tree.insert(1, "value1".to_string()).await.unwrap();
    tree.insert(2, "value2".to_string()).await.unwrap();

    let val = tree.get(1).await.unwrap();
    assert_eq!(val.unwrap().as_ref(), &"value1".to_string());

    let val2 = tree.get(2).await.unwrap();
    assert_eq!(val2.unwrap().as_ref(), &"value2".to_string());

    let missing = tree.get(3).await.unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn contains_and_remove() {
    let tree = AsyncMerkleSearchTree::new_temporary().unwrap();

    tree.insert(10, "ten".to_string()).await.unwrap();
    assert!(tree.contains(10).await.unwrap());
    assert!(!tree.contains(99).await.unwrap());

    tree.remove(10).await.unwrap();
    assert!(!tree.contains(10).await.unwrap());
}

#[tokio::test]
async fn commit() {
    let tree = AsyncMerkleSearchTree::new_temporary().unwrap();

    tree.insert(5, "five".to_string()).await.unwrap();
    let (offset, hash) = tree.commit().await.unwrap();

    // Root hash should be non-zero
    assert_ne!(hash, Hash::from([0u8; 32]));
    assert!(offset > 0);
}

#[tokio::test]
async fn compact() {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("compact.mst");

    let tree = AsyncMerkleSearchTree::new_temporary().unwrap();

    // Insert some keys
    for i in 0..10 {
        tree.insert(i, format!("val{}", i)).await.unwrap();
    }

    // Compact to a new file
    tree.compact(file_path.to_str().unwrap().to_string())
        .await
        .unwrap();

    // Check that keys still exist
    for i in 0..10 {
        let val = tree.get(i).await.unwrap();
        assert_eq!(val.unwrap().as_ref(), &format!("val{}", i));
    }
}

#[tokio::test]
async fn multiple_operations() {
    let tree = AsyncMerkleSearchTree::new_temporary().unwrap();

    // Insert multiple
    for i in 0..20 {
        tree.insert(i, format!("v{}", i)).await.unwrap();
    }

    // Remove even
    for i in 0..20 {
        if i % 2 == 0 {
            tree.remove(i).await.unwrap();
        }
    }

    // Check remaining
    for i in 0..20 {
        let contains = tree.contains(i).await.unwrap();
        if i % 2 == 0 {
            assert!(!contains);
        } else {
            assert!(contains);
            let val = tree.get(i).await.unwrap().unwrap();
            assert_eq!(val.as_ref(), &format!("v{}", i));
        }
    }

    // Commit after all operations
    let (_offset, _hash) = tree.commit().await.unwrap();
}
