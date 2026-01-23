use super::*;
use test::Bencher;

fn generate_key(i: u64) -> Vec<u8> {
    // Use Big Endian so byte lexicographical order matches integer order
    i.to_be_bytes().to_vec()
}

/// Helper to populate a tree with `count` items
fn setup_tree(count: u64) -> MerkleSearchTree<Vec<u8>> {
    let mut tree = MerkleSearchTree::new_temporary().unwrap();
    for i in 0..count {
        tree.insert(generate_key(i)).unwrap();
    }
    tree
}

#[bench]
fn insert_into_empty(b: &mut Bencher) {
    b.iter(|| {
        // We create a fresh tree every time to measure the "cold start" overhead
        let mut tree = MerkleSearchTree::new_temporary().unwrap();
        let key = generate_key(1);
        test::black_box(tree.insert(key)).unwrap();
    });
}

#[bench]
fn insert_into_populated_1k(b: &mut Bencher) {
    let mut tree = setup_tree(1_000);
    // Start inserting from the end of the current range
    let mut i = 1_000;

    b.iter(|| {
        let key = generate_key(i);
        i += 1;
        test::black_box(tree.insert(key)).unwrap();
    });
}

#[bench]
fn insert_into_populated_10k(b: &mut Bencher) {
    let mut tree = setup_tree(10_000);
    let mut i = 10_000;

    b.iter(|| {
        let key = generate_key(i);
        i += 1;
        test::black_box(tree.insert(key)).unwrap();
    });
}

#[bench]
fn contains_hit(b: &mut Bencher) {
    let tree = setup_tree(10_000);
    let key = generate_key(5_000); // Key in the middle

    b.iter(|| {
        test::black_box(tree.contains(&key)).unwrap();
    });
}

#[bench]
fn contains_miss(b: &mut Bencher) {
    let tree = setup_tree(10_000);
    let key = generate_key(99_999); // Key outside range

    b.iter(|| {
        test::black_box(tree.contains(&key)).unwrap();
    });
}

#[bench]
fn remove_present(b: &mut Bencher) {
    let mut tree = setup_tree(10_000);
    let key = generate_key(99_999);

    // To verify removal cost repeatedly, we must insert then remove.
    // This benchmarks the pair (Insert + Delete).
    b.iter(|| {
        tree.insert(key.clone()).unwrap();
        test::black_box(tree.remove(&key)).unwrap();
    });
}

#[bench]
fn remove_missing(b: &mut Bencher) {
    let mut tree = setup_tree(10_000);
    let key = generate_key(99_999);

    // Benchmarking purely the traversal to find nothing to delete
    b.iter(|| {
        test::black_box(tree.remove(&key)).unwrap();
    });
}

#[bench]
fn root_hash(b: &mut Bencher) {
    let tree = setup_tree(100);
    // Accessing the cached hash should be instant
    b.iter(|| {
        test::black_box(tree.root_hash());
    });
}

#[bench]
fn flush_no_changes(b: &mut Bencher) {
    let mut tree = setup_tree(1_000);
    // Ensure initial state is clean
    tree.flush().unwrap();

    // Measure overhead of calling flush when nothing is dirty.
    // This tests the efficiency of the dirty checking logic.
    b.iter(|| {
        test::black_box(tree.flush()).unwrap();
    });
}
