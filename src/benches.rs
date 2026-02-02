use super::*;
use test::Bencher;

fn generate_key(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn generate_value(i: u64) -> u64 {
    i
}

/// Helper to populate a tree with `count` items
fn setup_tree(count: u64) -> MerkleSearchTree<Vec<u8>, u64> {
    let mut tree = MerkleSearchTree::new_temporary().unwrap();
    for i in 0..count {
        tree.insert(generate_key(i), generate_value(i)).unwrap();
    }
    tree
}

#[bench]
fn insert_into_empty(b: &mut Bencher) {
    b.iter(|| {
        let mut tree = MerkleSearchTree::new_temporary().unwrap();
        let key = generate_key(1);
        let val = generate_value(1);
        test::black_box(tree.insert(key, val)).unwrap();
    });
}

#[bench]
fn insert_into_populated_1k(b: &mut Bencher) {
    let mut tree = setup_tree(1_000);
    let mut i = 1_000;

    b.iter(|| {
        let key = generate_key(i);
        let val = generate_value(i);
        i += 1;
        test::black_box(tree.insert(key, val)).unwrap();
    });
}

#[bench]
fn insert_into_populated_10k(b: &mut Bencher) {
    let mut tree = setup_tree(10_000);
    let mut i = 10_000;

    b.iter(|| {
        let key = generate_key(i);
        let val = generate_value(i);
        i += 1;
        test::black_box(tree.insert(key, val)).unwrap();
    });
}

#[bench]
fn contains_hit(b: &mut Bencher) {
    let tree = setup_tree(10_000);
    let key = generate_key(5_000);

    b.iter(|| {
        test::black_box(tree.contains(&key)).unwrap();
    });
}

#[bench]
fn get_hit(b: &mut Bencher) {
    let tree = setup_tree(10_000);
    let key = generate_key(5_000);

    b.iter(|| {
        test::black_box(tree.get(&key)).unwrap();
    });
}

#[bench]
fn contains_miss(b: &mut Bencher) {
    let tree = setup_tree(10_000);
    let key = generate_key(99_999);

    b.iter(|| {
        test::black_box(tree.contains(&key)).unwrap();
    });
}

#[bench]
fn remove_present(b: &mut Bencher) {
    let mut tree = setup_tree(10_000);
    let key = generate_key(99_999);
    let val = generate_value(99_999);

    b.iter(|| {
        tree.insert(key.clone(), val).unwrap();
        test::black_box(tree.remove(&key)).unwrap();
    });
}

#[bench]
fn remove_missing(b: &mut Bencher) {
    let mut tree = setup_tree(10_000);
    let key = generate_key(99_999);

    b.iter(|| {
        test::black_box(tree.remove(&key)).unwrap();
    });
}

#[bench]
fn root_hash(b: &mut Bencher) {
    let tree = setup_tree(100);
    b.iter(|| {
        test::black_box(tree.root_hash());
    });
}

#[bench]
fn flush_no_changes(b: &mut Bencher) {
    let mut tree = setup_tree(1_000);
    tree.commit().unwrap();

    b.iter(|| {
        test::black_box(tree.commit()).unwrap();
    });
}
