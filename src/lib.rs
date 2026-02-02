#![doc = include_str!("../README.md")]
#![cfg_attr(test, feature(test))]

#[cfg(test)]
extern crate test;

#[cfg(test)]
mod benches;
#[cfg(test)]
mod tests;

mod node;
mod store;
mod tree;

pub use tree::MerkleSearchTree;

use serde::{Deserialize, Serialize};

pub type Hash = [u8; 32];
pub(crate) type NodeId = u64;
pub(crate) const PAGE_SIZE: u64 = 4096;

/// A trait for types that can serve as keys.
pub trait MerkleKey: Ord + std::fmt::Debug + Serialize + for<'a> Deserialize<'a> {}
impl<T> MerkleKey for T where T: Ord + std::fmt::Debug + Serialize + for<'a> Deserialize<'a> {}

/// A trait for types that can serve as values.
pub trait MerkleValue: std::fmt::Debug + Serialize + for<'a> Deserialize<'a> {}
impl<T> MerkleValue for T where T: std::fmt::Debug + Serialize + for<'a> Deserialize<'a> {}
