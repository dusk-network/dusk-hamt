// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

#![no_std]

//! Hamt

pub mod annotation;

pub mod value;
use value::{Value, ValueMut};

extern crate alloc;
use alloc::boxed::Box;

use core::hash::{Hash, Hasher};
use core::mem;

use microkelvin::{
    Branch, BranchMut, Child, ChildMut, Compound, Step, Walk, Walker,
};
use ranno::{Annotated, Annotation};
use seahash::SeaHasher;

#[derive(Debug, Default, Clone, Hash)]
pub struct KvPair<K, V> {
    pub key: K,
    pub val: V,
}

#[derive(Debug)]
enum Bucket<K, V, A> {
    Empty,
    Leaf(KvPair<K, V>),
    Node(Annotated<Box<Hamt<K, V, A>>, A>),
}

impl<K, V, A> Bucket<K, V, A> {
    const fn new() -> Self {
        Self::Empty
    }

    fn take(&mut self) -> Self {
        mem::replace(self, Bucket::Empty)
    }
}

impl<K, V, A> Clone for Bucket<K, V, A>
where
    A: Annotation<Hamt<K, V, A>>,
    KvPair<K, V>: Clone,
{
    fn clone(&self) -> Self {
        match self {
            Bucket::Empty => Bucket::Empty,
            Bucket::Leaf(leaf) => Bucket::Leaf(leaf.clone()),
            Bucket::Node(node) => Bucket::Node(node.clone()),
        }
    }
}

impl<K, V, A> Default for Bucket<K, V, A>
where
    A: Annotation<Hamt<K, V, A>>,
{
    fn default() -> Self {
        Bucket::Empty
    }
}

#[derive(Debug)]
pub struct Hamt<K, V, A>([Bucket<K, V, A>; 4]);

pub type Map<K, V> = Hamt<K, V, ()>;

impl<K, V> Annotation<Map<K, V>> for () {
    fn from_child(_: &Map<K, V>) -> Self {}
}

impl<K, V, A> Compound<A> for Hamt<K, V, A> {
    type Leaf = KvPair<K, V>;

    fn child(&self, index: usize) -> Child<Self, A> {
        match self.0.get(index) {
            Some(Bucket::Empty) => Child::Empty,
            Some(Bucket::Leaf(ref kv)) => Child::Leaf(kv),
            Some(Bucket::Node(ref nd)) => Child::Node(nd),
            None => Child::EndOfNode,
        }
    }

    fn child_mut(&mut self, index: usize) -> ChildMut<Self, A> {
        match self.0.get_mut(index) {
            Some(Bucket::Empty) => ChildMut::Empty,
            Some(Bucket::Leaf(ref mut kv)) => ChildMut::Leaf(kv),
            Some(Bucket::Node(ref mut nd)) => ChildMut::Node(nd),
            None => ChildMut::EndOfNode,
        }
    }
}

impl<K, V, A> Clone for Hamt<K, V, A>
where
    A: Annotation<Hamt<K, V, A>>,
    KvPair<K, V>: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K, V, A> Default for Hamt<K, V, A>
where
    A: Annotation<Hamt<K, V, A>>,
{
    fn default() -> Self {
        Hamt(Default::default())
    }
}

#[inline(always)]
fn slot(digest: u64, depth: usize) -> usize {
    let derived = hash(&(digest + depth as u64));
    (derived % 4) as usize
}

#[inline(always)]
fn hash<T>(t: &T) -> u64
where
    T: Hash,
{
    let mut hasher = SeaHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}

struct PathWalker {
    digest: u64,
    depth: usize,
}

impl PathWalker {
    fn new(digest: u64) -> Self {
        PathWalker { digest, depth: 0 }
    }
}

impl<C, A> Walker<C, A> for PathWalker
where
    C: Compound<A>,
{
    fn walk(&mut self, walk: Walk<C, A>) -> Step {
        let slot = slot(self.digest, self.depth);
        self.depth += 1;
        match walk.child(slot) {
            Child::Leaf(_) => Step::Found(slot),
            Child::Node(_) => Step::Into(slot),
            Child::Empty | Child::EndOfNode => Step::Abort,
        }
    }
}

impl<K, V, A> Hamt<K, V, A> {
    /// Creates a new empty Hamt
    pub const fn new() -> Self {
        Self([Bucket::new(), Bucket::new(), Bucket::new(), Bucket::new()])
    }
}

impl<K, V, A> Hamt<K, V, A>
where
    K: Hash + Eq,
    A: Annotation<Hamt<K, V, A>>,
{
    pub fn insert(&mut self, key: K, val: V) -> Option<V> {
        let digest = hash(&key);
        self._insert(key, val, digest, 0)
    }

    fn _insert(
        &mut self,
        key: K,
        val: V,
        digest: u64,
        depth: usize,
    ) -> Option<V> {
        let slot = slot(digest, depth);
        let bucket = &mut self.0[slot];

        match bucket.take() {
            Bucket::Empty => {
                *bucket = Bucket::Leaf(KvPair { key, val });
                None
            }
            Bucket::Leaf(KvPair {
                key: old_key,
                val: old_val,
            }) => {
                if key == old_key {
                    *bucket = Bucket::Leaf(KvPair { key, val });
                    Some(old_val)
                } else {
                    let mut new_node = Hamt::new();
                    let old_hash = hash(&old_key);

                    new_node._insert(key, val, digest, depth + 1);
                    new_node._insert(old_key, old_val, old_hash, depth + 1);

                    let annotated = Annotated::new(Box::new(new_node));
                    *bucket = Bucket::Node(annotated);

                    None
                }
            }
            Bucket::Node(mut node) => {
                let result =
                    node.child_mut()._insert(key, val, digest, depth + 1);
                *bucket = Bucket::Node(node);
                result
            }
        }
    }

    /// Collapse node into a leaf if singleton
    fn collapse(&mut self) -> Option<KvPair<K, V>> {
        match &mut self.0 {
            [leaf @ Bucket::Leaf(..), Bucket::Empty, Bucket::Empty, Bucket::Empty]
            | [Bucket::Empty, leaf @ Bucket::Leaf(..), Bucket::Empty, Bucket::Empty]
            | [Bucket::Empty, Bucket::Empty, leaf @ Bucket::Leaf(..), Bucket::Empty]
            | [Bucket::Empty, Bucket::Empty, Bucket::Empty, leaf @ Bucket::Leaf(..)] => {
                if let Bucket::Leaf(pair @ KvPair { .. }) =
                    mem::replace(leaf, Bucket::Empty)
                {
                    Some(pair)
                } else {
                    unreachable!("Match above guarantees a `Bucket::Leaf`")
                }
            }
            _ => None,
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let digest = hash(key);
        self._remove(key, digest, 0)
    }

    fn _remove(&mut self, key: &K, digest: u64, depth: usize) -> Option<V> {
        let slot = slot(digest, depth);
        let bucket = &mut self.0[slot];

        match bucket.take() {
            Bucket::Empty => None,
            Bucket::Leaf(KvPair {
                key: old_key,
                val: old_val,
            }) => {
                if *key == old_key {
                    Some(old_val)
                } else {
                    None
                }
            }

            Bucket::Node(mut annotated) => {
                let mut child = annotated.child_mut();
                let node = &mut *child;

                let result = node._remove(key, digest, depth + 1);
                // since we moved the bucket with `take()`, we need to put it
                // back.
                if let Some(pair) = node.collapse() {
                    *bucket = Bucket::Leaf(KvPair {
                        key: pair.key,
                        val: pair.val,
                    });
                } else {
                    *bucket = Bucket::Node(annotated);
                }
                result
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<Value<K, V, A>> {
        let digest = hash(key);

        Branch::walk(self, PathWalker::new(digest))
            .filter(|branch| &branch.key == key)
            .map(From::from)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<K, V, A>> {
        let digest = hash(key);

        BranchMut::walk(self, PathWalker::new(digest))
            .filter(|branch| &branch.key == key)
            .map(From::from)
    }
}
