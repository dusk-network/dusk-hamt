// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

// #![no_std]

//! Hamt
use core::hash::{Hash, Hasher};
use core::mem;
use core::ops::DerefMut;

use microkelvin::{
    AWrap, Annotation, ArchivedChild, ArchivedCompound, Child, ChildMut,
    Compound, Keyed, Link, MappedBranch, Slot, Slots, Step, Walker,
};
use rkyv::{Archive, Deserialize, Infallible, Serialize};
use seahash::SeaHasher;

#[derive(Clone, Debug, Archive, Serialize, Deserialize)]
pub struct KvPair<K, V> {
    key: K,
    val: V,
}

impl<K, V> KvPair<K, V> {
    pub fn value(&self) -> &V {
        &self.val
    }
}

impl<K, V> ArchivedKvPair<K, V>
where
    K: Archive,
    V: Archive,
{
    pub fn value(&self) -> &V::Archived {
        &self.val
    }
}

impl<K, V> Keyed<K> for KvPair<K, V> {
    fn key(&self) -> &K {
        &self.key
    }
}

impl<K, V> Keyed<K> for ArchivedKvPair<K, V>
where
    K: Archive<Archived = K>,
    V: Archive,
{
    fn key(&self) -> &K {
        &self.key
    }
}

#[derive(Clone, Debug, Serialize, Archive, Deserialize)]
pub enum Bucket<K, V, A>
where
    A: Annotation<KvPair<K, V>>,
{
    Empty,
    Leaf(KvPair<K, V>),
    Node(Link<Hamt<K, V, A>, A>),
}

#[derive(Clone, Debug, Archive, Serialize, Deserialize)]
pub struct Hamt<K, V, A>([Bucket<K, V, A>; 4])
where
    A: Annotation<KvPair<K, V>>;

pub type Map<K, V> = Hamt<K, V, ()>;

impl<K, V, A> Compound<A> for Hamt<K, V, A>
where
    K: Archive,
    V: Archive,
    A: Annotation<KvPair<K, V>>,
{
    type Leaf = KvPair<K, V>;

    fn child(&self, ofs: usize) -> Child<Self, A> {
        match self.0.get(ofs) {
            Some(Bucket::Empty) => Child::Empty,
            Some(Bucket::Leaf(ref kv)) => Child::Leaf(kv),
            Some(Bucket::Node(ref nd)) => Child::Node(nd),
            None => Child::EndOfNode,
        }
    }

    fn child_mut(&mut self, ofs: usize) -> ChildMut<Self, A> {
        match self.0.get_mut(ofs) {
            Some(Bucket::Empty) => ChildMut::Empty,
            Some(Bucket::Leaf(ref mut kv)) => ChildMut::Leaf(kv),
            Some(Bucket::Node(ref mut nd)) => ChildMut::Node(nd),
            None => ChildMut::EndOfNode,
        }
    }
}

impl<K, V, A> ArchivedCompound<Hamt<K, V, A>, A> for ArchivedHamt<K, V, A>
where
    K: Archive,
    V: Archive,
    A: Annotation<KvPair<K, V>>,
{
    fn child(&self, ofs: usize) -> ArchivedChild<Hamt<K, V, A>, A> {
        match self.0.get(ofs) {
            Some(ArchivedBucket::Leaf(l)) => ArchivedChild::Leaf(l),
            Some(ArchivedBucket::Node(n)) => ArchivedChild::Node(n),
            Some(ArchivedBucket::Empty) => ArchivedChild::Empty,
            None => ArchivedChild::EndOfNode,
        }
    }
}

impl<K, V, A> Bucket<K, V, A>
where
    A: Annotation<KvPair<K, V>>,
{
    fn take(&mut self) -> Self {
        mem::replace(self, Bucket::Empty)
    }
}

impl<K, V, A> Default for Bucket<K, V, A>
where
    A: Annotation<KvPair<K, V>>,
{
    fn default() -> Self {
        Bucket::Empty
    }
}

impl<K, V, A> Default for Hamt<K, V, A>
where
    A: Annotation<KvPair<K, V>>,
{
    fn default() -> Self {
        Hamt(Default::default())
    }
}

#[inline(always)]
fn slot(from: u64, depth: usize) -> usize {
    let derived = hash(&(from + depth as u64));
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

impl<'a, C, A> Walker<C, A> for PathWalker
where
    C: Compound<A> + Archive,
    C::Archived: ArchivedCompound<C, A>,
    A: Annotation<C::Leaf>,
{
    fn walk(&mut self, slots: impl Slots<C, A>) -> microkelvin::Step {
        let slot = slot(self.digest, self.depth);
        self.depth += 1;
        match slots.slot(slot) {
            Slot::Leaf(_) | Slot::ArchivedLeaf(_) | Slot::Annotation(_) => {
                Step::Found(slot)
            }
            Slot::Empty | Slot::End => Step::Abort,
        }
    }
}

impl<K, V, A> Hamt<K, V, A>
where
    K: Archive<Archived = K> + Clone + Eq + Hash,
    V: Archive + Clone,
    A: Annotation<KvPair<K, V>>,
    Self: Archive,
    <Hamt<K, V, A> as Archive>::Archived:
        ArchivedCompound<Self, A> + Deserialize<Self, Infallible>,
{
    /// Creates a new empty Hamt
    pub fn new() -> Self {
        Self::default()
    }

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
                    let old_digest = hash(&old_key);

                    new_node._insert(key, val, digest, depth + 1);
                    new_node._insert(old_key, old_val, old_digest, depth + 1);
                    *bucket = Bucket::Node(Link::new(new_node));
                    None
                }
            }
            Bucket::Node(mut node) => {
                let result =
                    node.inner_mut()._insert(key, val, digest, depth + 1);
                // since we moved the bucket with `take()`, we need to put it back.
                *bucket = Bucket::Node(node);
                result
            }
        }
    }

    /// Collapse node into a leaf if singleton
    fn collapse(&mut self) -> Option<(K, V)> {
        match &mut self.0 {
            [leaf @ Bucket::Leaf(..), Bucket::Empty, Bucket::Empty, Bucket::Empty]
            | [Bucket::Empty, leaf @ Bucket::Leaf(..), Bucket::Empty, Bucket::Empty]
            | [Bucket::Empty, Bucket::Empty, leaf @ Bucket::Leaf(..), Bucket::Empty]
            | [Bucket::Empty, Bucket::Empty, Bucket::Empty, leaf @ Bucket::Leaf(..)] => {
                if let Bucket::Leaf(KvPair { key, val }) =
                    mem::replace(leaf, Bucket::Empty)
                {
                    Some((key, val))
                } else {
                    unreachable!("Match above guarantees a `Bucket::Leaf`")
                }
            }
            _ => None,
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let mut hasher = SeaHasher::new();
        key.hash(&mut hasher);
        let digest = hasher.finish();
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

            Bucket::Node(mut link) => {
                let node = link.inner_mut();
                let result = node._remove(key, digest, depth + 1);
                // since we moved the bucket with `take()`, we need to put it back.
                if let Some((key, val)) = node.collapse() {
                    *bucket = Bucket::Leaf(KvPair { key, val });
                } else {
                    drop(node);
                    *bucket = Bucket::Node(link);
                }
                result
            }
        }
    }

    pub fn get<'a>(&'a self, key: &K) -> Option<MappedBranch<Self, A, V>> {
        self.walk(PathWalker::new(hash(key)))
            .filter(|branch| branch.leaf().key() == key)
            .map(|b| {
                b.map_leaf(|leaf| match leaf {
                    AWrap::Memory(kv) => AWrap::Memory(&kv.val),
                    AWrap::Archived(kv) => AWrap::Archived(&kv.val),
                })
            })
    }

    pub fn get_mut<'a>(
        &'a mut self,
        key: &K,
    ) -> Option<impl DerefMut<Target = V> + 'a> {
        let hash = hash(key);
        self.walk_mut(PathWalker::new(hash))
            .filter(|branch| &(*branch).key == key)
            .map(|b| b.map_leaf(|leaf| &mut leaf.val))
    }
}
