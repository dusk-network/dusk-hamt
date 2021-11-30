// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

#![no_std]

//! Hamt
use core::borrow::Borrow;
use core::hash::{Hash, Hasher};
use core::mem;

use microkelvin::{
    Annotation, ArchivedChild, ArchivedCompound, Child, ChildMut, Compound,
    Discriminant, Keyed, Link, MappedBranch, MappedBranchMut, MaybeArchived,
    Step, Store, Stored, Walkable, Walker,
};
use rkyv::{Archive, Deserialize, Serialize};
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

pub trait Lookup<C, K, V, A, S>
where
    C: Compound<A, S, Leaf = KvPair<K, V>>,
    K: Archive<Archived = K>,
    V: Archive,
    S: Store,
{
    fn get<'a>(
        &'a self,
        key: &K,
    ) -> Option<MappedBranch<'a, C, A, S, MaybeArchived<V>>>;

    fn get_mut<'a>(
        &'a mut self,
        key: &K,
    ) -> Option<MappedBranchMut<'a, C, A, S, V>>;
}

impl<K, V, A, S> Lookup<Self, K, V, A, S> for Hamt<K, V, A, S>
where
    K: Archive<Archived = K> + Hash,
    V: Archive,
    A: Annotation<KvPair<K, V>>,
    S: Store,
{
    fn get<'a>(
        &'a self,
        key: &K,
    ) -> Option<MappedBranch<'a, Self, A, S, MaybeArchived<V>>> {
        self.walk(PathWalker::new(hash(key))).map(|branch| {
            branch.map_leaf(|kv| match kv {
                MaybeArchived::Memory(kv) => MaybeArchived::Memory(kv.value()),
                MaybeArchived::Archived(kv) => {
                    MaybeArchived::Archived(kv.value())
                }
            })
        })
    }

    fn get_mut(&mut self, _key: &K) -> Option<MappedBranchMut<Self, A, S, V>> {
        todo!()
    }
}

impl<C, K, V, A, S> Lookup<C, K, V, A, S> for Stored<C, S>
where
    K: Archive<Archived = K>,
    V: Archive,
    C: Compound<A, S, Leaf = KvPair<K, V>>,
    S: Store,
{
    fn get(&self, _key: &K) -> Option<MappedBranch<C, A, S, MaybeArchived<V>>> {
        todo!()
    }

    fn get_mut(&mut self, _key: &K) -> Option<MappedBranchMut<C, A, S, V>> {
        todo!()
    }
}

#[derive(Clone, Serialize, Archive, Deserialize)]
#[archive(bound(serialize = "
  A: Archive + Clone + Annotation<KvPair<K, V>>,
  K: Archive,
  V: Archive,
  S: Store<Storage = __S>,"))]
#[archive(bound(deserialize = "
  KvPair<K, V>: Archive + Clone,
  <KvPair<K, V> as Archive>::Archived: Deserialize<KvPair<K, V>, S>,
  A: Clone + Annotation<KvPair<K, V>>,
  for<'a> &'a mut __D: Borrow<S>,
  __D: Store,"))]
pub enum Bucket<K, V, A, S>
where
    S: Store,
{
    Empty,
    Leaf(KvPair<K, V>),
    Node(#[omit_bounds] Link<Hamt<K, V, A, S>, A, S>),
}

#[derive(Clone, Archive, Serialize, Deserialize)]
pub struct Hamt<K, V, A, S>([Bucket<K, V, A, S>; 4])
where
    S: Store;

impl<K, V, A, S> Compound<A, S> for Hamt<K, V, A, S>
where
    S: Store,
    K: Archive,
    V: Archive,
    A: Annotation<KvPair<K, V>>,
{
    type Leaf = KvPair<K, V>;

    fn child(&self, ofs: usize) -> Child<Self, A, S> {
        match self.0.get(ofs) {
            Some(Bucket::Empty) => Child::Empty,
            Some(Bucket::Leaf(ref kv)) => Child::Leaf(kv),
            Some(Bucket::Node(ref nd)) => Child::Link(nd),
            None => Child::End,
        }
    }

    fn child_mut(&mut self, ofs: usize) -> ChildMut<Self, A, S> {
        match self.0.get_mut(ofs) {
            Some(Bucket::Empty) => ChildMut::Empty,
            Some(Bucket::Leaf(ref mut kv)) => ChildMut::Leaf(kv),
            Some(Bucket::Node(ref mut nd)) => ChildMut::Link(nd),
            None => ChildMut::End,
        }
    }
}

impl<K, V, A, S> ArchivedCompound<Hamt<K, V, A, S>, A, S>
    for ArchivedHamt<K, V, A, S>
where
    K: Archive,
    V: Archive,
    A: Annotation<KvPair<K, V>>,
    S: Store,
{
    fn child(&self, ofs: usize) -> ArchivedChild<Hamt<K, V, A, S>, A, S> {
        match self.0.get(ofs) {
            Some(ArchivedBucket::Leaf(l)) => ArchivedChild::Leaf(l),
            Some(ArchivedBucket::Node(n)) => ArchivedChild::Link(n),
            Some(ArchivedBucket::Empty) => ArchivedChild::Empty,
            None => ArchivedChild::End,
        }
    }
}

impl<K, V, A, S> Bucket<K, V, A, S>
where
    A: Annotation<KvPair<K, V>>,
    S: Store,
{
    fn take(&mut self) -> Self {
        mem::replace(self, Bucket::Empty)
    }
}

impl<K, V, A, S> Default for Bucket<K, V, A, S>
where
    A: Annotation<KvPair<K, V>>,
    S: Store,
{
    fn default() -> Self {
        Bucket::Empty
    }
}

impl<K, V, A, S> Default for Hamt<K, V, A, S>
where
    A: Annotation<KvPair<K, V>>,
    S: Store,
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

/// A walker
pub struct PathWalker {
    digest: u64,
    depth: usize,
}

impl PathWalker {
    fn new(digest: u64) -> Self {
        PathWalker { digest, depth: 0 }
    }

    fn from_key<K: Hash>(key: &K) -> Self {
        PathWalker {
            digest: hash(key),
            depth: 0,
        }
    }
}

impl<'a, C, A, S> Walker<C, A, S> for PathWalker
where
    C: Compound<A, S> + Archive,
    C::Archived: ArchivedCompound<C, A, S>,
    C::Leaf: Archive,
    A: Annotation<C::Leaf>,
    S: Store,
{
    fn walk(&mut self, level: impl Walkable<C, A, S>) -> Step {
        let slot = slot(self.digest, self.depth);
        self.depth += 1;
        match level.probe(slot) {
            Discriminant::Leaf(_) | Discriminant::Annotation(_) => {
                Step::Found(slot)
            }
            Discriminant::Empty | Discriminant::End => Step::Abort,
        }
    }
}

impl<K, V, A, S> Hamt<K, V, A, S>
where
    K: Archive<Archived = K> + Clone + Eq + Hash,
    V: Archive + Clone,
    A: Annotation<KvPair<K, V>>,
    Self: Archive,
    <Hamt<K, V, A, S> as Archive>::Archived:
        ArchivedCompound<Self, A, S> + Deserialize<Self, S>,
    S: Store,
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
}
