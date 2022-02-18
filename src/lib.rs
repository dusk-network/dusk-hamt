// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

// #![no_std]

//! Hamt
use core::mem;
use core::ops::{Deref, DerefMut};

use canonical::{Canon, CanonError, Id};
use canonical_derive::Canon;

use microkelvin::{
    Annotation, Branch, BranchMut, Child, ChildMut, Compound, GenericChild,
    GenericTree, Link, Step, Walker,
};

#[derive(Clone, Debug)]
pub struct KvPair<K, V> {
    pub key: K,
    pub val: V,
}

impl<K, V> Canon for KvPair<K, V>
where
    K: Canon,
    V: Canon,
{
    fn encode(&self, sink: &mut canonical::Sink) {
        self.key.encode(sink);
        self.val.encode(sink);
    }

    fn decode(source: &mut canonical::Source) -> Result<Self, CanonError> {
        Ok(KvPair {
            key: K::decode(source)?,
            val: V::decode(source)?,
        })
    }

    fn encoded_len(&self) -> usize {
        println!("commputing key length");
        let a = self.key.encoded_len();
        println!("commputing val length");
        let b = self.val.encoded_len();
        println!("ok");
        a + b
    }
}

#[derive(Clone, Canon, Debug)]
enum Bucket<K, V, A>
where
    A: Annotation<KvPair<K, V>>,
{
    Empty,
    Leaf(KvPair<K, V>),
    Node(Link<Hamt<K, V, A>, A>),
}

#[derive(Clone, Canon, Debug)]
pub struct Hamt<K, V, A>([Bucket<K, V, A>; 4])
where
    A: Annotation<KvPair<K, V>>;

pub type Map<K, V> = Hamt<K, V, ()>;

impl<K, V, A> Compound<A> for Hamt<K, V, A>
where
    A: Annotation<KvPair<K, V>>,
    K: Canon,
    V: Canon,
    A: Canon,
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

    fn from_generic(generic: &GenericTree) -> Result<Self, CanonError> {
        let mut s = Self::default();
        for (i, child) in generic.children().iter().enumerate() {
            match child {
                GenericChild::Empty => (),
                GenericChild::Leaf(leaf) => s.0[i] = Bucket::Leaf(leaf.cast()?),
                GenericChild::Link(id, a) => {
                    s.0[i] = Bucket::Node(Link::new_persisted(*id, a.cast()?));
                }
            }
        }
        Ok(s)
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

fn slot<H>(hash: &H, depth: usize) -> usize
where
    H: AsRef<[u8]>,
{
    // calculate the slot for key at depth x
    (hash.as_ref()[depth] % 4) as usize
}

struct PathWalker<'a> {
    hash: &'a [u8; 32],
    depth: usize,
}

impl<'a> PathWalker<'a> {
    fn new(hash: &'a [u8; 32]) -> Self {
        PathWalker { hash, depth: 0 }
    }
}

impl<'a, C, A> Walker<C, A> for PathWalker<'a>
where
    C: Compound<A>,
{
    fn walk(&mut self, walk: microkelvin::Walk<C, A>) -> microkelvin::Step {
        let slot = slot(self.hash, self.depth);
        self.depth += 1;
        match walk.child(slot) {
            Child::Leaf(_) => Step::Found(slot),
            Child::Node(_) => Step::Into(slot),
            Child::Empty | Child::EndOfNode => Step::Abort,
        }
    }
}

impl<K, V, A> Hamt<K, V, A>
where
    K: Eq + Canon,
    V: Canon,
    A: Annotation<KvPair<K, V>> + Canon,
{
    /// Creates a new empty Hamt
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: K, val: V) -> Result<Option<V>, CanonError> {
        let hash = Id::new(&key).hash();
        self._insert(key, val, hash, 0)
    }

    fn _insert(
        &mut self,
        key: K,
        val: V,
        hash: [u8; 32],
        depth: usize,
    ) -> Result<Option<V>, CanonError> {
        let slot = slot(&hash, depth);
        let bucket = &mut self.0[slot];

        match bucket.take() {
            Bucket::Empty => {
                *bucket = Bucket::Leaf(KvPair { key, val });
                Ok(None)
            }
            Bucket::Leaf(KvPair {
                key: old_key,
                val: old_val,
            }) => {
                if key == old_key {
                    *bucket = Bucket::Leaf(KvPair { key, val });
                    Ok(Some(old_val))
                } else {
                    let mut new_node = Hamt::new();
                    let old_hash = Id::new(&old_key).hash();

                    new_node._insert(key, val, hash, depth + 1)?;
                    new_node._insert(old_key, old_val, old_hash, depth + 1)?;
                    *bucket = Bucket::Node(Link::new(new_node));
                    Ok(None)
                }
            }
            Bucket::Node(mut node) => {
                let result =
                    node.inner_mut()?._insert(key, val, hash, depth + 1);
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

    pub fn remove(&mut self, key: &K) -> Result<Option<V>, CanonError> {
        let hash = Id::new(key).hash();
        self._remove(key, hash, 0)
    }

    fn _remove(
        &mut self,
        key: &K,
        hash: [u8; 32],
        depth: usize,
    ) -> Result<Option<V>, CanonError> {
        let slot = slot(&hash, depth);
        let bucket = &mut self.0[slot];

        match bucket.take() {
            Bucket::Empty => Ok(None),
            Bucket::Leaf(KvPair {
                key: old_key,
                val: old_val,
            }) => {
                if *key == old_key {
                    Ok(Some(old_val))
                } else {
                    Ok(None)
                }
            }

            Bucket::Node(mut link) => {
                let mut node = link.inner_mut()?;
                let result = node._remove(key, hash, depth + 1);
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

    pub fn get<'a>(
        &'a self,
        key: &K,
    ) -> Result<Option<impl Deref<Target = V> + 'a>, CanonError> {
        let hash = Id::new(key).hash();

        Ok(Branch::walk(self, PathWalker::new(&hash))?
            .filter(|branch| &(*branch).key == key)
            .map(|b| b.map_leaf(|leaf| &leaf.val)))
    }

    pub fn get_mut<'a>(
        &'a mut self,
        key: &K,
    ) -> Result<Option<impl DerefMut<Target = V> + 'a>, CanonError> {
        let hash = Id::new(key).hash();
        Ok(BranchMut::walk(self, PathWalker::new(&hash))?
            .filter(|branch| &(*branch).key == key)
            .map(|b| b.map_leaf(|leaf| &mut leaf.val)))
    }
}
