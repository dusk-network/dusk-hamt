// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

//! Hamt
use core::mem;

use canonical::{Canon, Store};
use canonical_derive::Canon;

use microkelvin::{Annotated, Annotation, Child, ChildMut, Compound};

#[derive(Clone, Canon, Debug)]
enum Bucket<K, V, A, S>
where
    K: Canon<S>,
    V: Canon<S>,
    A: Canon<S>,
    S: Store,
{
    Empty,
    Leaf(K, V),
    Node(Annotated<Hamt<K, V, A, S>, S>),
}

#[derive(Clone, Canon, Debug)]
pub struct Hamt<K, V, A, S>([Bucket<K, V, A, S>; 4])
where
    K: Canon<S>,
    V: Canon<S>,
    A: Canon<S>,
    S: Store;

impl<K, V, A, S> Compound<S> for Hamt<K, V, A, S>
where
    K: Canon<S>,
    V: Canon<S>,
    A: Canon<S>,
    S: Store,
{
    type Leaf = (K, V);
    type Annotation = A;

    fn child(&self, _ofs: usize) -> Child<Self, S> {
        todo!()
    }

    fn child_mut(&mut self, _ofs: usize) -> ChildMut<Self, S> {
        todo!()
    }
}

impl<K, V, A, S> Bucket<K, V, A, S>
where
    K: Canon<S>,
    V: Canon<S>,
    A: Canon<S>,
    S: Store,
{
    fn take(&mut self) -> Self {
        mem::replace(self, Bucket::Empty)
    }
}

impl<K, V, A, S> Default for Bucket<K, V, A, S>
where
    K: Canon<S>,
    V: Canon<S>,
    A: Canon<S>,
    S: Store,
{
    fn default() -> Self {
        Bucket::Empty
    }
}

impl<K, V, A, S> Default for Hamt<K, V, A, S>
where
    K: Canon<S>,
    V: Canon<S>,
    A: Canon<S>,
    S: Store,
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

impl<K, V, A, S> Hamt<K, V, A, S>
where
    K: Canon<S> + Eq,
    V: Canon<S>,
    A: Canon<S>,
    S: Store,
{
    /// Creates a new empty Hamt
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: K, val: V) -> Result<Option<V>, S::Error>
    where
        A: Annotation<Self, S>,
    {
        let hash = S::ident(&key);
        self._insert(key, val, hash, 0)
    }

    fn _insert(
        &mut self,
        key: K,
        val: V,
        hash: S::Ident,
        depth: usize,
    ) -> Result<Option<V>, S::Error>
    where
        A: Annotation<Self, S>,
    {
        let slot = slot(&hash, depth);
        let bucket = &mut self.0[slot];

        match bucket.take() {
            Bucket::Empty => {
                *bucket = Bucket::Leaf(key, val);
                Ok(None)
            }
            Bucket::Leaf(old_key, old_val) => {
                if key == old_key {
                    *bucket = Bucket::Leaf(key, val);
                    Ok(Some(old_val))
                } else {
                    let mut new_node = Hamt::<_, _, A, S>::new();
                    let old_hash = S::ident(&old_key);

                    new_node._insert(key, val, hash, depth + 1)?;
                    new_node._insert(old_key, old_val, old_hash, depth + 1)?;
                    *bucket = Bucket::Node(Annotated::new(new_node));
                    Ok(None)
                }
            }
            Bucket::Node(mut node) => {
                let result = node.val_mut()?._insert(key, val, hash, depth + 1);
                // since we moved the bucket with `take()`, we need to put it back.
                *bucket = Bucket::Node(node);
                result
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> Result<Option<V>, S::Error>
    where
        A: Annotation<Self, S>,
    {
        let hash = S::ident(key);
        self._remove(key, hash, 0)
    }

    fn _remove(
        &mut self,
        key: &K,
        hash: S::Ident,
        depth: usize,
    ) -> Result<Option<V>, S::Error>
    where
        A: Annotation<Self, S>,
    {
        let slot = slot(&hash, depth);
        let bucket = &mut self.0[slot];

        match bucket.take() {
            Bucket::Empty => Ok(None),
            Bucket::Leaf(old_key, old_val) => {
                if *key == old_key {
                    Ok(Some(old_val))
                } else {
                    Ok(None)
                }
            }

            Bucket::Node(mut node) => {
                let result = node.val_mut()?._remove(key, hash, depth + 1);
                // since we moved the bucket with `take()`, we need to put it back.
                *bucket = Bucket::Node(node);
                result
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use canonical_host::MemStore;

    #[test]
    fn trivial() {
        let mut nt = Hamt::<u32, u32, (), MemStore>::new();
        assert_eq!(nt.remove(&0).unwrap(), None);
    }

    #[test]
    fn replace() {
        let mut nt = Hamt::<u32, u32, (), MemStore>::new();
        assert_eq!(nt.insert(0, 38).unwrap(), None);
        assert_eq!(nt.insert(0, 0).unwrap(), Some(38));
    }

    #[test]
    fn insert_remove() {
        let mut nt = Hamt::<_, _, (), MemStore>::new();
        nt.insert(8, 8).unwrap();
        assert_eq!(nt.remove(&8).unwrap(), Some(8));
    }

    #[test]
    fn double() {
        let mut nt = Hamt::<_, _, (), MemStore>::new();
        nt.insert(0, 0).unwrap();
        nt.insert(1, 1).unwrap();
        assert_eq!(nt.remove(&1).unwrap(), Some(1));
        assert_eq!(nt.remove(&0).unwrap(), Some(0));
    }

    #[test]
    fn multiple() {
        let n = 32;

        let mut nt = Hamt::<_, _, (), MemStore>::new();

        for i in 0..n {
            nt.insert(i, i).unwrap();
        }

        for i in 0..n {
            assert_eq!(nt.remove(&i).unwrap(), Some(i));
        }
    }
}
