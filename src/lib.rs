// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

//! Hamt
use core::mem;
use core::ops::{Deref, DerefMut};

use canonical::{Canon, CanonError, Id};
use canonical_derive::Canon;

use microkelvin::{
    Annotated, Branch, BranchMut, Child, ChildMut, Combine, Compound,
};

#[derive(Clone, Canon, Debug)]
pub struct KvPair<K, V>(K, V);

#[derive(Clone, Canon, Debug)]
enum Bucket<K, V, A> {
    Empty,
    Leaf(KvPair<K, V>),
    Node(Annotated<Hamt<K, V, A>, A>),
}

#[derive(Clone, Canon, Debug)]
pub struct Hamt<K, V, A>([Bucket<K, V, A>; 4]);

impl<K, V, A> Compound<A> for Hamt<K, V, A>
where
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
}

impl<K, V, A> Bucket<K, V, A> {
    fn take(&mut self) -> Self {
        mem::replace(self, Bucket::Empty)
    }
}

impl<K, V, A> Default for Bucket<K, V, A> {
    fn default() -> Self {
        Bucket::Empty
    }
}

impl<K, V, A> Default for Hamt<K, V, A> {
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

impl<K, V, A> Hamt<K, V, A>
where
    K: Eq + Canon,
    V: Canon,
    A: Combine<Self, A> + Canon,
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
                *bucket = Bucket::Leaf(KvPair(key, val));
                Ok(None)
            }
            Bucket::Leaf(KvPair(old_key, old_val)) => {
                if key == old_key {
                    *bucket = Bucket::Leaf(KvPair(key, val));
                    Ok(Some(old_val))
                } else {
                    let mut new_node = Hamt::new();
                    let old_hash = Id::new(&old_key).hash();

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

    /// Collapse node into a leaf if singleton
    fn collapse(&mut self) -> Option<(K, V)> {
        match &mut self.0 {
            [leaf @ Bucket::Leaf(..), Bucket::Empty, Bucket::Empty, Bucket::Empty]
            | [Bucket::Empty, leaf @ Bucket::Leaf(..), Bucket::Empty, Bucket::Empty]
            | [Bucket::Empty, Bucket::Empty, leaf @ Bucket::Leaf(..), Bucket::Empty]
            | [Bucket::Empty, Bucket::Empty, Bucket::Empty, leaf @ Bucket::Leaf(..)] => {
                if let Bucket::Leaf(KvPair(key, val)) =
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
            Bucket::Leaf(KvPair(old_key, old_val)) => {
                if *key == old_key {
                    Ok(Some(old_val))
                } else {
                    Ok(None)
                }
            }

            Bucket::Node(mut annotated) => {
                let mut node = annotated.val_mut()?;
                let result = node._remove(key, hash, depth + 1);
                // since we moved the bucket with `take()`, we need to put it back.
                if let Some((key, val)) = node.collapse() {
                    *bucket = Bucket::Leaf(KvPair(key, val));
                } else {
                    drop(node);
                    *bucket = Bucket::Node(annotated);
                }
                result
            }
        }
    }

    #[cfg(test)]
    fn correct_empty_state(&self) -> bool {
        match self.0 {
            [Bucket::Empty, Bucket::Empty, Bucket::Empty, Bucket::Empty] => {
                true
            }
            _ => false,
        }
    }

    pub fn get<'a>(
        &'a self,
        key: &K,
    ) -> Result<Option<impl Deref<Target = V> + 'a>, CanonError> {
        let hash = Id::new(key).hash();
        let mut depth = 0;
        Ok(Branch::path(self, || {
            let ofs = slot(&hash, depth);
            depth += 1;
            ofs
        })?
        .filter(|branch| &(*branch).0 == key)
        .map(|b| b.map_leaf(|leaf| &leaf.1)))
    }

    pub fn get_mut<'a>(
        &'a mut self,
        key: &K,
    ) -> Result<Option<impl DerefMut<Target = V> + 'a>, CanonError> {
        let hash = Id::new(key).hash();
        let mut depth = 0;
        Ok(BranchMut::path(self, || {
            let ofs = slot(&hash, depth);
            depth += 1;
            ofs
        })?
        .filter(|branch| &(*branch).0 == key)
        .map(|b| b.map_leaf_mut(|leaf| &mut leaf.1)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use microkelvin::{Cardinality, Nth};

    #[test]
    fn trivial() {
        let mut hamt = Hamt::<u32, u32, ()>::new();
        assert_eq!(hamt.remove(&0).unwrap(), None);
    }

    #[test]
    fn replace() {
        let mut hamt = Hamt::<u32, u32, ()>::new();
        assert_eq!(hamt.insert(0, 38).unwrap(), None);
        assert_eq!(hamt.insert(0, 0).unwrap(), Some(38));
    }

    #[test]
    fn multiple() {
        let n = 1024;

        let mut hamt = Hamt::<_, _, ()>::new();

        for i in 0..n {
            hamt.insert(i, i).unwrap();
        }

        for i in 0..n {
            assert_eq!(hamt.remove(&i).unwrap(), Some(i));
        }

        assert!(hamt.correct_empty_state());
    }

    #[test]
    fn insert_get() {
        let n = 1024;

        let mut hamt = Hamt::<_, _, ()>::new();

        for i in 0..n {
            hamt.insert(i, i).unwrap();
        }

        for i in 0..n {
            assert_eq!(*hamt.get(&i).unwrap().unwrap(), i);
        }
    }

    #[test]
    fn nth() {
        let n: u64 = 1024;

        let mut hamt = Hamt::<_, _, Cardinality>::new();

        let mut result: Vec<u64> = vec![];
        let mut sorted = vec![];

        for i in 0..n {
            hamt.insert(i, i).unwrap();
        }

        for i in 0..n {
            let res = hamt.nth(i).unwrap();
            result.push(res.unwrap().1);
            sorted.push(i);
        }

        result.sort();

        assert_eq!(result, sorted);
    }

    #[test]
    fn insert_get_mut() {
        let n = 1024;

        let mut hamt = Hamt::<_, _, ()>::new();

        for i in 0..n {
            hamt.insert(i, i).unwrap();
        }

        for i in 0..n {
            *hamt.get_mut(&i).unwrap().unwrap() += 1;
        }

        for i in 0..n {
            assert_eq!(*hamt.get(&i).unwrap().unwrap(), i + 1);
        }
    }
}

#[test]
fn iterate() {
    let n: u64 = 1024;

    use microkelvin::{Cardinality, Nth};

    let mut hamt = Hamt::<_, _, Cardinality>::new();

    let mut reference = vec![];
    let mut from_iter = vec![];

    for i in 0..n {
        hamt.insert(i, i).unwrap();
        reference.push(i);
    }

    for leaf in hamt.nth(0).unwrap().unwrap() {
        let val = leaf.unwrap().1;
        from_iter.push(val);
    }

    reference.sort();
    from_iter.sort();

    assert_eq!(from_iter, reference)
}
