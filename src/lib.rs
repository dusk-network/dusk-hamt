// This Source Code Form is subject to the terms of the Mozilla Public
// Liycense, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

//! Hamt
use core::mem;
use core::ops::{Deref, DerefMut};

use canonical::{Canon, CanonError, Id, Store};
use canonical_derive::Canon;

use microkelvin::{
    Annotated, Branch, BranchMut, Child, ChildMut, Compound, Nth,
};

#[derive(Clone, Canon, Debug)]
enum Bucket<K, V, A>
where
    K: Canon,
    V: Canon,
    A: Canon,
{
    Empty,
    Leaf((K, V)),
    Node(Annotated<Hamt<K, V, A>, A>),
}

#[derive(Clone, Canon, Debug)]
pub struct Hamt<K, V, A>([Bucket<K, V, A>; 4])
where
    K: Canon,
    V: Canon,
    A: Canon;

impl<K, V, A> Compound<A> for Hamt<K, V, A>
where
    K: Canon,
    V: Canon,
    A: Canon,
{
    type Leaf = (K, V);

    fn child(&self, ofs: usize) -> Child<Self, A> {
        match (ofs, &self.0) {
            (0, [Bucket::Leaf(ref kv), _, _, _]) => Child::Leaf(kv),
            (1, [_, Bucket::Leaf(ref kv), _, _]) => Child::Leaf(kv),
            (2, [_, _, Bucket::Leaf(ref kv), _]) => Child::Leaf(kv),
            (3, [_, _, _, Bucket::Leaf(ref kv)]) => Child::Leaf(kv),
            (0, [Bucket::Node(ref an), _, _, _]) => Child::Node(an),
            (1, [_, Bucket::Node(ref an), _, _]) => Child::Node(an),
            (2, [_, _, Bucket::Node(ref an), _]) => Child::Node(an),
            (3, [_, _, _, Bucket::Node(ref an)]) => Child::Node(an),
            _ => Child::EndOfNode,
        }
    }

    fn child_mut(&mut self, ofs: usize) -> ChildMut<Self, A> {
        match (ofs, &mut self.0) {
            (0, [Bucket::Leaf(ref mut kv), _, _, _]) => ChildMut::Leaf(kv),
            (1, [_, Bucket::Leaf(ref mut kv), _, _]) => ChildMut::Leaf(kv),
            (2, [_, _, Bucket::Leaf(ref mut kv), _]) => ChildMut::Leaf(kv),
            (3, [_, _, _, Bucket::Leaf(ref mut kv)]) => ChildMut::Leaf(kv),
            (0, [Bucket::Node(ref mut an), _, _, _]) => ChildMut::Node(an),
            (1, [_, Bucket::Node(ref mut an), _, _]) => ChildMut::Node(an),
            (2, [_, _, Bucket::Node(ref mut an), _]) => ChildMut::Node(an),
            (3, [_, _, _, Bucket::Node(ref mut an)]) => ChildMut::Node(an),
            _ => ChildMut::EndOfNode,
        }
    }
}

impl<K, V, A> Bucket<K, V, A>
where
    K: Canon,
    V: Canon,
    A: Canon,
{
    fn take(&mut self) -> Self {
        mem::replace(self, Bucket::Empty)
    }
}

impl<K, V, A> Default for Bucket<K, V, A>
where
    K: Canon,
    V: Canon,
    A: Canon,
{
    fn default() -> Self {
        Bucket::Empty
    }
}

impl<K, V, A> Default for Hamt<K, V, A>
where
    K: Canon,
    V: Canon,
    A: Canon,
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

impl<K, V, A> Hamt<K, V, A>
where
    K: Canon + Eq,
    V: Canon,
    A: Canon,
{
    /// Creates a new empty Hamt
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: K, val: V) -> Result<Option<V>, CanonError> {
        let hash = Store::canon_hash(&key);

        println!("inserting key with hash {:?}", hash);

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
                *bucket = Bucket::Leaf((key, val));
                Ok(None)
            }
            Bucket::Leaf((old_key, old_val)) => {
                if key == old_key {
                    *bucket = Bucket::Leaf((key, val));
                    Ok(Some(old_val))
                } else {
                    let mut new_node = Hamt::new();
                    let old_hash = Store::canon_hash(&old_key);

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
                if let Bucket::Leaf((key, val)) =
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
        let hash = Store::canon_hash(key);
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
            Bucket::Leaf((old_key, old_val)) => {
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
                    *bucket = Bucket::Leaf((key, val));
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
        let hash = Store::canon_hash(key);
        let mut depth = 0;
        Ok(Branch::path(self, || {
            let ofs = slot(&hash, depth);
            depth += 1;
            ofs
        })?
        .filter(|branch| &(*branch).0 == key)
        .map(|branch| ValRef(branch)))
    }

    pub fn get_mut<'a>(
        &'a mut self,
        key: &K,
    ) -> Result<Option<impl DerefMut<Target = V> + 'a>, CanonError> {
        let hash = Store::canon_hash(key);
        let mut depth = 0;
        Ok(BranchMut::path(self, || {
            let ofs = slot(&hash, depth);
            depth += 1;
            ofs
        })?
        .filter(|branch| &(*branch).0 == key)
        .map(|branch| ValRefMut(branch)))
    }
}

struct ValRef<'a, K, V, A>(Branch<'a, Hamt<K, V, A>, A>)
where
    K: Canon,
    V: Canon,
    A: Canon;

impl<'a, K, V, A> Deref for ValRef<'a, K, V, A>
where
    K: Canon,
    V: Canon,
    A: Canon,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &(*self.0).1
    }
}

struct ValRefMut<'a, K, V, A>(BranchMut<'a, Hamt<K, V, A>, A>)
where
    K: Canon,
    V: Canon,
    A: Canon;

impl<'a, K, V, A> Deref for ValRefMut<'a, K, V, A>
where
    K: Canon,
    V: Canon,
    A: Canon,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &(*self.0).1
    }
}

impl<'a, K, V, A> DerefMut for ValRefMut<'a, K, V, A>
where
    K: Canon,
    V: Canon,
    A: Canon,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut (*self.0).1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use microkelvin::Cardinality;

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
    fn insert_remove() {
        let mut hamt = Hamt::<_, _, ()>::new();
        hamt.insert(8, 8).unwrap();
        assert_eq!(hamt.remove(&8).unwrap(), Some(8));
    }

    #[test]
    fn double() {
        let mut hamt = Hamt::<_, _, ()>::new();
        println!("insert 0");
        hamt.insert(0, 0).unwrap();
        println!("insert 1");
        hamt.insert(1, 1).unwrap();
        assert_eq!(hamt.remove(&1).unwrap(), Some(1));
        assert_eq!(hamt.remove(&0).unwrap(), Some(0));
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
            sorted.push(i);
            let res = hamt.nth(i).unwrap().unwrap().1;
            result.push(res);
        }

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
