// This Source Code Form is subject to the terms of the Mozilla Public
// Liycense, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

//! Hamt
use core::mem;
use core::ops::{Deref, DerefMut};

use canonical::{Canon, Store};
use canonical_derive::Canon;

use microkelvin::{
    Annotated, Annotation, Branch, BranchMut, Child, ChildMut, Compound,
};

#[derive(Clone, Canon, Debug)]
enum Bucket<K, V, A, S>
where
    K: Canon<S>,
    V: Canon<S>,
    A: Canon<S>,
    S: Store,
{
    Empty,
    Leaf((K, V)),
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

    fn child(&self, ofs: usize) -> Child<Self, S> {
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

    fn child_mut(&mut self, ofs: usize) -> ChildMut<Self, S> {
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
    A: Canon<S> + Annotation<Self, <Self as Compound<S>>::Leaf>,
    S: Store,
{
    /// Creates a new empty Hamt
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: K, val: V) -> Result<Option<V>, S::Error> {
        let hash = S::ident(&key);
        self._insert(key, val, hash, 0)
    }

    fn _insert(
        &mut self,
        key: K,
        val: V,
        hash: S::Ident,
        depth: usize,
    ) -> Result<Option<V>, S::Error> {
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
                    let mut new_node = Hamt::<_, _, _, S>::new();
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

    pub fn remove(&mut self, key: &K) -> Result<Option<V>, S::Error> {
        let hash = S::ident(key);
        self._remove(key, hash, 0)
    }

    fn _remove(
        &mut self,
        key: &K,
        hash: S::Ident,
        depth: usize,
    ) -> Result<Option<V>, S::Error> {
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
    ) -> Result<Option<impl Deref<Target = V> + 'a>, S::Error>
    where
        K: core::fmt::Debug,
        V: core::fmt::Debug,
        S: core::fmt::Debug,
    {
        let hash = S::ident(key);
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
    ) -> Result<Option<impl DerefMut<Target = V> + 'a>, S::Error>
    where
        K: core::fmt::Debug,
        V: core::fmt::Debug,
        S: core::fmt::Debug,
    {
        let hash = S::ident(key);
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

struct ValRef<'a, K, V, A, S>(Branch<'a, Hamt<K, V, A, S>, S>)
where
    K: Canon<S>,
    V: Canon<S>,
    A: Canon<S>,
    S: Store;

impl<'a, K, V, A, S> Deref for ValRef<'a, K, V, A, S>
where
    K: Canon<S>,
    V: Canon<S>,
    A: Canon<S>,
    S: Store,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &(*self.0).1
    }
}

struct ValRefMut<'a, K, V, A, S>(BranchMut<'a, Hamt<K, V, A, S>, S>)
where
    K: Canon<S>,
    V: Canon<S>,
    A: Canon<S> + Annotation<Hamt<K, V, A, S>, (K, V)>,
    S: Store;

impl<'a, K, V, A, S> Deref for ValRefMut<'a, K, V, A, S>
where
    K: Canon<S>,
    V: Canon<S>,
    A: Canon<S> + Annotation<Hamt<K, V, A, S>, (K, V)>,
    S: Store,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &(*self.0).1
    }
}

impl<'a, K, V, A, S> DerefMut for ValRefMut<'a, K, V, A, S>
where
    K: Canon<S>,
    V: Canon<S>,
    A: Canon<S> + Annotation<Hamt<K, V, A, S>, (K, V)>,
    S: Store,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut (*self.0).1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use canonical_host::MemStore;

    #[test]
    fn trivial() {
        let mut hamt = Hamt::<u32, u32, (), MemStore>::new();
        assert_eq!(hamt.remove(&0).unwrap(), None);
    }

    #[test]
    fn replace() {
        let mut hamt = Hamt::<u32, u32, (), MemStore>::new();
        assert_eq!(hamt.insert(0, 38).unwrap(), None);
        assert_eq!(hamt.insert(0, 0).unwrap(), Some(38));
    }

    #[test]
    fn insert_remove() {
        let mut hamt = Hamt::<_, _, (), MemStore>::new();
        hamt.insert(8, 8).unwrap();
        assert_eq!(hamt.remove(&8).unwrap(), Some(8));
    }

    #[test]
    fn double() {
        let mut hamt = Hamt::<_, _, (), MemStore>::new();
        hamt.insert(0, 0).unwrap();
        hamt.insert(1, 1).unwrap();
        assert_eq!(hamt.remove(&1).unwrap(), Some(1));
        assert_eq!(hamt.remove(&0).unwrap(), Some(0));
    }

    #[test]
    fn multiple() {
        let n = 1024;

        let mut hamt = Hamt::<_, _, (), MemStore>::new();

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

        let mut hamt = Hamt::<_, _, (), MemStore>::new();

        for i in 0..n {
            hamt.insert(i, i).unwrap();
        }

        for i in 0..n {
            assert_eq!(*hamt.get(&i).unwrap().unwrap(), i);
        }
    }

    #[test]
    fn insert_get_mut() {
        let n = 1024;

        let mut hamt = Hamt::<_, _, (), MemStore>::new();

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
