// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

extern crate alloc;

use alloc::vec::Vec;
use canonical::Canon;
use dusk_hamt::*;
use microkelvin::{Cardinality, Nth};

fn correct_empty_state<K, V, A>(hamt: &Hamt<K, V, A>) -> bool
where
    K: Canon,
    V: Canon,
    A: Canon,
{
    hamt.as_ref().iter().fold(true, |acc, bucket| {
        acc && match bucket {
            Bucket::Empty => true,
            _ => false,
        }
    })
}

#[test]
fn trivial() {
    let mut hamt: Map<u32, u32> = Map::new();
    assert_eq!(hamt.remove(&0).unwrap(), None);
}

#[test]
fn replace() {
    let mut hamt: Map<u32, u32> = Map::new();
    assert_eq!(hamt.insert(0, 38).unwrap(), None);
    assert_eq!(hamt.insert(0, 0).unwrap(), Some(38));
}

#[test]
fn multiple() {
    let n = 1024;

    let mut hamt = Map::new();

    for i in 0..n {
        hamt.insert(i, i).unwrap();
    }

    for i in 0..n {
        assert_eq!(hamt.remove(&i).unwrap(), Some(i));
    }

    assert!(correct_empty_state(&hamt));
}

#[test]
fn insert_get() {
    let n = 1024;

    let mut hamt = Map::new();

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
        result.push(res.unwrap().as_ref().clone());
        sorted.push(i);
    }

    result.sort();

    assert_eq!(result, sorted);
}

#[test]
fn insert_get_mut() {
    let n = 1024;

    let mut hamt = Map::new();

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
        let val = leaf.unwrap().as_ref().clone();
        from_iter.push(val);
    }

    reference.sort();
    from_iter.sort();

    assert_eq!(from_iter, reference)
}
