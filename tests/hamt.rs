// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

use dusk_hamt::annotation::Cardinality;
use dusk_hamt::{Hamt, Map};
use microkelvin::{Child, Compound};

fn correct_empty_state<C, A>(c: C) -> bool
where
    C: Compound<A>,
{
    for i in 0.. {
        match c.child(i) {
            Child::EndOfNode => return true,
            Child::Empty => (),
            _ => return false,
        }
    }
    unreachable!()
}

#[test]
fn trivial() {
    let mut map = Map::<u32, u32>::new();
    assert_eq!(map.remove(&0), None);
}

#[test]
fn replace() {
    let mut map = Map::new();
    assert_eq!(map.insert(0, 38), None);
    assert_eq!(map.insert(0, 0), Some(38));
}

#[test]
fn multiple() {
    let n = 1024;

    let mut hamt = Map::new();

    for i in 0..n {
        hamt.insert(i, i);
    }

    for i in 0..n {
        assert_eq!(hamt.remove(&i), Some(i));
    }

    assert!(correct_empty_state(hamt));
}

#[test]
fn insert_get() {
    let n = 1024;

    let mut hamt = Map::new();

    for i in 0..n {
        hamt.insert(i, i);
    }

    for i in 0..n {
        assert_eq!(*hamt.get(&i).expect("Some(_)"), i);
    }
}

#[test]
fn nth() {
    let n: u64 = 1024;

    let mut hamt = Hamt::<_, _, Cardinality>::new();

    let mut result: Vec<u64> = vec![];
    let mut sorted = vec![];

    for i in 0..n {
        hamt.insert(i, i);
    }

    for i in 0..n {
        let res = hamt.nth(i).expect("Some(_)");
        result.push(res.val);
        sorted.push(i);
    }

    result.sort_unstable();

    assert_eq!(result, sorted);
}

#[test]
fn insert_get_mut() {
    let n = 1024;

    let mut hamt = Map::new();

    for i in 0..n {
        hamt.insert(i, i);
    }

    for i in 0..n {
        *hamt.get_mut(&i).expect("Some(_)") += 1;
    }

    for i in 0..n {
        assert_eq!(*hamt.get(&i).expect("Some(_)"), i + 1);
    }
}

#[test]
fn iterate() {
    let n: u64 = 1024;

    let mut hamt = Hamt::<_, _, Cardinality>::new();

    let mut reference = vec![];
    let mut from_iter = vec![];

    for i in 0..n {
        hamt.insert(i, i);
        reference.push(i);
    }

    for leaf in hamt.nth(0).expect("Some(_)") {
        let val = leaf.val;
        from_iter.push(val);
    }

    reference.sort_unstable();
    from_iter.sort_unstable();

    assert_eq!(from_iter, reference);
}
