// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

use dusk_hamt::{Hamt, Lookup};
use microkelvin::{
    All, Annotation, Cardinality, Child, Compound, Keyed, MaybeArchived, Nth,
    OffsetLen,
};
use rkyv::rend::LittleEndian;

fn correct_empty_state<C, A, I>(c: C) -> bool
where
    C: Compound<A, I>,
    A: Annotation<C::Leaf>,
{
    for i in 0.. {
        match c.child(i) {
            Child::End => return true,
            Child::Empty => (),
            _ => return false,
        }
    }
    unreachable!()
}

#[test]
fn trivial() {
    let mut hamt = Hamt::<LittleEndian<u32>, u32, (), OffsetLen>::new();
    assert_eq!(hamt.remove(&0.into()), None);
}

#[test]
fn replace() {
    let mut hamt = Hamt::<LittleEndian<u32>, u32, (), OffsetLen>::new();
    assert_eq!(hamt.insert(0.into(), 38), None);
    assert_eq!(hamt.insert(0.into(), 0), Some(38));
}

#[test]
fn multiple() {
    let n: u32 = 1024;

    let mut hamt = Hamt::<LittleEndian<u32>, _, (), OffsetLen>::new();

    for i in 0..n {
        hamt.insert(i.into(), i);
    }

    for i in 0..n {
        assert_eq!(hamt.remove(&i.into()), Some(i));
    }

    assert!(correct_empty_state(hamt));
}

#[test]
fn insert_get_immut() {
    let n: u32 = 1024;

    let mut hamt = Hamt::<LittleEndian<u32>, _, (), OffsetLen>::new();

    for i in 0..n {
        hamt.insert(i.into(), i);
    }

    for i in 0..n {
        assert_eq!(hamt.get(&i.into()).expect("Some(_)").leaf(), i);
    }
}

#[test]
fn nth() {
    let n: u64 = 1024;

    let mut hamt =
        Hamt::<LittleEndian<u64>, u64, Cardinality, OffsetLen>::new();

    let mut result: Vec<LittleEndian<u64>> = vec![];
    let mut sorted = vec![];

    for i in 0..n {
        hamt.insert(i.into(), i.into());
    }

    for i in 0..n {
        let res = hamt.walk(Nth(i.into())).expect("Some(_)");
        result.push(*res.leaf().key());
        sorted.push(i);
    }

    result.sort_unstable();

    assert_eq!(result, sorted);
}

#[test]
fn insert_get_mut() {
    let n = 1024;

    let mut hamt = Hamt::<LittleEndian<u32>, _, (), OffsetLen>::new();

    for i in 0..n {
        hamt.insert(i.into(), i);
    }

    for i in 0..n {
        *hamt.get_mut(&i.into()).expect("Some(_)").leaf_mut() += 1;
    }

    for i in 0..n {
        assert_eq!(hamt.get(&i.into()).expect("Some(_)").leaf(), i + 1);
    }
}

#[test]
fn iterate() {
    let n: u64 = 1024;

    use microkelvin::{Cardinality, Nth};

    let mut hamt = Hamt::<
        LittleEndian<u64>,
        LittleEndian<u64>,
        Cardinality,
        OffsetLen,
    >::new();

    let mut reference = vec![];
    let mut gotten: Vec<u64> = vec![];
    let mut from_iter: Vec<u64> = vec![];
    let mut from_nth: Vec<u64> = vec![];

    for i in 0..n {
        hamt.insert(i.into(), i.into());
        reference.push(i);
    }

    for i in 0..n {
        let i: LittleEndian<u64> = *hamt.get(&i.into()).unwrap().leaf();
        gotten.push(i.into());
    }

    for i in 0..n {
        if let MaybeArchived::Memory(kv) = hamt.walk(Nth(i)).unwrap().leaf() {
            let v = kv.value();

            from_nth.push(v.into());
        }
    }

    let branch = hamt.walk(All).expect("Some(_)");

    for leaf in branch {
        let val = leaf.key();
        from_iter.push(val.into());
    }

    assert_eq!(from_iter, from_nth);

    reference.sort_unstable();
    from_iter.sort_unstable();
    from_nth.sort_unstable();

    assert_eq!(reference, from_iter);
    assert_eq!(from_iter, gotten);
    assert_eq!(gotten, from_nth);
}
