// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

use canonical::CanonError;
use dusk_hamt::Hamt;
use microkelvin::{Annotation, Cardinality, Child, Compound, Nth};

fn correct_empty_state<C, A>(c: C) -> bool
where
    C: Compound<A>,
    A: Annotation<C::Leaf>,
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
fn trivial() -> Result<(), CanonError> {
    let mut hamt = Hamt::<u32, u32, ()>::new();
    assert_eq!(hamt.remove(&0)?, None);

    Ok(())
}

#[test]
fn replace() -> Result<(), CanonError> {
    let mut hamt = Hamt::<u32, u32, ()>::new();
    assert_eq!(hamt.insert(0, 38)?, None);
    assert_eq!(hamt.insert(0, 0)?, Some(38));

    Ok(())
}

#[test]
fn multiple() -> Result<(), CanonError> {
    let n = 1024;

    let mut hamt = Hamt::<_, _, ()>::new();

    for i in 0..n {
        hamt.insert(i, i)?;
    }

    for i in 0..n {
        assert_eq!(hamt.remove(&i)?, Some(i));
    }

    assert!(correct_empty_state(hamt));

    Ok(())
}

#[test]
fn insert_get() -> Result<(), CanonError> {
    let n = 1024;

    let mut hamt = Hamt::<_, _, ()>::new();

    for i in 0..n {
        hamt.insert(i, i)?;
    }

    for i in 0..n {
        assert_eq!(*hamt.get(&i)?.expect("Some(_)"), i);
    }

    Ok(())
}

#[test]
fn nth() -> Result<(), CanonError> {
    let n: u64 = 1024;

    let mut hamt = Hamt::<_, _, Cardinality>::new();

    let mut result: Vec<u64> = vec![];
    let mut sorted = vec![];

    for i in 0..n {
        hamt.insert(i, i)?;
    }

    for i in 0..n {
        let res = hamt.nth(i)?.expect("Some(_)");
        result.push(res.val);
        sorted.push(i);
    }

    result.sort_unstable();

    assert_eq!(result, sorted);

    Ok(())
}

#[test]
fn insert_get_mut() -> Result<(), CanonError> {
    let n = 1024;

    let mut hamt = Hamt::<_, _, ()>::new();

    for i in 0..n {
        hamt.insert(i, i)?;
    }

    for i in 0..n {
        *hamt.get_mut(&i)?.expect("Some(_)") += 1;
    }

    for i in 0..n {
        assert_eq!(*hamt.get(&i)?.expect("Some(_)"), i + 1);
    }

    Ok(())
}

#[test]
fn iterate() -> Result<(), CanonError> {
    let n: u64 = 1024;

    use microkelvin::{Cardinality, Nth};

    let mut hamt = Hamt::<_, _, Cardinality>::new();

    let mut reference = vec![];
    let mut from_iter = vec![];

    for i in 0..n {
        hamt.insert(i, i)?;
        reference.push(i);
    }

    for leaf in hamt.nth(0)?.expect("Some(_)") {
        let val = leaf?.val;
        from_iter.push(val);
    }

    reference.sort_unstable();
    from_iter.sort_unstable();

    assert_eq!(from_iter, reference);

    Ok(())
}
