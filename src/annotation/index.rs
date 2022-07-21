// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

use crate::annotation::Cardinality;
use crate::Hamt;

use core::borrow::Borrow;

use microkelvin::{Branch, BranchMut, Child, Step, Walk, Walker};
use ranno::Annotation;

impl<K, V, A> Hamt<K, V, A>
where
    A: Annotation<Self> + Borrow<Cardinality>,
{
    /// Construct a [`Branch`] pointing to the `nth` element, if any
    pub fn nth(&self, index: u64) -> Option<Branch<Self, A>> {
        Branch::walk(self, Index(index))
    }

    /// Construct a [`BranchMut`] pointing to the `nth` element, if any
    pub fn nth_mut(&mut self, index: u64) -> Option<BranchMut<Self, A>> {
        BranchMut::walk(self, Index(index))
    }
}

struct Index(u64);

impl<K, V, A> Walker<Hamt<K, V, A>, A> for Index
where
    A: Annotation<Hamt<K, V, A>> + Borrow<Cardinality>,
{
    fn walk(&mut self, walk: Walk<Hamt<K, V, A>, A>) -> Step {
        for i in 0.. {
            match walk.child(i) {
                Child::Leaf(_) => {
                    if self.0 == 0 {
                        return Step::Found(i);
                    } else {
                        self.0 -= 1
                    }
                }
                Child::Node(node) => {
                    let anno = node.anno();
                    let c = (*anno).borrow();

                    let c = **c;

                    if self.0 < c {
                        return Step::Into(i);
                    }

                    self.0 -= c;
                }
                Child::Empty => (),
                Child::EndOfNode => return Step::Abort,
            }
        }
        unreachable!()
    }
}
