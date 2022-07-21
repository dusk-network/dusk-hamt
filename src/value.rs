// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

use crate::Hamt;

use core::ops::{Deref, DerefMut};

use microkelvin::{Branch, BranchMut};
use ranno::Annotation;

/// Dereferences into a Hamt value
#[derive(Debug)]
pub struct Value<'a, K, V, A> {
    branch: Branch<'a, Hamt<K, V, A>, A>,
}

impl<'a, K, V, A> From<Branch<'a, Hamt<K, V, A>, A>> for Value<'a, K, V, A> {
    fn from(branch: Branch<'a, Hamt<K, V, A>, A>) -> Self {
        Self { branch }
    }
}

impl<'a, K, V, A> Deref for Value<'a, K, V, A> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.branch.deref().val
    }
}

/// Mutably dereferences into a Hamt value
#[derive(Debug)]
pub struct ValueMut<'a, K, V, A> {
    branch: BranchMut<'a, Hamt<K, V, A>, A>,
}

impl<'a, K, V, A> From<BranchMut<'a, Hamt<K, V, A>, A>>
    for ValueMut<'a, K, V, A>
{
    fn from(branch: BranchMut<'a, Hamt<K, V, A>, A>) -> Self {
        Self { branch }
    }
}

impl<'a, K, V, A> Deref for ValueMut<'a, K, V, A>
where
    A: Annotation<Hamt<K, V, A>>,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.branch.deref().val
    }
}

impl<'a, K, V, A> DerefMut for ValueMut<'a, K, V, A>
where
    A: Annotation<Hamt<K, V, A>>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.branch.deref_mut().val
    }
}
