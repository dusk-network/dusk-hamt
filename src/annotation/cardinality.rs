// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

use crate::{Bucket, Hamt};

use core::ops::Deref;

use ranno::Annotation;

/// Cardinality of the Hamt.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cardinality(u64);

impl From<u64> for Cardinality {
    fn from(c: u64) -> Self {
        Self(c)
    }
}

impl Deref for Cardinality {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<u64> for Cardinality {
    fn eq(&self, other: &u64) -> bool {
        self.0.eq(other)
    }
}

impl<K, V> Annotation<Hamt<K, V, Cardinality>> for Cardinality {
    fn from_child(hamt: &Hamt<K, V, Cardinality>) -> Self {
        let mut cardinality = 0;

        for bucket in &hamt.0 {
            match bucket {
                Bucket::Empty => {}
                Bucket::Leaf(_) => cardinality += 1,
                Bucket::Node(node) => {
                    let anno = node.anno();
                    cardinality += **anno;
                }
            }
        }

        cardinality.into()
    }
}
