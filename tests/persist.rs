// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

use dusk_hamt::{Hamt, Lookup};
use microkelvin::{HostStore, StoreRef};
use rkyv::rend::LittleEndian;

#[test]
fn persist_across_threads() {
    let n: u64 = 1024;

    let store = StoreRef::new(HostStore::new());

    let mut hamt = Hamt::<LittleEndian<u64>, u64, (), _>::new();

    for i in 0..n {
        let le: LittleEndian<u64> = i.into();
        hamt.insert(le, i + 1);
    }

    let stored = store.store(&hamt);

    // it should now be available from other threads

    std::thread::spawn(move || {
        for i in 0..n {
            let le: LittleEndian<u64> = i.into();
            assert_eq!(stored.get(&le).unwrap().leaf(), i + 1);
        }
    })
    .join()
    .expect("thread to join cleanly");

    // then empty the original

    for i in 0..n {
        let le: LittleEndian<u64> = i.into();
        assert_eq!(hamt.remove(&le), Some(i + 1));
    }
}
