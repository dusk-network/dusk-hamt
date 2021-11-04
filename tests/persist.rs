// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

use dusk_hamt::Hamt;
use microkelvin::Portal;
use rkyv::rend::LittleEndian;

#[test]
fn persist_across_threads() {
    let n: u64 = 1024;

    let mut hamt = Hamt::<LittleEndian<u64>, u64, ()>::new();

    for i in 0..n {
        let le: LittleEndian<u64> = i.into();
        hamt.insert(le, i + 1);
    }

    let persisted = Portal::put(&hamt);

    // it should now be available from other threads

    std::thread::spawn(move || {
        let restored = Portal::get(persisted);

        for i in 0..n {
            let le: LittleEndian<u64> = i.into();
            let branch = restored.get(&le).expect("Some(branch)");
            let r = branch.leaf();
            assert_eq!(r, i + 1);
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
