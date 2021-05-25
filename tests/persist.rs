// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright (c) DUSK NETWORK. All rights reserved.

#[cfg(feature = "persistance")]
mod persist {

    use dusk_hamt::Hamt;
    use microkelvin::{
        BackendCtor, Compound, DiskBackend, PersistError, Persistance,
    };

    #[test]
    fn persist_across_threads() -> Result<(), PersistError> {
        let n: u64 = 1024;

        let mut hamt = Hamt::<u64, u64, ()>::new();

        for i in 0..n {
            hamt.insert(i, i + 1)?;
        }

        let backend = BackendCtor::new(|| DiskBackend::ephemeral());

        let persisted = Persistance::persist(&backend, &hamt)?;

        // it should now be available from other threads

        std::thread::spawn(move || {
            let restored_generic = persisted.restore()?;

            let mut restored: Hamt<u64, u64, ()> =
                Hamt::from_generic(&restored_generic)?;

            for i in 0..n {
                assert_eq!(restored.remove(&i)?, Some(i + 1));
            }

            Ok(()) as Result<(), PersistError>
        })
        .join()
        .expect("thread to join cleanly")?;

        // then empty the original

        for i in 0..n {
            assert_eq!(hamt.remove(&i)?, Some(i + 1));
        }

        Ok(())
    }
}
