# dusk-hamt

[![Repository](https://img.shields.io/badge/github-hamt-blueviolet?logo=github)](https://github.com/dusk-network/dusk-hamt)
![Build Status](https://github.com/dusk-network/dusk-hamt/workflows/build/badge.svg)
[![Documentation](https://img.shields.io/badge/docs-hamt-blue?logo=rust)](https://docs.rs/dusk-hamt/)

Dusk HAMT is a map data structure that is also a tree

## Usage example
```rust
use dusk_hamt::Map;

let n = 1024;
let mut map = Map::new();

for i in 0..n {
    hamt.insert(i, i);
}

for i in 0..n {
    assert_eq!(hamt.get(&i).expect("Some(_)").val, i);
}
```
