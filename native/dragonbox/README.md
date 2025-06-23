# Dragonbox

[<img alt="github" src="https://img.shields.io/badge/github-dtolnay/dragonbox-8da0cb?style=for-the-badge&labelColor=555555&logo=github" height="20">](https://github.com/dtolnay/dragonbox)
[<img alt="crates.io" src="https://img.shields.io/crates/v/dragonbox.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/dragonbox)
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-dragonbox-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">](https://docs.rs/dragonbox)
[<img alt="build status" src="https://img.shields.io/github/actions/workflow/status/dtolnay/dragonbox/ci.yml?branch=master&style=for-the-badge" height="20">](https://github.com/dtolnay/dragonbox/actions?query=branch%3Amaster)

This crate contains a basic port of <https://github.com/jk-jeon/dragonbox> to
Rust for benchmarking purposes.

Please see the upstream repo for an explanation of the approach and comparison
to the Ryū algorithm.

<br>

## Example

```rust
fn main() {
    let mut buffer = dragonbox::Buffer::new();
    let printed = buffer.format(1.234);
    assert_eq!(printed, "1.234E0");
}
```

<br>

## Performance (lower is better)

![performance](https://raw.githubusercontent.com/dtolnay/dragonbox/master/performance.png)

<br>

#### License

<sup>
Licensed under either of <a href="LICENSE-Apache2-LLVM">Apache License, Version
2.0 with LLVM Exceptions</a> or <a href="LICENSE-Boost">Boost Software License
Version 1.0</a> at your option.
</sup>

<br>

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
</sub>
