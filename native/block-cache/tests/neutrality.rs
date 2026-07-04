// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! CI guard for the crate's storage neutrality: `datafusion-comet-block-cache` must never
//! pull in `object_store`, `opendal`, or a Comet crate. This keeps the tier internals
//! swappable and the core extractable, as required by OBJECT_STORE_CACHE_DESIGN.md §2.1.

use std::process::Command;

const FORBIDDEN: &[&str] = &["object_store", "opendal", "datafusion-comet-core"];

#[test]
fn dependency_tree_is_storage_neutral() {
    // Resolve the crate manifest so the test works regardless of the CWD chosen by the
    // test harness.
    let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
    let output = Command::new(env!("CARGO"))
        .args([
            "tree",
            "--manifest-path",
            manifest,
            "-p",
            "datafusion-comet-block-cache",
            "--edges",
            "normal,build",
            "--prefix",
            "none",
        ])
        .output();

    let output = match output {
        Ok(o) => o,
        // If cargo cannot be spawned (unusual outside CI), do not fail spuriously.
        Err(e) => {
            eprintln!("skipping neutrality check: could not run `cargo tree`: {e}");
            return;
        }
    };
    assert!(
        output.status.success(),
        "`cargo tree` failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let tree = String::from_utf8_lossy(&output.stdout);
    for forbidden in FORBIDDEN {
        // Match crate names at a line start (name is the first token per `--prefix none`).
        let hit = tree
            .lines()
            .any(|line| line.split_whitespace().next() == Some(*forbidden));
        assert!(
            !hit,
            "block-cache must stay storage-API-neutral but its dependency tree contains `{forbidden}`:\n{tree}"
        );
    }
}
