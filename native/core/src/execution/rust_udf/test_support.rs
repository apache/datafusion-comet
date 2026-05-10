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

#![cfg(test)]

//! Test helpers for the `rust_udf` module.

use std::path::PathBuf;
use std::process::Command;

/// Return the absolute path to the `comet-test-udfs` cdylib.
///
/// The path is set by `core/build.rs` via the `COMET_TEST_UDFS_LIB` env
/// var. If the file doesn't exist (e.g., the test crate hasn't been
/// built yet because `cargo test -p datafusion-comet` only builds rlibs
/// for default targets), best-effort runs `cargo build -p comet-test-udfs`
/// and returns the path either way — callers should assert existence
/// before relying on it.
pub fn test_udfs_path() -> PathBuf {
    let p = PathBuf::from(env!("COMET_TEST_UDFS_LIB"));
    if !p.exists() {
        let _ = Command::new(env!("CARGO"))
            .args(["build", "-p", "comet-test-udfs"])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .status();
    }
    p
}
