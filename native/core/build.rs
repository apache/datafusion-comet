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

fn main() {
    // Expose the path of the comet-test-udfs cdylib to test code via
    // COMET_TEST_UDFS_LIB. Cargo doesn't propagate cdylib outputs as
    // DEP_<...>_OUT_DIR for non-rlib crates, so we compute the path
    // from OUT_DIR.
    if let Ok(out_dir) = std::env::var("OUT_DIR") {
        let out_path = std::path::PathBuf::from(out_dir);
        // OUT_DIR is .../target/<profile>/build/<pkg-hash>/out
        let target_profile_dir = out_path
            .ancestors()
            .nth(3)
            .map(|p| p.to_path_buf())
            .unwrap_or_default();
        let dylib_ext = if cfg!(target_os = "macos") {
            "dylib"
        } else if cfg!(target_os = "windows") {
            "dll"
        } else {
            "so"
        };
        let lib_path = target_profile_dir.join(format!("libcomet_test_udfs.{dylib_ext}"));
        println!("cargo:rustc-env=COMET_TEST_UDFS_LIB={}", lib_path.display());
    }
    println!("cargo:rerun-if-changed=../comet-test-udfs/src/lib.rs");
    println!("cargo:rerun-if-changed=../comet-test-udfs/Cargo.toml");
}
