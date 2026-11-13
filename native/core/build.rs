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

use std::env;
use std::path::Path;

fn target_profile_dir(out_dir: &Path) -> &Path {
    out_dir
        .ancestors()
        .find(|path| path.file_name().is_some_and(|name| name == "build"))
        .and_then(Path::parent)
        .expect("OUT_DIR must be inside Cargo's profile build directory")
}

fn main() {
    // The `hdfs-sys` dependency (pulled in by the default `hdfs-opendal` feature)
    // links against `libjvm` and bakes an absolute `-L` path to the JDK's libjvm
    // directory into its build-script output. That output is cached and replayed
    // when only downstream crates change. On CI runners where `setup-java` floats
    // the Zulu patch version, the cached path can disappear, and linking the final
    // `libcomet.so` fails with `cannot find -ljvm`.
    //
    // `core` is that final `cdylib`, so emit a search path for the currently
    // resolved JDK here: the linker then finds `libjvm` regardless of any stale
    // path replayed from a dependency's cached build script. Re-run when JAVA_HOME
    // or the resolved directory changes so it self-heals across JDK swaps.
    println!("cargo:rerun-if-env-changed=JAVA_HOME");
    if let Ok(java_home) = env::var("JAVA_HOME") {
        // libjvm lives at $JAVA_HOME/lib/server for every JDK Comet supports (11+).
        let server = Path::new(&java_home).join("lib").join("server");
        if server.is_dir() {
            let server = server.display();
            println!("cargo:rerun-if-changed={server}");
            println!("cargo:rustc-link-search=native={server}");
        }
    }

    // Expose the path of the comet-test-udfs cdylib to test code via
    // COMET_TEST_UDFS_LIB. Cargo doesn't propagate cdylib outputs as
    // DEP_<...>_OUT_DIR for non-rlib crates, so we compute the path
    // from OUT_DIR.
    if let Ok(out_dir) = std::env::var("OUT_DIR") {
        let target_profile_dir = target_profile_dir(Path::new(&out_dir));
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

#[cfg(test)]
mod tests {
    use super::target_profile_dir;
    use std::path::Path;

    #[test]
    fn target_profile_dir_handles_cargo_build_directory_layouts() {
        for (out_dir, expected) in [
            (
                "target/debug/build/datafusion-comet-b279/out",
                "target/debug",
            ),
            (
                "target/debug/build/datafusion-comet/b279/out",
                "target/debug",
            ),
            (
                "target/custom-profile/build/datafusion-comet/b279/out",
                "target/custom-profile",
            ),
            (
                "custom-target/aarch64-apple-darwin/debug/build/datafusion-comet/b279/out",
                "custom-target/aarch64-apple-darwin/debug",
            ),
        ] {
            assert_eq!(target_profile_dir(Path::new(out_dir)), Path::new(expected));
        }
    }

    #[test]
    #[should_panic(expected = "OUT_DIR must be inside Cargo's profile build directory")]
    fn target_profile_dir_rejects_paths_without_build_directory() {
        target_profile_dir(Path::new("target/debug/out"));
    }
}
