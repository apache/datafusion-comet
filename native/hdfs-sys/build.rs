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

//! Links `libjvm` and either finds a system `libhdfs` or compiles the vendored
//! Apache Hadoop sources under `libhdfs/hdfs_3_3/`.
//!
//! The resolution order matches the crates.io `hdfs-sys` crate this stands in
//! for, so setting `HDFS_LIB_DIR` or `HADOOP_HOME` still selects a system
//! `libhdfs`. Note that such a library will not carry the HDFS-16021 fix; see
//! `README.md`.

use std::env;
use std::path::Path;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// The only vendored libhdfs version. Upstream ships every Hadoop release from
/// 2.2 onwards behind a feature; Comet builds `hdfs_3_3` and nothing else.
const SRC: &str = "libhdfs/hdfs_3_3";

fn main() -> Result<()> {
    // Nothing to link against when docs.rs builds documentation.
    if env::var_os("DOCS_RS").is_some() {
        return Ok(());
    }

    link_jvm()?;

    if !find_system_libhdfs()? {
        build_vendored_libhdfs()?;
    }

    Ok(())
}

/// Points the linker at the `libjvm` belonging to the JDK that `java-locator`
/// resolves, which is `JAVA_HOME` when it is set.
fn link_jvm() -> Result<()> {
    let jvm_path = java_locator::locate_jvm_dyn_library()?;
    println!("cargo:rustc-link-lib=jvm");
    println!("cargo:rustc-link-search=native={jvm_path}");
    Ok(())
}

/// Returns `true` when a prebuilt `libhdfs` was found and linked.
///
/// Checks `HDFS_LIB_DIR` first, then `HADOOP_HOME`. `HDFS_STATIC` selects static
/// linking. The `vendored` feature skips the search outright.
fn find_system_libhdfs() -> Result<bool> {
    println!("cargo:rerun-if-env-changed=HDFS_LIB_DIR");
    println!("cargo:rerun-if-env-changed=HDFS_STATIC");
    println!("cargo:rerun-if-env-changed=HADOOP_HOME");

    if cfg!(feature = "vendored") {
        return Ok(false);
    }

    let lib_dir = if let Ok(lib_dir) = env::var("HDFS_LIB_DIR") {
        lib_dir
    } else if let Ok(hadoop_home) = env::var("HADOOP_HOME") {
        format!("{hadoop_home}/lib/native")
    } else {
        return Ok(false);
    };

    let mode = if env::var_os("HDFS_STATIC").is_some() {
        "static"
    } else {
        "dylib"
    };
    println!("cargo:rustc-link-search=native={lib_dir}");
    println!("cargo:rustc-link-lib={mode}=hdfs");

    Ok(true)
}

/// Compiles the vendored Hadoop C into a static `libhdfs.a`.
///
/// The file list follows `hadoop-hdfs-native-client/src/CMakeLists.txt` for the
/// 3.3 tree: the three top-level translation units, `jclasses.c` (added in 3.3)
/// and the POSIX platform layer. `htable.c` was removed in 3.3, and the Windows
/// platform layer is not vendored because Comet ships no Windows native builds.
fn build_vendored_libhdfs() -> Result<()> {
    let java_home = java_locator::locate_java_home()?;

    println!("cargo:rustc-link-lib=static=hdfs");

    let mut builder = cc::Build::new();
    // The vendored sources are Hadoop's, not ours, and are not warning-clean.
    builder.warnings(false);
    builder.flag_if_supported("-w");
    builder.flag_if_supported("-fvisibility=hidden");
    // Restore the pre-GCC-10 tentative-definition behaviour the sources assume.
    builder.flag_if_supported("-fcommon");

    builder.include(format!("{java_home}/include"));
    if cfg!(target_os = "linux") {
        builder.include(format!("{java_home}/include/linux"));
    }
    if cfg!(target_os = "macos") {
        builder.include(format!("{java_home}/include/darwin"));
    }

    builder
        .include("libhdfs")
        .include(SRC)
        .include(format!("{SRC}/include"))
        .include(format!("{SRC}/os"))
        .include(format!("{SRC}/os/posix"));

    for file in [
        "exception.c",
        "jni_helper.c",
        "hdfs.c",
        "jclasses.c",
        "os/posix/mutexes.c",
        "os/posix/thread.c",
        "os/posix/thread_local_storage.c",
    ] {
        let path = format!("{SRC}/{file}");
        assert!(Path::new(&path).exists(), "missing vendored source {path}");
        println!("cargo:rerun-if-changed={path}");
        builder.file(path);
    }

    builder.compile("hdfs");
    Ok(())
}
