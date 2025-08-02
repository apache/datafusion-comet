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
use std::path::PathBuf;

fn main() {
    let vec_flags = &get_build_flags();

    build_hdfs_lib(vec_flags);

    build_minidfs_lib(vec_flags);

    build_ffi(vec_flags);
}

fn build_ffi(flags: &[String]) {
    let (header, output) = ("c_src/wrapper.h", "hdfs-native.rs");
    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed={header}");
    println!("cargo:rerun-if-changed={}", get_hdfs_file_path("hdfs.h"));
    println!(
        "cargo:rerun-if-changed={}",
        get_minidfs_file_path("native_mini_dfs.h")
    );

    // To avoid the order issue of dependent dynamic libraries
    println!("cargo:rustc-link-lib=jvm");

    let bindings = bindgen::Builder::default()
        .header(header)
        .allowlist_function("nmd.*")
        .allowlist_function("hdfs.*")
        .allowlist_function("hadoop.*")
        .clang_args(flags)
        .rustified_enum("tObjectKind")
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join(output))
        .expect("Couldn't write bindings!");
}

fn build_hdfs_lib(flags: &[String]) {
    println!("cargo:rerun-if-changed={}", get_hdfs_file_path("hdfs.c"));

    let mut builder = cc::Build::new();

    // includes
    builder
        .include(get_hdfs_source_dir())
        .include(format!("{}/os/posix", get_hdfs_source_dir()));

    // files
    builder
        .file(get_hdfs_file_path("exception.c"))
        .file(get_hdfs_file_path("htable.c"))
        .file(get_hdfs_file_path("jni_helper.c"))
        .file(get_hdfs_file_os_path("mutexes.c"))
        .file(get_hdfs_file_os_path("thread_local_storage.c"))
        .file(get_hdfs_file_path("hdfs.c"));

    for flag in flags {
        builder.flag(flag);
    }

    // disable the warning message
    builder.warnings(false);

    builder.compile("hdfs");
}

fn build_minidfs_lib(flags: &[String]) {
    println!(
        "cargo:rerun-if-changed={}",
        get_minidfs_file_path("native_mini_dfs.c")
    );

    let mut builder = cc::Build::new();

    // includes
    builder
        .include(get_hdfs_source_dir())
        .include(format!("{}/os/posix", get_hdfs_source_dir()));

    // files
    builder.file(get_minidfs_file_path("native_mini_dfs.c"));

    for flag in flags {
        builder.flag(flag.as_str());
    }
    builder.compile("minidfs");
}

fn get_build_flags() -> Vec<String> {
    let mut result = vec![];

    result.extend(get_java_dependency());
    result.push(String::from("-Wno-incompatible-pointer-types"));

    result
}

fn get_java_dependency() -> Vec<String> {
    let mut result = vec![];

    // Include directories
    let java_home = java_locator::locate_java_home()
        .expect("JAVA_HOME could not be found, trying setting the variable manually");
    result.push(format!("-I{java_home}/include"));
    #[cfg(target_os = "linux")]
    result.push(format!("-I{java_home}/include/linux"));
    #[cfg(target_os = "macos")]
    result.push(format!("-I{java_home}/include/darwin"));

    // libjvm link
    let jvm_lib_location = java_locator::locate_jvm_dyn_library().unwrap();
    println!("cargo:rustc-link-search=native={jvm_lib_location}");
    println!("cargo:rustc-link-lib=jvm");

    // For tests, add libjvm path to rpath, this does not propagate upwards,
    // unless building an .so, as per Cargo specs, so is only used when testing
    println!("cargo:rustc-link-arg=-Wl,-rpath,{jvm_lib_location}");

    result
}

fn get_hdfs_file_path(filename: &'static str) -> String {
    format!("{}/{}", get_hdfs_source_dir(), filename)
}

fn get_hdfs_file_os_path(filename: &'static str) -> String {
    format!("{}/os/posix/{}", get_hdfs_source_dir(), filename)
}

fn get_hdfs_source_dir() -> &'static str {
    "c_src/libhdfs"
}

fn get_minidfs_file_path(filename: &'static str) -> String {
    format!("{}/{}", "c_src/libminidfs", filename)
}
