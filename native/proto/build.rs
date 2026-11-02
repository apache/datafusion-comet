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

//! Build script for generating codes from .proto files.

use std::{fs, io::Result, path::Path};

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=src/proto/");

    let out_dir = "src/generated";
    if !Path::new(out_dir).is_dir() {
        fs::create_dir(out_dir)?;
    }

    prost_build::Config::new().out_dir(out_dir).compile_protos(
        &[
            "src/proto/expr.proto",
            "src/proto/metric.proto",
            "src/proto/partitioning.proto",
            "src/proto/operator.proto",
            "src/proto/config.proto",
            "src/proto/native_executors.proto",
        ],
        &["src/proto"],
    )?;

    println!(
        "cargo:rustc-env=COMET_PROTO_FINGERPRINT={:016x}",
        proto_fingerprint("src/proto")?
    );
    Ok(())
}

/// A hash of every .proto file, so two builds can tell whether they speak the same plan format.
/// FNV-1a rather than the standard library's hasher, whose output can change between Rust
/// releases: a driver and a native executor cluster may be built with different compilers.
fn proto_fingerprint(dir: &str) -> Result<u64> {
    let mut paths: Vec<_> = fs::read_dir(dir)?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<_>>()?;
    paths.retain(|path| path.extension().is_some_and(|ext| ext == "proto"));
    paths.sort();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for path in paths {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        for byte in name.bytes().chain(fs::read(&path)?) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    }
    Ok(hash)
}
