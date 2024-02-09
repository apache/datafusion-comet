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
    println!("cargo:rerun-if-changed=src/execution/proto/*.proto");

    let out_dir = "src/execution/generated";
    if !Path::new(out_dir).is_dir() {
        fs::create_dir(out_dir)?;
    }

    prost_build::Config::new().out_dir(out_dir).compile_protos(
        &[
            "src/execution/proto/expr.proto",
            "src/execution/proto/partitioning.proto",
            "src/execution/proto/operator.proto",
        ],
        &["src/execution/proto"],
    )?;
    Ok(())
}
