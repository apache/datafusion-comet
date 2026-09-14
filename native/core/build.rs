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
}
