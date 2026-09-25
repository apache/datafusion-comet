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

use std::sync::atomic::{AtomicU64, Ordering};
use std::{env, fs, io::Write, path::PathBuf};

/// Returns file handle for a temp file in 'target' directory with a provided content
pub fn get_temp_file(file_name: &str, content: &[u8]) -> fs::File {
    // build tmp path to a file in "target/debug/testdata"
    let mut path_buf = env::current_dir().unwrap();
    path_buf.push("target");
    path_buf.push("debug");
    path_buf.push("testdata");
    fs::create_dir_all(&path_buf).unwrap();
    path_buf.push(file_name);

    // write file content
    let mut tmp_file = fs::File::create(path_buf.as_path()).unwrap();
    tmp_file.write_all(content).unwrap();
    tmp_file.sync_all().unwrap();

    // return file handle for both read and write
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path_buf.as_path());
    assert!(file.is_ok());
    file.unwrap()
}

/// Returns a path, in the same `target/debug/testdata` directory, that no other caller will pick.
///
/// The directory is shared by every test in the crate and `cargo nextest` runs each test in its
/// own process, several at a time, so the name has to be unique across processes as well as within
/// one. Drawing it from 65536 random values was not: `fs::File::create` truncates, so two tests
/// landing on the same name leave one of them reading a Parquet file that the other has just
/// emptied, and it fails with an out-of-range read that says nothing about the real cause. The
/// process ID plus a counter is unique among the processes alive at any one time, which is all
/// that is needed — a leftover file from an earlier run is simply overwritten.
pub fn get_temp_filename() -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    let mut path_buf = env::current_dir().unwrap();
    path_buf.push("target");
    path_buf.push("debug");
    path_buf.push("testdata");
    fs::create_dir_all(&path_buf).unwrap();
    path_buf.push(format!(
        "{}-{}",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ));

    path_buf
}
