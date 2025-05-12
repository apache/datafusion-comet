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

use datafusion::common::instant::Instant;
use once_cell::sync::Lazy;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};

pub(crate) static RECORDER: Lazy<Recorder> = Lazy::new(Recorder::new);

/// Log events using Chrome trace format JSON
/// https://github.com/catapult-project/catapult/blob/main/tracing/README.md
pub struct Recorder {
    now: Instant,
    writer: Arc<Mutex<BufWriter<File>>>,
}

impl Recorder {
    pub fn new() -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("comet-event-trace.json")
            .expect("Error writing tracing");

        let mut writer = BufWriter::new(file);

        // Write start of JSON array. Note that there is no requirement to write
        // the closing ']'.
        writer
            .write_all("[ ".as_bytes())
            .expect("Error writing tracing");
        Self {
            now: Instant::now(),
            writer: Arc::new(Mutex::new(writer)),
        }
    }
    pub fn begin_task(&self, name: &str) {
        self.log_event(name, "B")
    }

    pub fn end_task(&self, name: &str) {
        self.log_event(name, "E")
    }

    pub fn log_memory_usage(&self, name: &str, usage_bytes: u64) {
        let usage_mb = (usage_bytes as f64 / 1024.0 / 1024.0) as usize;
        let json = format!(
            "{{ \"name\": \"{name}\", \"cat\": \"PERF\", \"ph\": \"C\", \"pid\": 1, \"tid\": {}, \"ts\": {}, \"args\": {{ \"{name}\": {usage_mb} }} }},\n",
            Self::get_thread_id(),
            self.now.elapsed().as_micros()
        );
        let mut writer = self.writer.lock().unwrap();
        writer
            .write_all(json.as_bytes())
            .expect("Error writing tracing");
    }

    fn log_event(&self, name: &str, ph: &str) {
        let json = format!(
            "{{ \"name\": \"{}\", \"cat\": \"PERF\", \"ph\": \"{ph}\", \"pid\": 1, \"tid\": {}, \"ts\": {} }},\n",
            name,
            Self::get_thread_id(),
            self.now.elapsed().as_micros()
        );
        let mut writer = self.writer.lock().unwrap();
        writer
            .write_all(json.as_bytes())
            .expect("Error writing tracing");
    }

    fn get_thread_id() -> u64 {
        let thread_id = std::thread::current().id();
        format!("{:?}", thread_id)
            .trim_start_matches("ThreadId(")
            .trim_end_matches(")")
            .parse()
            .expect("Error parsing thread id")
    }
}

pub(crate) fn trace_begin(name: &str) {
    RECORDER.begin_task(name);
}

pub(crate) fn trace_end(name: &str) {
    RECORDER.end_task(name);
}

pub(crate) fn log_memory_usage(name: &str, value: u64) {
    RECORDER.log_memory_usage(name, value);
}
