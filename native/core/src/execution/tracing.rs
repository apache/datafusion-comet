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

pub(crate) static RECORDER: Lazy<Recorder> = Lazy::new(|| Recorder::new());

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
            .unwrap();

        let mut writer = BufWriter::new(file);

        // Write start of JSON array. Note that there is no requirement to write
        // the closing ']'.
        writer.write_all("[ ".as_bytes()).unwrap();
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

    pub fn log_counter(&self, name: &str, value: u64) {
        let json = format!(
            "{{ \"name\": \"{name}\", \"cat\": \"PERF\", \"ph\": \"C\", \"pid\": 1, \"tid\": {}, \"ts\": {}, \"args\": {{ \"{name}\": {value} }} }},\n",
            Self::get_thread_id(),
            self.now.elapsed().as_nanos()
        );
        let mut writer = self.writer.lock().unwrap();
        writer.write_all(json.as_bytes()).unwrap();
    }

    fn log_event(&self, name: &str, ph: &str) {
        let json = format!(
            "{{ \"name\": \"{}\", \"cat\": \"PERF\", \"ph\": \"{ph}\", \"pid\": 1, \"tid\": {}, \"ts\": {} }},\n",
            name,
            Self::get_thread_id(),
            self.now.elapsed().as_nanos()
        );
        let mut writer = self.writer.lock().unwrap();
        writer.write_all(json.as_bytes()).unwrap();
    }

    fn get_thread_id() -> u64 {
        let thread_id = std::thread::current().id();
        format!("{:?}", thread_id)
            .trim_start_matches("ThreadId(")
            .trim_end_matches(")")
            .parse()
            .unwrap()
    }
}

pub(crate) fn trace_begin(name: &str) {
    RECORDER.begin_task(name);
}

pub(crate) fn trace_end(name: &str) {
    RECORDER.end_task(name);
}

pub(crate) fn log_counter(name: &str, value: u64) {
    RECORDER.log_counter(name, value);
}
pub(crate) struct TraceGuard<'a> {
    label: &'a str,
}

impl<'a> TraceGuard<'a> {
    pub fn new(label: &'a str) -> Self {
        trace_begin(label);
        Self { label }
    }
}

impl Drop for TraceGuard<'_> {
    fn drop(&mut self) {
        trace_end(self.label);
    }
}
