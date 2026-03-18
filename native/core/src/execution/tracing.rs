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
use std::sync::Mutex;

pub(crate) static RECORDER: Lazy<Recorder> = Lazy::new(Recorder::new);

/// Log events using Chrome trace format JSON
/// https://github.com/catapult-project/catapult/blob/main/tracing/README.md
pub struct Recorder {
    now: Instant,
    pid: u32,
    /// None if the trace file could not be opened or a write error has occurred.
    writer: Mutex<Option<BufWriter<File>>>,
}

impl Recorder {
    pub fn new() -> Self {
        let pid = std::process::id();
        // Include the PID in the filename so that each executor process writes to
        // its own file, avoiding interleaved output and data corruption.
        let path = format!("comet-event-trace-{pid}.json");
        let writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .ok()
            .and_then(|file| {
                let mut w = BufWriter::new(file);
                // Write start of JSON array. Note that there is no requirement to
                // write the closing ']'.
                w.write_all(b"[ ").ok()?;
                Some(w)
            });

        Self {
            now: Instant::now(),
            pid,
            writer: Mutex::new(writer),
        }
    }

    pub fn begin_task(&self, name: &str) {
        self.log_event(name, "B")
    }

    pub fn end_task(&self, name: &str) {
        self.log_event(name, "E")
    }

    pub fn log_memory_usage(&self, name: &str, usage_bytes: u64) {
        let key = format!("{name}_bytes");
        let json = format!(
            "{{ \"name\": \"{name}\", \"cat\": \"PERF\", \"ph\": \"C\", \"pid\": {}, \"tid\": {}, \"ts\": {}, \"args\": {{ \"{key}\": {usage_bytes} }} }},\n",
            self.pid,
            Self::get_thread_id(),
            self.now.elapsed().as_micros()
        );
        self.write(&json);
    }

    fn log_event(&self, name: &str, ph: &str) {
        let json = format!(
            "{{ \"name\": \"{}\", \"cat\": \"PERF\", \"ph\": \"{ph}\", \"pid\": {}, \"tid\": {}, \"ts\": {} }},\n",
            name,
            self.pid,
            Self::get_thread_id(),
            self.now.elapsed().as_micros()
        );
        self.write(&json);
    }

    fn write(&self, json: &str) {
        if let Ok(mut guard) = self.writer.lock() {
            if let Some(ref mut w) = *guard {
                if w.write_all(json.as_bytes()).is_err() {
                    // Disable tracing on write failure to avoid repeated errors.
                    *guard = None;
                }
            }
        }
    }

    fn get_thread_id() -> u64 {
        std::thread::current().id().as_u64().get()
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.writer.lock() {
            if let Some(ref mut w) = *guard {
                let _ = w.flush();
            }
        }
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

pub(crate) fn with_trace<T, F>(label: &str, tracing_enabled: bool, f: F) -> T
where
    F: FnOnce() -> T,
{
    if tracing_enabled {
        trace_begin(label);
    }

    let result = f();

    if tracing_enabled {
        trace_end(label);
    }

    result
}

pub(crate) async fn with_trace_async<F, Fut, T>(label: &str, tracing_enabled: bool, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    if tracing_enabled {
        trace_begin(label);
    }

    let result = f().await;

    if tracing_enabled {
        trace_end(label);
    }

    result
}
