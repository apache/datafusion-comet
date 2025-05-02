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

#[cfg(feature = "tracing")]
use datafusion::common::instant::Instant;
#[cfg(feature = "tracing")]
use once_cell::sync::Lazy;

#[cfg(feature = "tracing")]
pub(crate) static RECORDER: Lazy<Recorder> = Lazy::new(|| Recorder::new());

/// Log events using Chrome trace format JSON
/// https://github.com/catapult-project/catapult/blob/main/tracing/README.md
#[cfg(feature = "tracing")]
pub struct Recorder {
    now: Instant,
}

#[cfg(feature = "tracing")]
impl Recorder {
    pub fn new() -> Self {
        // Write start of JSON array. Note that there is no requirement to write
        // the closing ']'.
        print!("[ ");
        Self {
            now: Instant::now(),
        }
    }
    pub fn begin_task(&self, name: &str) {
        self.log_event(name, "B")
    }

    pub fn end_task(&self, name: &str) {
        self.log_event(name, "E")
    }

    fn log_event(&self, name: &str, ph: &str) {
        let thread_id = std::thread::current().id();

        // TODO could be more efficient
        let id_num: u64 = format!("{:?}", thread_id)
            .trim_start_matches("ThreadId(")
            .trim_end_matches(")")
            .parse()
            .unwrap();

        println!(
            "{{ \"name\": \"{}\", \"cat\": \"PERF\", \"ph\": \"{ph}\", \"pid\": 1, \"tid\": {}, \"ts\": {} }},",
            name,
            id_num,
            self.now.elapsed().as_nanos()
        );
    }
}

#[allow(unused_variables)]
pub(crate) fn trace_begin(name: &str) {
    #[cfg(feature = "tracing")]
    RECORDER.begin_task(name);
}

#[allow(unused_variables)]
pub(crate) fn trace_end(name: &str) {
    #[cfg(feature = "tracing")]
    RECORDER.end_task(name);
}
