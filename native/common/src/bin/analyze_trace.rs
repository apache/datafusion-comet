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

//! Analyzes a Comet chrome trace event log (`comet-event-trace.json`) and
//! compares the process-wide native allocation counter against the total memory
//! reserved by Comet's memory pools. Reports any points where the allocated
//! bytes exceed the total pool size.
//!
//! Usage:
//!   cargo run --bin analyze_trace -- <path-to-comet-event-trace.json>

use datafusion_comet_common::tracing::POOL_TOTAL_METRIC as POOL_TOTAL_COUNTER;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::{env, fs::File};

/// The process-wide allocation counters the tool understands, most preferred first.
///
/// `native_allocated` (emitted by every build) counts only the bytes Rust code holds from the
/// global allocator, so it is the tighter comparison against pool reservations.
/// `jemalloc_allocated` (the `jemalloc` feature) also includes jemalloc's own metadata. A trace
/// that carries both is analyzed against `native_allocated` alone; a trace with neither cannot be
/// analyzed.
const ALLOCATED_COUNTERS: [&str; 2] = ["native_allocated", "jemalloc_allocated"];

// The process-wide total of Comet's memory pool reservations, imported from the producer so the
// two cannot drift apart.
//
// Preferred over summing the per-thread `thread_NNN_comet_memory_reserved` counters. Those report
// the full reservation of a task-shared pool once per thread that references it, so adding them
// across threads multiplies a shared pool by its thread count. A trace without this counter is
// still analyzed from the per-thread sum, with a warning, so older traces remain readable.

/// A single Chrome trace event (only the fields we care about).
#[derive(Deserialize)]
struct TraceEvent {
    name: String,
    ph: String,
    tid: u64,
    ts: u64,
    #[serde(default)]
    args: HashMap<String, serde_json::Value>,
}

/// Snapshot of memory state at a given timestamp.
struct MemorySnapshot {
    ts: u64,
    allocated: u64,
    pool_total: u64,
}

/// What one pool-total source says about the trace.
///
/// Both sources are accumulated in the same pass and one is reported, because whether the trace
/// carries [`POOL_TOTAL_COUNTER`] is only known once a line containing it has been read.
#[derive(Default)]
struct Analysis {
    /// Highest pool total this source observed.
    peak_pool_total: u64,
    /// Largest `allocated - pool_total` over the comparisons this source could make.
    peak_excess: u64,
    /// A sample of the points where allocation exceeded the total.
    violations: Vec<MemorySnapshot>,
    /// How many comparisons this source made. Zero means it never produced a usable pair, which
    /// is worth reporting rather than passing off as "allocation never exceeded the total".
    comparisons: u64,
}

impl Analysis {
    fn observe_total(&mut self, pool_total: u64) {
        self.peak_pool_total = self.peak_pool_total.max(pool_total);
    }

    fn compare(&mut self, ts: u64, allocated: u64, pool_total: u64) {
        self.comparisons += 1;
        self.observe_total(pool_total);

        let Some(excess) = allocated.checked_sub(pool_total).filter(|e| *e > 0) else {
            return;
        };
        self.peak_excess = self.peak_excess.max(excess);
        // Sample the violations rather than listing every one, but always keep the worst.
        if self.violations.is_empty()
            || ts.saturating_sub(self.violations.last().unwrap().ts) > 1_000_000
            || excess == self.peak_excess
        {
            self.violations.push(MemorySnapshot {
                ts,
                allocated,
                pool_total,
            });
        }
    }
}

fn format_bytes(bytes: u64) -> String {
    const MB: f64 = 1024.0 * 1024.0;
    format!("{:.1} MB", bytes as f64 / MB)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: analyze_trace <path-to-comet-event-trace.json>");
        std::process::exit(1);
    }

    let file = File::open(&args[1]).expect("Failed to open trace file");
    let reader = BufReader::new(file);

    // Index into ALLOCATED_COUNTERS of the counter being analyzed, once one has been seen
    let mut source: Option<usize> = None;
    // Latest allocated value (global, not per-thread)
    let mut latest_allocated: u64 = 0;
    // Per-thread pool reservations: thread_NNN -> bytes. Reported at the end, and summed as the
    // pool total only for traces that predate the process-wide counter.
    let mut pool_by_thread: HashMap<String, u64> = HashMap::new();
    // Whether the trace carries the process-wide total, and its last value.
    let mut pool_total_counter: Option<u64> = None;
    // The allocation sample most recently emitted on each thread, waiting for that thread's
    // matching pool total. `executePlan` emits the allocation counter and the process-wide total
    // back to back on one thread, so pairing them by thread compares two values sampled at the
    // same instant. Comparing the latest allocation against whatever total arrived last instead
    // pairs a fresh allocation with a stale reservation and invents excess that was never held.
    let mut allocated_awaiting_total: HashMap<u64, u64> = HashMap::new();
    // Comparisons against the process-wide counter, and against the legacy per-thread sum.
    let mut paired = Analysis::default();
    let mut legacy = Analysis::default();
    // Track peak values
    let mut peak_allocated: u64 = 0;
    let mut counter_events: u64 = 0;

    // Each line is one JSON event, possibly with a trailing comma.
    // The file starts with "[ " on the first event line or as a prefix.
    for line in reader.lines() {
        let line = line.expect("Failed to read line");
        let trimmed = line.trim();

        // Skip empty lines or bare array brackets
        if trimmed.is_empty() || trimmed == "[" || trimmed == "]" {
            continue;
        }

        // Strip leading "[ " (first event) and trailing comma
        let json_str = trimmed
            .trim_start_matches("[ ")
            .trim_start_matches('[')
            .trim_end_matches(',');

        if json_str.is_empty() {
            continue;
        }

        // Only parse counter events (they contain "\"ph\": \"C\"")
        if !json_str.contains("\"ph\": \"C\"") {
            continue;
        }

        let event: TraceEvent = match serde_json::from_str(json_str) {
            Ok(e) => e,
            Err(_) => continue,
        };

        if event.ph != "C" {
            continue;
        }

        counter_events += 1;

        if let Some(rank) = ALLOCATED_COUNTERS
            .iter()
            .position(|name| *name == event.name)
        {
            match source {
                // A preferred counter is present in this trace; ignore the other one.
                Some(current) if current < rank => continue,
                Some(current) if current == rank => {}
                // First sighting of a more preferred counter. Start over so the peaks and
                // violations reported all come from a single source.
                _ => {
                    source = Some(rank);
                    latest_allocated = 0;
                    peak_allocated = 0;
                    allocated_awaiting_total.clear();
                    paired = Analysis::default();
                    legacy = Analysis::default();
                }
            }
            if let Some(val) = event.args.get(&event.name) {
                latest_allocated = val.as_u64().unwrap_or(0);
                peak_allocated = peak_allocated.max(latest_allocated);
                allocated_awaiting_total.insert(event.tid, latest_allocated);
            }
        } else if event.name == POOL_TOTAL_COUNTER {
            // Must be matched before the per-thread branch below, whose `contains` check would
            // otherwise also match this name and fold the process-wide total into the map.
            if let Some(val) = event.args.get(&event.name) {
                let pool_total = val.as_u64().unwrap_or(0);
                pool_total_counter = Some(pool_total);
                paired.observe_total(pool_total);
                // Only compare against the allocation sampled in this thread's own group. An
                // observed zero reservation is a real value that allocation can exceed, so a
                // paired zero is a genuine comparison, not a missing sample.
                if let Some(allocated) = allocated_awaiting_total.remove(&event.tid) {
                    paired.compare(event.ts, allocated, pool_total);
                }
            }
        } else if event.name.contains("comet_memory_reserved") {
            // Name format: thread_NNN_comet_memory_reserved
            let thread_key = event.name.clone();
            if let Some(val) = event.args.get(&event.name) {
                let bytes = val.as_u64().unwrap_or(0);
                pool_by_thread.insert(thread_key, bytes);
            }
        } else {
            // Skip jvm_heap_used and other counters
            continue;
        }

        // Legacy association, for traces recorded before the process-wide total existed: compare
        // the latest allocation against the running per-thread sum after every counter event.
        // There is nothing to pair on in those traces, so this is the best they support.
        //
        // A comparison needs one sample of each side, not a positive one. A reservation that has
        // been observed at zero is a real value, and allocation standing above it is exactly the
        // signal worth finding: memory still held after the pool released it. Requiring a positive
        // total instead would drop those samples silently.
        let per_thread_sum: u64 = pool_by_thread.values().sum();
        legacy.observe_total(per_thread_sum);
        if source.is_some() && !pool_by_thread.is_empty() {
            legacy.compare(event.ts, latest_allocated, per_thread_sum);
        }
    }

    let Some(source) = source.map(|rank| ALLOCATED_COUNTERS[rank]) else {
        eprintln!(
            "No process-wide allocation counter found in the trace: expected one of {}. \
             Was the trace produced by a native library older than the `native_allocated` \
             counter?",
            ALLOCATED_COUNTERS.join(", ")
        );
        std::process::exit(1);
    };

    let have_pool_total_counter = pool_total_counter.is_some();
    let analysis = if have_pool_total_counter {
        &paired
    } else {
        &legacy
    };

    // Print summary
    println!("=== Comet Trace Memory Analysis ===\n");
    println!("Counter events parsed: {counter_events}");
    println!("Allocation counter:    {source}");
    if have_pool_total_counter {
        println!("Pool total source:     {POOL_TOTAL_COUNTER} (process-wide)");
    } else {
        println!(
            "Pool total source:     sum of {} per-thread counters",
            pool_by_thread.len()
        );
        println!(
            "WARNING: this trace predates {POOL_TOTAL_COUNTER}, so the total below is the sum of\n\
             the per-thread counters. That is not the same measure: a shared pool reports its full\n\
             reservation on every thread referencing it, which inflates the total, while a thread\n\
             that has not reported yet contributes nothing, which deflates it. The excess below can\n\
             err in either direction."
        );
    }
    println!("Peak {source}:   {}", format_bytes(peak_allocated));
    println!(
        "Peak pool total:           {}",
        format_bytes(analysis.peak_pool_total)
    );
    println!(
        "Peak excess ({source} - pool): {}",
        format_bytes(analysis.peak_excess)
    );
    println!();

    if analysis.comparisons == 0 {
        if have_pool_total_counter {
            println!(
                "No allocation sample was emitted next to a {POOL_TOTAL_COUNTER} sample on the\n\
                 same thread, so there was nothing to compare. The two are emitted together when\n\
                 a traced plan finishes executing."
            );
        } else {
            println!(
                "No pool reservation samples in the trace, so there is nothing to compare against."
            );
        }
    } else if analysis.violations.is_empty() {
        println!("OK: {source} never exceeded the total pool reservation.");
    } else {
        println!(
            "WARNING: {source} exceeded pool reservation at {} sampled points:\n",
            analysis.violations.len()
        );
        println!(
            "{:>14}  {:>18}  {:>14}  {:>14}",
            "Time (us)", source, "pool_total", "excess"
        );
        println!("{}", "-".repeat(66));
        for snap in &analysis.violations {
            let excess = snap.allocated - snap.pool_total;
            println!(
                "{:>14}  {:>18}  {:>14}  {:>14}",
                snap.ts,
                format_bytes(snap.allocated),
                format_bytes(snap.pool_total),
                format_bytes(excess),
            );
        }
    }

    // Show final per-thread pool state
    println!("\n--- Final per-thread pool reservations ---\n");
    let mut threads: Vec<_> = pool_by_thread.iter().collect();
    threads.sort_by_key(|(k, _)| (*k).clone());
    for (thread, bytes) in &threads {
        println!("  {thread}: {}", format_bytes(**bytes));
    }
    println!(
        "\n  Total: {}",
        format_bytes(pool_total_counter.unwrap_or_else(|| pool_by_thread.values().sum()))
    );
}
