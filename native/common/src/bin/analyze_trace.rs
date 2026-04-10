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
//! compares jemalloc usage against the sum of per-thread Comet memory pool
//! reservations. Reports any points where jemalloc exceeds the total pool size.
//!
//! Usage:
//!   cargo run --bin analyze_trace -- <path-to-comet-event-trace.json>

use serde::Deserialize;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::{env, fs::File};

/// A single Chrome trace event (only the fields we care about).
#[derive(Deserialize)]
struct TraceEvent {
    name: String,
    ph: String,
    #[allow(dead_code)]
    tid: u64,
    ts: u64,
    #[serde(default)]
    args: HashMap<String, serde_json::Value>,
}

/// Snapshot of memory state at a given timestamp.
struct MemorySnapshot {
    ts: u64,
    jemalloc: u64,
    pool_total: u64,
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

    // Latest jemalloc value (global, not per-thread)
    let mut latest_jemalloc: u64 = 0;
    // Per-thread pool reservations: thread_NNN -> bytes
    let mut pool_by_thread: HashMap<String, u64> = HashMap::new();
    // Points where jemalloc exceeded pool total
    let mut violations: Vec<MemorySnapshot> = Vec::new();
    // Track peak values
    let mut peak_jemalloc: u64 = 0;
    let mut peak_pool_total: u64 = 0;
    let mut peak_excess: u64 = 0;
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

        if event.name == "jemalloc_allocated" {
            if let Some(val) = event.args.get("jemalloc_allocated") {
                latest_jemalloc = val.as_u64().unwrap_or(0);
                if latest_jemalloc > peak_jemalloc {
                    peak_jemalloc = latest_jemalloc;
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

        // After each jemalloc or pool update, check the current state
        let pool_total: u64 = pool_by_thread.values().sum();
        if pool_total > peak_pool_total {
            peak_pool_total = pool_total;
        }

        if latest_jemalloc > 0 && pool_total > 0 && latest_jemalloc > pool_total {
            let excess = latest_jemalloc - pool_total;
            if excess > peak_excess {
                peak_excess = excess;
            }
            // Record violation (sample - don't record every single one)
            if violations.is_empty()
                || event.ts - violations.last().unwrap().ts > 1_000_000
                || excess == peak_excess
            {
                violations.push(MemorySnapshot {
                    ts: event.ts,
                    jemalloc: latest_jemalloc,
                    pool_total,
                });
            }
        }
    }

    // Print summary
    println!("=== Comet Trace Memory Analysis ===\n");
    println!("Counter events parsed: {counter_events}");
    println!("Threads with memory pools: {}", pool_by_thread.len());
    println!("Peak jemalloc allocated:   {}", format_bytes(peak_jemalloc));
    println!(
        "Peak pool total:           {}",
        format_bytes(peak_pool_total)
    );
    println!(
        "Peak excess (jemalloc - pool): {}",
        format_bytes(peak_excess)
    );
    println!();

    if violations.is_empty() {
        println!("OK: jemalloc never exceeded the total pool reservation.");
    } else {
        println!(
            "WARNING: jemalloc exceeded pool reservation at {} sampled points:\n",
            violations.len()
        );
        println!(
            "{:>14}  {:>14}  {:>14}  {:>14}",
            "Time (us)", "jemalloc", "pool_total", "excess"
        );
        println!("{}", "-".repeat(62));
        for snap in &violations {
            let excess = snap.jemalloc - snap.pool_total;
            println!(
                "{:>14}  {:>14}  {:>14}  {:>14}",
                snap.ts,
                format_bytes(snap.jemalloc),
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
    println!("\n  Total: {}", format_bytes(pool_by_thread.values().sum()));
}
