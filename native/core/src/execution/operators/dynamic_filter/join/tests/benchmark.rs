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

//! Reproduce the five schema-guard fixtures from PR #6067 with fresh native plans.
//!
//! Run from native/ using the standard optimized release profile:
//! COMET_SCHEMA_GUARD_BENCH_DIR=/tmp/comet-schema-guard cargo test --release \
//!   -p datafusion-comet --lib schema_guard_benchmark -- --ignored --nocapture
//!
//! Build and archive each revision's test binary separately, then run them in
//! base/head/head/base order against the same COMET_SCHEMA_GUARD_BENCH_DIR. Each
//! process performs two warmups and seven measured samples per case/mode,
//! alternating the ON/OFF execution order. Combine the fourteen measured samples
//! per revision/mode when comparing medians; do not average process medians.
//! Fixture creation/validation and session construction are outside the timer.
//! Timing includes scan/join planning and execution with warm local caches.
//! Fixture names are ASCII, so case-insensitive resolution needs no initialized
//! JVM or Java classes. The binary may still link libjvm; include the JDK's
//! lib/server directory in LD_LIBRARY_PATH when running an archived binary.

use super::read_adaptations::{ParquetJoinCase, ParquetJoinFixture};
use super::*;
use std::time::{Duration, Instant};

struct Measurement {
    elapsed: Duration,
    data_bytes: usize,
    metadata_bytes: usize,
    row_groups_read: usize,
}

impl Measurement {
    fn print(&self, case: ParquetJoinCase, enabled: bool, sample: &str) {
        println!(
            "{},{enabled},{sample},{:.3},{},{},{}",
            case.name(),
            self.elapsed.as_secs_f64() * 1_000.0,
            self.data_bytes,
            self.row_groups_read,
            self.metadata_bytes,
        );
    }
}

async fn measure(
    fixture: &ParquetJoinFixture,
    session: &Arc<SessionContext>,
    enabled: bool,
) -> Measurement {
    let started = Instant::now();
    let (scan, plan) = fixture.plan(session, enabled);
    let output = collect(Arc::clone(&plan), session.task_ctx())
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(row_count(&output), fixture.output_rows);
    if enabled {
        assert_eq!(metric(&plan, "dynamic_filter_join_filters_attached"), 1);
    }
    let row_groups_pruned = pruning_metric(&scan, "row_groups_pruned_statistics");
    assert!(row_groups_pruned <= fixture.row_groups);
    Measurement {
        elapsed,
        data_bytes: ParquetJoinFixture::data_bytes(&scan),
        metadata_bytes: scan
            .metrics()
            .unwrap()
            .sum_by_name("scan_io_metadata_bytes")
            .unwrap()
            .as_usize(),
        row_groups_read: fixture.row_groups - row_groups_pruned,
    }
}

#[tokio::test]
#[ignore = "manual release benchmark; creates the five Parquet fixtures from PR #6067"]
#[allow(clippy::assertions_on_constants)] // Reject debug runs, not ordinary debug test builds.
async fn schema_guard_benchmark() {
    assert!(!cfg!(debug_assertions), "run this benchmark with --release");
    let temporary_directory = tempfile::tempdir().unwrap();
    let directory = std::env::var_os("COMET_SCHEMA_GUARD_BENCH_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| temporary_directory.path().to_path_buf());
    std::fs::create_dir_all(&directory).unwrap();
    let directory = directory.canonicalize().unwrap();
    eprintln!(
        "Parquet schema-guard benchmark fixtures: {}",
        directory.display()
    );
    println!("case,enabled,sample,elapsed_ms,data_bytes,row_groups_read,metadata_bytes");
    for case in [
        ParquetJoinCase::Direct,
        ParquetJoinCase::Promotion,
        ParquetJoinCase::StructSubset,
        ParquetJoinCase::WideSelective,
        ParquetJoinCase::WideNonPruning,
    ] {
        let (files, rows_per_file, rows_per_group) = match case {
            ParquetJoinCase::WideSelective | ParquetJoinCase::WideNonPruning => (128, 2048, 1024),
            _ => (16, 65_536, 8192),
        };
        let fixture =
            ParquetJoinFixture::new(&directory, case, files, rows_per_file, rows_per_group);
        let session = ParquetJoinFixture::session();
        let mut samples: [Vec<Measurement>; 2] = [Vec::new(), Vec::new()];
        for iteration in 0..9 {
            for enabled in [iteration % 2 != 0, iteration % 2 == 0] {
                let measurement = measure(&fixture, &session, enabled).await;
                if iteration >= 2 {
                    measurement.print(case, enabled, &(iteration - 2).to_string());
                    samples[usize::from(enabled)].push(measurement);
                }
            }
        }
        for (mode, samples) in samples.iter_mut().enumerate() {
            // Reader work must be deterministic even when elapsed times vary.
            assert!(samples.iter().all(|sample| {
                sample.data_bytes == samples[0].data_bytes
                    && sample.row_groups_read == samples[0].row_groups_read
            }));
            samples.sort_by_key(|sample| sample.elapsed);
            samples[samples.len() / 2].print(case, mode != 0, "median");
        }
    }
}
