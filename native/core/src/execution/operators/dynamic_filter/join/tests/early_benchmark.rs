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

//! Reproducible component benchmark for selective and all-matching join chains.
//!
//! Run this ignored test with an optimized build and identical settings on both
//! revisions. It covers decoded native batches, not Spark or Parquet I/O.

use super::*;
use std::time::Instant;

fn benchmark_input(rows: usize, keys: usize, batch_rows: usize) -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("payload", DataType::Int32, false),
    ]));
    let batches = (0..rows)
        .step_by(batch_rows)
        .map(|offset| {
            let end = (offset + batch_rows).min(rows);
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from_iter_values(
                        (offset..end).map(|i| (i % keys) as i32),
                    )),
                    Arc::new(Int32Array::from_iter_values(
                        (offset..end).map(|i| i as i32),
                    )),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    memory_exec(batches)
}

fn benchmark_join(
    build: Arc<dyn ExecutionPlan>,
    probe: Arc<dyn ExecutionPlan>,
    probe_key: usize,
    config: &ConfigOptions,
) -> Arc<dyn ExecutionPlan> {
    let join = HashJoinExec::try_new(
        build,
        probe,
        vec![(
            Arc::new(Column::new("key", 0)),
            Arc::new(Column::new("key", probe_key)),
        )],
        None,
        &JoinType::Inner,
        None,
        PartitionMode::Partitioned,
        NullEquality::NullEqualsNothing,
        false,
    )
    .unwrap();
    PhysicalPlanner::apply_join_dynamic_filter(Arc::new(join), true, config).unwrap()
}

#[tokio::test]
#[ignore = "component benchmark; run explicitly with an optimized build"]
async fn early_join_filter_component_benchmark() {
    let rows = 1usize << 22;
    let keys = 4096usize;
    let batch_rows = 8192;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(batch_rows));
    let fact = benchmark_input(rows, keys, batch_rows);
    let dimension = benchmark_input(keys, keys, batch_rows);
    let repetitions = std::env::var("COMET_EARLY_BENCH_REPETITIONS")
        .ok()
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(5);
    for (case, accepted_keys) in [
        ("selective", 16usize),
        ("four_percent", 164usize),
        ("all_matching", keys),
    ] {
        let selection = benchmark_input(accepted_keys, keys, batch_rows);
        let expected_rows = rows / keys * accepted_keys;
        let expected_checksum: i64 = (0..rows)
            .filter(|i| i % keys < accepted_keys)
            .map(|i| i as i64)
            .sum();
        // Exclude construction of the in-memory inputs from the timed execution.
        let inner = benchmark_join(
            Arc::clone(&dimension),
            Arc::clone(&fact),
            0,
            ctx.copied_config().options(),
        );
        let outer = benchmark_join(
            selection,
            Arc::clone(&inner),
            2,
            ctx.copied_config().options(),
        );
        for repetition in 0..repetitions + 2 {
            let started = Instant::now();
            let mut stream = outer.execute(0, ctx.task_ctx()).unwrap();
            let mut actual_rows = 0;
            let mut checksum = 0i64;
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                actual_rows += batch.num_rows();
                let payload = batch
                    .column(5)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                checksum += payload.values().iter().map(|v| *v as i64).sum::<i64>();
            }
            let elapsed = started.elapsed();
            assert_eq!(actual_rows, expected_rows);
            assert_eq!(checksum, expected_checksum);
            if repetition >= 2 {
                println!("EARLY_JOIN_BENCH case={case} iteration={} rows={actual_rows} checksum={checksum} elapsed_ns={}", repetition - 2, elapsed.as_nanos());
            }
        }
        let count = |plan: &Arc<dyn ExecutionPlan>, name: &str| {
            let metrics = plan.metrics().unwrap();
            if name == "output_rows" {
                metrics.output_rows().unwrap_or(0)
            } else {
                metrics.sum_by_name(name).map(|m| m.as_usize()).unwrap_or(0)
            }
        };
        println!("EARLY_JOIN_METRICS case={case} intermediate_output_rows={} early_evaluated={} early_pruned={} early_bypassed={}",
            count(&inner, "output_rows"), count(&outer, "dynamic_filter_early_rows_evaluated"),
            count(&outer, "dynamic_filter_early_rows_pruned"), count(&outer, "dynamic_filter_early_rows_bypassed"));
    }
}
