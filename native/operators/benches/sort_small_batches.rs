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

//! Micro-benchmarks for `SortInputCoalesceExec` in front of DataFusion's `SortExec`.
//!
//! A reduce task that reads a shuffle from many map tasks gets one small batch per block. The
//! sort merges one stream per buffered batch, so its cost grows with the number of batches.
//! This compares the sort with and without combining its input, over a range of input batch
//! sizes, with enough memory to sort in memory and with a memory limit that forces spills, and
//! with one sort key and with two. DataFusion combines runs itself only for one key in memory.
//! Keys arrive either scattered, so every batch spans the whole key range, or already in order,
//! the most predictable merge. Keys have a fixed length and no value is null.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Fields};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{common::collect, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_comet_operators::SortInputCoalesceExec;
use tokio::runtime::Runtime;

const ROWS: usize = 200_000;
/// The session batch size, which is also the combined batch's row target.
const BATCH_SIZE: usize = 8192;
/// About a fifth of the input, so the sort spills several times.
const SPILL_MEMORY_LIMIT: usize = 24 * 1024 * 1024;

/// A string key, a low-cardinality group key, and wide rows: eight longs and a struct, like an
/// aggregate's state.
fn batches(rows_per_batch: usize, scattered: bool) -> Vec<RecordBatch> {
    let fields = Fields::from(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    (0..ROWS)
        .step_by(rows_per_batch)
        .map(|start| {
            let rows = rows_per_batch.min(ROWS - start);
            let ids: Vec<usize> = (start..start + rows)
                .map(|i| {
                    if scattered {
                        i.wrapping_mul(2_654_435_761) % ROWS
                    } else {
                        i
                    }
                })
                .collect();
            let mut columns: Vec<(String, ArrayRef)> = vec![(
                "key".into(),
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|i| format!("https://example.com/{i:08}")),
                )),
            )];
            columns.push((
                "g".into(),
                Arc::new(Int64Array::from_iter_values(
                    ids.iter().map(|&i| (i % 64) as i64),
                )),
            ));
            for c in 0..8 {
                columns.push((
                    format!("v{c}"),
                    Arc::new(Int64Array::from_iter_values(
                        ids.iter().map(|&i| (i * (c + 1)) as i64),
                    )),
                ));
            }
            columns.push((
                "s".into(),
                Arc::new(StructArray::new(
                    fields.clone(),
                    vec![
                        Arc::new(Int64Array::from_iter_values(ids.iter().map(|&i| i as i64))),
                        Arc::new(StringArray::from_iter_values(
                            ids.iter().map(|i| format!("value-{}", i % 1000)),
                        )),
                    ],
                    None,
                )),
            ));
            RecordBatch::try_from_iter(columns).unwrap()
        })
        .collect()
}

fn sort(
    rt: &Runtime,
    input: &[RecordBatch],
    keys: &[&str],
    coalesce: bool,
    memory_limit: Option<usize>,
) {
    let schema = input[0].schema();
    let mut runtime = RuntimeEnvBuilder::new();
    if let Some(limit) = memory_limit {
        runtime = runtime.with_memory_limit(limit, 1.0);
    }
    let config = SessionConfig::new()
        .with_batch_size(BATCH_SIZE)
        .with_sort_spill_reservation_bytes(1024 * 1024);
    let ctx = SessionContext::new_with_config_rt(config.clone(), runtime.build_arc().unwrap());
    let source =
        MemorySourceConfig::try_new_exec(&[input.to_vec()], Arc::clone(&schema), None).unwrap();
    let sort_input: Arc<dyn ExecutionPlan> = if coalesce {
        let gate = config.options().execution.sort_in_place_threshold_bytes;
        Arc::new(SortInputCoalesceExec::new(source, BATCH_SIZE, gate))
    } else {
        source
    };
    let ordering =
        LexOrdering::new(keys.iter().map(|name| {
            PhysicalSortExpr::new(col(name, &schema).unwrap(), SortOptions::default())
        }))
        .unwrap();
    let sort = Arc::new(SortExec::new(ordering, sort_input));
    let output = rt
        .block_on(collect(sort.execute(0, ctx.task_ctx()).unwrap()))
        .unwrap();
    assert_eq!(output.iter().map(|b| b.num_rows()).sum::<usize>(), ROWS);
}

fn bench(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let key_sets: [(&str, &[&str]); 2] = [("one_key", &["key"]), ("two_keys", &["g", "key"])];
    for (name, memory_limit) in [("in_memory", None), ("spill", Some(SPILL_MEMORY_LIMIT))] {
        for (keys_name, keys) in key_sets {
            for (shape, scattered) in [("scattered", true), ("in_order", false)] {
                let mut group =
                    c.benchmark_group(format!("sort_small_batches/{name}/{keys_name}/{shape}"));
                group.sample_size(10);
                for rows_per_batch in [16, 64, 256, 384, 512, 1024, 8192] {
                    let input = batches(rows_per_batch, scattered);
                    for (label, coalesce) in [("raw", false), ("coalesced", true)] {
                        group.bench_with_input(
                            BenchmarkId::new(label, rows_per_batch),
                            &input,
                            |b, input| b.iter(|| sort(&rt, input, keys, coalesce, memory_limit)),
                        );
                    }
                }
                group.finish();
            }
        }
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
