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

//! Micro-benchmarks for `ExplodeExec`, the operator behind Spark's `explode` and `posexplode`.
//!
//! `CometExplodeBenchmark` on the JVM side measures the same operator end to end, where the
//! Parquet scan and the counting aggregate are a large share of the total. This one runs the
//! operator over in-memory batches so a change to the unnesting kernels shows up undiluted.
//!
//! The dimensions are the ones that drive its cost: how far each row fans out, the element type
//! being unnested, how many columns are replicated alongside the generated one, and whether the
//! input has the NULL rows that force outer semantics.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, ListArray, StringArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use comet::execution::operators::ExplodeExec;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::UnnestOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::unnest::ListUnnest;
use datafusion::physical_plan::{common::collect, ExecutionPlan};
use datafusion::prelude::SessionConfig;
use tokio::runtime::Runtime;

/// Input rows per batch, and batches per run. 8192 is DataFusion's default `batch_size`, so the
/// operator chunks the input rather than seeing it whole.
const ROWS_PER_BATCH: usize = 8192;
const BATCHES: usize = 8;

/// The element types worth distinguishing: a fixed-width primitive, a variable-width type whose
/// gather has to rebuild offsets and copy bytes, and a nested type that gathers per field.
#[derive(Clone, Copy, PartialEq)]
enum Element {
    Int64,
    Utf8,
    Struct,
}

impl Element {
    fn name(self) -> &'static str {
        match self {
            Element::Int64 => "bigint",
            Element::Utf8 => "string",
            Element::Struct => "struct",
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Element::Int64 => DataType::Int64,
            Element::Utf8 => DataType::Utf8,
            Element::Struct => DataType::Struct(self.struct_fields()),
        }
    }

    fn struct_fields(self) -> Fields {
        Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ])
    }

    /// A flat child array of `count` elements, which the list offsets then carve into rows.
    fn values(self, count: usize) -> ArrayRef {
        let ints = Int64Array::from_iter_values((0..count).map(|i| i as i64));
        match self {
            Element::Int64 => Arc::new(ints),
            Element::Utf8 => Arc::new(StringArray::from_iter_values(
                (0..count).map(|i| format!("str_{i}")),
            )),
            Element::Struct => {
                let strings = StringArray::from_iter_values((0..count).map(|i| format!("str_{i}")));
                Arc::new(StructArray::new(
                    self.struct_fields(),
                    vec![Arc::new(ints), Arc::new(strings)],
                    None,
                ))
            }
        }
    }
}

/// One input batch: a `List` column of `fan_out`-element rows, plus `carried` passthrough
/// columns that unnesting has to replicate.
///
/// With `nulls`, every tenth row is a NULL list. That is the shape `explode_outer` sees, and it
/// is also what decides whether the unnested column can be sliced out of the child or has to be
/// gathered: a NULL row under outer semantics is padded, which breaks the run.
fn input_batch(element: Element, fan_out: usize, carried: usize, nulls: bool) -> RecordBatch {
    let total = ROWS_PER_BATCH * fan_out;
    let offsets: Vec<i32> = (0..=ROWS_PER_BATCH).map(|r| (r * fan_out) as i32).collect();
    let null_buffer =
        nulls.then(|| NullBuffer::from_iter((0..ROWS_PER_BATCH).map(|row| row % 10 != 0)));

    let list = ListArray::new(
        Arc::new(Field::new("item", element.data_type(), true)),
        OffsetBuffer::new(offsets.into()),
        element.values(total),
        null_buffer,
    );

    let mut fields = vec![Field::new("arr", list.data_type().clone(), true)];
    let mut columns: Vec<ArrayRef> = vec![Arc::new(list)];
    for c in 0..carried {
        fields.push(Field::new(format!("k{c}"), DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from_iter_values(
            (0..ROWS_PER_BATCH).map(|r| (r + c) as i64),
        )));
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// The operator's output schema: the unnested element column, then the passthrough columns.
///
/// This mirrors what the planner builds, except that the planner puts the passthrough columns
/// first; the order does not change the work, only which index the unnest targets.
fn output_schema(element: Element, carried: usize) -> SchemaRef {
    let mut fields = vec![Field::new("arr", element.data_type(), true)];
    for c in 0..carried {
        fields.push(Field::new(format!("k{c}"), DataType::Int64, true));
    }
    Arc::new(Schema::new(fields))
}

fn explode_plan(
    element: Element,
    fan_out: usize,
    carried: usize,
    outer: bool,
) -> Arc<dyn ExecutionPlan> {
    let batches: Vec<RecordBatch> = (0..BATCHES)
        .map(|_| input_batch(element, fan_out, carried, outer))
        .collect();
    let schema = batches[0].schema();
    let source = MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap();

    Arc::new(
        ExplodeExec::new(
            source,
            vec![ListUnnest {
                index_in_input_schema: 0,
                depth: 1,
            }],
            vec![],
            output_schema(element, carried),
            UnnestOptions {
                preserve_nulls: outer,
                recursions: vec![],
            },
        )
        .unwrap(),
    )
}

fn run(runtime: &Runtime, plan: &Arc<dyn ExecutionPlan>, ctx: &Arc<TaskContext>) {
    let stream = plan.execute(0, Arc::clone(ctx)).unwrap();
    let batches = runtime.block_on(collect(stream)).unwrap();
    assert!(!batches.is_empty());
}

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let ctx = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(ROWS_PER_BATCH)),
    );

    let mut group = c.benchmark_group("explode_fan_out");
    for fan_out in [2usize, 10, 100] {
        let plan = explode_plan(Element::Int64, fan_out, 0, false);
        group.bench_with_input(BenchmarkId::from_parameter(fan_out), &fan_out, |b, _| {
            b.iter(|| run(&runtime, &plan, &ctx))
        });
    }
    group.finish();

    let mut group = c.benchmark_group("explode_element_type");
    for element in [Element::Int64, Element::Utf8, Element::Struct] {
        let plan = explode_plan(element, 10, 0, false);
        group.bench_function(element.name(), |b| b.iter(|| run(&runtime, &plan, &ctx)));
    }
    group.finish();

    let mut group = c.benchmark_group("explode_carried_columns");
    for carried in [0usize, 3] {
        let plan = explode_plan(Element::Int64, 10, carried, false);
        group.bench_with_input(BenchmarkId::from_parameter(carried), &carried, |b, _| {
            b.iter(|| run(&runtime, &plan, &ctx))
        });
    }
    group.finish();

    // NULL rows under outer semantics are padded, so this is the shape that cannot be served by
    // slicing the child and has to gather instead. Kept as its own group so the two paths are
    // not averaged together.
    let mut group = c.benchmark_group("explode_outer_with_nulls");
    for element in [Element::Int64, Element::Utf8] {
        let plan = explode_plan(element, 10, 0, true);
        group.bench_function(element.name(), |b| b.iter(|| run(&runtime, &plan, &ctx)));
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
