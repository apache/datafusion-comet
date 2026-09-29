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
//! being unnested, how many columns are replicated alongside the generated one, whether the input
//! holds the NULL and empty rows that outer semantics pad, and whether a parallel positions column
//! is unnested alongside the array as `posexplode` does.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, ListArray, StringArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use comet::execution::operators::ExplodeExec;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::{NullHandling, UnnestOptions};
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

/// Which rows the input holds.
///
/// `Dense` is the plain `explode` shape, where every row fans out and the unnested column can be
/// sliced straight out of the child. `NullsAndEmpties` is the shape `explode_outer` exists for:
/// both a NULL row and an empty row are padded to one NULL, which breaks the contiguous run and
/// forces the gather. Spark treats the two identically, so a benchmark of the outer path that
/// holds only NULL rows leaves the empty-row substitution unmeasured.
#[derive(Clone, Copy, PartialEq)]
enum RowMix {
    Dense,
    NullsAndEmpties,
}

impl RowMix {
    fn name(self) -> &'static str {
        match self {
            RowMix::Dense => "dense",
            RowMix::NullsAndEmpties => "nulls_and_empties",
        }
    }

    /// The per-row element count. Every tenth row is NULL and every tenth is empty, so a batch
    /// holds a fifth padded rows.
    fn row_len(self, fan_out: usize, row: usize) -> usize {
        match self {
            RowMix::Dense => fan_out,
            RowMix::NullsAndEmpties if self.is_null(row) || row % 10 == 5 => 0,
            RowMix::NullsAndEmpties => fan_out,
        }
    }

    fn is_null(self, row: usize) -> bool {
        self == RowMix::NullsAndEmpties && row.is_multiple_of(10)
    }

    /// The options the planner builds for this shape. `Dense` stands in for plain `explode`,
    /// which drops NULL and empty rows alike.
    fn unnest_options(self) -> UnnestOptions {
        UnnestOptions::new().with_null_handling(match self {
            RowMix::Dense => NullHandling::Drop,
            RowMix::NullsAndEmpties => NullHandling::PreserveAndExpandEmpty,
        })
    }
}

/// One input batch: a `List` column of `mix`-shaped rows, optionally a parallel `List<Int32>` of
/// positions, plus `carried` passthrough columns that unnesting has to replicate.
///
/// The positions column is what `ListPositionsExpr` builds for `posexplode`: the same offsets and
/// the same validity as the array, with values `0..len`. Unnesting the two together is the only
/// shape that exercises the multi-array row-wise maximum in `find_longest_length`.
fn input_batch(
    element: Element,
    fan_out: usize,
    carried: usize,
    mix: RowMix,
    positions: bool,
) -> RecordBatch {
    let offsets: Vec<i32> = std::iter::once(0)
        .chain((0..ROWS_PER_BATCH).scan(0i32, |end, row| {
            *end += mix.row_len(fan_out, row) as i32;
            Some(*end)
        }))
        .collect();
    let total = *offsets.last().unwrap() as usize;
    let offsets = OffsetBuffer::new(offsets.into());

    let nulls = (mix != RowMix::Dense)
        .then(|| NullBuffer::from_iter((0..ROWS_PER_BATCH).map(|row| !mix.is_null(row))));

    let list = ListArray::new(
        Arc::new(Field::new("item", element.data_type(), true)),
        offsets.clone(),
        element.values(total),
        nulls.clone(),
    );

    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();

    if positions {
        // Per-row lengths come back off the offsets rather than a second pass over `row_len`.
        let pos_values =
            Int32Array::from_iter_values(offsets.windows(2).flat_map(|w| 0..w[1] - w[0]));
        let pos = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            offsets,
            Arc::new(pos_values),
            nulls,
        );
        fields.push(Field::new("pos", pos.data_type().clone(), true));
        columns.push(Arc::new(pos));
    }

    fields.push(Field::new("arr", list.data_type().clone(), true));
    columns.push(Arc::new(list));

    for c in 0..carried {
        fields.push(Field::new(format!("k{c}"), DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from_iter_values(
            (0..ROWS_PER_BATCH).map(|r| (r + c) as i64),
        )));
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// The operator's output schema: the unnested position column when positional, the unnested
/// element column, then the passthrough columns.
///
/// This mirrors what the planner builds, except that the planner puts the passthrough columns
/// first; the order does not change the work, only which index the unnest targets.
fn output_schema(element: Element, carried: usize, positions: bool) -> SchemaRef {
    let mut fields = Vec::new();
    if positions {
        fields.push(Field::new("pos", DataType::Int32, true));
    }
    fields.push(Field::new("arr", element.data_type(), true));
    for c in 0..carried {
        fields.push(Field::new(format!("k{c}"), DataType::Int64, true));
    }
    Arc::new(Schema::new(fields))
}

fn explode_plan(
    element: Element,
    fan_out: usize,
    carried: usize,
    mix: RowMix,
    positions: bool,
) -> Arc<dyn ExecutionPlan> {
    let batches: Vec<RecordBatch> = (0..BATCHES)
        .map(|_| input_batch(element, fan_out, carried, mix, positions))
        .collect();
    let schema = batches[0].schema();
    let source = MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap();

    // Positional unnesting targets the positions column and the array together, in the order the
    // planner projects them: `0..=1` when positional, just the array at 0 otherwise.
    let list_unnests = (0..=usize::from(positions))
        .map(|index_in_input_schema| ListUnnest {
            index_in_input_schema,
            depth: 1,
        })
        .collect();

    Arc::new(
        ExplodeExec::new(
            source,
            list_unnests,
            vec![],
            output_schema(element, carried, positions),
            mix.unnest_options(),
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
        let plan = explode_plan(Element::Int64, fan_out, 0, RowMix::Dense, false);
        group.bench_with_input(BenchmarkId::from_parameter(fan_out), &fan_out, |b, _| {
            b.iter(|| run(&runtime, &plan, &ctx))
        });
    }
    group.finish();

    let mut group = c.benchmark_group("explode_element_type");
    for element in [Element::Int64, Element::Utf8, Element::Struct] {
        let plan = explode_plan(element, 10, 0, RowMix::Dense, false);
        group.bench_function(element.name(), |b| b.iter(|| run(&runtime, &plan, &ctx)));
    }
    group.finish();

    let mut group = c.benchmark_group("explode_carried_columns");
    for carried in [0usize, 3] {
        let plan = explode_plan(Element::Int64, 10, carried, RowMix::Dense, false);
        group.bench_with_input(BenchmarkId::from_parameter(carried), &carried, |b, _| {
            b.iter(|| run(&runtime, &plan, &ctx))
        });
    }
    group.finish();

    // NULL and empty rows under outer semantics are padded, so this is the shape that cannot be
    // served by slicing the child and has to gather instead. Kept as its own group so the two
    // paths are not averaged together.
    let mut group = c.benchmark_group("explode_outer_with_nulls");
    for element in [Element::Int64, Element::Utf8] {
        let plan = explode_plan(element, 10, 0, RowMix::NullsAndEmpties, false);
        group.bench_function(element.name(), |b| b.iter(|| run(&runtime, &plan, &ctx)));
    }
    group.finish();

    // `posexplode` unnests the positions column alongside the array, which is the only shape that
    // reaches the multi-array row-wise maximum in `find_longest_length`. Short arrays are the
    // interesting case: the per-batch length work is fixed, so the shorter the rows the larger
    // its share of the total.
    let mut group = c.benchmark_group("posexplode_fan_out");
    for mix in [RowMix::Dense, RowMix::NullsAndEmpties] {
        for fan_out in [2usize, 10] {
            let plan = explode_plan(Element::Int64, fan_out, 0, mix, true);
            group.bench_with_input(BenchmarkId::new(mix.name(), fan_out), &fan_out, |b, _| {
                b.iter(|| run(&runtime, &plan, &ctx))
            });
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
