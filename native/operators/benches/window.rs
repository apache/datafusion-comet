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

//! Micro-benchmarks for the native operator behind `CometWindowExec`.
//!
//! The planner turns each Spark window expression into a DataFusion window expression and runs
//! them in `BoundedWindowAggExec` over its sorted child whenever every expression can run in
//! bounded memory, which covers the default `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
//! frame. A function name resolves to DataFusion's window UDF when one is registered
//! (`last_value`, `first_value`, `row_number`) and to the aggregate UDF otherwise (`max`), as in
//! `PhysicalPlanner::find_df_window_function`. This bench builds the operator the same way over
//! in-memory batches that are already sorted on the partition and order keys, so the window
//! work is not diluted by the sort that feeds it in a real plan. `CometWindowExec` reports no
//! time of its own, so this is also the only place its cost shows up separately.
//!
//! The dimensions are the ones that drive its cost: how many rows each PARTITION BY group holds,
//! which function runs and whether it ignores NULLs, the type of its argument, and how many
//! columns the operator buffers for each partition next to the keys.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::common::ScalarValue;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::{FunctionRegistry, TaskContext};
use datafusion::logical_expr::{
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::windows::{create_window_expr, BoundedWindowAggExec};
use datafusion::physical_plan::{ExecutionPlan, InputOrderMode};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use tokio::runtime::Runtime;

/// Input rows per batch, and batches per run. 8192 is the default `spark.comet.batchSize`, so
/// a PARTITION BY group larger than that spans several input batches, as it does in a task.
const ROWS_PER_BATCH: usize = 8192;
const BATCHES: usize = 32;
const ROWS: usize = ROWS_PER_BATCH * BATCHES;

/// The PARTITION BY keys, the ORDER BY key and the argument, ahead of any carried columns.
const KEYS: [&str; 3] = ["visit", "tab", "app"];
const ORDER_BY: &str = "ts";
const ARG: &str = "v";

/// The window functions worth distinguishing, each built the way the planner builds it.
#[derive(Clone, Copy, PartialEq)]
enum Func {
    /// `row_number()`: no argument and no frame, so it is the operator's per-partition and
    /// per-batch bookkeeping with almost nothing on top.
    RowNumber,
    /// `max(v)`: an aggregate UDF, which DataFusion wraps in `PlainAggregateWindowExpr` and
    /// updates with only the rows that enter the growing frame.
    Max,
    /// `last(v)`: the `last_value` window UDF. The frame start moves up as rows are produced.
    Last,
    /// `first(v, true)`: the `first_value` window UDF with IGNORE NULLS, which stops looking
    /// once it has seen a non-NULL value.
    FirstIgnoreNulls,
    /// `last(v, true)`: the `last_value` window UDF with IGNORE NULLS, which is what the planner
    /// builds for Spark's `last(v, true)` today.
    LastIgnoreNulls,
    /// `last(v, true)` built from the `last_value` aggregate UDF instead. The planner does not
    /// build this, it is here as the point of comparison for the window UDF.
    LastIgnoreNullsAggregate,
}

impl Func {
    fn name(self) -> &'static str {
        match self {
            Func::RowNumber => "row_number",
            Func::Max => "max",
            Func::Last => "last",
            Func::FirstIgnoreNulls => "first_ignore_nulls",
            Func::LastIgnoreNulls => "last_ignore_nulls",
            Func::LastIgnoreNullsAggregate => "last_ignore_nulls_aggregate",
        }
    }

    fn ignore_nulls(self) -> bool {
        matches!(
            self,
            Func::FirstIgnoreNulls | Func::LastIgnoreNulls | Func::LastIgnoreNullsAggregate
        )
    }

    /// The function and its arguments, resolved as `find_df_window_function` resolves them.
    fn definition(
        self,
        ctx: &SessionContext,
        schema: &SchemaRef,
    ) -> (WindowFunctionDefinition, Vec<Arc<dyn PhysicalExpr>>) {
        let state = ctx.state();
        let arg = vec![col(ARG, schema).unwrap()];
        match self {
            Func::RowNumber => (
                WindowFunctionDefinition::WindowUDF(state.udwf("row_number").unwrap()),
                vec![],
            ),
            Func::Max => (
                WindowFunctionDefinition::AggregateUDF(state.udaf("max").unwrap()),
                arg,
            ),
            Func::Last | Func::LastIgnoreNulls => (
                WindowFunctionDefinition::WindowUDF(state.udwf("last_value").unwrap()),
                arg,
            ),
            Func::FirstIgnoreNulls => (
                WindowFunctionDefinition::WindowUDF(state.udwf("first_value").unwrap()),
                arg,
            ),
            Func::LastIgnoreNullsAggregate => (
                WindowFunctionDefinition::AggregateUDF(state.udaf("last_value").unwrap()),
                arg,
            ),
        }
    }
}

/// The argument's type. A struct makes every value the operator touches row by row a nested
/// `ScalarValue`, which is the case in plans that carry several columns through `last` at once.
#[derive(Clone, Copy, PartialEq)]
enum Arg {
    Int64,
    Struct,
}

impl Arg {
    fn name(self) -> &'static str {
        match self {
            Arg::Int64 => "bigint",
            Arg::Struct => "struct",
        }
    }

    fn struct_fields() -> Fields {
        Fields::from(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("i", DataType::Int32, true),
            Field::new("t", DataType::Utf8, true),
            Field::new("u", DataType::Utf8, true),
        ])
    }

    fn data_type(self) -> DataType {
        match self {
            Arg::Int64 => DataType::Int64,
            Arg::Struct => DataType::Struct(Self::struct_fields()),
        }
    }

    /// The argument for rows `start..start + len`, NULL on every other row, so that IGNORE
    /// NULLS has values to skip and the frame always holds some.
    fn values(self, start: usize, len: usize) -> ArrayRef {
        let rows = start..start + len;
        let nulls = NullBuffer::from_iter(rows.clone().map(|r| r % 2 == 0));
        match self {
            Arg::Int64 => Arc::new(Int64Array::new(
                rows.map(|r| r as i64).collect(),
                Some(nulls),
            )),
            Arg::Struct => {
                let strings = |prefix: &str| -> ArrayRef {
                    Arc::new(StringArray::from_iter_values(
                        rows.clone().map(|r| format!("{prefix}-{:08}", r % 100_000)),
                    ))
                };
                Arc::new(StructArray::new(
                    Self::struct_fields(),
                    vec![
                        strings("algo"),
                        Arc::new(BooleanArray::from_iter(
                            rows.clone().map(|r| Some(r % 7 == 0)),
                        )),
                        Arc::new(Int32Array::from_iter_values(
                            rows.clone().map(|r| (r % 40) as i32),
                        )),
                        strings("sub"),
                        strings("container"),
                    ],
                    Some(nulls),
                ))
            }
        }
    }
}

/// A window shape: the function, its argument, the PARTITION BY group size, and how many extra
/// string columns ride along. The operator buffers every input column per partition, so the
/// carried columns are paid for once per partition in each input batch.
#[derive(Clone, Copy)]
struct Shape {
    func: Func,
    arg: Arg,
    rows_per_partition: usize,
    carried: usize,
}

impl Shape {
    fn schema(&self) -> SchemaRef {
        let mut fields: Vec<Field> = KEYS
            .iter()
            .map(|k| Field::new(*k, DataType::Utf8, true))
            .collect();
        fields.push(Field::new(ORDER_BY, DataType::Int64, true));
        fields.push(Field::new(ARG, self.arg.data_type(), true));
        fields.extend((0..self.carried).map(|c| Field::new(format!("c{c}"), DataType::Utf8, true)));
        Arc::new(Schema::new(fields))
    }

    /// Batch `index` of the input. Rows are sorted on the keys and then on the order key, and a
    /// new PARTITION BY group starts every `rows_per_partition` rows. The first key changes every
    /// ten groups, like a session holding several groups.
    fn batch(&self, schema: &SchemaRef, index: usize) -> RecordBatch {
        let start = index * ROWS_PER_BATCH;
        let rows = start..start + ROWS_PER_BATCH;
        let group = |r: usize| r / self.rows_per_partition;
        let strings = |f: &dyn Fn(usize) -> String| -> ArrayRef {
            Arc::new(StringArray::from_iter_values(rows.clone().map(f)))
        };
        let mut columns: Vec<ArrayRef> = vec![
            strings(&|r| format!("{:016x}-{:019}", 0x5eed, group(r) / 10)),
            strings(&|r| ["Today", "Games", "Apps"][group(r) / 10 % 3].to_string()),
            strings(&|r| format!("{:010}", 1_000_000_000 + group(r))),
            Arc::new(Int64Array::from_iter_values(rows.clone().map(|r| r as i64))),
            self.arg.values(start, ROWS_PER_BATCH),
        ];
        for c in 0..self.carried {
            columns.push(strings(&|r| format!("carried-{c}-{:06}", r % 1_000_000)));
        }
        RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
    }

    fn plan(&self, ctx: &SessionContext, input: &[RecordBatch]) -> Arc<dyn ExecutionPlan> {
        let schema = input[0].schema();
        let sort = |name: &str| PhysicalSortExpr::new_default(col(name, &schema).unwrap());
        let ordering: Vec<PhysicalSortExpr> =
            KEYS.iter().chain([&ORDER_BY]).map(|k| sort(k)).collect();
        let source = MemorySourceConfig::try_new(&[input.to_vec()], Arc::clone(&schema), None)
            .unwrap()
            .try_with_sort_information(vec![LexOrdering::new(ordering).unwrap()])
            .unwrap();
        let input: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(source);

        let partition_by: Vec<_> = KEYS.iter().map(|k| col(k, &schema).unwrap()).collect();
        // What the planner builds for Spark's default frame with an ORDER BY.
        let frame = WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::Preceding(ScalarValue::Int64(None)),
            WindowFrameBound::CurrentRow,
        );
        let (func, args) = self.func.definition(ctx, &schema);
        let expr = create_window_expr(
            &func,
            self.func.name().to_string(),
            &args,
            &partition_by,
            &[sort(ORDER_BY)],
            Arc::new(frame),
            Arc::clone(&schema),
            self.func.ignore_nulls(),
            false,
            None,
        )
        .unwrap();
        assert!(expr.uses_bounded_memory());
        Arc::new(
            BoundedWindowAggExec::try_new(vec![expr], input, InputOrderMode::Sorted, true).unwrap(),
        )
    }
}

fn run(runtime: &Runtime, plan: &Arc<dyn ExecutionPlan>, ctx: &Arc<TaskContext>) {
    let rows = runtime.block_on(async {
        let mut stream = plan.execute(0, Arc::clone(ctx)).unwrap();
        let mut rows = 0;
        while let Some(batch) = stream.next().await {
            rows += batch.unwrap().num_rows();
        }
        rows
    });
    assert_eq!(rows, ROWS);
}

fn bench_shape(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    id: BenchmarkId,
    shape: Shape,
    runtime: &Runtime,
    session: &SessionContext,
    task_ctx: &Arc<TaskContext>,
) {
    let schema = shape.schema();
    let input: Vec<RecordBatch> = (0..BATCHES).map(|i| shape.batch(&schema, i)).collect();
    let plan = shape.plan(session, &input);
    group.bench_function(id, |b| b.iter(|| run(runtime, &plan, task_ctx)));
}

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let session = SessionContext::new();
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(ROWS_PER_BATCH)),
    );
    let shape = |func, arg, rows_per_partition, carried| Shape {
        func,
        arg,
        rows_per_partition,
        carried,
    };

    // The two windows of a plan that sorts clickstream rows by session and then takes
    // `last(struct(...), true)` and `max(int)` over them, with the 18 other columns that plan
    // carries. The group size is the dimension that separates the two costs: fixed work per
    // PARTITION BY group at the small end, work that grows with the frame at the large end.
    let mut group = c.benchmark_group("window_rows_per_partition");
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(ROWS as u64));
    for (func, arg) in [
        (Func::LastIgnoreNulls, Arg::Struct),
        (Func::Max, Arg::Int64),
    ] {
        for rows_per_partition in [1usize, 4, 64, 1024, 16384] {
            let id = BenchmarkId::new(
                format!("{}_{}", func.name(), arg.name()),
                rows_per_partition,
            );
            let s = shape(func, arg, rows_per_partition, 18);
            bench_shape(&mut group, id, s, &runtime, &session, &task_ctx);
        }
    }
    group.finish();

    // Every function over the same narrow bigint input, at a small and a large group size.
    let mut group = c.benchmark_group("window_function");
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(ROWS as u64));
    for rows_per_partition in [4usize, 16384] {
        for func in [
            Func::RowNumber,
            Func::Max,
            Func::Last,
            Func::FirstIgnoreNulls,
            Func::LastIgnoreNulls,
            Func::LastIgnoreNullsAggregate,
        ] {
            let id = BenchmarkId::new(func.name(), rows_per_partition);
            let s = shape(func, Arg::Int64, rows_per_partition, 0);
            bench_shape(&mut group, id, s, &runtime, &session, &task_ctx);
        }
    }
    group.finish();

    // The argument type alone, at a group size where per-row work dominates.
    let mut group = c.benchmark_group("window_argument_type");
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(ROWS as u64));
    for arg in [Arg::Int64, Arg::Struct] {
        let id = BenchmarkId::new(Func::LastIgnoreNulls.name(), arg.name());
        let s = shape(Func::LastIgnoreNulls, arg, 4, 0);
        bench_shape(&mut group, id, s, &runtime, &session, &task_ctx);
    }
    group.finish();

    // How much the columns buffered next to the keys cost when every row is its own group.
    let mut group = c.benchmark_group("window_carried_columns");
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(ROWS as u64));
    for carried in [0usize, 4, 18] {
        let id = BenchmarkId::new(Func::Max.name(), carried);
        let s = shape(Func::Max, Arg::Int64, 1, carried);
        bench_shape(&mut group, id, s, &runtime, &session, &task_ctx);
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
