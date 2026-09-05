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

use super::*;
use std::fmt::Display;
use std::hash::{Hash, Hasher};

use crate::execution::planner::PhysicalPlanner;
use crate::parquet::parquet_exec::init_datasource_exec;
use arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array, Int8Array, RecordBatch};
use arrow::compute::cast;
use arrow::datatypes::{Field, Schema};
use datafusion::common::test_util::batches_to_sort_string;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_comet_spark_expr::RandExpr;
use datafusion_datasource::file::FileSource;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

fn input(
    values: Vec<Option<i32>>,
    key_type: &DataType,
    key_index: usize,
) -> Arc<dyn ExecutionPlan> {
    let payload = Arc::new(Int32Array::from_iter_values(0..values.len() as i32)) as ArrayRef;
    let key = cast(&Int32Array::from(values), key_type).unwrap();
    let mut fields = vec![
        Field::new("key", key_type.clone(), true),
        Field::new("payload", DataType::Int32, false),
    ];
    let mut columns = vec![key, payload];
    fields.swap(0, key_index);
    columns.swap(0, key_index);
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    // Multiple build batches prove that an early subset of keys cannot prune
    // matches belonging to a later batch.
    let batches = if batch.num_rows() == 0 {
        vec![batch]
    } else {
        (0..batch.num_rows())
            .step_by(2)
            .map(|offset| batch.slice(offset, 2.min(batch.num_rows() - offset)))
            .collect()
    };
    memory_exec(batches)
}

fn memory_exec(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    MemorySourceConfig::try_new_exec(std::slice::from_ref(&batches), batches[0].schema(), None)
        .unwrap()
}

fn join(
    build: Arc<dyn ExecutionPlan>,
    probe: Arc<dyn ExecutionPlan>,
    swap: bool,
) -> Arc<dyn ExecutionPlan> {
    let build_key = Arc::new(Column::new("key", 1)) as Arc<dyn PhysicalExpr>;
    let probe_key = Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>;
    let (left, right, on) = if swap {
        (probe, build, vec![(probe_key, build_key)])
    } else {
        (build, probe, vec![(build_key, probe_key)])
    };
    let join = HashJoinExec::try_new(
        left,
        right,
        on,
        None,
        &JoinType::Inner,
        None,
        PartitionMode::Partitioned,
        NullEquality::NullEqualsNothing,
        false,
    )
    .unwrap();
    if swap {
        join.swap_inputs(PartitionMode::Partitioned).unwrap()
    } else {
        Arc::new(join)
    }
}

fn metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        return metric(projection.input(), name);
    }
    plan.metrics()
        .unwrap()
        .sum_by_name(name)
        .unwrap()
        .as_usize()
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// Inspect the batch given to the real completed predicate, without changing its result.
#[derive(Debug, Eq)]
struct AssertKeyOnlyBatch {
    child: Arc<dyn PhysicalExpr>,
    key_values_ptr: usize,
}

impl PartialEq for AssertKeyOnlyBatch {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.key_values_ptr == other.key_values_ptr
    }
}

impl Hash for AssertKeyOnlyBatch {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.key_values_ptr.hash(state);
    }
}

impl Display for AssertKeyOnlyBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AssertKeyOnlyBatch({})", self.child)
    }
}

impl PhysicalExpr for AssertKeyOnlyBatch {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        assert_eq!(
            batch.num_columns(),
            1,
            "predicate must not receive payload columns"
        );
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            keys.values().as_ptr() as usize,
            self.key_values_ptr,
            "projecting the join key must not copy its values"
        );
        self.child.evaluate(batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self {
            child: children.remove(0),
            key_values_ptr: self.key_values_ptr,
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

#[tokio::test]
async fn completed_filter_evaluates_only_the_shared_probe_key() {
    // With no nulls and only 1/8 of rows inside the build bounds, DataFusion's AND
    // evaluation preselects those rows before evaluating hash_lookup. A permutation
    // spreads the selected rows throughout the batch, forcing payload copies if the
    // full probe batch reaches the predicate.
    let keys = Arc::new(Int32Array::from_iter_values(
        (0..8192).map(|row| (row * 641) % 8192),
    ));
    let mut fields = (0..32)
        .map(|column| Field::new(format!("payload_{column}"), DataType::Int64, false))
        .collect::<Vec<_>>();
    let mut columns = (0..32)
        .map(|column| {
            Arc::new(Int64Array::from_iter_values(
                (0..8192).map(move |row| i64::from(row) * 32 + i64::from(column)),
            )) as ArrayRef
        })
        .collect::<Vec<_>>();
    let key_index = 17;
    fields.insert(key_index, Field::new("key", DataType::Int32, false));
    columns.insert(key_index, Arc::clone(&keys) as ArrayRef);
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let probe = memory_exec(vec![batch.clone()]);
    let join = HashJoinExec::try_new(
        input((0..1024).map(Some).collect(), &DataType::Int32, 0),
        Arc::clone(&probe),
        vec![(
            Arc::new(Column::new("key", 0)),
            Arc::new(Column::new("key", key_index)),
        )],
        None,
        &JoinType::Inner,
        None,
        PartitionMode::Partitioned,
        NullEquality::NullEqualsNothing,
        false,
    )
    .unwrap();
    let session = SessionContext::new();
    let expected = collect(
        Arc::new(join.builder().build().unwrap()),
        session.task_ctx(),
    )
    .await
    .unwrap();
    let wrapper =
        DynamicFilterJoinExec::new(&join, session.copied_config().options().as_ref().clone())
            .unwrap();
    let runtime = wrapper.build_runtime_join().unwrap();
    let predicate = Arc::clone(runtime.join.dynamic_filter_expr().unwrap());
    let actual = datafusion::physical_plan::common::collect(
        wrapper
            .execute_runtime_join(runtime.join, 0, session.task_ctx())
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        batches_to_sort_string(&actual),
        batches_to_sort_string(&expected)
    );

    // Inspect the actual build-generated bounds AND hash-membership expression. Its key
    // remains at index 17 here, so the consumer must also remap every nested reference.
    let completed = predicate.current().unwrap();
    assert!(completed.to_string().contains("hash_lookup"));
    assert!(completed.to_string().contains("AND"));
    predicate
        .update(Arc::new(AssertKeyOnlyBatch {
            child: completed,
            key_values_ptr: keys.values().as_ptr() as usize,
        }))
        .unwrap();
    let consumer: Arc<dyn ExecutionPlan> = Arc::new(DynamicFilterExec::new(probe, predicate));
    let filtered = collect(consumer, session.task_ctx()).await.unwrap();
    let selected = BooleanArray::from(
        keys.values()
            .iter()
            .map(|key| *key < 1024)
            .collect::<Vec<_>>(),
    );
    let expected = filter_record_batch(&batch, &selected).unwrap();
    assert_eq!(expected.num_rows(), 1024);
    assert_eq!(filtered, vec![expected]);
}

#[tokio::test]
async fn completed_build_filters_both_sides_and_session_inlist_settings() {
    for key_type in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        for swap in [false, true] {
            for max_inlist_size in [0, 1024 * 1024] {
                let mut config = SessionConfig::new();
                config
                    .options_mut()
                    .optimizer
                    .hash_join_inlist_pushdown_max_size = max_inlist_size;
                let session = SessionContext::new_with_config(config);
                for build_values in [
                    vec![Some(-5), Some(20), None, Some(20), Some(90)],
                    vec![],
                    vec![None, None],
                ] {
                    let build = input(build_values.clone(), &key_type, 1);
                    let probe = input((-100..=100).map(Some).chain([None]).collect(), &key_type, 0);
                    let plain = join(Arc::clone(&build), Arc::clone(&probe), swap);
                    let attached = PhysicalPlanner::apply_join_dynamic_filter(
                        join(build, probe, swap),
                        true,
                        session.copied_config().options(),
                    )
                    .unwrap();
                    let native_join =
                        if let Some(projection) = attached.downcast_ref::<ProjectionExec>() {
                            projection.input()
                        } else {
                            &attached
                        };
                    assert!(native_join.is::<DynamicFilterJoinExec>());
                    assert_eq!(plain.schema(), attached.schema());
                    let expected = collect(plain, session.task_ctx()).await.unwrap();
                    let actual = collect(Arc::clone(&attached), session.task_ctx())
                        .await
                        .unwrap();
                    assert_eq!(
                        batches_to_sort_string(&actual),
                        batches_to_sort_string(&expected)
                    );
                    if build_values.iter().any(Option::is_some) {
                        assert_eq!(
                            row_count(&actual),
                            4,
                            "duplicates and late build keys must match"
                        );
                        assert_eq!(metric(&attached, "dynamic_filter_rows_evaluated"), 202);
                        assert!(metric(&attached, "dynamic_filter_rows_pruned") >= 199);
                        assert_eq!(metric(&attached, "dynamic_filter_rows_bypassed"), 0);
                    } else {
                        assert_eq!(row_count(&actual), 0);
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn placeholder_updates_and_errors_are_not_hidden() {
    let source = input((0..10).map(Some).collect(), &DataType::Int32, 1);
    let predicate = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![Arc::new(Column::new("key", 1))],
        lit(true),
    ));
    let wrapper: Arc<dyn ExecutionPlan> =
        Arc::new(DynamicFilterExec::new(source, Arc::clone(&predicate)));
    let task = SessionContext::new().task_ctx();
    let mut stream = wrapper.execute(0, Arc::clone(&task)).unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 2);
    predicate
        .update(Arc::new(BinaryExpr::new(
            Arc::new(Column::new("key", 1)),
            Operator::Lt,
            lit(2i32),
        )))
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 0);
    predicate.update(lit(false)).unwrap();
    while let Some(batch) = stream.next().await {
        assert_eq!(batch.unwrap().num_rows(), 0);
    }
    assert_eq!(metric(&wrapper, "dynamic_filter_rows_bypassed"), 2);
    assert_eq!(metric(&wrapper, "dynamic_filter_rows_pruned"), 8);
    assert_eq!(metric(&wrapper, "dynamic_filter_rows_evaluated"), 8);

    // Reset must not preserve an old condition, even while another owner
    // still holds the previous predicate.
    let reset = Arc::clone(&wrapper).reset_state().unwrap();
    let reset_output = collect(Arc::clone(&reset), Arc::clone(&task))
        .await
        .unwrap();
    assert_eq!(row_count(&reset_output), 10);
    assert_eq!(metric(&reset, "dynamic_filter_rows_pruned"), 0);
    assert_eq!(metric(&reset, "dynamic_filter_rows_bypassed"), 10);

    predicate.update(lit(42i32)).unwrap();
    let error = collect(wrapper, task).await.unwrap_err();
    assert!(error.to_string().contains("must evaluate to a Boolean"));
}

fn plain_join() -> HashJoinExec {
    let plan = join(
        input(vec![Some(10)], &DataType::Int32, 1),
        input(vec![Some(10), Some(20)], &DataType::Int32, 0),
        false,
    );
    plan.downcast_ref::<HashJoinExec>()
        .unwrap()
        .builder()
        .build()
        .unwrap()
}

fn assert_skipped(join: HashJoinExec, config: &ConfigOptions) {
    let plain: Arc<dyn ExecutionPlan> = Arc::new(join);
    let attached =
        PhysicalPlanner::apply_join_dynamic_filter(Arc::clone(&plain), true, config).unwrap();
    assert!(
        Arc::ptr_eq(&plain, &attached),
        "fallback must preserve the original plan"
    );
    assert!(attached
        .downcast_ref::<HashJoinExec>()
        .unwrap()
        .dynamic_filter_expr()
        .is_none());
}

#[test]
fn skips_unsupported_joins_and_session_disables() {
    let default = ConfigOptions::default();
    for join_type in [
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftSemi,
        JoinType::RightSemi,
        JoinType::LeftAnti,
        JoinType::RightAnti,
        JoinType::LeftMark,
        JoinType::RightMark,
    ] {
        assert_skipped(
            plain_join().builder().with_type(join_type).build().unwrap(),
            &default,
        );
    }
    assert_skipped(
        plain_join()
            .builder()
            .with_null_equality(NullEquality::NullEqualsNull)
            .build()
            .unwrap(),
        &default,
    );
    assert_skipped(
        plain_join()
            .builder()
            .with_partition_mode(PartitionMode::Auto)
            .build()
            .unwrap(),
        &default,
    );
    assert_skipped(
        plain_join()
            .builder()
            .with_type(JoinType::LeftAnti)
            .with_null_aware(true)
            .with_partition_mode(PartitionMode::CollectLeft)
            .build()
            .unwrap(),
        &default,
    );
    for option in [
        "enable_dynamic_filter_pushdown",
        "enable_join_dynamic_filter_pushdown",
        "preserve_file_partitions",
    ] {
        let mut config = ConfigOptions::default();
        match option {
            "enable_dynamic_filter_pushdown" => {
                config.optimizer.enable_dynamic_filter_pushdown = false
            }
            "enable_join_dynamic_filter_pushdown" => {
                config.optimizer.enable_join_dynamic_filter_pushdown = false
            }
            _ => config.optimizer.preserve_file_partitions = 1,
        }
        assert_skipped(plain_join(), &config);
    }
}

#[test]
fn skips_unsupported_keys_and_multiple_native_partitions() {
    for key_type in [
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Decimal128(10, 0),
    ] {
        let plan = join(
            input(vec![Some(10)], &key_type, 1),
            input(vec![Some(10)], &key_type, 0),
            false,
        );
        let same = PhysicalPlanner::apply_join_dynamic_filter(
            Arc::clone(&plan),
            true,
            &ConfigOptions::default(),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&plan, &same));
    }
    let plain = plain_join();
    let (build, probe) = plain.on()[0].clone();
    let computed = Arc::new(BinaryExpr::new(
        Arc::clone(&probe),
        Operator::Plus,
        lit(1i32),
    ));
    for keys in [
        vec![(Arc::clone(&build), computed as Arc<dyn PhysicalExpr>)],
        vec![(Arc::clone(&build), Arc::clone(&probe)), (build, probe)],
    ] {
        assert_skipped(
            plain.builder().with_on(keys).build().unwrap(),
            &ConfigOptions::default(),
        );
    }
    for replace_build in [false, true] {
        let schema = if replace_build {
            plain.left().schema()
        } else {
            plain.right().schema()
        };
        let two_partitions: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
            &[
                vec![RecordBatch::new_empty(Arc::clone(&schema))],
                vec![RecordBatch::new_empty(Arc::clone(&schema))],
            ],
            schema,
            None,
        )
        .unwrap();
        let children = if replace_build {
            vec![two_partitions, Arc::clone(plain.right())]
        } else {
            vec![Arc::clone(plain.left()), two_partitions]
        };
        assert_skipped(
            plain
                .builder()
                .with_new_children(children)
                .unwrap()
                .build()
                .unwrap(),
            &ConfigOptions::default(),
        );
    }
}

#[tokio::test]
async fn independent_attempts_do_not_share_build_domains() {
    let session = SessionContext::new();
    for build_key in [5, 90] {
        let plan = join(
            input(vec![Some(build_key)], &DataType::Int32, 1),
            input(vec![Some(5), Some(90)], &DataType::Int32, 0),
            false,
        );
        let attached = PhysicalPlanner::apply_join_dynamic_filter(
            plan,
            true,
            session.copied_config().options(),
        )
        .unwrap();
        let output = collect(Arc::clone(&attached), session.task_ctx())
            .await
            .unwrap();
        assert_eq!(row_count(&output), 1);
        let keys = output
            .iter()
            .flat_map(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(keys, vec![build_key]);
        assert_eq!(metric(&attached, "dynamic_filter_rows_pruned"), 1);
    }
}

fn single_key_join_plans(
    build: Arc<dyn ExecutionPlan>,
    probe: Arc<dyn ExecutionPlan>,
    mode: PartitionMode,
) -> HashJoinExec {
    HashJoinExec::try_new(
        build,
        probe,
        vec![(
            Arc::new(Column::new("key", 0)),
            Arc::new(Column::new("key", 0)),
        )],
        None,
        &JoinType::Inner,
        None,
        mode,
        NullEquality::NullEqualsNothing,
        false,
    )
    .unwrap()
}

fn single_key_join(build: ArrayRef, probe: ArrayRef, mode: PartitionMode) -> HashJoinExec {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "key",
        build.data_type().clone(),
        true,
    )]));
    let build = memory_exec(vec![
        RecordBatch::try_new(Arc::clone(&schema), vec![build]).unwrap()
    ]);
    let probe = memory_exec(vec![RecordBatch::try_new(schema, vec![probe]).unwrap()]);
    single_key_join_plans(build, probe, mode)
}

fn parquet_probe(
    values: Vec<i32>,
    session: &Arc<SessionContext>,
    max_row_group_rows: usize,
) -> (tempfile::NamedTempFile, Arc<DataSourceExec>) {
    // Put the key after an unused physical column and project only the key.
    // Reader attachment must remap key@0 in the join to key@1 in the file.
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Int32, false),
        Field::new("key", DataType::Int32, false),
    ]));
    let required_schema = Arc::new(Schema::new(vec![file_schema.field(1).clone()]));
    let row_count = values.len();
    let batch = RecordBatch::try_new(
        Arc::clone(&file_schema),
        vec![
            Arc::new(Int32Array::from_iter_values(0..row_count as i32)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(max_row_group_rows))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(
        file.reopen().unwrap(),
        Arc::clone(&file_schema),
        Some(props),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    let metadata = writer.close().unwrap();
    assert_eq!(
        metadata.num_row_groups(),
        row_count.div_ceil(max_row_group_rows)
    );
    assert!(metadata
        .row_groups()
        .iter()
        .all(|group| { group.num_rows() > 0 && group.num_rows() as usize <= max_row_group_rows }));

    let non_null: Arc<dyn PhysicalExpr> =
        Arc::new(IsNotNullExpr::new(Arc::new(Column::new("key", 0))));
    let scan = init_datasource_exec(
        required_schema,
        Some(file_schema),
        None,
        ObjectStoreUrl::local_filesystem(),
        vec![vec![PartitionedFile::from_path(
            file.path().to_string_lossy().into_owned(),
        )
        .unwrap()]],
        Some(vec![1]),
        Some(vec![non_null]),
        None,
        "UTC",
        true,
        false,
        false,
        false,
        session,
        false,
        false,
        false,
    )
    .unwrap();
    (file, scan)
}

fn filtered_probe(scan: &Arc<DataSourceExec>) -> Arc<CometFilterExec> {
    let predicate: Arc<dyn PhysicalExpr> =
        Arc::new(IsNotNullExpr::new(Arc::new(Column::new("key", 0))));
    Arc::new(CometFilterExec::from_datafusion(
        FilterExec::try_new(predicate, Arc::clone(scan) as Arc<dyn ExecutionPlan>).unwrap(),
    ))
}

fn find_dynamic_filter(expr: &Arc<dyn PhysicalExpr>) -> Option<&DynamicFilterPhysicalExpr> {
    if let Some(filter) = expr.downcast_ref::<DynamicFilterPhysicalExpr>() {
        return Some(filter);
    }
    expr.children()
        .into_iter()
        .find_map(|child| find_dynamic_filter(child))
}

/// Accept nested null-check conjunctions while retaining all other filter
/// boundaries, including OR and computed or potentially failing expressions.
#[test]
fn reader_filter_crosses_only_direct_column_null_checks() {
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
    let direct_null_check: Arc<dyn PhysicalExpr> = Arc::new(IsNotNullExpr::new(Arc::clone(&key)));
    assert!(is_direct_column_null_checks(&direct_null_check));
    let other_null_check: Arc<dyn PhysicalExpr> =
        Arc::new(IsNotNullExpr::new(Arc::new(Column::new("other", 1))));
    let conjunction: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&direct_null_check),
        Operator::And,
        Arc::clone(&other_null_check),
    ));
    let nested: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        conjunction,
        Operator::And,
        Arc::clone(&direct_null_check),
    ));
    assert!(is_direct_column_null_checks(&nested));
    let right_nested: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&direct_null_check),
        Operator::And,
        Arc::new(BinaryExpr::new(
            Arc::clone(&other_null_check),
            Operator::And,
            Arc::clone(&direct_null_check),
        )),
    ));
    assert!(is_direct_column_null_checks(&right_nested));
    let disjunction: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&direct_null_check),
        Operator::Or,
        other_null_check,
    ));
    assert!(!is_direct_column_null_checks(&disjunction));

    let comparison: Arc<dyn PhysicalExpr> =
        Arc::new(BinaryExpr::new(Arc::clone(&key), Operator::Gt, lit(0_i32)));
    let conjunction: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&direct_null_check),
        Operator::And,
        comparison,
    ));
    assert!(!is_direct_column_null_checks(&conjunction));

    let computed: Arc<dyn PhysicalExpr> =
        Arc::new(BinaryExpr::new(key, Operator::Plus, lit(1_i32)));
    let computed_null_check: Arc<dyn PhysicalExpr> = Arc::new(IsNotNullExpr::new(computed));
    assert!(!is_direct_column_null_checks(&computed_null_check));
}

/// Exercise a real Parquet reader with three distinct nullable columns and
/// remapped projection. Both statistics-only and row-filter reads must retain
/// the AND residual: build keys 2 and 3 fail its payload checks, leaving key 1.
#[tokio::test]
async fn reader_filter_crosses_null_check_conjunction_and_retains_residual() {
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Int32, true),
        Field::new("key", DataType::Int32, true),
        Field::new("other", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&file_schema),
        vec![
            Arc::new(Int32Array::from(vec![
                Some(10),
                None,
                Some(30),
                Some(40),
                Some(50),
            ])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                None,
            ])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(1),
                None,
                Some(1),
                Some(1),
            ])),
        ],
    )
    .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(
        file.reopen().unwrap(),
        Arc::clone(&file_schema),
        Some(props),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let required_schema = Arc::new(Schema::new(vec![
        file_schema.field(1).clone(),
        file_schema.field(0).clone(),
        file_schema.field(2).clone(),
    ]));
    let mut outputs = Vec::new();
    for (row_filter, enabled) in [(false, false), (false, true), (true, false), (true, true)] {
        let mut config = SessionConfig::new()
            .with_target_partitions(1)
            .with_parquet_page_index_pruning(false);
        config.options_mut().execution.parquet.pushdown_filters = row_filter;
        let session = Arc::new(SessionContext::new_with_config(config));
        let scan = init_datasource_exec(
            Arc::clone(&required_schema),
            Some(Arc::clone(&file_schema)),
            None,
            ObjectStoreUrl::local_filesystem(),
            vec![vec![PartitionedFile::from_path(
                file.path().to_string_lossy().into_owned(),
            )
            .unwrap()]],
            Some(vec![1, 0, 2]),
            None,
            None,
            "UTC",
            true,
            false,
            false,
            false,
            &session,
            false,
            false,
            false,
        )
        .unwrap();
        let checks = [("key", 0), ("payload", 1), ("other", 2)].map(|(name, index)| {
            Arc::new(IsNotNullExpr::new(Arc::new(Column::new(name, index))))
                as Arc<dyn PhysicalExpr>
        });
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::clone(&checks[0]),
                Operator::And,
                Arc::clone(&checks[1]),
            )),
            Operator::And,
            Arc::clone(&checks[2]),
        ));
        let filter = Arc::new(CometFilterExec::from_datafusion(
            FilterExec::try_new(
                Arc::clone(&predicate),
                Arc::clone(&scan) as Arc<dyn ExecutionPlan>,
            )
            .unwrap(),
        ));
        let build_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, true)]));
        let build = memory_exec(vec![RecordBatch::try_new(
            build_schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap()]);
        let join = single_key_join_plans(build, filter, PartitionMode::Partitioned);
        let plan: Arc<dyn ExecutionPlan> = if enabled {
            let wrapper = DynamicFilterJoinExec::new(
                &join,
                session.copied_config().options().as_ref().clone(),
            )
            .unwrap();
            let runtime = wrapper.build_runtime_join().unwrap();
            assert!(runtime.reader_filter_attached);
            let consumer = runtime
                .join
                .right()
                .downcast_ref::<DynamicFilterExec>()
                .unwrap();
            let retained = consumer.input.downcast_ref::<CometFilterExec>().unwrap();
            assert_eq!(retained.predicate().to_string(), predicate.to_string());
            Arc::new(wrapper)
        } else {
            Arc::new(join)
        };
        let output = collect(Arc::clone(&plan), session.task_ctx())
            .await
            .unwrap();
        assert_eq!(row_count(&output), 1);
        if enabled {
            if row_filter {
                let metrics = scan.metrics().unwrap();
                assert!(
                    metrics
                        .sum_by_name("pushdown_rows_pruned")
                        .unwrap()
                        .as_usize()
                        > 0,
                    "{metrics}"
                );
            }
            assert_eq!(
                plan.metrics()
                    .unwrap()
                    .sum_by_name("dynamic_filter_reader_filters_attached")
                    .unwrap()
                    .as_usize(),
                1
            );
        }
        outputs.push(batches_to_sort_string(&output));
    }
    assert!(outputs.windows(2).all(|pair| pair[0] == pair[1]));
}

#[tokio::test]
async fn reader_filter_does_not_cross_fetch_limits() {
    for limit_filter in [false, true] {
        let mut outputs = Vec::new();
        for enabled in [false, true] {
            let session = Arc::new(SessionContext::new_with_config(
                SessionConfig::new().with_target_partitions(1),
            ));
            let (_file, scan) = parquet_probe((0..4).collect(), &session, 1);
            let probe = if limit_filter {
                filtered_probe(&scan).with_fetch(Some(1)).unwrap()
            } else {
                scan.with_fetch(Some(1)).unwrap()
            };
            let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
            let build = memory_exec(vec![RecordBatch::try_new(
                schema,
                vec![Arc::new(Int32Array::from(vec![3]))],
            )
            .unwrap()]);
            let join = single_key_join_plans(build, probe, PartitionMode::Partitioned);
            let plan: Arc<dyn ExecutionPlan> = if enabled {
                PhysicalPlanner::apply_join_dynamic_filter(
                    Arc::new(join),
                    true,
                    session.copied_config().options(),
                )
                .unwrap()
            } else {
                Arc::new(join)
            };
            let output = collect(Arc::clone(&plan), session.task_ctx())
                .await
                .unwrap();
            outputs.push(row_count(&output));
            if enabled {
                assert_eq!(metric(&plan, "dynamic_filter_reader_filters_skipped"), 1);
            }
        }
        assert_eq!(outputs, vec![0, 0]);
    }
}

#[tokio::test]
async fn reader_filter_does_not_cross_seeded_rand_probe_filter() {
    let mut outputs = Vec::new();
    for enabled in [false, true] {
        let mut config = SessionConfig::new()
            .with_target_partitions(1)
            .with_parquet_page_index_pruning(false);
        config.options_mut().execution.parquet.pushdown_filters = false;
        let session = Arc::new(SessionContext::new_with_config(config));

        // Four one-row groups make reader pruning observable in Rand's state. Without the
        // filter boundary, pruning keys 0-2 makes key 3 receive the first draw (0.619...)
        // instead of the fourth (0.263...), changing whether it passes rand(42) < 0.5.
        let (_file, scan) = parquet_probe((0..4).collect(), &session, 1);
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let is_not_null: Arc<dyn PhysicalExpr> = Arc::new(IsNotNullExpr::new(Arc::clone(&key)));
        let random_below_half: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(RandExpr::new(42)),
            Operator::Lt,
            lit(0.5_f64),
        ));
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            is_not_null,
            Operator::And,
            random_below_half,
        ));
        let filter = Arc::new(CometFilterExec::from_datafusion(
            FilterExec::try_new(predicate, Arc::clone(&scan) as Arc<dyn ExecutionPlan>).unwrap(),
        ));

        let build_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let build = memory_exec(vec![RecordBatch::try_new(
            build_schema,
            vec![Arc::new(Int32Array::from(vec![3]))],
        )
        .unwrap()]);
        let join = single_key_join_plans(
            build,
            Arc::clone(&filter) as Arc<dyn ExecutionPlan>,
            PartitionMode::Partitioned,
        );
        let plan: Arc<dyn ExecutionPlan> = if enabled {
            PhysicalPlanner::apply_join_dynamic_filter(
                Arc::new(join),
                true,
                session.copied_config().options(),
            )
            .unwrap()
        } else {
            Arc::new(join)
        };
        let batches = collect(Arc::clone(&plan), session.task_ctx())
            .await
            .unwrap();
        let probe_keys = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        outputs.push(probe_keys);

        if enabled {
            let dynamic_metric = |name| {
                plan.metrics()
                    .and_then(|metrics| metrics.sum_by_name(name))
                    .map_or(0, |value| value.as_usize())
            };
            assert_eq!(dynamic_metric("dynamic_filter_reader_filters_attached"), 0);
            assert_eq!(dynamic_metric("dynamic_filter_reader_filters_skipped"), 1);
            assert_eq!(dynamic_metric("dynamic_filter_rows_evaluated"), 1);
            assert_eq!(dynamic_metric("dynamic_filter_rows_pruned"), 0);
            assert_eq!(pruning_metric(&scan, "row_groups_pruned_statistics"), 0);
            assert_eq!(filter.metrics().unwrap().output_rows().unwrap(), 1);
        }
    }
    assert_eq!(outputs, vec![vec![3], vec![3]]);
}

fn two_batch_build() -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
    memory_exec(
        [150, 250]
            .into_iter()
            .map(|key| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(vec![key]))],
                )
                .unwrap()
            })
            .collect(),
    )
}

fn pruning_metric(plan: &Arc<DataSourceExec>, name: &str) -> usize {
    let metrics = plan.metrics().unwrap();
    let Some(value) = metrics.sum_by_name(name) else {
        return 0;
    };
    let MetricValue::PruningMetrics {
        pruning_metrics, ..
    } = value
    else {
        panic!("expected pruning metric {name}: {metrics}");
    };
    pruning_metrics.pruned()
}

async fn run_parquet_join(values: Vec<i32>, enabled: bool) -> (usize, usize, usize, usize, usize) {
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_parquet_page_index_pruning(false);
    // Isolate row-group pruning. The residual batch filter remains responsible
    // for membership filtering after the reader applies conservative statistics.
    config.options_mut().execution.parquet.pushdown_filters = false;
    let session = Arc::new(SessionContext::new_with_config(config));
    let (_file, scan) = parquet_probe(values, &session, 100);
    let filter = filtered_probe(&scan);
    let probe = Arc::clone(&filter) as Arc<dyn ExecutionPlan>;
    let join = single_key_join_plans(two_batch_build(), probe, PartitionMode::Partitioned);
    let plan: Arc<dyn ExecutionPlan> = if enabled {
        PhysicalPlanner::apply_join_dynamic_filter(
            Arc::new(join),
            true,
            session.copied_config().options(),
        )
        .unwrap()
    } else {
        Arc::new(join)
    };
    let output = collect(Arc::clone(&plan), session.task_ctx())
        .await
        .unwrap();
    let attached = plan
        .metrics()
        .and_then(|metrics| metrics.sum_by_name("dynamic_filter_reader_filters_attached"))
        .map_or(0, |metric| metric.as_usize());
    (
        row_count(&output),
        pruning_metric(&scan, "row_groups_pruned_statistics"),
        scan.metrics()
            .unwrap()
            .sum_by_name("bytes_scanned")
            .unwrap()
            .as_usize(),
        attached,
        filter.metrics().unwrap().output_rows().unwrap_or_default(),
    )
}

#[tokio::test]
async fn broadcast_filter_reaches_parquet_reader_after_complete_build() {
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_parquet_page_index_pruning(false);
    config.options_mut().execution.parquet.pushdown_filters = false;
    let session = Arc::new(SessionContext::new_with_config(config));
    let (_file, scan) = parquet_probe((0..400).collect(), &session, 100);
    let join = single_key_join_plans(
        two_batch_build(),
        filtered_probe(&scan),
        PartitionMode::Partitioned,
    );
    let wrapper =
        DynamicFilterJoinExec::new(&join, session.copied_config().options().as_ref().clone())
            .unwrap();
    let runtime = wrapper.build_runtime_join().unwrap();
    assert!(runtime.reader_filter_attached);
    let consumer = runtime
        .join
        .right()
        .downcast_ref::<DynamicFilterExec>()
        .unwrap();
    let filter = consumer.input.downcast_ref::<CometFilterExec>().unwrap();
    let reader = filter.input().downcast_ref::<DataSourceExec>().unwrap();
    let (_, source) = reader.downcast_to_file_source::<ParquetSource>().unwrap();
    let reader_filter = source.filter().unwrap();
    let reader_filter = find_dynamic_filter(&reader_filter).unwrap();
    let join_filter = runtime.join.dynamic_filter_expr().unwrap();
    assert_eq!(
        reader_filter.inner().expression_id,
        join_filter.inner().expression_id
    );
    let remapped_key = reader_filter.remapped_children().unwrap()[0]
        .downcast_ref::<Column>()
        .unwrap();
    assert_eq!(remapped_key.name(), "key");
    assert_eq!(remapped_key.index(), 1);

    let (disabled_rows, _, disabled_bytes, _, disabled_filter_rows) =
        run_parquet_join((0..400).collect(), false).await;
    let (enabled_rows, enabled_pruned, enabled_bytes, attached, enabled_filter_rows) =
        run_parquet_join((0..400).collect(), true).await;
    assert_eq!(disabled_rows, 2);
    assert_eq!(enabled_rows, disabled_rows);
    assert_eq!(enabled_pruned, 2);
    assert!(
        enabled_bytes < disabled_bytes,
        "reader filter should avoid data reads: enabled={enabled_bytes}, disabled={disabled_bytes}"
    );
    assert_eq!(attached, 1);
    assert_eq!(disabled_filter_rows, 400);
    assert_eq!(enabled_filter_rows, 200);

    // Every row group's bounds span both build keys, so the same attached
    // filter is safe but cannot avoid I/O on this deliberately poor layout.
    let mut unclustered = Vec::with_capacity(400);
    for group in 0..4 {
        unclustered.extend(0..50);
        unclustered.extend(350..400);
        if group == 0 {
            unclustered[group * 100 + 1] = 150;
            unclustered[group * 100 + 51] = 250;
        }
    }
    let (
        unclustered_disabled_rows,
        _,
        unclustered_disabled_bytes,
        _,
        unclustered_disabled_filter_rows,
    ) = run_parquet_join(unclustered.clone(), false).await;
    let (
        unclustered_enabled_rows,
        unclustered_pruned,
        unclustered_enabled_bytes,
        attached,
        unclustered_enabled_filter_rows,
    ) = run_parquet_join(unclustered, true).await;
    assert_eq!(unclustered_enabled_rows, 2);
    assert_eq!(unclustered_enabled_rows, unclustered_disabled_rows);
    assert_eq!(unclustered_pruned, 0);
    assert_eq!(unclustered_enabled_bytes, unclustered_disabled_bytes);
    assert_eq!(attached, 1);
    assert_eq!(unclustered_disabled_filter_rows, 400);
    assert_eq!(unclustered_enabled_filter_rows, 400);
}

fn limited_session(bytes: usize) -> (SessionContext, Arc<dyn MemoryPool>) {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()
        .unwrap();
    (
        SessionContext::new_with_config_rt(SessionConfig::new(), runtime),
        pool,
    )
}

#[tokio::test]
async fn runtime_domains_release_with_streams_while_plans_remain_alive() {
    for mode in [PartitionMode::Partitioned, PartitionMode::CollectLeft] {
        for finish in [
            "eof",
            "cancel_before_poll",
            "cancel_after_output",
            "build_error",
        ] {
            let (session, pool) = limited_session(if finish == "build_error" {
                1
            } else {
                8 * 1024 * 1024
            });
            // Sparse keys and more than 150 distinct values require the map
            // strategy even without the IN-list override.
            let join = single_key_join(
                Arc::new(Int64Array::from_iter_values(
                    (0..4096).map(|i| i * 1_000_003),
                )),
                Arc::new(Int64Array::from(vec![0, 1])),
                mode,
            );
            let plan = DynamicFilterJoinExec::new(&join, ConfigOptions::default()).unwrap();
            let producer = plan.build_runtime_join().unwrap();
            let predicate = Arc::downgrade(producer.join.dynamic_filter_expr().unwrap());
            let mut stream = plan
                .execute_runtime_join(producer.join, 0, session.task_ctx())
                .unwrap();
            assert!(predicate.upgrade().is_some());

            match finish {
                "cancel_before_poll" => drop(stream),
                "build_error" => {
                    assert!(stream.next().await.unwrap().is_err());
                    assert!(stream.next().await.is_none());
                }
                _ => {
                    assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 1);
                    assert!(pool.reserved() > 0);
                    assert!(predicate.upgrade().is_some());
                    if finish == "eof" {
                        assert!(stream.next().await.is_none());
                        // Retain the exhausted stream as well as the plan.
                        assert_eq!(pool.reserved(), 0);
                        assert!(predicate.upgrade().is_none());
                    } else {
                        drop(stream);
                    }
                    let metrics = plan.metrics().unwrap();
                    assert_eq!(metrics.output_rows(), Some(1));
                    assert_eq!(
                        metrics
                            .sum_by_name("dynamic_filter_rows_pruned")
                            .unwrap()
                            .as_usize(),
                        1
                    );
                }
            }
            // The accumulator also owns this predicate. A zero reservation
            // alone would miss the old plan-owned, unaccounted map retention.
            assert_eq!(pool.reserved(), 0, "{mode:?}: {finish}");
            assert!(predicate.upgrade().is_none(), "{mode:?}: {finish}");
            assert!(plan.template.dynamic_filter_expr().is_none());
        }
    }
}

fn expression_nodes(expr: &Arc<dyn PhysicalExpr>) -> usize {
    1 + expr
        .children()
        .into_iter()
        .map(expression_nodes)
        .sum::<usize>()
}

#[tokio::test]
async fn duplicate_heavy_builds_do_not_materialize_unreserved_inlists() {
    for mode in [PartitionMode::Partitioned, PartitionMode::CollectLeft] {
        let (session, pool) = limited_session(1024 * 1024);
        let join = single_key_join(
            Arc::new(Int8Array::from(vec![1; 65_536])),
            Arc::new(Int8Array::from(vec![1, 2])),
            mode,
        );
        let config = session.copied_config();
        let configured_limit = config
            .options()
            .optimizer
            .hash_join_inlist_pushdown_max_size;
        assert!(configured_limit > 0);
        let plan = DynamicFilterJoinExec::new(&join, config.options().as_ref().clone()).unwrap();
        let producer = plan.build_runtime_join().unwrap();
        let predicate = Arc::downgrade(producer.join.dynamic_filter_expr().unwrap());
        let mut stream = plan
            .execute_runtime_join(producer.join, 0, session.task_ctx())
            .unwrap();
        let mut rows = stream.next().await.unwrap().unwrap().num_rows();
        {
            let predicate = predicate.upgrade().unwrap();
            let current = predicate.current().unwrap();
            // Bound the published expression, rather than relying on pool
            // reservations, which do not account for IN-list construction.
            assert!(expression_nodes(&current) < 32);
        }
        while let Some(batch) = stream.next().await {
            rows += batch.unwrap().num_rows();
        }
        assert_eq!(rows, 65_536, "duplicate build rows must still join");
        assert_eq!(pool.reserved(), 0);
        assert!(predicate.upgrade().is_none());
        assert_eq!(
            session
                .copied_config()
                .options()
                .optimizer
                .hash_join_inlist_pushdown_max_size,
            configured_limit,
            "the execution override must not change the session"
        );
        assert_eq!(
            plan.metrics()
                .unwrap()
                .sum_by_name("dynamic_filter_rows_pruned")
                .unwrap()
                .as_usize(),
            1
        );
    }
}

#[tokio::test]
async fn executions_and_resets_have_independent_producers() {
    for mode in [PartitionMode::Partitioned, PartitionMode::CollectLeft] {
        let (session, pool) = limited_session(1024 * 1024);
        let join = single_key_join(
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(Int64Array::from(vec![5, 90])),
            mode,
        );
        let plan = Arc::new(DynamicFilterJoinExec::new(&join, ConfigOptions::default()).unwrap());
        let first = plan.build_runtime_join().unwrap();
        let first_filter = Arc::downgrade(first.join.dynamic_filter_expr().unwrap());
        let second = plan.build_runtime_join().unwrap();
        let second_filter = Arc::downgrade(second.join.dynamic_filter_expr().unwrap());
        assert!(!first_filter.ptr_eq(&second_filter));
        let mut first = plan
            .execute_runtime_join(first.join, 0, session.task_ctx())
            .unwrap();
        let mut second = plan
            .execute_runtime_join(second.join, 0, session.task_ctx())
            .unwrap();
        assert_eq!(first.next().await.unwrap().unwrap().num_rows(), 1);
        assert_eq!(second.next().await.unwrap().unwrap().num_rows(), 1);
        drop(first);
        assert!(first_filter.upgrade().is_none());
        assert!(second_filter.upgrade().is_some());
        assert!(pool.reserved() > 0);
        assert!(second.next().await.is_none());
        assert!(second_filter.upgrade().is_none());
        assert_eq!(pool.reserved(), 0);

        let reset = Arc::clone(&plan).reset_state().unwrap();
        assert_eq!(
            row_count(&collect(reset, session.task_ctx()).await.unwrap()),
            1
        );
        let replacement = single_key_join(
            Arc::new(Int64Array::from(vec![90])),
            Arc::new(Int64Array::from(vec![5, 90])),
            mode,
        );
        let rewritten = Arc::clone(&plan)
            .with_new_children(vec![
                Arc::clone(replacement.left()),
                Arc::clone(replacement.right()),
            ])
            .unwrap();
        let output = collect(rewritten, session.task_ctx()).await.unwrap();
        assert_eq!(row_count(&output), 1);
        assert_eq!(
            output[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            90
        );
        assert_eq!(pool.reserved(), 0);
    }
}

#[tokio::test]
async fn child_replacement_rechecks_join_key_types() {
    let session = SessionContext::new();
    for mode in [PartitionMode::Partitioned, PartitionMode::CollectLeft] {
        let join = plain_join()
            .builder()
            .with_partition_mode(mode)
            .build()
            .unwrap();
        let plan = Arc::new(
            DynamicFilterJoinExec::try_new(&join, &ConfigOptions::default())
                .unwrap()
                .unwrap(),
        );
        for (key_type, supported) in [
            (DataType::Int64, true),
            (DataType::Float64, false),
            (DataType::Utf8, false),
        ] {
            let rewritten = Arc::clone(&plan)
                .with_new_children(vec![
                    input(vec![Some(90)], &key_type, 1),
                    input(vec![Some(5), Some(90)], &key_type, 0),
                ])
                .unwrap();
            assert_eq!(rewritten.is::<DynamicFilterJoinExec>(), supported);
            if !supported {
                assert!(rewritten
                    .downcast_ref::<HashJoinExec>()
                    .unwrap()
                    .dynamic_filter_expr()
                    .is_none());
            }
            let output = collect(Arc::clone(&rewritten), session.task_ctx())
                .await
                .unwrap();
            assert_eq!(row_count(&output), 1);
            if supported {
                assert_eq!(metric(&rewritten, "dynamic_filter_rows_pruned"), 1);
                let reset = rewritten.reset_state().unwrap();
                assert!(reset.is::<DynamicFilterJoinExec>());
                let reset_output = collect(Arc::clone(&reset), session.task_ctx())
                    .await
                    .unwrap();
                assert_eq!(row_count(&reset_output), 1);
                assert_eq!(metric(&reset, "dynamic_filter_rows_pruned"), 1);
            }
        }
    }
}

#[test]
fn child_replacement_rechecks_native_partition_counts() {
    for mode in [PartitionMode::Partitioned, PartitionMode::CollectLeft] {
        let join = plain_join()
            .builder()
            .with_partition_mode(mode)
            .build()
            .unwrap();
        let plan = Arc::new(
            DynamicFilterJoinExec::try_new(&join, &ConfigOptions::default())
                .unwrap()
                .unwrap(),
        );
        for replace_build in [false, true] {
            let schema = if replace_build {
                join.left().schema()
            } else {
                join.right().schema()
            };
            let two_partitions: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
                &[
                    vec![RecordBatch::new_empty(Arc::clone(&schema))],
                    vec![RecordBatch::new_empty(Arc::clone(&schema))],
                ],
                schema,
                None,
            )
            .unwrap();
            let children = if replace_build {
                vec![two_partitions, Arc::clone(join.right())]
            } else {
                vec![Arc::clone(join.left()), two_partitions]
            };
            let rewritten = Arc::clone(&plan).with_new_children(children).unwrap();
            assert!(rewritten
                .downcast_ref::<HashJoinExec>()
                .unwrap()
                .dynamic_filter_expr()
                .is_none());
        }
    }
}
