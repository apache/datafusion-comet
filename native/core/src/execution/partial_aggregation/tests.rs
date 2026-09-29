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

use std::sync::Mutex;

use arrow::array::{Float64Array, Int32Array, Int64Array, ListArray, ListBuilder, StringBuilder};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::physical_expr::expressions::{Column, StatsType};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion_comet_spark_expr::{
    Avg as CometAvg, CometCollectSet, Correlation, Covariance, SparkPercentile, Stddev, Variance,
};
use futures::{FutureExt, StreamExt};

fn expression(fun: Arc<AggregateUDF>, datatype: DataType) -> Arc<AggregateFunctionExpr> {
    let schema = Arc::new(Schema::new(vec![Field::new("v", datatype, true)]));
    Arc::new(
        AggregateExprBuilder::new(fun, vec![Arc::new(Column::new("v", 0))])
            .schema(schema)
            .alias("result")
            .build()
            .unwrap(),
    )
}

#[test]
fn singleton_fallback_preserves_multifield_states_and_accumulated_prefix() {
    let expr = expression(
        Arc::new(AggregateUDF::new_from_impl(CometAvg::new(
            "avg",
            DataType::Float64,
        ))),
        DataType::Float64,
    );
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
    let wrapped = wrap_aggregate_expr(expr, schema).unwrap();
    let mut accumulator = wrapped.create_groups_accumulator().unwrap();
    accumulator
        .update_batch(
            &[Arc::new(Float64Array::from(vec![Some(10.0), Some(20.0)]))],
            &[0, 0],
            None,
            1,
        )
        .unwrap();

    let rows = SINGLETON_CHUNK_ROWS * 2 + 7;
    let values: ArrayRef = Arc::new(Float64Array::from_iter(
        (0..rows + 2).map(|i| (i % 5 != 0).then_some(i as f64)),
    ));
    let values = values.slice(1, rows);
    let filter = BooleanArray::from_iter((0..rows).map(|i| match i % 3 {
        0 => Some(true),
        1 => Some(false),
        _ => None,
    }));
    let states = accumulator
        .convert_to_state(&[Arc::clone(&values)], Some(&filter))
        .unwrap();
    let sums = states[0].as_any().downcast_ref::<Float64Array>().unwrap();
    let counts = states[1].as_any().downcast_ref::<Int64Array>().unwrap();
    for i in 0..rows {
        let contributes = values.is_valid(i) && filter.is_valid(i) && filter.value(i);
        assert_eq!(counts.value(i), i64::from(contributes));
        assert_eq!(
            sums.value(i),
            if contributes { (i + 1) as f64 } else { 0.0 }
        );
    }

    // Conversion creates fresh state and cannot consume a previously accumulated group.
    let prefix = accumulator.state(EmitTo::All).unwrap();
    assert_eq!(
        prefix[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    assert_eq!(
        prefix[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        30.0
    );

    let empty = accumulator
        .convert_to_state(&[Arc::new(Float64Array::from(Vec::<f64>::new()))], None)
        .unwrap();
    assert_eq!(empty.len(), 2);
    assert!(empty.iter().all(|state| state.is_empty()));
}

#[test]
fn floating_min_max_preserve_nan_and_signed_zero_across_bypass() {
    let inputs = vec![
        vec![Some(f64::NAN), Some(1.0), None],
        vec![Some(1.0), Some(f64::NAN), None],
        vec![Some(-0.0), Some(0.0)],
        vec![Some(0.0), Some(-0.0)],
        vec![Some(f64::NEG_INFINITY), Some(f64::INFINITY), Some(f64::NAN)],
    ];
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
    for fun in [min_udaf(), max_udaf()] {
        let expr = expression(fun, DataType::Float64);
        let wrapped = wrap_aggregate_expr(Arc::clone(&expr), Arc::clone(&schema)).unwrap();
        for input in &inputs {
            let values: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(input.clone()))];
            let mut ordinary = create_groups_accumulator(&expr).unwrap();
            ordinary
                .update_batch(&values, &vec![0; input.len()], None, 1)
                .unwrap();
            let expected = ordinary.evaluate(EmitTo::All).unwrap();
            let converter = wrapped.create_groups_accumulator().unwrap();
            let states = converter.convert_to_state(&values, None).unwrap();
            let mut merged = create_groups_accumulator(&expr).unwrap();
            merged
                .merge_batch(&states, &vec![0; input.len()], 1)
                .unwrap();
            let result = merged.evaluate(EmitTo::All).unwrap();
            let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
            let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(
                result.value(0).to_bits(),
                expected.value(0).to_bits(),
                "{}: {input:?}",
                expr.name()
            );
        }
    }
}

#[test]
fn wrapping_preserves_state_schema_and_optimized_count_conversion() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let expr = expression(count_udaf(), DataType::Int64);
    let wrapped = wrap_aggregate_expr(Arc::clone(&expr), schema).unwrap();
    assert_eq!(wrapped.field(), expr.field());
    assert_eq!(
        wrapped.state_fields().unwrap(),
        expr.state_fields().unwrap()
    );
    let accumulator = wrapped.create_groups_accumulator().unwrap();
    let states = accumulator
        .convert_to_state(
            &[Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]))],
            None,
        )
        .unwrap();
    assert_eq!(
        states[0].as_any().downcast_ref::<Int64Array>().unwrap(),
        &Int64Array::from(vec![1, 0, 1])
    );
}

#[test]
fn scalar_only_aggregate_retains_all_arguments_and_filter_semantics() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, true),
        Field::new("w", DataType::Int64, true),
    ]));
    let expr = Arc::new(
        AggregateExprBuilder::new(
            count_udaf(),
            vec![Arc::new(Column::new("v", 0)), Arc::new(Column::new("w", 1))],
        )
        .schema(Arc::clone(&schema))
        .alias("both")
        .build()
        .unwrap(),
    );
    assert!(!expr.groups_accumulator_supported());
    let wrapped = wrap_aggregate_expr(expr, schema).unwrap();
    let accumulator = wrapped.create_groups_accumulator().unwrap();
    let states = accumulator
        .convert_to_state(
            &[
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(4)])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(4)])),
            ],
            Some(&BooleanArray::from(vec![true, true, true, false])),
        )
        .unwrap();
    assert_eq!(
        states[0].as_any().downcast_ref::<Int64Array>().unwrap(),
        &Int64Array::from(vec![1, 0, 0, 0])
    );
}

#[test]
fn comet_multifield_and_list_states_merge_after_generic_conversion() {
    let functions: Vec<(Arc<AggregateUDF>, usize)> = vec![
        (
            Arc::new(AggregateUDF::new_from_impl(CometAvg::new(
                "avg",
                DataType::Float64,
            ))),
            1,
        ),
        (
            Arc::new(AggregateUDF::new_from_impl(Variance::new(
                "variance",
                DataType::Float64,
                StatsType::Population,
                true,
            ))),
            1,
        ),
        (
            Arc::new(AggregateUDF::new_from_impl(Stddev::new(
                "stddev",
                DataType::Float64,
                StatsType::Population,
                true,
            ))),
            1,
        ),
        (
            Arc::new(AggregateUDF::new_from_impl(Covariance::new(
                "covariance",
                DataType::Float64,
                StatsType::Population,
                true,
            ))),
            2,
        ),
        (
            Arc::new(AggregateUDF::new_from_impl(Correlation::new(
                "correlation",
                DataType::Float64,
                true,
            ))),
            2,
        ),
        (
            Arc::new(AggregateUDF::new_from_impl(
                SparkPercentile::try_new(0.5).unwrap(),
            )),
            1,
        ),
        (
            Arc::new(AggregateUDF::new_from_impl(CometCollectSet::new())),
            1,
        ),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Float64, true),
        Field::new("y", DataType::Float64, true),
    ]));
    let values: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(3.0),
            None,
            Some(7.0),
            Some(5.0),
            Some(9.0),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(2.0),
            Some(6.0),
            Some(10.0),
            None,
            Some(10.0),
            Some(18.0),
        ])),
    ];
    let filter = BooleanArray::from(vec![
        Some(true),
        Some(true),
        Some(true),
        Some(true),
        Some(false),
        None,
    ]);
    let groups = [0, 0, 0, 1, 1, 1];
    for (fun, arity) in functions {
        let name = fun.name().to_string();
        let args: Vec<Arc<dyn PhysicalExpr>> = (0..arity)
            .map(|i| Arc::new(Column::new(schema.field(i).name(), i)) as Arc<dyn PhysicalExpr>)
            .collect();
        let expr = Arc::new(
            AggregateExprBuilder::new(fun, args)
                .schema(Arc::clone(&schema))
                .alias(&name)
                .build()
                .unwrap(),
        );
        let wrapped = wrap_aggregate_expr(Arc::clone(&expr), Arc::clone(&schema)).unwrap();
        assert_eq!(
            wrapped.state_fields().unwrap(),
            expr.state_fields().unwrap()
        );
        let mut ordinary = create_groups_accumulator(&expr).unwrap();
        ordinary
            .update_batch(&values[..arity], &groups, Some(&filter), 2)
            .unwrap();
        let expected = ordinary.evaluate(EmitTo::All).unwrap();
        let converter = wrapped.create_groups_accumulator().unwrap();
        let states = converter
            .convert_to_state(&values[..arity], Some(&filter))
            .unwrap();
        let mut final_accumulator = create_groups_accumulator(&expr).unwrap();
        final_accumulator.merge_batch(&states, &groups, 2).unwrap();
        let result = final_accumulator.evaluate(EmitTo::All).unwrap();
        if name == "collect_set" {
            // Collect-set's element order is unspecified, including without bypass.
            let sorted_values = |array: &ArrayRef, row| {
                let list = array.as_any().downcast_ref::<ListArray>().unwrap();
                let value = list.value(row);
                let values = value.as_any().downcast_ref::<Float64Array>().unwrap();
                let mut values = values.values().to_vec();
                values.sort_by(f64::total_cmp);
                values
            };
            for group in 0..2 {
                assert_eq!(
                    sorted_values(&result, group),
                    sorted_values(&expected, group)
                );
            }
        } else {
            assert_eq!(result.to_data(), expected.to_data(), "{name}");
        }
    }
}

#[derive(Debug)]
struct ObserveContextExec {
    input: Arc<dyn ExecutionPlan>,
    observed: Arc<Mutex<Vec<Arc<TaskContext>>>>,
    polled_memory: Option<Arc<Mutex<Vec<usize>>>>,
}

impl DisplayAs for ObserveContextExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ObserveContextExec")
    }
}

impl ExecutionPlan for ObserveContextExec {
    fn name(&self) -> &str {
        "ObserveContextExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children.remove(0),
            observed: Arc::clone(&self.observed),
            polled_memory: self.polled_memory.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.observed.lock().unwrap().push(Arc::clone(&context));
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let Some(polled_memory) = self.polled_memory.clone() else {
            return Ok(input);
        };
        let schema = input.schema();
        let stream = input.then(move |batch| {
            let context = Arc::clone(&context);
            let polled_memory = Arc::clone(&polled_memory);
            async move {
                // Let the consumer retain a batch across an asynchronous input poll.
                tokio::task::yield_now().await;
                polled_memory
                    .lock()
                    .unwrap()
                    .push(context.memory_pool().reserved());
                batch
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[test]
fn local_policy_restores_original_context_for_nested_children() {
    for (parent_eligible, child_eligible) in [(false, true), (true, false), (true, true)] {
        let mut config = SessionConfig::new();
        config
            .options_mut()
            .execution
            .skip_partial_aggregation_probe_ratio_threshold = 1.1;
        config.set_extension(Arc::new(PartialAggregationConfig {
            probe_ratio_threshold: 0.73,
            native_shuffle: true,
            allow_numerical_differences: false,
        }));
        let context = Arc::new(TaskContext::default().with_session_config(config));
        let source_contexts = Arc::new(Mutex::new(vec![]));
        let source = Arc::new(ObserveContextExec {
            input: Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
            observed: Arc::clone(&source_contexts),
            polled_memory: None,
        });
        let child_contexts = Arc::new(Mutex::new(vec![]));
        let child = Arc::new(
            PartialAggregationExec::try_new(
                Arc::new(ObserveContextExec {
                    input: source,
                    observed: Arc::clone(&child_contexts),
                    polled_memory: None,
                }),
                child_eligible,
                "child",
            )
            .unwrap(),
        );
        let parent_contexts = Arc::new(Mutex::new(vec![]));
        let parent = PartialAggregationExec::try_new(
            Arc::new(ObserveContextExec {
                input: child,
                observed: Arc::clone(&parent_contexts),
                polled_memory: None,
            }),
            parent_eligible,
            "parent",
        )
        .unwrap();
        drop(parent.execute(0, Arc::clone(&context)).unwrap());
        let source = source_contexts.lock().unwrap();
        assert!(Arc::ptr_eq(&source[0], &context));
        for (observed, eligible) in [
            (&parent_contexts, parent_eligible),
            (&child_contexts, child_eligible),
        ] {
            let observed = observed.lock().unwrap();
            assert_eq!(
                observed[0]
                    .session_config()
                    .options()
                    .execution
                    .skip_partial_aggregation_probe_ratio_threshold,
                if eligible { 0.73 } else { 1.1 }
            );
            assert!(Arc::ptr_eq(
                &observed[0].runtime_env(),
                &context.runtime_env()
            ));
        }
    }
}

#[tokio::test]
async fn adaptive_transition_drains_prefix_once_and_releases_memory_on_drop() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("v", DataType::Float64, true),
    ]));
    let batches: Vec<RecordBatch> = [(0, 0), (32, 2), (0, 4), (0, 6)]
        .into_iter()
        .map(|(start, extra)| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from_iter_values(start..start + 64)),
                    Arc::new(Float64Array::from_iter_values(
                        (start..start + 64).map(|key| f64::from(2 * key + extra)),
                    )),
                ],
            )
            .unwrap()
        })
        .collect();
    let mut results = vec![];
    for (migration, requested_ratio) in [false, true]
        .into_iter()
        .flat_map(|migration| [0.7, 1.1].map(|ratio| (migration, ratio)))
    {
        let input = MemorySourceConfig::try_new_exec(
            std::slice::from_ref(&batches),
            Arc::clone(&schema),
            None,
        )
        .unwrap();
        let avg = Arc::new(
            AggregateExprBuilder::new(
                Arc::new(AggregateUDF::new_from_impl(CometAvg::new(
                    "avg",
                    DataType::Float64,
                ))),
                vec![Arc::new(Column::new("v", 1))],
            )
            .schema(Arc::clone(&schema))
            .alias("avg")
            .build()
            .unwrap(),
        );
        let count = Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![Arc::new(Column::new("v", 1))])
                .schema(Arc::clone(&schema))
                .alias("count")
                .build()
                .unwrap(),
        );
        let group_by = || {
            PhysicalGroupBy::new_single(vec![(
                Arc::new(Column::new("k", 0)) as Arc<dyn PhysicalExpr>,
                "k".to_string(),
            )])
        };
        let partial: Arc<dyn ExecutionPlan> = Arc::new(
            PartialAggregationExec::try_new(
                Arc::new(
                    AggregateExec::try_new(
                        AggregateMode::Partial,
                        group_by(),
                        vec![
                            wrap_aggregate_expr(Arc::clone(&avg), Arc::clone(&schema)).unwrap(),
                            wrap_aggregate_expr(Arc::clone(&count), Arc::clone(&schema)).unwrap(),
                        ],
                        vec![None, None],
                        input,
                        Arc::clone(&schema),
                    )
                    .unwrap(),
                ),
                true,
                "qualified",
            )
            .unwrap(),
        );
        let final_plan: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                group_by(),
                vec![avg, count],
                vec![None, None],
                Arc::clone(&partial),
                Arc::clone(&schema),
            )
            .unwrap(),
        );
        let mut config = SessionConfig::new()
            .with_batch_size(16)
            .set_bool("datafusion.execution.enable_migration_aggregate", migration);
        config
            .options_mut()
            .execution
            .skip_partial_aggregation_probe_rows_threshold = 128;
        config
            .options_mut()
            .execution
            .skip_partial_aggregation_probe_ratio_threshold = 1.1;
        config.set_extension(Arc::new(PartialAggregationConfig {
            probe_ratio_threshold: requested_ratio,
            native_shuffle: true,
            allow_numerical_differences: true,
        }));
        let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
                .build()
                .unwrap(),
        );
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(config)
                .with_runtime(runtime),
        );
        let output = collect(final_plan, Arc::clone(&context)).await.unwrap();
        let mut values = std::collections::BTreeMap::new();
        for batch in output {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let averages = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let counts = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(values
                    .insert(keys.value(row), (averages.value(row), counts.value(row)))
                    .is_none());
            }
        }
        assert_eq!(values.len(), 96);
        assert_eq!(values.values().map(|(_, count)| count).sum::<i64>(), 256);
        let metrics = partial.metrics().unwrap();
        let skipped = metrics
            .sum_by_name("skipped_aggregation_rows")
            .map(|value| value.as_usize())
            .unwrap_or(0);
        assert_eq!(skipped, if requested_ratio < 1.0 { 128 } else { 0 });
        assert_eq!(
            metrics.output_rows(),
            Some(if skipped > 0 { 224 } else { 96 })
        );
        assert_eq!(pool.reserved(), 0);
        results.push(values);

        // Stop while the materialized prefix still has pending output slices.
        let mut stream = partial.execute(0, context).unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 16);
        drop(stream);
        assert_eq!(pool.reserved(), 0);
    }
    for result in &results[1..] {
        assert_eq!(&results[0], result);
    }
}

#[tokio::test]
async fn partial_merge_bypass_preserves_weighted_states_after_transition() {
    let original_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("v", DataType::Int64, true),
        Field::new("w", DataType::Int64, true),
    ]));
    let original = Arc::new(
        AggregateExprBuilder::new(
            count_udaf(),
            vec![Arc::new(Column::new("v", 1)), Arc::new(Column::new("w", 2))],
        )
        .schema(Arc::clone(&original_schema))
        .alias("count")
        .build()
        .unwrap(),
    );
    assert!(!original.groups_accumulator_supported());
    let mut fields = vec![Arc::new(Field::new("k", DataType::Int32, false))];
    fields.extend(original.state_fields().unwrap());
    let state_schema = Arc::new(Schema::new(fields));
    let batches: Vec<_> = [0, 32, 0]
        .into_iter()
        .map(|start| {
            RecordBatch::try_new(
                Arc::clone(&state_schema),
                vec![
                    Arc::new(Int32Array::from_iter_values(start..start + 64)),
                    // Each row represents several original rows, not a fresh COUNT input.
                    Arc::new(Int64Array::from_iter_values(
                        (start..start + 64).map(|key| i64::from(key % 3 + 2)),
                    )),
                ],
            )
            .unwrap()
        })
        .collect();
    let expected: std::collections::BTreeMap<_, _> = (0..96)
        .map(|key| {
            let occurrences = i64::from(key < 64) * 2 + i64::from(key >= 32);
            (key, i64::from(key % 3 + 2) * occurrences)
        })
        .collect();

    for migration in [false, true] {
        for ratio in [0.8, 1.1] {
            let input = MemorySourceConfig::try_new_exec(
                std::slice::from_ref(&batches),
                Arc::clone(&state_schema),
                None,
            )
            .unwrap();
            let merged = Arc::new(
                AggregateExprBuilder::new(
                    Arc::new(AggregateUDF::new_from_impl(
                        MergeAsPartialUDF::new(&original).unwrap(),
                    )),
                    vec![Arc::new(Column::new(state_schema.field(1).name(), 1))],
                )
                .schema(Arc::clone(&state_schema))
                .alias("count")
                .build()
                .unwrap(),
            );
            let group_by = || {
                PhysicalGroupBy::new_single(vec![(
                    Arc::new(Column::new("k", 0)) as Arc<dyn PhysicalExpr>,
                    "k".to_string(),
                )])
            };
            let partial: Arc<dyn ExecutionPlan> = Arc::new(
                PartialAggregationExec::try_new(
                    Arc::new(
                        AggregateExec::try_new(
                            AggregateMode::Partial,
                            group_by(),
                            vec![wrap_aggregate_expr(merged, Arc::clone(&state_schema)).unwrap()],
                            vec![None],
                            input,
                            Arc::clone(&state_schema),
                        )
                        .unwrap(),
                    ),
                    true,
                    "qualified-state-merge",
                )
                .unwrap(),
            );
            let final_plan: Arc<dyn ExecutionPlan> = Arc::new(
                AggregateExec::try_new(
                    AggregateMode::Final,
                    group_by(),
                    vec![Arc::clone(&original)],
                    vec![None],
                    Arc::clone(&partial),
                    Arc::clone(&original_schema),
                )
                .unwrap(),
            );
            let mut config = SessionConfig::new()
                .with_batch_size(16)
                .set_bool("datafusion.execution.enable_migration_aggregate", migration);
            config
                .options_mut()
                .execution
                .skip_partial_aggregation_probe_rows_threshold = 64;
            config
                .options_mut()
                .execution
                .skip_partial_aggregation_probe_ratio_threshold = ratio;
            let context = Arc::new(TaskContext::default().with_session_config(config));
            let output = collect(final_plan, context).await.unwrap();
            let mut result = std::collections::BTreeMap::new();
            for batch in output {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let counts = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    assert!(result.insert(keys.value(row), counts.value(row)).is_none());
                }
            }
            assert_eq!(result, expected);
            let skipped = partial
                .metrics()
                .unwrap()
                .sum_by_name("skipped_aggregation_rows")
                .map(|value| value.as_usize())
                .unwrap_or(0);
            assert_eq!(skipped, if ratio < 1.0 { 128 } else { 0 });
        }
    }
}

#[tokio::test]
async fn bypass_coalesces_filtered_fragments_without_losing_states() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let mut offset = 0;
    let batches: Vec<_> = [5, 7, 4, 3, 0, 8, 1, 7, 16, 5]
        .into_iter()
        .map(|len| {
            let start = offset;
            offset += len;
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from_iter_values(
                        (start..offset).map(|i| i % 24),
                    )),
                    Arc::new(Int64Array::from_iter(
                        (start..offset).map(|i| (i % 3 != 0).then_some(i64::from(i))),
                    )),
                ],
            )
            .unwrap()
        })
        .collect();
    let mut results = vec![];
    for migration in [false, true] {
        for ratio in [1.1, 0.8] {
            let input = MemorySourceConfig::try_new_exec(
                std::slice::from_ref(&batches),
                Arc::clone(&schema),
                None,
            )
            .unwrap();
            let count = Arc::new(
                AggregateExprBuilder::new(count_udaf(), vec![Arc::new(Column::new("v", 1))])
                    .schema(Arc::clone(&schema))
                    .alias("count")
                    .build()
                    .unwrap(),
            );
            let aggregate = Arc::new(
                AggregateExec::try_new(
                    AggregateMode::Partial,
                    PhysicalGroupBy::new_single(vec![(
                        Arc::new(Column::new("k", 0)) as Arc<dyn PhysicalExpr>,
                        "k".to_string(),
                    )]),
                    vec![wrap_aggregate_expr(count, Arc::clone(&schema)).unwrap()],
                    vec![None],
                    input,
                    Arc::clone(&schema),
                )
                .unwrap(),
            );
            let partial: Arc<dyn ExecutionPlan> =
                Arc::new(PartialAggregationExec::try_new(aggregate, true, "qualified").unwrap());
            let mut config = SessionConfig::new()
                .with_batch_size(16)
                .set_bool("datafusion.execution.enable_migration_aggregate", migration);
            config
                .options_mut()
                .execution
                .skip_partial_aggregation_probe_rows_threshold = 16;
            config
                .options_mut()
                .execution
                .skip_partial_aggregation_probe_ratio_threshold = ratio;
            let output = collect(
                Arc::clone(&partial),
                Arc::new(TaskContext::default().with_session_config(config)),
            )
            .await
            .unwrap();
            if ratio < 1.0 {
                assert_eq!(
                    output.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
                    vec![16, 19, 16, 5]
                );
                assert_eq!(
                    partial
                        .metrics()
                        .unwrap()
                        .sum_by_name("skipped_aggregation_rows")
                        .unwrap()
                        .as_usize(),
                    40
                );
            }
            if ratio < 1.0 {
                let full_input = batches[8]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let full_output = output[2]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(full_input.values().as_ptr(), full_output.values().as_ptr());
            }
            let mut counts = std::collections::BTreeMap::new();
            for batch in output {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    *counts.entry(keys.value(row)).or_insert(0i64) += values.value(row);
                }
            }
            assert_eq!(counts.len(), 24);
            assert_eq!(counts.values().sum::<i64>(), 37);
            results.push(counts);
        }
    }
    assert!(results.iter().all(|result| result == &results[0]));
}

#[tokio::test]
async fn partial_output_accounts_large_states_and_flushes_under_pressure() {
    let state_batch = |key: i32, value_bytes: usize| {
        let mut lists = ListBuilder::new(StringBuilder::new());
        for _ in 0..2 {
            lists.values().append_value("x".repeat(value_bytes));
            lists.append(true);
        }
        let lists: ArrayRef = Arc::new(lists.finish());
        RecordBatch::try_from_iter(vec![
            (
                "k",
                Arc::new(Int32Array::from(vec![key, key + 1])) as ArrayRef,
            ),
            ("state", lists),
        ])
        .unwrap()
    };
    let batches = vec![
        state_batch(0, 64 * 1024),
        state_batch(2, 64 * 1024),
        state_batch(4, 64 * 1024),
        state_batch(6, MAX_PARTIAL_OUTPUT_BYTES / 4),
        state_batch(8, MAX_PARTIAL_OUTPUT_BYTES / 2 + 1),
    ];
    let schema = batches[0].schema();
    let batch_bytes = batches[0].get_array_memory_size();
    // Admit two inputs and their concatenation, but refuse the third input.
    let limit = batch_bytes * 4;
    let pool = Arc::new(FairSpillPool::new(limit));
    // Buffering workspace must not claim another equal share beside the aggregate.
    let _aggregate = MemoryConsumer::new("aggregate")
        .with_can_spill(true)
        .register(&(Arc::clone(&pool) as Arc<dyn MemoryPool>));
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
            .build()
            .unwrap(),
    );
    let mut config = SessionConfig::new().with_batch_size(16);
    config
        .options_mut()
        .execution
        .skip_partial_aggregation_probe_ratio_threshold = 0.8;
    let context = Arc::new(
        TaskContext::default()
            .with_session_config(config)
            .with_runtime(runtime),
    );
    let polled_memory = Arc::new(Mutex::new(vec![]));
    let input =
        MemorySourceConfig::try_new_exec(std::slice::from_ref(&batches), Arc::clone(&schema), None)
            .unwrap();
    let partial = PartialAggregationExec::try_new(
        Arc::new(ObserveContextExec {
            input,
            observed: Default::default(),
            polled_memory: Some(Arc::clone(&polled_memory)),
        }),
        true,
        "qualified",
    )
    .unwrap();

    let mut stream = partial.execute(0, Arc::clone(&context)).unwrap();
    assert!(stream.next().now_or_never().is_none());
    assert!(stream.next().now_or_never().is_none());
    assert_eq!(pool.reserved(), batch_bytes * 2);
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    polled_memory.lock().unwrap().clear();

    let mut stream = partial.execute(0, context).unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 4);
    // Output is available after three inputs, before consuming the entire stream.
    assert_eq!(
        *polled_memory.lock().unwrap(),
        vec![0, batch_bytes * 2, limit]
    );
    assert_eq!(pool.reserved(), 0);
    let mut output = vec![first];
    while let Some(batch) = stream.next().await {
        output.push(batch.unwrap());
        assert_eq!(pool.reserved(), 0);
    }
    assert_eq!(
        output.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
        vec![4, 2, 2, 2]
    );
    // Refused and individually oversized states remain zero-copy and ordered.
    assert!(batches[3].get_array_memory_size() * 2 > limit);
    assert!(batches[4].get_array_memory_size() > MAX_PARTIAL_OUTPUT_BYTES);
    assert!(Arc::ptr_eq(output[2].column(1), batches[3].column(1)));
    assert!(Arc::ptr_eq(output[3].column(1), batches[4].column(1)));
    assert_eq!(
        concat_batches(&schema, &output).unwrap(),
        concat_batches(&schema, &batches).unwrap()
    );
    assert!(polled_memory
        .lock()
        .unwrap()
        .iter()
        .all(|bytes| *bytes <= limit));
    assert_eq!(pool.reserved(), 0);
}
