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
use crate::execution::metrics::utils::to_native_metric_node;
use datafusion::physical_plan::{aggregates::AggregateExec, collect};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use spark_expression::agg_expr::ExprStruct as AggExprStruct;
use spark_operator::{AggregateMode, HashAggregate};

fn native_scan(is_max: bool) -> (tempfile::NamedTempFile, Operator) {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, true)]));
    let values = if is_max {
        vec![20, 21, 10, 11, 0, 1]
    } else {
        vec![0, 1, 10, 11, 20, 21]
    };
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(values))],
    )
    .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_dictionary_enabled(false)
        .build();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    assert_eq!(writer.close().unwrap().num_row_groups(), 3);

    let field = spark_operator::SparkStructField {
        name: "key".to_owned(),
        data_type: Some(create_proto_datatype()),
        nullable: true,
        ..Default::default()
    };
    let file_size = file.as_file().metadata().unwrap().len() as i64;
    let scan = Operator {
        op_struct: Some(OpStruct::NativeScan(spark_operator::NativeScan {
            common: Some(spark_operator::NativeScanCommon {
                data_schema: vec![field.clone()],
                required_schema: vec![field],
                projection_vector: vec![0],
                session_timezone: "UTC".to_owned(),
                ..Default::default()
            }),
            file_partition: Some(spark_operator::SparkFilePartition {
                partitioned_file: vec![spark_operator::SparkPartitionedFile {
                    file_path: url::Url::from_file_path(file.path()).unwrap().to_string(),
                    length: file_size,
                    file_size,
                    ..Default::default()
                }],
            }),
            ..Default::default()
        })),
        ..Default::default()
    };
    (file, scan)
}

fn min_max(is_max: bool) -> spark_expression::AggExpr {
    let child = Some(create_bound_reference(0));
    let datatype = Some(create_proto_datatype());
    spark_expression::AggExpr {
        expr_struct: Some(if is_max {
            AggExprStruct::Max(spark_expression::Max { child, datatype })
        } else {
            AggExprStruct::Min(spark_expression::Min { child, datatype })
        }),
        ..Default::default()
    }
}

fn aggregate(input: Operator, aggregate: HashAggregate) -> Operator {
    Operator {
        children: vec![input],
        op_struct: Some(OpStruct::HashAgg(aggregate)),
        ..Default::default()
    }
}

#[tokio::test]
async fn serialized_min_max_filter_controls_pruning_and_metric_ownership() {
    for is_max in [false, true] {
        let (_file, scan) = native_scan(is_max);
        for enabled in [false, true] {
            let mut config = SessionConfig::new()
                .with_batch_size(2)
                .with_target_partitions(1)
                .with_parquet_page_index_pruning(false);
            config.options_mut().execution.parquet.pushdown_filters = false;
            let session = Arc::new(SessionContext::new_with_config(config));
            let planner = PhysicalPlanner::new(Arc::clone(&session), 0);
            let operator = aggregate(
                scan.clone(),
                HashAggregate {
                    agg_exprs: vec![min_max(is_max)],
                    mode: AggregateMode::Partial as i32,
                    dynamic_filter_enabled: enabled,
                    ..Default::default()
                },
            );
            // Spark can have several partitions, while each native task reads its
            // own one-partition scan and owns an independent aggregate bound.
            let (scans, _, planned) = planner.create_plan(&operator, &mut vec![], 4).unwrap();
            assert!(scans.is_empty());
            assert_eq!(
                planned.native_plan.name(),
                if enabled {
                    "CometMinMaxReaderFilterExec"
                } else {
                    "AggregateExec"
                }
            );
            if !enabled {
                assert!(planned
                    .native_plan
                    .dynamic_expressions_produced()
                    .is_empty());
            }
            let batches = collect(Arc::clone(&planned.native_plan), session.task_ctx())
                .await
                .unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                if is_max { 21 } else { 0 }
            );
            let metrics = to_native_metric_node(&planned).unwrap();
            assert_eq!(metrics.metrics["output_rows"], 1);
            assert!(!metrics.metrics.contains_key("bytes_scanned"));
            assert_eq!(metrics.children.len(), 1);
            let scan_metrics = &metrics.children[0].metrics;
            assert_eq!(scan_metrics["output_rows"], if enabled { 2 } else { 6 });
            assert_eq!(
                scan_metrics["row_groups_pruned_dynamic_filter"],
                if enabled { 2 } else { 0 }
            );
            if enabled {
                assert_eq!(metrics.metrics["dynamic_filter_minmax_filters_attached"], 1);
                assert_eq!(
                    metrics
                        .metrics
                        .get("dynamic_filter_minmax_filters_skipped")
                        .copied()
                        .unwrap_or_default(),
                    0
                );
            } else {
                assert!(!metrics
                    .metrics
                    .contains_key("dynamic_filter_minmax_filters_attached"));
            }
        }
    }
}

#[test]
fn state_merging_and_jvm_input_keep_ordinary_aggregation() {
    let (_file, scan) = native_scan(false);
    let planner = PhysicalPlanner::default();
    for (input, mode) in [
        (scan.clone(), AggregateMode::Final),
        (scan, AggregateMode::PartialMerge),
        (create_scan(), AggregateMode::Partial),
    ] {
        let operator = aggregate(
            input,
            HashAggregate {
                agg_exprs: vec![min_max(false)],
                mode: mode as i32,
                dynamic_filter_enabled: true,
                ..Default::default()
            },
        );
        let (_, _, planned) = planner.create_plan(&operator, &mut vec![], 1).unwrap();
        let aggregate = if mode == AggregateMode::PartialMerge {
            &planned.additional_native_plans[0]
        } else {
            &planned.native_plan
        };
        assert_eq!(aggregate.name(), "AggregateExec");
    }
}

#[test]
fn min_max_keeps_real_casts_and_computed_arguments() {
    let (_file, scan) = native_scan(false);
    let planner = PhysicalPlanner::default();
    let literal = Expr {
        expr_struct: Some(ExprStruct::Literal(spark_expression::Literal {
            datatype: Some(create_proto_datatype()),
            value: Some(literal::Value::IntVal(7)),
            is_null: false,
        })),
        ..Default::default()
    };
    for (child, datatype) in [
        (create_bound_reference(0), create_int64_datatype()),
        (literal, create_proto_datatype()),
    ] {
        let operator = aggregate(
            scan.clone(),
            HashAggregate {
                agg_exprs: vec![spark_expression::AggExpr {
                    expr_struct: Some(AggExprStruct::Min(spark_expression::Min {
                        child: Some(child),
                        datatype: Some(datatype),
                    })),
                    ..Default::default()
                }],
                mode: AggregateMode::Partial as i32,
                dynamic_filter_enabled: true,
                ..Default::default()
            },
        );
        let (_, _, planned) = planner.create_plan(&operator, &mut vec![], 1).unwrap();
        let aggregate = planned.native_plan.downcast_ref::<AggregateExec>().unwrap();
        assert!(aggregate.aggr_expr()[0].expressions()[0]
            .is::<datafusion::physical_expr::expressions::CastExpr>());
    }
}
