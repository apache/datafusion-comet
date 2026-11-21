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

//! Runs assembled stage graphs on DataFusion alone, without Ballista: each exchange runs as a
//! DataFusion repartition in this process.

mod common;

use comet::execution::native_executors::MAX_PARTITIONS;
use comet_native_executors::assembly::build_plan;
use common::*;
use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Int32Type, Int64Type};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{collect, ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionContext;
use datafusion_comet_proto::spark_expression::{
    expr::ExprStruct, literal::Value, Expr, IfExpr, Literal, Subquery,
};
use datafusion_comet_proto::spark_native_executors::{QueryRequest, Stage, StageInput};
use datafusion_comet_proto::spark_operator::{
    operator::OpStruct, IcebergWrite, NativeScan, NativeScanCommon, Operator, ParquetWriter,
    Projection, Scan, ShuffleWriter, SparkFilePartition, SparkPartitionedFile,
};
use prost::Message;
use std::cmp::Ordering;
use std::sync::Arc;

async fn assemble(request: QueryRequest) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    tokio::task::spawn_blocking(move || build_plan(request))
        .await
        .unwrap()
}

/// Runs `request` and returns its single column, sorted.
async fn run(request: QueryRequest) -> Vec<i64> {
    let plan = assemble(request).await.unwrap();
    let batches = collect(plan, SessionContext::new().task_ctx())
        .await
        .unwrap();
    sorted_values::<Int64Type>(&batches)
}

#[tokio::test(flavor = "multi_thread")]
async fn leaf_stage_runs_every_partition() {
    let request = query(vec![range_stage(100, 4)]);
    assert_eq!(run(request).await, (0..100).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn hash_exchange_moves_every_row() {
    let request = query(vec![
        range_stage(100, 4),
        reading_stage(0, hash_on_first_column(3), 3),
    ]);
    assert_eq!(run(request).await, (0..100).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn single_partition_exchange_collects_every_row() {
    let request = query(vec![
        range_stage(100, 4),
        reading_stage(0, single_partition(), 1),
    ]);
    assert_eq!(run(request).await, (0..100).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn range_exchange_without_an_ordering_is_read_as_one_partition() {
    // A stage that declares no ordering has nothing to merge its partitions by.
    let request = query(vec![
        range_stage(100, 4),
        reading_stage(0, range_partitioning(4, vec![]), 4),
    ]);
    let plan = assemble(request.clone()).await.unwrap();
    assert_eq!(plan.output_partitioning().partition_count(), 1);
    assert_eq!(run(request).await, (0..100).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn range_exchange_is_sorted_in_each_partition_and_merged() {
    let order = first_column_descending(long_type());
    let request = query(vec![
        range_stage(1000, 4),
        sorting_stage(0, range_partitioning(4, vec![]), order, long_type()),
    ]);
    let plan = assemble(request).await.unwrap();
    let merge = plan
        .downcast_ref::<SortPreservingMergeExec>()
        .expect("the stage's sorted partitions are merged");
    assert!(merge.expr()[0].options.descending);
    // The stage reads the producer's four partitions without an exchange.
    assert_eq!(merge.input().output_partitioning().partition_count(), 4);
    let batches = collect(plan, SessionContext::new().task_ctx())
        .await
        .unwrap();
    let values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(values, (0..1000).rev().collect::<Vec<_>>());
}

/// A merge compares raw values, which order floats differently from Comet's sort.
#[tokio::test(flavor = "multi_thread")]
async fn range_exchange_sorted_by_a_float_is_read_as_one_partition() {
    let mut producer = range_stage(1000, 4);
    producer.output_schema = vec![field(double_type())];
    let order = first_column_descending(double_type());
    let request = query(vec![
        producer,
        sorting_stage(0, range_partitioning(4, vec![]), order, double_type()),
    ]);
    let plan = assemble(request).await.unwrap();
    assert!(!plan.is::<SortPreservingMergeExec>());
    assert!(plan.children()[1].is::<CoalescePartitionsExec>());
    assert_eq!(plan.output_partitioning().partition_count(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn partition_plans_must_match_the_partitions() {
    let mut stage = range_stage(100, 4);
    let block = stage.plan.as_ref().unwrap().encode_to_vec();
    stage.partition_plans = vec![block.clone(), block];
    let error = assemble(query(vec![stage])).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("has 4 partitions but 2 partition plans"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn file_stage_cannot_read_a_hash_exchange() {
    // A bucketed scan joined with a side Spark shuffled by Murmur3 would meet DataFusion's hash.
    let mut stage = reading_stage(0, hash_on_first_column(3), 3);
    let block = stage.plan.as_ref().unwrap().encode_to_vec();
    stage.partition_plans = vec![block.clone(), block.clone(), block];
    let error = assemble(query(vec![range_stage(100, 4), stage]))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("can't also read a hash exchange"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn file_stage_can_read_a_single_partition_exchange() {
    let mut stage = reading_stage(0, single_partition(), 1);
    stage.partition_plans = vec![stage.plan.as_ref().unwrap().encode_to_vec()];
    let plan = assemble(query(vec![range_stage(100, 4), stage]))
        .await
        .unwrap();
    assert_eq!(plan.output_partitioning().partition_count(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn reading_stage_has_as_many_partitions_as_its_exchange() {
    // The partition count a driver gives a stage that reads an exchange doesn't matter.
    let request = query(vec![
        range_stage(100, 4),
        reading_stage(0, hash_on_first_column(3), 5),
    ]);
    let plan = assemble(request).await.unwrap();
    assert_eq!(plan.output_partitioning().partition_count(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn output_is_cast_to_the_types_the_stage_declares() {
    // A range produces longs. A stage declaring an int column gets ints, as a Spark task's
    // shuffle writer casts a column whose native type drifted from the one Spark declared.
    let mut stage = range_stage(100, 4);
    stage.output_schema = vec![field(int_type())];
    let plan = assemble(query(vec![stage])).await.unwrap();
    assert_eq!(plan.schema().field(0).data_type(), &DataType::Int32);
    let batches = collect(plan, SessionContext::new().task_ctx())
        .await
        .unwrap();
    assert_eq!(
        sorted_values::<Int32Type>(&batches),
        (0..100).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn batches_have_the_schema_the_stage_declares() {
    // `if(true, id, null)` has no nulls, but the block's output leaves it nullable. Spark can
    // declare such a column not null, as it does a join key that a filter has checked, and a
    // distributed engine checks each batch a stage sends against the stage's schema.
    let literal = |value: Option<Value>, data_type| Expr {
        expr_struct: Some(ExprStruct::Literal(Literal {
            is_null: value.is_none(),
            value,
            datatype: Some(data_type),
        })),
        ..Default::default()
    };
    let column = Expr {
        expr_struct: Some(ExprStruct::If(Box::new(IfExpr {
            if_expr: Some(Box::new(literal(Some(Value::BoolVal(true)), bool_type()))),
            true_expr: Some(Box::new(bound(0, long_type()))),
            false_expr: Some(Box::new(literal(None, long_type()))),
        }))),
        ..Default::default()
    };
    let mut stage = range_stage(100, 4);
    stage.plan = Some(Operator {
        op_struct: Some(OpStruct::Projection(Projection {
            project_list: vec![column],
        })),
        children: vec![stage.plan.take().unwrap()],
        ..Default::default()
    });
    stage.output_schema[0].nullable = false;
    let plan = assemble(query(vec![stage])).await.unwrap();
    let batches = collect(Arc::clone(&plan), SessionContext::new().task_ctx())
        .await
        .unwrap();
    assert!(batches.iter().all(|batch| batch.schema() == plan.schema()));
    assert_eq!(
        sorted_values::<Int64Type>(&batches),
        (0..100).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn declared_columns_must_match_the_block() {
    let mut stage = range_stage(100, 4);
    stage.output_schema = vec![field(long_type()), field(long_type())];
    let error = assemble(query(vec![stage])).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("produces 1 columns but the stage declares 2"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stage_without_partitions_is_rejected() {
    let mut stage = range_stage(100, 4);
    stage.num_partitions = 0;
    let error = assemble(query(vec![stage])).await.unwrap_err();
    assert!(error.to_string().contains("no partitions"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn block_that_calls_into_the_jvm_is_rejected() {
    // A scalar subquery reads its value from the JVM, which a native executor doesn't have.
    let subquery = Expr {
        expr_struct: Some(ExprStruct::Subquery(Subquery {
            id: 1,
            datatype: Some(long_type()),
        })),
        ..Default::default()
    };
    let mut stage = range_stage(100, 4);
    stage.plan = Some(Operator {
        op_struct: Some(OpStruct::Projection(Projection {
            project_list: vec![subquery],
        })),
        children: vec![stage.plan.take().unwrap()],
        ..Default::default()
    });
    let error = assemble(query(vec![stage])).await.unwrap_err();
    assert!(error.to_string().contains("calls into the JVM"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn file_stage_is_checked_without_its_object_store() {
    // The scheduler checks a stage without reading its files, so it builds no object store and
    // uses no storage credentials. Nothing can build a store for this URL, so only the stage's
    // run finds that out.
    let scan = |partitioned_file| Operator {
        op_struct: Some(OpStruct::NativeScan(NativeScan {
            common: Some(NativeScanCommon {
                required_schema: vec![field(long_type())],
                data_schema: vec![field(long_type())],
                projection_vector: vec![0],
                session_timezone: "UTC".to_string(),
                ..Default::default()
            }),
            file_partition: Some(SparkFilePartition { partitioned_file }),
            ..Default::default()
        })),
        ..Default::default()
    };
    let file = SparkPartitionedFile {
        file_path: "no-such-store://bucket/part-0.parquet".to_string(),
        length: 16,
        file_size: 16,
        ..Default::default()
    };
    let stage = Stage {
        plan: Some(scan(vec![])),
        partition_plans: vec![scan(vec![file]).encode_to_vec()],
        num_partitions: 1,
        output_schema: vec![field(long_type())],
        ..Default::default()
    };
    let plan = assemble(query(vec![stage])).await.unwrap();
    let error = collect(plan, SessionContext::new().task_ctx())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no-such-store"), "{error}");
}

/// The values of column `column` of `batches`, in order.
fn longs(batches: &[RecordBatch], column: usize) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column)
                .as_primitive::<Int64Type>()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect()
}

/// A stage of `n` rows in `slices` partitions with two columns: a key that is null for every
/// fifth row and otherwise `id % 7`, so it has many duplicates, and `id`.
fn keyed_stage(n: i64, slices: i32) -> Stage {
    let id = || bound(0, long_type());
    let key = if_else(
        equal(remainder(id(), long_literal(5)), long_literal(0)),
        null_long(),
        remainder(id(), long_literal(7)),
    );
    projecting_stage(
        range_stage(n, slices),
        vec![key, id()],
        vec![long_type(), long_type()],
    )
}

fn keyed_rows(n: i64) -> Vec<(Option<i64>, i64)> {
    (0..n)
        .map(|id| ((id % 5 != 0).then_some(id % 7), id))
        .collect()
}

fn compare_keys(a: &Option<i64>, b: &Option<i64>, descending: bool, nulls_first: bool) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) if nulls_first => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) if nulls_first => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(a), Some(b)) if descending => b.cmp(a),
        (Some(a), Some(b)) => a.cmp(b),
    }
}

async fn run_in_order(request: QueryRequest) -> (Arc<dyn ExecutionPlan>, Vec<RecordBatch>) {
    let plan = assemble(request).await.unwrap();
    let batches = collect(Arc::clone(&plan), SessionContext::new().task_ctx())
        .await
        .unwrap();
    (plan, batches)
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_honors_every_null_ordering() {
    for descending in [false, true] {
        for nulls_first in [false, true] {
            let order = sort_key(0, long_type(), descending, nulls_first);
            let request = query(vec![
                keyed_stage(500, 4),
                sorting_stage_by(
                    0,
                    range_partitioning(4, vec![]),
                    vec![order],
                    vec![long_type(), long_type()],
                ),
            ]);
            let (plan, batches) = run_in_order(request).await;
            assert!(plan.is::<SortPreservingMergeExec>());
            let mut expected = keyed_rows(500)
                .into_iter()
                .map(|(key, _)| key)
                .collect::<Vec<_>>();
            expected.sort_by(|a, b| compare_keys(a, b, descending, nulls_first));
            assert_eq!(
                longs(&batches, 0),
                expected,
                "descending={descending} nulls_first={nulls_first}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_honors_a_multi_key_order() {
    // The first key has many duplicates and nulls, so the second decides most of the order.
    let orders = vec![
        sort_key(0, long_type(), false, true),
        sort_key(1, long_type(), true, false),
    ];
    let request = query(vec![
        keyed_stage(500, 4),
        sorting_stage_by(
            0,
            range_partitioning(4, vec![]),
            orders,
            vec![long_type(), long_type()],
        ),
    ]);
    let (plan, batches) = run_in_order(request).await;
    assert!(plan.is::<SortPreservingMergeExec>());
    let mut expected = keyed_rows(500);
    expected.sort_by(|a, b| compare_keys(&a.0, &b.0, false, true).then(b.1.cmp(&a.1)));
    let rows = longs(&batches, 0)
        .into_iter()
        .zip(longs(&batches, 1).into_iter().map(Option::unwrap))
        .collect::<Vec<_>>();
    assert_eq!(rows, expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_orders_strings_as_bytes() {
    // As text "10" < "2", which differs from the numbers' order.
    let key = cast(bound(0, long_type()), string_type());
    let producer = projecting_stage(range_stage(200, 4), vec![key], vec![string_type()]);
    let order = sort_key(0, string_type(), false, false);
    let request = query(vec![
        producer,
        sorting_stage(0, range_partitioning(4, vec![]), order, string_type()),
    ]);
    let (plan, batches) = run_in_order(request).await;
    assert!(plan.is::<SortPreservingMergeExec>());
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_string::<i32>()
                .iter()
                .map(|value| value.unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut expected = (0..200).map(|id| id.to_string()).collect::<Vec<_>>();
    expected.sort();
    assert_eq!(values, expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_tolerates_empty_producer_partitions() {
    // Two rows in four partitions leave at least two of them empty.
    let order = first_column_descending(long_type());
    let request = query(vec![
        range_stage(2, 4),
        sorting_stage(0, range_partitioning(4, vec![]), order, long_type()),
    ]);
    let (plan, batches) = run_in_order(request).await;
    assert!(plan.is::<SortPreservingMergeExec>());
    assert_eq!(longs(&batches, 0), vec![Some(1), Some(0)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn ordered_stage_with_one_partition_needs_no_merge() {
    let order = first_column_descending(long_type());
    let request = query(vec![
        range_stage(100, 1),
        sorting_stage(0, range_partitioning(1, vec![]), order, long_type()),
    ]);
    let (plan, batches) = run_in_order(request).await;
    assert!(!plan.is::<SortPreservingMergeExec>());
    assert_eq!(plan.output_partitioning().partition_count(), 1);
    assert_eq!(
        longs(&batches, 0),
        (0..100).rev().map(Some).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn hash_exchange_partition_counts_are_bounded() {
    for count in [i32::MIN, -1, 0, MAX_PARTITIONS as i32 + 1, i32::MAX] {
        let request = query(vec![
            range_stage(100, 4),
            reading_stage(0, hash_on_first_column(count), 3),
        ]);
        let error = assemble(request).await.unwrap_err().to_string();
        assert!(
            error.contains(&format!(
                "between 1 and {MAX_PARTITIONS} partitions, not {count}"
            )),
            "{error}"
        );
    }
    let request = query(vec![
        range_stage(100, 4),
        reading_stage(0, hash_on_first_column(MAX_PARTITIONS as i32), 3),
    ]);
    let plan = assemble(request).await.unwrap();
    assert_eq!(plan.output_partitioning().partition_count(), MAX_PARTITIONS);
}

#[tokio::test(flavor = "multi_thread")]
async fn stage_partition_counts_are_bounded() {
    for count in [MAX_PARTITIONS as u32 + 1, 1 << 31, u32::MAX] {
        let mut stage = range_stage(100, 4);
        stage.num_partitions = count;
        let error = assemble(query(vec![stage])).await.unwrap_err().to_string();
        assert!(
            error.contains(&format!("more than the {MAX_PARTITIONS} allowed")),
            "{error}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_size_is_bounded() {
    for batch_size in ["0", "2000000", "18446744073709551615"] {
        let mut request = query(vec![range_stage(100, 4)]);
        request
            .conf
            .insert("spark.comet.batchSize".to_string(), batch_size.to_string());
        let error = assemble(request).await.unwrap_err().to_string();
        assert!(
            error.contains("spark.comet.batchSize must be between 1 and"),
            "{error}"
        );
    }
    let mut request = query(vec![range_stage(100, 4)]);
    request
        .conf
        .insert("spark.comet.batchSize".to_string(), "4096".to_string());
    assemble(request).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn block_must_read_as_many_inputs_as_the_stage_declares() {
    let leaf_less_input = StageInput {
        producer: 0,
        partitioning: Some(single_partition()),
        broadcast: false,
    };
    // A block with no input leaves, and an input.
    let mut stage = range_stage(100, 4);
    stage.inputs = vec![leaf_less_input.clone()];
    let error = assemble(query(vec![range_stage(100, 4), stage]))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("reads 0 inputs but the stage declares 1"),
        "{error}"
    );
    // A block with one input leaf, and two inputs.
    let mut stage = reading_stage(0, single_partition(), 1);
    stage.inputs.push(leaf_less_input);
    let error = assemble(query(vec![range_stage(100, 4), stage]))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("reads 1 inputs but the stage declares 2"),
        "{error}"
    );
    // A join's two input leaves, and one input.
    let mut stage = joining_stage(0, 1, 3);
    stage.inputs.pop();
    let error = assemble(query(vec![range_stage(100, 4), range_stage(100, 2), stage]))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("reads 2 inputs but the stage declares 1"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn every_partition_plan_must_read_as_many_inputs_as_the_stage_declares() {
    let range = range_stage(100, 2);
    let block = range.plan.as_ref().unwrap().encode_to_vec();
    let with_leaf = Operator {
        op_struct: Some(OpStruct::Scan(Scan {
            fields: vec![long_type()],
            source: "test".to_string(),
        })),
        ..Default::default()
    }
    .encode_to_vec();
    let mut stage = range;
    stage.partition_plans = vec![block, with_leaf];
    let error = assemble(query(vec![stage])).await.unwrap_err();
    assert!(
        error.to_string().contains("Plan of partition 1")
            && error
                .to_string()
                .contains("reads 1 inputs but the stage declares 0"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn writer_operators_are_refused() {
    let writers = [
        (
            "ParquetWriter",
            OpStruct::ParquetWriter(ParquetWriter::default()),
        ),
        (
            "IcebergWrite",
            OpStruct::IcebergWrite(IcebergWrite::default()),
        ),
        (
            "ShuffleWriter",
            OpStruct::ShuffleWriter(ShuffleWriter::default()),
        ),
    ];
    for (name, writer) in writers {
        let writing = |child: Operator| Operator {
            op_struct: Some(writer.clone()),
            children: vec![child],
            ..Default::default()
        };
        let expected = format!("can't contain a {name} operator");
        let range = range_stage(100, 1);
        let block = range.plan.clone().unwrap();

        // At the root of the block.
        let mut stage = range.clone();
        stage.plan = Some(writing(block.clone()));
        let error = assemble(query(vec![stage])).await.unwrap_err();
        assert!(error.to_string().contains(&expected), "{error}");

        // Below other operators.
        let mut stage = range.clone();
        stage.plan = Some(Operator {
            op_struct: Some(OpStruct::Projection(Projection::default())),
            children: vec![writing(block.clone())],
            ..Default::default()
        });
        let error = assemble(query(vec![stage])).await.unwrap_err();
        assert!(error.to_string().contains(&expected), "{error}");

        // In a partition's own copy of the block.
        let mut stage = range;
        stage.partition_plans = vec![writing(block).encode_to_vec()];
        let error = assemble(query(vec![stage])).await.unwrap_err();
        assert!(error.to_string().contains(&expected), "{error}");
    }
}
