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
// Proves `CometFragmentExec` runs a Comet plan fragment whose input-leaf `Scan`
// is fed by the node's DataFusion child stream (the R2 shuffle-reader shape,
// stood in for here by an in-memory child and a `CometScanExec` child), and
// that such a fragment survives Ballista's physical-plan (de)serialization.

#![cfg(feature = "ballista")]

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use datafusion::common::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    displayable, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message;

use comet::execution::ballista::{CometFragmentExec, CometPhysicalCodec, CometScanExec};
use datafusion_comet_proto::spark_expression::{
    data_type::DataTypeId, expr::ExprStruct, literal, BinaryExpr, BoundReference, DataType, Expr,
    Literal,
};
use datafusion_comet_proto::spark_operator::{
    operator::OpStruct, Filter, NativeScan, NativeScanCommon, Operator, Scan, SparkFilePartition,
    SparkPartitionedFile, SparkStructField,
};

/// A minimal in-memory DataFusion leaf yielding a fixed set of batches, standing
/// in for a shuffle reader (or any upstream DataFusion child) that feeds a
/// `CometFragmentExec`'s `Scan` input leaf.
#[derive(Debug)]
struct InMemoryChildExec {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl InMemoryChildExec {
    fn new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            batches,
            schema,
            props,
        }
    }
}

impl DisplayAs for InMemoryChildExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "InMemoryChildExec")
    }
}

impl ExecutionPlan for InMemoryChildExec {
    fn name(&self) -> &str {
        "InMemoryChildExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            self.batches.clone(),
            Arc::clone(&self.schema),
            None,
        )?))
    }
}

fn int32_type() -> DataType {
    DataType {
        type_id: DataTypeId::Int32 as i32,
        type_info: None,
    }
}

/// Build a Comet `Operator` proto: `Filter(gt(col0, 2))` over a `Scan` leaf with
/// one Int32 column. The `Scan` (op #100) is the input leaf fed by a child
/// stream; the `Filter` proves an operator is applied on top of the child rows.
fn build_filter_over_scan_proto() -> Vec<u8> {
    let scan = Scan {
        fields: vec![int32_type()],
        source: "fragment-child-test".to_string(),
    };
    let scan_op = Operator {
        children: vec![],
        plan_id: 1,
        op_struct: Some(OpStruct::Scan(scan)),
    };

    let col0 = Expr {
        expr_struct: Some(ExprStruct::Bound(BoundReference {
            index: 0,
            datatype: Some(int32_type()),
        })),
        ..Default::default()
    };
    let lit2 = Expr {
        expr_struct: Some(ExprStruct::Literal(Literal {
            value: Some(literal::Value::IntVal(2)),
            datatype: Some(int32_type()),
            is_null: false,
        })),
        ..Default::default()
    };
    let predicate = Expr {
        expr_struct: Some(ExprStruct::Gt(Box::new(BinaryExpr {
            left: Some(Box::new(col0)),
            right: Some(Box::new(lit2)),
        }))),
        ..Default::default()
    };
    let filter_op = Operator {
        children: vec![scan_op],
        plan_id: 2,
        op_struct: Some(OpStruct::Filter(Filter {
            predicate: Some(predicate),
        })),
    };
    filter_op.encode_to_vec()
}

fn int32_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "a",
        ArrowDataType::Int32,
        true,
    )]))
}

/// A `CometFragmentExec` whose `Scan` leaf is fed by an in-memory DataFusion
/// child must pass the child rows through the fragment and apply the fragment's
/// `Filter` (col0 > 2) to them.
#[tokio::test(flavor = "multi_thread")]
async fn fragment_scan_leaf_fed_by_child() -> anyhow::Result<()> {
    let schema = int32_schema();
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as _],
    )?;
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![4, 5])) as _],
    )?;
    let child: Arc<dyn ExecutionPlan> =
        Arc::new(InMemoryChildExec::new(vec![batch1, batch2], schema));

    let proto = build_filter_over_scan_proto();
    let fragment: Arc<dyn ExecutionPlan> =
        Arc::new(CometFragmentExec::try_new(proto, vec![child])?);

    let ctx = SessionContext::new();
    let mut stream = fragment.execute(0, ctx.task_ctx())?;
    let mut values: Vec<i32> = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        values.extend(col.values().iter().copied());
    }

    // Child produced 1..=5; the fragment's Filter keeps col0 > 2.
    assert_eq!(
        values,
        vec![3, 4, 5],
        "child rows must flow through and be filtered"
    );
    Ok(())
}

/// Write a tiny Parquet file with a single int32 column `a` = [1..=5].
fn write_test_parquet(path: &std::path::Path) -> anyhow::Result<()> {
    let schema = int32_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
    )?;
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// Build a Comet `Operator` proto: a single `NativeScan` over `parquet_path`.
fn build_native_scan_proto(parquet_path: &std::path::Path) -> anyhow::Result<Vec<u8>> {
    let field_a = SparkStructField {
        name: "a".to_string(),
        data_type: Some(int32_type()),
        nullable: true,
        metadata: Default::default(),
    };
    let common = NativeScanCommon {
        required_schema: vec![field_a.clone()],
        data_schema: vec![field_a],
        projection_vector: vec![0],
        session_timezone: "UTC".to_string(),
        source: "comet-fragment-child-native-scan".to_string(),
        ..Default::default()
    };
    let file_size = std::fs::metadata(parquet_path)?.len() as i64;
    let partitioned_file = SparkPartitionedFile {
        file_path: format!("file://{}", parquet_path.display()),
        start: 0,
        length: file_size,
        file_size,
        partition_values: vec![],
    };
    let native_scan = NativeScan {
        common: Some(common),
        file_partition: Some(SparkFilePartition {
            partitioned_file: vec![partitioned_file],
        }),
    };
    Ok(Operator {
        children: vec![],
        plan_id: 0,
        op_struct: Some(OpStruct::NativeScan(native_scan)),
    }
    .encode_to_vec())
}

/// A `CometFragmentExec` (with a `CometScanExec` child, so the whole tree is
/// serializable) must survive Ballista's physical-plan codec round-trip and
/// produce the same filtered result on the far side.
#[tokio::test(flavor = "multi_thread")]
async fn fragment_codec_roundtrip() -> anyhow::Result<()> {
    let parquet_path = std::env::temp_dir().join("comet_fragment_child_codec_roundtrip.parquet");
    write_test_parquet(&parquet_path)?;

    // Child = CometScanExec over the parquet (round-trips via COMET_MAGIC);
    // parent fragment = Filter(col0 > 2) over a Scan input leaf.
    let child: Arc<dyn ExecutionPlan> = Arc::new(CometScanExec::try_new(build_native_scan_proto(
        &parquet_path,
    )?)?);
    let fragment_proto = build_filter_over_scan_proto();
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(CometFragmentExec::try_new(fragment_proto, vec![child])?);
    println!(
        "original plan:\n{}",
        displayable(plan.as_ref()).indent(false)
    );

    // --- Encode (scheduler side) ---
    let codec = CometPhysicalCodec::default();
    let node = PhysicalPlanNode::try_from_physical_plan(Arc::clone(&plan), &codec)?;
    let bytes = node.encode_to_vec();

    // --- Ship bytes, decode in a fresh context (executor side) ---
    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    let node2 = PhysicalPlanNode::decode(&bytes[..])?;
    let plan2 = node2.try_into_physical_plan(task_ctx.as_ref(), &codec)?;
    println!(
        "reconstructed plan (executor side):\n{}",
        displayable(plan2.as_ref()).indent(false)
    );

    // --- Execute the reconstructed plan ---
    let mut stream = plan2.execute(0, task_ctx)?;
    let mut values: Vec<i32> = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        values.extend(col.values().iter().copied());
    }

    assert_eq!(
        values,
        vec![3, 4, 5],
        "fragment result must be identical after codec round-trip"
    );
    Ok(())
}