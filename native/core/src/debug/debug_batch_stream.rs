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

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

/// Wraps a `SendableRecordBatchStream` to print each batch as it flows through.
pub fn dbg_batch_stream(stream: SendableRecordBatchStream) -> SendableRecordBatchStream {
    use futures::StreamExt;
    let schema = stream.schema();
    let printing_stream = stream.map(|batch_result| {
        match &batch_result {
            Ok(batch) => {
                dbg!(batch, batch.schema());
                for (col_idx, column) in batch.columns().iter().enumerate() {
                    dbg!(col_idx, column, column.nulls());
                }
            }
            Err(e) => {
                println!("batch error: {:?}", e);
            }
        }
        batch_result
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, printing_stream))
}

/// `ExecutionPlan` wrapper that prints every batch produced by `inner`.
#[derive(Debug)]
pub struct DebugExecutionDataStream {
    label: String,
    inner: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
}

impl DebugExecutionDataStream {
    pub fn new(
        label: impl Into<String>,
        inner: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> Self {
        Self {
            label: label.into(),
            inner,
        }
    }
}

impl datafusion::physical_plan::DisplayAs for DebugExecutionDataStream {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "DebugExecutionDataStream[{}]", self.label)
    }
}

impl datafusion::physical_plan::ExecutionPlan for DebugExecutionDataStream {
    fn name(&self) -> &str {
        "DebugExecutionDataStream"
    }
    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        vec![&self.inner]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Ok(Arc::new(DebugExecutionDataStream::new(
            self.label.clone(),
            Arc::clone(&children[0]),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        eprintln!(
            "[comet-debug] DebugExecutionDataStream[{}] execute(partition={})",
            self.label, partition
        );
        let stream = self.inner.execute(partition, context)?;
        Ok(dbg_batch_stream(stream))
    }
}

/// `PhysicalExpr` wrapper that prints every `evaluate()` call: input
/// `RecordBatch` and the resulting `ColumnarValue`.
#[derive(Debug)]
pub struct DebugExecutionDataPhyExpr {
    label: String,
    inner: Arc<dyn PhysicalExpr>,
}

impl DebugExecutionDataPhyExpr {
    pub fn new(label: impl Into<String>, inner: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            label: label.into(),
            inner,
        }
    }
}

impl fmt::Display for DebugExecutionDataPhyExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DebugExecutionDataPhyExpr[{}]({})",
            self.label, self.inner
        )
    }
}

impl PartialEq for DebugExecutionDataPhyExpr {
    fn eq(&self, other: &Self) -> bool {
        self.label == other.label && self.inner.eq(&other.inner)
    }
}
impl Eq for DebugExecutionDataPhyExpr {}
impl Hash for DebugExecutionDataPhyExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label.hash(state);
        self.inner.hash(state);
    }
}

impl PhysicalExpr for DebugExecutionDataPhyExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.inner.data_type(input_schema)
    }
    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.inner.nullable(input_schema)
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        eprintln!(
            "[comet-debug] DebugExecutionDataPhyExpr[{}].evaluate(rows={}, cols={})",
            self.label,
            batch.num_rows(),
            batch.num_columns()
        );
        dbg!(batch, batch.schema());
        let out = self.inner.evaluate(batch)?;
        match &out {
            ColumnarValue::Array(arr) => {
                dbg!(arr.len(), arr.nulls(), arr);
            }
            ColumnarValue::Scalar(s) => {
                dbg!(s);
            }
        }
        Ok(out)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(DebugExecutionDataPhyExpr::new(
            self.label.clone(),
            Arc::clone(&children[0]),
        )))
    }
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_sql(f)
    }
}
