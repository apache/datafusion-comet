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

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{exec_err, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    apply_expression_roots, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

use crate::execution::python_udf::ArrowPythonUdf;

#[derive(Debug, Clone)]
pub struct ArrowPythonUdfSpec {
    pub command: Vec<u8>,
    pub args: Vec<Arc<dyn PhysicalExpr>>,
    pub arg_names: Vec<String>,
    pub return_type: DataType,
    pub return_name: String,
    pub python_version: String,
}

/// Evaluates scalar PyArrow UDFs inside the native pipeline. Workers are
/// instantiated in `execute`, once per partition, so Python function state
/// never leaks between Spark tasks.
#[derive(Debug)]
pub struct ArrowPythonUdfExec {
    child: Arc<dyn ExecutionPlan>,
    specs: Vec<ArrowPythonUdfSpec>,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl ArrowPythonUdfExec {
    pub fn try_new(child: Arc<dyn ExecutionPlan>, specs: Vec<ArrowPythonUdfSpec>) -> Result<Self> {
        if specs.is_empty() {
            return exec_err!("ArrowPythonUdfExec requires at least one UDF");
        }
        let mut fields: Vec<Field> = child
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        for spec in &specs {
            if spec.args.len() != spec.arg_names.len() {
                return exec_err!("ArrowPythonUdf argument names are not aligned with arguments");
            }
            for arg in &spec.args {
                arg.data_type(&child.schema())?;
            }
            fields.push(Field::new(
                &spec.return_name,
                spec.return_type.clone(),
                true,
            ));
        }
        let schema = Arc::new(Schema::new(fields));
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            child.output_partitioning().clone(),
            EmissionType::Incremental,
            child.boundedness(),
        ));
        Ok(Self {
            child,
            specs,
            schema,
            cache,
        })
    }

    fn evaluate_batch(
        specs: &[ArrowPythonUdfSpec],
        workers: &[ArrowPythonUdf],
        schema: SchemaRef,
        batch: RecordBatch,
    ) -> Result<RecordBatch> {
        let mut columns = batch.columns().to_vec();
        for (spec, worker) in specs.iter().zip(workers) {
            let args: Vec<ArrayRef> = spec
                .args
                .iter()
                .map(|arg| arg.evaluate(&batch)?.into_array(batch.num_rows()))
                .collect::<Result<_>>()?;
            columns.push(worker.evaluate_named(&args, &spec.arg_names, batch.num_rows())?);
        }
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

impl DisplayAs for ArrowPythonUdfExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "CometArrowPythonUdfExec: {} UDF(s)", self.specs.len())
            }
        }
    }
}

impl ExecutionPlan for ArrowPythonUdfExec {
    fn name(&self) -> &str {
        "CometArrowPythonUdfExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(self.specs.iter().flat_map(|spec| spec.args.iter()), f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return exec_err!("ArrowPythonUdfExec requires exactly one child");
        }
        Ok(Arc::new(Self::try_new(
            Arc::clone(&children[0]),
            self.specs.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.child.execute(partition, context)?;
        let workers: Vec<_> = self
            .specs
            .iter()
            .map(|spec| {
                ArrowPythonUdf::from_command(
                    &spec.command,
                    spec.return_type.clone(),
                    true,
                    true,
                    &spec.python_version,
                )
            })
            .collect::<std::result::Result<_, _>>()?;
        let specs = self.specs.clone();
        let schema = Arc::clone(&self.schema);
        let stream = input
            .map(move |batch| Self::evaluate_batch(&specs, &workers, Arc::clone(&schema), batch?));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}
