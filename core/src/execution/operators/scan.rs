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

use std::{
    any::Any,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::Stream;
use itertools::Itertools;

use arrow::compute::{cast_with_options, CastOptions};
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use datafusion::{
    execution::TaskContext,
    physical_expr::*,
    physical_plan::{ExecutionPlan, *},
};
use datafusion_common::{DataFusionError, Result as DataFusionResult};

#[derive(Debug, Clone)]
pub struct ScanExec {
    pub batch: Arc<Mutex<Option<InputBatch>>>,
    pub data_types: Vec<DataType>,
}

impl ScanExec {
    pub fn new(batch: InputBatch, data_types: Vec<DataType>) -> Self {
        Self {
            batch: Arc::new(Mutex::new(Some(batch))),
            data_types,
        }
    }

    /// Feeds input batch into this `Scan`.
    pub fn set_input_batch(&mut self, input: InputBatch) {
        *self.batch.try_lock().unwrap() = Some(input);
    }

    /// Checks if the input data type `dt` is a dictionary type with primitive value type.
    /// If so, unpacks it and returns the primitive value type.
    ///
    /// Otherwise, this returns the original data type.
    ///
    /// This is necessary since DataFusion doesn't handle dictionary array with values
    /// being primitive type.
    ///
    /// TODO: revisit this once DF has imprved its dictionary type support. Ideally we shouldn't
    ///   do this in Comet but rather let DF to handle it for us.
    fn unpack_dictionary_type(dt: &DataType) -> DataType {
        if let DataType::Dictionary(_, vt) = dt {
            if !matches!(vt.as_ref(), DataType::Utf8 | DataType::Binary) {
                return vt.as_ref().clone();
            }
        }

        dt.clone()
    }
}

impl ExecutionPlan for ScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // `unwrap` is safe because `schema` is only called during converting
        // Spark plan to DataFusion plan. At the moment, `batch` is not EOF.
        let binding = self.batch.try_lock().unwrap();
        let input_batch = binding.as_ref().unwrap();

        let fields = match input_batch {
            // Note that if `columns` is empty, we'll get an empty schema
            InputBatch::Batch(columns, _) => {
                columns
                    .iter()
                    .enumerate()
                    .map(|(idx, c)| {
                        let datatype = Self::unpack_dictionary_type(c.data_type());
                        // We don't use the field name. Put a placeholder.
                        if matches!(datatype, DataType::Dictionary(_, _)) {
                            Field::new_dict(
                                format!("col_{}", idx),
                                datatype,
                                true,
                                idx as i64,
                                false,
                            )
                        } else {
                            Field::new(format!("col_{}", idx), datatype, true)
                        }
                    })
                    .collect::<Vec<Field>>()
            }
            _ => self
                .data_types
                .iter()
                .enumerate()
                .map(|(idx, dt)| Field::new(format!("col_{}", idx), dt.clone(), true))
                .collect(),
        };

        Arc::new(Schema::new(fields))
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _: usize,
        _: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        Ok(Box::pin(ScanStream::new(self.clone(), self.schema())))
    }
}

impl DisplayAs for ScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ScanExec")?;
                let fields: Vec<String> = self
                    .data_types
                    .iter()
                    .enumerate()
                    .map(|(idx, dt)| format!("col_{idx:}: {dt:}"))
                    .collect();
                write!(f, ": schema=[{}]", fields.join(", "))?;
            }
        }
        Ok(())
    }
}

/// A async-stream feeds input batch from `Scan` into DataFusion physical plan.
struct ScanStream {
    /// The `Scan` node producing input batches
    scan: ScanExec,
    /// Schema representing the data
    schema: SchemaRef,
}

impl ScanStream {
    pub fn new(scan: ScanExec, schema: SchemaRef) -> Self {
        Self { scan, schema }
    }

    fn build_record_batch(
        &self,
        columns: &[ArrayRef],
        num_rows: usize,
    ) -> DataFusionResult<RecordBatch, DataFusionError> {
        let schema_fields = self.schema.fields();
        assert_eq!(columns.len(), schema_fields.len());

        // Cast if necessary
        let cast_options = CastOptions::default();
        let new_columns: Vec<ArrayRef> = columns
            .iter()
            .zip(schema_fields.iter())
            .map(|(column, f)| {
                if column.data_type() != f.data_type() {
                    cast_with_options(column, f.data_type(), &cast_options).unwrap()
                } else {
                    column.clone()
                }
            })
            .collect();

        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        RecordBatch::try_new_with_options(self.schema.clone(), new_columns, &options)
            .map_err(DataFusionError::ArrowError)
    }
}

impl Stream for ScanStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut scan_batch = self.scan.batch.try_lock().unwrap();
        let input_batch = &*scan_batch;
        let result = match input_batch {
            // Input batch is not ready.
            None => Poll::Pending,
            Some(batch) => match batch {
                InputBatch::EOF => Poll::Ready(None),
                InputBatch::Batch(columns, num_rows) => {
                    Poll::Ready(Some(self.build_record_batch(columns, *num_rows)))
                }
            },
        };

        // Reset the current input batch so it won't be processed again
        *scan_batch = None;
        result
    }
}

impl RecordBatchStream for ScanStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Clone, Debug)]
pub enum InputBatch {
    /// The end of input batches.
    EOF,

    /// A normal batch with columns and number of rows.
    /// It is possible to have zero-column batch with non-zero number of rows,
    /// i.e. reading empty schema from scan.
    Batch(Vec<ArrayRef>, usize),
}

impl InputBatch {
    /// Constructs a `InputBatch` from columns and optional number of rows.
    /// If `num_rows` is none, this function will calculate it from given
    /// columns.
    pub fn new(columns: Vec<ArrayRef>, num_rows: Option<usize>) -> Self {
        let num_rows = num_rows.unwrap_or_else(|| {
            let lengths = columns.iter().map(|a| a.len()).unique().collect::<Vec<_>>();
            assert!(lengths.len() <= 1, "Columns have different lengths.");

            if lengths.is_empty() {
                // All are scalar values
                1
            } else {
                lengths[0]
            }
        });

        InputBatch::Batch(columns, num_rows)
    }
}
