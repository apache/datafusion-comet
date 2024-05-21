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
use arrow_array::{make_array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    errors::CometError,
    execution::{
        datafusion::planner::TEST_EXEC_CONTEXT_ID, operators::ExecutionError,
        utils::SparkArrowConvert,
    },
    jvm_bridge::{jni_call, JVMClasses},
};
use datafusion::{
    execution::TaskContext,
    physical_expr::*,
    physical_plan::{ExecutionPlan, *},
};
use datafusion_common::{arrow_datafusion_err, DataFusionError, Result as DataFusionResult};
use jni::{
    objects::{GlobalRef, JLongArray, JObject, ReleaseMode},
    sys::jlongArray,
};

#[derive(Debug, Clone)]
pub struct ScanExec {
    /// The ID of the execution context that owns this subquery. We use this ID to retrieve the JVM
    /// environment `JNIEnv` from the execution context.
    pub exec_context_id: i64,
    /// The input source of scan node. It is a global reference of JVM `CometBatchIterator` object.
    pub input_source: Option<Arc<GlobalRef>>,
    /// The data types of columns of the input batch. Converted from Spark schema.
    pub data_types: Vec<DataType>,
    /// The input batch of input data. Used to determine the schema of the input data.
    /// It is also used in unit test to mock the input data from JVM.
    pub batch: Arc<Mutex<Option<InputBatch>>>,
    /// The name of the source of this scan node. It is used for debugging purpose.
    pub name: String,
    cache: PlanProperties,
}

impl ScanExec {
    pub fn new(
        exec_context_id: i64,
        name: String,
        input_source: Option<Arc<GlobalRef>>,
        data_types: Vec<DataType>,
    ) -> Result<Self, CometError> {
        // Scan's schema is determined by the input batch, so we need to set it before execution.
        let first_batch = if let Some(input_source) = input_source.as_ref() {
            ScanExec::get_next(exec_context_id, input_source.as_obj())?
        } else {
            InputBatch::EOF
        };

        let schema = scan_schema(&first_batch, &data_types);

        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Ok(Self {
            exec_context_id,
            name,
            input_source,
            data_types,
            batch: Arc::new(Mutex::new(Some(first_batch))),
            cache,
        })
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

    /// Feeds input batch into this `Scan`. Only used in unit test.
    pub fn set_input_batch(&mut self, input: InputBatch) {
        *self.batch.try_lock().unwrap() = Some(input);
    }

    /// Pull next input batch from JVM.
    pub fn get_next_batch(&mut self) -> Result<(), CometError> {
        let mut current_batch = self.batch.try_lock().unwrap();

        if self.input_source.is_none() {
            // This is a unit test. We don't need to call JNI.
            return Ok(());
        }

        if current_batch.is_none() {
            let next_batch = ScanExec::get_next(
                self.exec_context_id,
                self.input_source.as_ref().unwrap().as_obj(),
            )?;
            *current_batch = Some(next_batch);
        }

        Ok(())
    }

    /// Invokes JNI call to get next batch.
    fn get_next(exec_context_id: i64, iter: &JObject) -> Result<InputBatch, CometError> {
        if exec_context_id == TEST_EXEC_CONTEXT_ID {
            // This is a unit test. We don't need to call JNI.
            return Ok(InputBatch::EOF);
        }

        let mut env = JVMClasses::get_env();

        if iter.is_null() {
            return Err(CometError::from(ExecutionError::GeneralError(format!(
                "Null batch iterator object. Plan id: {}",
                exec_context_id
            ))));
        }

        let batch_object: JObject = unsafe {
            jni_call!(&mut env,
            comet_batch_iterator(iter).next() -> JObject)?
        };

        if batch_object.is_null() {
            return Err(CometError::from(ExecutionError::GeneralError(format!(
                "Null batch object. Plan id: {}",
                exec_context_id
            ))));
        }

        let batch_object = unsafe { JLongArray::from_raw(batch_object.as_raw() as jlongArray) };

        let addresses = unsafe { env.get_array_elements(&batch_object, ReleaseMode::NoCopyBack)? };

        let mut inputs: Vec<ArrayRef> = vec![];

        // First element is the number of rows.
        let num_rows = unsafe { *addresses.as_ptr() as i64 };

        if num_rows < 0 {
            return Ok(InputBatch::EOF);
        }

        let array_num = addresses.len() - 1;
        if array_num % 2 != 0 {
            return Err(CometError::Internal(format!(
                "Invalid number of Arrow Array addresses: {}",
                array_num
            )));
        }

        let num_arrays = array_num / 2;
        let array_elements = unsafe { addresses.as_ptr().add(1) };

        let mut i: usize = 0;
        while i < num_arrays {
            let array_ptr = unsafe { *(array_elements.add(i * 2)) };
            let schema_ptr = unsafe { *(array_elements.add(i * 2 + 1)) };
            let array_data = ArrayData::from_spark((array_ptr, schema_ptr))?;

            // TODO: validate array input data

            inputs.push(make_array(array_data));
            i += 1;
        }

        Ok(InputBatch::new(inputs, Some(num_rows as usize)))
    }
}

fn scan_schema(input_batch: &InputBatch, data_types: &[DataType]) -> SchemaRef {
    let fields = match input_batch {
        // Note that if `columns` is empty, we'll get an empty schema
        InputBatch::Batch(columns, _) => {
            columns
                .iter()
                .enumerate()
                .map(|(idx, c)| {
                    let datatype = ScanExec::unpack_dictionary_type(c.data_type());
                    // We don't use the field name. Put a placeholder.
                    if matches!(datatype, DataType::Dictionary(_, _)) {
                        Field::new_dict(format!("col_{}", idx), datatype, true, idx as i64, false)
                    } else {
                        Field::new(format!("col_{}", idx), datatype, true)
                    }
                })
                .collect::<Vec<Field>>()
        }
        _ => data_types
            .iter()
            .enumerate()
            .map(|(idx, dt)| Field::new(format!("col_{}", idx), dt.clone(), true))
            .collect(),
    };

    Arc::new(Schema::new(fields))
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
        scan_schema(input_batch, &self.data_types)
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

    fn properties(&self) -> &PlanProperties {
        &self.cache
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
            .map_err(|e| arrow_datafusion_err!(e))
    }
}

impl Stream for ScanStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut scan_batch = self.scan.batch.try_lock().unwrap();
        let input_batch = &*scan_batch;

        let input_batch = if let Some(batch) = input_batch {
            batch
        } else {
            return Poll::Pending;
        };

        let result = match input_batch {
            InputBatch::EOF => Poll::Ready(None),
            InputBatch::Batch(columns, num_rows) => {
                Poll::Ready(Some(self.build_record_batch(columns, *num_rows)))
            }
        };

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
