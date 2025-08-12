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

use crate::{
    errors::CometError,
    execution::{
        operators::ExecutionError, planner::TEST_EXEC_CONTEXT_ID, utils::SparkArrowConvert,
    },
    jvm_bridge::{jni_call, JVMClasses},
};
use arrow::array::{make_array, ArrayData, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ffi::FFI_ArrowArray;
use arrow::ffi::FFI_ArrowSchema;
use datafusion::common::{arrow_datafusion_err, DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::{
    execution::TaskContext,
    physical_expr::*,
    physical_plan::{ExecutionPlan, *},
};
use futures::Stream;
use itertools::Itertools;
use jni::objects::JValueGen;
use jni::objects::{GlobalRef, JObject};
use jni::sys::jsize;
use std::rc::Rc;
use std::{
    any::Any,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

/// ScanExec reads batches of data from Spark via JNI. The source of the scan could be a file
/// scan or the result of reading a broadcast or shuffle exchange. ScanExec isn't invoked
/// until the data is already available in the JVM. When CometExecIterator invokes
/// Native.executePlan, it passes in the memory addresses of the input batches.
#[derive(Debug, Clone)]
pub struct ScanExec {
    /// The ID of the execution context that owns this subquery. We use this ID to retrieve the JVM
    /// environment `JNIEnv` from the execution context.
    pub exec_context_id: i64,
    /// The input source of scan node. It is a global reference of JVM `CometBatchIterator` object.
    pub input_source: Option<Arc<GlobalRef>>,
    /// A description of the input source for informational purposes
    pub input_source_description: String,
    /// Whether this is a native_comet scan that reuses buffers
    pub has_buffer_reuse: bool,
    /// The data types of columns of the input batch. Converted from Spark schema.
    pub data_types: Vec<DataType>,
    /// Schema of first batch
    pub schema: SchemaRef,
    /// The input batch of input data. Used to determine the schema of the input data.
    /// It is also used in unit test to mock the input data from JVM.
    pub batch: Arc<Mutex<Option<InputBatch>>>,
    /// Cache of expensive-to-compute plan properties
    cache: PlanProperties,
    /// Metrics collector
    metrics: ExecutionPlanMetricsSet,
    /// Baseline metrics
    baseline_metrics: BaselineMetrics,
    /// Time waiting for JVM input plan to execute and return batches
    jvm_fetch_time: Time,
    /// Time spent in FFI
    arrow_ffi_time: Time,
}

impl ScanExec {
    pub fn new(
        exec_context_id: i64,
        input_source: Option<Arc<GlobalRef>>,
        input_source_description: &str,
        data_types: Vec<DataType>,
        has_buffer_reuse: bool,
    ) -> Result<Self, CometError> {
        let metrics_set = ExecutionPlanMetricsSet::default();
        let baseline_metrics = BaselineMetrics::new(&metrics_set, 0);
        let arrow_ffi_time = MetricBuilder::new(&metrics_set).subset_time("arrow_ffi_time", 0);
        let jvm_fetch_time = MetricBuilder::new(&metrics_set).subset_time("jvm_fetch_time", 0);

        // Scan's schema is determined by the input batch, so we need to set it before execution.
        // Note that we determine if arrays are dictionary-encoded based on the
        // first batch. The array may be dictionary-encoded in some batches and not others, and
        // ScanExec will cast arrays from all future batches to the type determined here, so we
        // may end up either unpacking dictionary arrays or dictionary-encoding arrays.
        // Dictionary-encoded primitive arrays are always unpacked.
        let first_batch = if let Some(input_source) = input_source.as_ref() {
            let mut timer = baseline_metrics.elapsed_compute().timer();
            let batch = ScanExec::get_next(
                exec_context_id,
                input_source.as_obj(),
                data_types.len(),
                &jvm_fetch_time,
                &arrow_ffi_time,
            )?;
            timer.stop();
            batch
        } else {
            InputBatch::EOF
        };

        let schema = scan_schema(&first_batch, &data_types);

        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            // The partitioning is not important because we are not using DataFusion's
            // query planner or optimizer
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Ok(Self {
            exec_context_id,
            input_source,
            input_source_description: input_source_description.to_string(),
            has_buffer_reuse,
            data_types,
            batch: Arc::new(Mutex::new(Some(first_batch))),
            cache,
            metrics: metrics_set,
            baseline_metrics,
            jvm_fetch_time,
            arrow_ffi_time,
            schema,
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
    /// TODO: revisit this once DF has improved its dictionary type support. Ideally we shouldn't
    ///   do this in Comet but rather let DF to handle it for us.
    fn unpack_dictionary_type(dt: &DataType) -> DataType {
        if let DataType::Dictionary(_, vt) = dt {
            if !matches!(
                vt.as_ref(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
            ) {
                // return the underlying data type
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
        if self.input_source.is_none() {
            // This is a unit test. We don't need to call JNI.
            return Ok(());
        }
        let mut timer = self.baseline_metrics.elapsed_compute().timer();

        let mut current_batch = self.batch.try_lock().unwrap();
        if current_batch.is_none() {
            let next_batch = ScanExec::get_next(
                self.exec_context_id,
                self.input_source.as_ref().unwrap().as_obj(),
                self.data_types.len(),
                &self.jvm_fetch_time,
                &self.arrow_ffi_time,
            )?;
            *current_batch = Some(next_batch);
        }

        timer.stop();

        Ok(())
    }

    /// Invokes JNI call to get next batch.
    fn get_next(
        exec_context_id: i64,
        iter: &JObject,
        num_cols: usize,
        jvm_fetch_time: &Time,
        arrow_ffi_time: &Time,
    ) -> Result<InputBatch, CometError> {
        if exec_context_id == TEST_EXEC_CONTEXT_ID {
            // This is a unit test. We don't need to call JNI.
            return Ok(InputBatch::EOF);
        }

        if iter.is_null() {
            return Err(CometError::from(ExecutionError::GeneralError(format!(
                "Null batch iterator object. Plan id: {exec_context_id}"
            ))));
        }

        let mut env = JVMClasses::get_env()?;

        let mut timer = jvm_fetch_time.timer();

        let num_rows: i32 = unsafe {
            jni_call!(&mut env,
        comet_batch_iterator(iter).has_next() -> i32)?
        };

        timer.stop();

        if num_rows == -1 {
            return Ok(InputBatch::EOF);
        }

        let mut timer = arrow_ffi_time.timer();

        let mut array_addrs = Vec::with_capacity(num_cols);
        let mut schema_addrs = Vec::with_capacity(num_cols);

        for _ in 0..num_cols {
            let arrow_array = Rc::new(FFI_ArrowArray::empty());
            let arrow_schema = Rc::new(FFI_ArrowSchema::empty());
            let (array_ptr, schema_ptr) = (
                Rc::into_raw(arrow_array) as i64,
                Rc::into_raw(arrow_schema) as i64,
            );

            array_addrs.push(array_ptr);
            schema_addrs.push(schema_ptr);
        }

        // Prepare the java array parameters
        let long_array_addrs = env.new_long_array(num_cols as jsize)?;
        let long_schema_addrs = env.new_long_array(num_cols as jsize)?;

        env.set_long_array_region(&long_array_addrs, 0, &array_addrs)?;
        env.set_long_array_region(&long_schema_addrs, 0, &schema_addrs)?;

        let array_obj = JObject::from(long_array_addrs);
        let schema_obj = JObject::from(long_schema_addrs);

        let array_obj = JValueGen::Object(array_obj.as_ref());
        let schema_obj = JValueGen::Object(schema_obj.as_ref());

        let num_rows: i32 = unsafe {
            jni_call!(&mut env,
        comet_batch_iterator(iter).next(array_obj, schema_obj) -> i32)?
        };

        // we already checked for end of results on call to has_next() so should always
        // have a valid row count when calling next()
        assert!(num_rows != -1);

        let mut inputs: Vec<ArrayRef> = Vec::with_capacity(num_cols);

        for i in 0..num_cols {
            let array_ptr = array_addrs[i];
            let schema_ptr = schema_addrs[i];
            let array_data = ArrayData::from_spark((array_ptr, schema_ptr))?;

            // TODO: validate array input data

            inputs.push(make_array(array_data));

            // Drop the Arcs to avoid memory leak
            unsafe {
                Rc::from_raw(array_ptr as *const FFI_ArrowArray);
                Rc::from_raw(schema_ptr as *const FFI_ArrowSchema);
            }
        }

        timer.stop();

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
                    Field::new(format!("col_{idx}"), datatype, true)
                })
                .collect::<Vec<Field>>()
        }
        _ => data_types
            .iter()
            .enumerate()
            .map(|(idx, dt)| Field::new(format!("col_{idx}"), dt.clone(), true))
            .collect(),
    };

    Arc::new(Schema::new(fields))
}

impl ExecutionPlan for ScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        if self.exec_context_id == TEST_EXEC_CONTEXT_ID {
            // `unwrap` is safe because `schema` is only called during converting
            // Spark plan to DataFusion plan. At the moment, `batch` is not EOF.
            let binding = self.batch.try_lock().unwrap();
            let input_batch = binding.as_ref().unwrap();
            scan_schema(input_batch, &self.data_types)
        } else {
            Arc::clone(&self.schema)
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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
        partition: usize,
        _: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        Ok(Box::pin(ScanStream::new(
            self.clone(),
            self.schema(),
            partition,
            self.baseline_metrics.clone(),
        )))
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn name(&self) -> &str {
        "ScanExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for ScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ScanExec: source=[{}], ", self.input_source_description)?;
                let fields: Vec<String> = self
                    .data_types
                    .iter()
                    .enumerate()
                    .map(|(idx, dt)| format!("col_{idx:}: {dt:}"))
                    .collect();
                write!(f, "schema=[{}]", fields.join(", "))?;
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
        Ok(())
    }
}

/// A async-stream feeds input batch from `Scan` into DataFusion physical plan.
struct ScanStream<'a> {
    /// The `Scan` node producing input batches
    scan: ScanExec,
    /// Schema representing the data
    schema: SchemaRef,
    /// Metrics
    baseline_metrics: BaselineMetrics,
    /// Cast options
    cast_options: CastOptions<'a>,
    /// elapsed time for casting columns to different data types during scan
    cast_time: Time,
}

impl ScanStream<'_> {
    pub fn new(
        scan: ScanExec,
        schema: SchemaRef,
        partition: usize,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let cast_time = MetricBuilder::new(&scan.metrics).subset_time("cast_time", partition);
        Self {
            scan,
            schema,
            baseline_metrics,
            cast_options: CastOptions::default(),
            cast_time,
        }
    }

    fn build_record_batch(
        &self,
        columns: &[ArrayRef],
        num_rows: usize,
    ) -> DataFusionResult<RecordBatch, DataFusionError> {
        let schema_fields = self.schema.fields();
        assert_eq!(columns.len(), schema_fields.len());

        // Cast dictionary-encoded primitive arrays to regular arrays and cast
        // Utf8/LargeUtf8/Binary arrays to dictionary-encoded if the schema is
        // defined as dictionary-encoded and the data in this batch is not
        // dictionary-encoded (could also be the other way around)
        let new_columns: Vec<ArrayRef> = columns
            .iter()
            .zip(schema_fields.iter())
            .map(|(column, f)| {
                if column.data_type() != f.data_type() {
                    let mut timer = self.cast_time.timer();
                    let cast_array = cast_with_options(column, f.data_type(), &self.cast_options);
                    timer.stop();
                    cast_array
                } else {
                    Ok(Arc::clone(column))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        RecordBatch::try_new_with_options(Arc::clone(&self.schema), new_columns, &options)
            .map_err(|e| arrow_datafusion_err!(e))
    }
}

impl Stream for ScanStream<'_> {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut timer = self.baseline_metrics.elapsed_compute().timer();
        let mut scan_batch = self.scan.batch.try_lock().unwrap();

        let input_batch = &*scan_batch;
        let input_batch = if let Some(batch) = input_batch {
            batch
        } else {
            timer.stop();
            return Poll::Pending;
        };

        let result = match input_batch {
            InputBatch::EOF => Poll::Ready(None),
            InputBatch::Batch(columns, num_rows) => {
                self.baseline_metrics.record_output(*num_rows);
                let maybe_batch = self.build_record_batch(columns, *num_rows);
                Poll::Ready(Some(maybe_batch))
            }
        };

        *scan_batch = None;

        timer.stop();

        result
    }
}

impl RecordBatchStream for ScanStream<'_> {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[derive(Clone, Debug)]
pub enum InputBatch {
    /// The end of input batches.
    EOF,

    /// A normal batch with columns and a number of rows.
    /// It is possible to have a zero-column batch with a non-zero number of rows,
    /// i.e. reading empty schema from scan.
    Batch(Vec<ArrayRef>, usize),
}

impl InputBatch {
    /// Constructs an ` InputBatch ` from columns and an optional number of rows.
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
