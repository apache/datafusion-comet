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
        operators::ExecutionError, planner::TEST_EXEC_CONTEXT_ID, shuffle::ShuffleStreamReader,
    },
    jvm_bridge::JVMClasses,
};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{arrow_datafusion_err, Result as DataFusionResult};
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
use jni::objects::{Global, JObject};
use std::{
    any::Any,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use super::scan::InputBatch;

/// ShuffleScanExec reads Arrow IPC streams from JVM via JNI and decodes them natively.
/// Unlike ScanExec which receives Arrow arrays via FFI, ShuffleScanExec receives a raw
/// InputStream from JVM and reads Arrow IPC streams using ShuffleStreamReader.
pub struct ShuffleScanExec {
    /// The ID of the execution context that owns this subquery.
    pub exec_context_id: i64,
    /// The input source: a global reference to a JVM InputStream object.
    pub input_source: Option<Arc<Global<JObject<'static>>>>,
    /// The data types of columns in the shuffle output.
    pub data_types: Vec<DataType>,
    /// Schema of the shuffle output.
    pub schema: SchemaRef,
    /// The current input batch, populated by get_next_batch() before poll_next().
    pub batch: Arc<Mutex<Option<InputBatch>>>,
    /// Cached ShuffleStreamReader, created lazily on first get_next call.
    stream_reader: Option<ShuffleStreamReader>,
    /// Cache of plan properties.
    cache: Arc<PlanProperties>,
    /// Metrics collector.
    metrics: ExecutionPlanMetricsSet,
    /// Baseline metrics.
    baseline_metrics: BaselineMetrics,
    /// Time spent decoding shuffle batches.
    decode_time: Time,
}

impl std::fmt::Debug for ShuffleScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShuffleScanExec")
            .field("exec_context_id", &self.exec_context_id)
            .field("data_types", &self.data_types)
            .field("schema", &self.schema)
            .field("stream_reader", &self.stream_reader.is_some())
            .finish()
    }
}

impl Clone for ShuffleScanExec {
    fn clone(&self) -> Self {
        Self {
            exec_context_id: self.exec_context_id,
            input_source: self.input_source.clone(),
            data_types: self.data_types.clone(),
            schema: Arc::clone(&self.schema),
            batch: Arc::clone(&self.batch),
            // stream_reader is not cloneable; cloned instances start without one
            // and will lazily create their own if needed.
            stream_reader: None,
            cache: self.cache.clone(),
            metrics: self.metrics.clone(),
            baseline_metrics: self.baseline_metrics.clone(),
            decode_time: self.decode_time.clone(),
        }
    }
}

impl ShuffleScanExec {
    pub fn new(
        exec_context_id: i64,
        input_source: Option<Arc<Global<JObject<'static>>>>,
        data_types: Vec<DataType>,
    ) -> Result<Self, CometError> {
        let metrics_set = ExecutionPlanMetricsSet::default();
        let baseline_metrics = BaselineMetrics::new(&metrics_set, 0);
        let decode_time = MetricBuilder::new(&metrics_set).subset_time("decode_time", 0);

        let schema = schema_from_data_types(&data_types);

        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Ok(Self {
            exec_context_id,
            input_source,
            data_types,
            batch: Arc::new(Mutex::new(None)),
            stream_reader: None,
            cache,
            metrics: metrics_set,
            baseline_metrics,
            schema,
            decode_time,
        })
    }

    /// Feeds input batch into this scan. Only used in unit tests.
    pub fn set_input_batch(&mut self, input: InputBatch) {
        *self.batch.try_lock().unwrap() = Some(input);
    }

    /// Pull next input batch from JVM. Called externally before poll_next()
    /// because JNI calls cannot happen from within poll_next on tokio threads.
    pub fn get_next_batch(&mut self) -> Result<(), CometError> {
        if self.input_source.is_none() {
            // Unit test mode - no JNI calls needed.
            return Ok(());
        }

        // Check if a batch is already pending without holding the lock during get_next
        let needs_batch = {
            let current_batch = self.batch.try_lock().unwrap();
            current_batch.is_none()
        };

        if needs_batch {
            let start = std::time::Instant::now();
            let next_batch = self.get_next()?;
            self.baseline_metrics
                .elapsed_compute()
                .add_duration(start.elapsed());
            let mut current_batch = self.batch.try_lock().unwrap();
            *current_batch = Some(next_batch);
        }

        Ok(())
    }

    /// Reads the next batch from the ShuffleStreamReader, creating it lazily on first call.
    fn get_next(&mut self) -> Result<InputBatch, CometError> {
        if self.exec_context_id == TEST_EXEC_CONTEXT_ID {
            return Ok(InputBatch::EOF);
        }

        // Lazily create the ShuffleStreamReader on first call
        if self.stream_reader.is_none() {
            let input_source = self.input_source.as_ref().ok_or_else(|| {
                CometError::from(ExecutionError::GeneralError(format!(
                    "Null shuffle input source. Plan id: {}",
                    self.exec_context_id
                )))
            })?;
            let mut env = JVMClasses::get_env()?;
            let reader =
                ShuffleStreamReader::new(&mut env, input_source.as_obj()).map_err(|e| {
                    CometError::from(ExecutionError::GeneralError(format!(
                        "Failed to create ShuffleStreamReader: {e}"
                    )))
                })?;
            self.stream_reader = Some(reader);
        }

        let reader = self.stream_reader.as_mut().unwrap();

        let mut decode_timer = self.decode_time.timer();
        let batch_opt = reader.next_batch().map_err(|e| {
            CometError::from(ExecutionError::GeneralError(format!(
                "Failed to read shuffle batch: {e}"
            )))
        })?;
        decode_timer.stop();

        match batch_opt {
            None => Ok(InputBatch::EOF),
            Some(batch) => {
                let num_rows = batch.num_rows();

                // Extract column arrays, unpacking any dictionary-encoded columns.
                // Native shuffle may dictionary-encode string/binary columns for efficiency,
                // but downstream DataFusion operators expect the value types declared in the
                // schema (e.g. Utf8, not Dictionary<Int32, Utf8>).
                let columns: Vec<ArrayRef> = batch
                    .columns()
                    .iter()
                    .map(|col| unpack_dictionary(col))
                    .collect();

                debug_assert_eq!(
                    columns.len(),
                    self.data_types.len(),
                    "Shuffle block column count mismatch: got {} but expected {}",
                    columns.len(),
                    self.data_types.len()
                );

                Ok(InputBatch::new(columns, Some(num_rows)))
            }
        }
    }
}

/// If `array` is dictionary-encoded, cast it to the value type. Otherwise return as-is.
fn unpack_dictionary(array: &ArrayRef) -> ArrayRef {
    if let DataType::Dictionary(_, value_type) = array.data_type() {
        arrow::compute::cast(array, value_type.as_ref()).expect("failed to unpack dictionary array")
    } else {
        Arc::clone(array)
    }
}

fn schema_from_data_types(data_types: &[DataType]) -> SchemaRef {
    let fields = data_types
        .iter()
        .enumerate()
        .map(|(idx, dt)| Field::new(format!("col_{idx}"), dt.clone(), true))
        .collect::<Vec<Field>>();

    Arc::new(Schema::new(fields))
}

impl ExecutionPlan for ShuffleScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
        Ok(Box::pin(ShuffleScanStream::new(
            self.clone(),
            partition,
            self.baseline_metrics.clone(),
        )))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn name(&self) -> &str {
        "ShuffleScanExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for ShuffleScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let fields: Vec<String> = self
                    .data_types
                    .iter()
                    .enumerate()
                    .map(|(idx, dt)| format!("col_{idx}: {dt}"))
                    .collect();
                write!(f, "ShuffleScanExec: schema=[{}]", fields.join(", "))?;
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
        Ok(())
    }
}

/// An async stream that feeds decoded shuffle batches into the DataFusion plan.
struct ShuffleScanStream {
    /// The ShuffleScanExec producing input batches.
    shuffle_scan: ShuffleScanExec,
    /// Metrics.
    baseline_metrics: BaselineMetrics,
}

impl ShuffleScanStream {
    pub fn new(
        shuffle_scan: ShuffleScanExec,
        _partition: usize,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        Self {
            shuffle_scan,
            baseline_metrics,
        }
    }
}

impl Stream for ShuffleScanStream {
    type Item = DataFusionResult<arrow::array::RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut timer = self.baseline_metrics.elapsed_compute().timer();
        let mut scan_batch = self.shuffle_scan.batch.try_lock().unwrap();

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
                let options =
                    arrow::array::RecordBatchOptions::new().with_row_count(Some(*num_rows));
                let maybe_batch = arrow::array::RecordBatch::try_new_with_options(
                    self.shuffle_scan.schema(),
                    columns.clone(),
                    &options,
                )
                .map_err(|e| arrow_datafusion_err!(e));
                Poll::Ready(Some(maybe_batch))
            }
        };

        *scan_batch = None;

        timer.stop();

        result
    }
}

impl RecordBatchStream for ShuffleScanStream {
    fn schema(&self) -> SchemaRef {
        self.shuffle_scan.schema()
    }
}

#[cfg(test)]
mod tests {
    use crate::execution::shuffle::CompressionCodec;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use std::io::Cursor;
    use std::sync::Arc;

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call FFI functions (zstd)
    fn test_read_compressed_ipc_block() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        // Write as Arrow IPC stream with compression
        let write_options = CompressionCodec::Zstd(1).ipc_write_options().unwrap();
        let mut buf = Vec::new();
        let mut writer =
            StreamWriter::try_new_with_options(&mut buf, &batch.schema(), write_options).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        // Read back using standard StreamReader
        let cursor = Cursor::new(&buf);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let decoded = reader.next().unwrap().unwrap();
        assert_eq!(decoded.num_rows(), 3);
        assert_eq!(decoded.num_columns(), 2);

        // Verify data
        let col0 = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col0.value(0), 1);
        assert_eq!(col0.value(1), 2);
        assert_eq!(col0.value(2), 3);
    }

    /// Tests that ShuffleScanExec correctly unpacks dictionary-encoded columns.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_dictionary_encoded_shuffle_block_is_unpacked() {
        use super::*;
        use arrow::array::StringDictionaryBuilder;
        use arrow::datatypes::Int32Type;
        use datafusion::physical_plan::ExecutionPlan;
        use futures::StreamExt;

        let mut dict_builder = StringDictionaryBuilder::<Int32Type>::new();
        dict_builder.append_value("hello");
        dict_builder.append_value("world");
        dict_builder.append_value("hello");
        let dict_array = dict_builder.finish();

        let dict_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "name",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]));
        let dict_batch = RecordBatch::try_new(
            Arc::clone(&dict_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(dict_array),
            ],
        )
        .unwrap();

        // Write as Arrow IPC stream with compression
        let write_options = CompressionCodec::Zstd(1).ipc_write_options().unwrap();
        let mut buf = Vec::new();
        let mut writer =
            StreamWriter::try_new_with_options(&mut buf, &dict_batch.schema(), write_options)
                .unwrap();
        writer.write(&dict_batch).unwrap();
        writer.finish().unwrap();

        // Read back using standard StreamReader
        let cursor = Cursor::new(&buf);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let decoded = reader.next().unwrap().unwrap();
        assert!(
            matches!(decoded.column(1).data_type(), DataType::Dictionary(_, _)),
            "Expected dictionary-encoded column from IPC, got {:?}",
            decoded.column(1).data_type()
        );

        // Create ShuffleScanExec with value types (Utf8, not Dictionary) — this is
        // what the protobuf schema provides.
        let mut scan = ShuffleScanExec::new(
            super::super::super::planner::TEST_EXEC_CONTEXT_ID,
            None,
            vec![DataType::Int32, DataType::Utf8],
        )
        .unwrap();

        // Feed the decoded batch through unpack_dictionary (simulating get_next)
        let columns: Vec<ArrayRef> = decoded
            .columns()
            .iter()
            .map(|col| super::unpack_dictionary(col))
            .collect();
        let input = InputBatch::new(columns, Some(decoded.num_rows()));
        scan.set_input_batch(input);

        // Execute and verify the output RecordBatch has the expected schema
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Arc::new(TaskContext::default());
            let mut stream = scan.execute(0, ctx).unwrap();
            let result_batch = stream.next().await.unwrap().unwrap();

            // Schema should have Utf8, not Dictionary
            assert_eq!(
                *result_batch.schema().field(1).data_type(),
                DataType::Utf8,
                "Expected Utf8 after dictionary unpacking"
            );

            // Verify data integrity
            let col1 = result_batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Column should be StringArray after unpacking");
            assert_eq!(col1.value(0), "hello");
            assert_eq!(col1.value(1), "world");
            assert_eq!(col1.value(2), "hello");
        });
    }
}
