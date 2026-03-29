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
        operators::ExecutionError, planner::TEST_EXEC_CONTEXT_ID, shuffle::ipc::read_ipc_compressed,
    },
    jvm_bridge::{jni_call, JVMClasses},
};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
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
use jni::objects::{GlobalRef, JByteBuffer, JObject};
use std::{
    any::Any,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use super::scan::InputBatch;

/// A StreamReader over owned bytes, used for streaming IPC batches one at a time.
type IpcBatchReader = StreamReader<std::io::Cursor<Vec<u8>>>;

/// ShuffleScanExec reads compressed shuffle blocks from JVM via JNI and decodes them natively.
/// Unlike ScanExec which receives Arrow arrays via FFI, ShuffleScanExec receives raw compressed
/// bytes from CometShuffleBlockIterator and decodes them using read_ipc_compressed().
#[derive(Debug, Clone)]
pub struct ShuffleScanExec {
    /// The ID of the execution context that owns this subquery.
    pub exec_context_id: i64,
    /// The input source: a global reference to a JVM CometShuffleBlockIterator object.
    pub input_source: Option<Arc<GlobalRef>>,
    /// The data types of columns in the shuffle output.
    pub data_types: Vec<DataType>,
    /// Schema of the shuffle output.
    pub schema: SchemaRef,
    /// The current input batch, populated by get_next_batch() before poll_next().
    pub batch: Arc<Mutex<Option<InputBatch>>>,
    /// Active IPC stream reader for streaming batches one at a time.
    /// Wrapped in Arc<Mutex> so ShuffleScanExec can derive Clone.
    ipc_stream_reader: Arc<Mutex<Option<IpcBatchReader>>>,
    /// Cache of plan properties.
    cache: PlanProperties,
    /// Metrics collector.
    metrics: ExecutionPlanMetricsSet,
    /// Baseline metrics.
    baseline_metrics: BaselineMetrics,
    /// Time spent decoding compressed shuffle blocks.
    decode_time: Time,
}

impl ShuffleScanExec {
    pub fn new(
        exec_context_id: i64,
        input_source: Option<Arc<GlobalRef>>,
        data_types: Vec<DataType>,
    ) -> Result<Self, CometError> {
        let metrics_set = ExecutionPlanMetricsSet::default();
        let baseline_metrics = BaselineMetrics::new(&metrics_set, 0);
        let decode_time = MetricBuilder::new(&metrics_set).subset_time("decode_time", 0);

        let schema = schema_from_data_types(&data_types);

        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Ok(Self {
            exec_context_id,
            input_source,
            data_types,
            batch: Arc::new(Mutex::new(None)),
            ipc_stream_reader: Arc::new(Mutex::new(None)),
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
            return Ok(());
        }
        let mut timer = self.baseline_metrics.elapsed_compute().timer();

        let mut current_batch = self.batch.try_lock().unwrap();
        if current_batch.is_none() {
            // Try to read the next batch from an active IPC stream reader
            let batch_from_reader = {
                let mut reader_guard = self.ipc_stream_reader.try_lock().unwrap();
                if let Some(reader) = reader_guard.as_mut() {
                    match reader.next() {
                        Some(Ok(batch)) => Some(batch),
                        _ => {
                            // Stream exhausted or error — drop the reader
                            *reader_guard = None;
                            None
                        }
                    }
                } else {
                    None
                }
            };

            if let Some(batch) = batch_from_reader {
                *current_batch = Some(Self::batch_to_input(&batch, &self.data_types));
            } else {
                let next_batch = Self::get_next(
                    self.exec_context_id,
                    self.input_source.as_ref().unwrap().as_obj(),
                    &self.data_types,
                    &self.decode_time,
                    &self.ipc_stream_reader,
                )?;
                *current_batch = Some(next_batch);
            }
        }

        timer.stop();
        Ok(())
    }

    fn batch_to_input(batch: &arrow::array::RecordBatch, data_types: &[DataType]) -> InputBatch {
        let num_rows = batch.num_rows();
        let columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| unpack_dictionary(col))
            .collect();

        debug_assert_eq!(
            columns.len(),
            data_types.len(),
            "Shuffle block column count mismatch: got {} but expected {}",
            columns.len(),
            data_types.len()
        );

        InputBatch::new(columns, Some(num_rows))
    }

    /// Invokes JNI calls to get the next shuffle block and decode it.
    /// For IPC stream format, creates a `StreamReader` that yields batches
    /// one at a time (stored in `ipc_stream_reader` for subsequent calls).
    fn get_next(
        exec_context_id: i64,
        iter: &JObject,
        data_types: &[DataType],
        decode_time: &Time,
        ipc_stream_reader: &Arc<Mutex<Option<IpcBatchReader>>>,
    ) -> Result<InputBatch, CometError> {
        if exec_context_id == TEST_EXEC_CONTEXT_ID {
            return Ok(InputBatch::EOF);
        }

        if iter.is_null() {
            return Err(CometError::from(ExecutionError::GeneralError(format!(
                "Null shuffle block iterator object. Plan id: {exec_context_id}"
            ))));
        }

        let mut env = JVMClasses::get_env()?;

        let block_length: i32 = unsafe {
            jni_call!(&mut env,
                comet_shuffle_block_iterator(iter).has_next() -> i32)?
        };

        if block_length == -1 {
            return Ok(InputBatch::EOF);
        }

        let buffer: JObject = unsafe {
            jni_call!(&mut env,
                comet_shuffle_block_iterator(iter).get_buffer() -> JObject)?
        };

        let byte_buffer = JByteBuffer::from(buffer);
        let raw_pointer = env.get_direct_buffer_address(&byte_buffer)?;
        let length = block_length as usize;
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(raw_pointer, length) };

        // Detect format: block starts with codec prefix, IPC stream starts with 0xFFFFFFFF
        let mut timer = decode_time.timer();
        let is_ipc_stream = length >= 4 && slice[0..4] == [0xFF, 0xFF, 0xFF, 0xFF];

        let batch = if is_ipc_stream {
            // Copy bytes into owned memory and create a StreamReader that
            // yields batches one at a time (no bulk materialization).
            let owned = slice.to_vec();
            let cursor = std::io::Cursor::new(owned);
            let mut reader = unsafe {
                StreamReader::try_new(cursor, None)?.with_skip_validation(true)
            };
            let first = match reader.next() {
                Some(Ok(batch)) => batch,
                Some(Err(e)) => {
                    timer.stop();
                    return Err(e.into());
                }
                None => {
                    timer.stop();
                    return Ok(InputBatch::EOF);
                }
            };
            // Store the reader so subsequent calls can pull more batches
            // without another JNI round-trip.
            *ipc_stream_reader.try_lock().unwrap() = Some(reader);
            first
        } else {
            read_ipc_compressed(slice)?
        };
        timer.stop();

        Ok(Self::batch_to_input(&batch, data_types))
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

    fn properties(&self) -> &PlanProperties {
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
    use crate::execution::shuffle::{CompressionCodec, ShuffleBlockWriter};
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::metrics::Time;
    use std::io::Cursor;
    use std::sync::Arc;

    use crate::execution::shuffle::ipc::read_ipc_compressed;

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

        // Write as compressed IPC
        let writer =
            ShuffleBlockWriter::try_new(&batch.schema(), CompressionCodec::Zstd(1)).unwrap();
        let mut buf = Cursor::new(Vec::new());
        let ipc_time = Time::new();
        writer.write_batch(&batch, &mut buf, &ipc_time).unwrap();

        // Read back (skip 16-byte header: 8 compressed_length + 8 field_count)
        let bytes = buf.into_inner();
        let body = &bytes[16..];

        let decoded = read_ipc_compressed(body).unwrap();
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
    /// Native shuffle may dictionary-encode string/binary columns, but the schema
    /// declares value types (e.g. Utf8). Without unpacking, RecordBatch creation
    /// fails with a schema mismatch.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_dictionary_encoded_shuffle_block_is_unpacked() {
        use super::*;
        use arrow::array::StringDictionaryBuilder;
        use arrow::datatypes::Int32Type;
        use datafusion::physical_plan::ExecutionPlan;
        use futures::StreamExt;

        // Build a batch with a dictionary-encoded string column (simulating what
        // the native shuffle writer produces for string columns).
        let mut dict_builder = StringDictionaryBuilder::<Int32Type>::new();
        dict_builder.append_value("hello");
        dict_builder.append_value("world");
        dict_builder.append_value("hello"); // repeated value, good for dictionary
        let dict_array = dict_builder.finish();

        // The IPC schema includes the dictionary type
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

        // Write as compressed IPC (preserves dictionary encoding)
        let writer =
            ShuffleBlockWriter::try_new(&dict_batch.schema(), CompressionCodec::Zstd(1)).unwrap();
        let mut buf = Cursor::new(Vec::new());
        let ipc_time = Time::new();
        writer
            .write_batch(&dict_batch, &mut buf, &ipc_time)
            .unwrap();
        let bytes = buf.into_inner();
        let body = &bytes[16..];

        // Confirm that read_ipc_compressed returns dictionary-encoded arrays
        let decoded = read_ipc_compressed(body).unwrap();
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
