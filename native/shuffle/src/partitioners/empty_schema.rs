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

use crate::metrics::ShufflePartitionerMetrics;
use crate::partitioners::ShufflePartitioner;
use crate::ShuffleBlockWriter;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use std::fs::OpenOptions;
use std::io::{BufWriter, Seek, Write};
use tokio::time::Instant;

/// A partitioner for zero-column schemas (e.g. queries where ColumnPruning removes all columns).
/// This handles shuffles for operations like COUNT(*) that produce empty-schema record batches
/// but contain a valid row count. Accumulates the total row count and writes a single
/// zero-column IPC batch to partition 0. All other partitions get empty entries in the index file.
pub(crate) struct EmptySchemaShufflePartitioner {
    output_data_file: String,
    output_index_file: String,
    schema: SchemaRef,
    shuffle_block_writer: ShuffleBlockWriter,
    num_output_partitions: usize,
    total_rows: usize,
    metrics: ShufflePartitionerMetrics,
}

impl EmptySchemaShufflePartitioner {
    pub(crate) fn try_new(
        output_data_file: String,
        output_index_file: String,
        schema: SchemaRef,
        num_output_partitions: usize,
        metrics: ShufflePartitionerMetrics,
        codec: crate::CompressionCodec,
    ) -> datafusion::common::Result<Self> {
        debug_assert!(
            schema.fields().is_empty(),
            "EmptySchemaShufflePartitioner requires a zero-column schema"
        );
        let shuffle_block_writer = ShuffleBlockWriter::try_new(schema.as_ref(), codec)?;
        Ok(Self {
            output_data_file,
            output_index_file,
            schema,
            shuffle_block_writer,
            num_output_partitions,
            total_rows: 0,
            metrics,
        })
    }
}

#[async_trait::async_trait]
impl ShufflePartitioner for EmptySchemaShufflePartitioner {
    async fn insert_batch(&mut self, batch: RecordBatch) -> datafusion::common::Result<()> {
        let start_time = Instant::now();
        let num_rows = batch.num_rows();
        if num_rows > 0 {
            self.total_rows += num_rows;
            self.metrics.baseline.record_output(num_rows);
        }
        self.metrics.input_batches.add(1);
        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());
        Ok(())
    }

    fn shuffle_write(&mut self) -> datafusion::common::Result<()> {
        let start_time = Instant::now();

        let output_data = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.output_data_file)
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;
        let mut output_data = BufWriter::new(output_data);

        // Write a single zero-column batch with the accumulated row count to partition 0
        if self.total_rows > 0 {
            let batch = RecordBatch::try_new_with_options(
                self.schema.clone(),
                vec![],
                &arrow::array::RecordBatchOptions::new().with_row_count(Some(self.total_rows)),
            )?;
            self.shuffle_block_writer.write_batch(
                &batch,
                &mut output_data,
                &self.metrics.encode_time,
            )?;
        }

        let mut write_timer = self.metrics.write_time.timer();
        output_data.flush()?;
        let data_file_length = output_data.stream_position()?;

        // Write index file: partition 0 spans [0, data_file_length), all others are empty
        let index_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.output_index_file)
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;
        let mut index_writer = BufWriter::new(index_file);
        index_writer.write_all(&0i64.to_le_bytes())?;
        for _ in 0..self.num_output_partitions {
            index_writer.write_all(&(data_file_length as i64).to_le_bytes())?;
        }
        index_writer.flush()?;
        write_timer.stop();

        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());
        Ok(())
    }
}
