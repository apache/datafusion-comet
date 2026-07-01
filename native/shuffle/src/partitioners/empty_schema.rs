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
use crate::writers::partition_writer::PartitionWriter;
use crate::ShuffleBlockWriter;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use std::fs::OpenOptions;
use std::io::{BufWriter, Seek, Write};
use std::iter;
use tokio::time::Instant;

/// A partitioner for zero-column schemas (e.g. queries where ColumnPruning removes all columns).
/// This handles shuffles for operations like COUNT(*) that produce empty-schema record batches
/// but contain a valid row count. Accumulates the total row count and writes a single
/// zero-column IPC batch to partition 0. All other partitions get empty entries in the index file.
pub(crate) struct EmptySchemaShufflePartitioner<T: PartitionWriter> {
    partition_writer: T,
    schema: SchemaRef,
    num_output_partitions: usize,
    total_rows: usize,
    metrics: ShufflePartitionerMetrics,
}

impl<T: PartitionWriter> EmptySchemaShufflePartitioner<T> {
    pub(crate) fn try_new(
        partition_writer: T,
        schema: SchemaRef,
        num_output_partitions: usize,
        metrics: ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<Self> {
        debug_assert!(
            schema.fields().is_empty(),
            "EmptySchemaShufflePartitioner requires a zero-column schema"
        );
        Ok(Self {
            partition_writer,
            schema,
            num_output_partitions,
            total_rows: 0,
            metrics,
        })
    }
}

#[async_trait::async_trait]
impl<T: PartitionWriter> ShufflePartitioner for EmptySchemaShufflePartitioner<T> {
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

        let mut write_timer = self.metrics.write_time.timer();

        // Write a single zero-column batch with the accumulated row count to partition 0
        let batch_opt = if self.total_rows > 0 {
            Some(Ok(RecordBatch::try_new_with_options(
                self.schema.clone(),
                vec![],
                &arrow::array::RecordBatchOptions::new().with_row_count(Some(self.total_rows)),
            )?))
        } else {
            None
        };
        self.partition_writer
            .finish_partition(0, &mut batch_opt.into_iter(), &self.metrics)?;
        for pid in 1..self.num_output_partitions {
            self.partition_writer
                .finish_partition(pid, &mut iter::empty(), &self.metrics)?;
        }
        self.partition_writer.finish_all(&self.metrics)?;
        write_timer.stop();

        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());
        Ok(())
    }
}
