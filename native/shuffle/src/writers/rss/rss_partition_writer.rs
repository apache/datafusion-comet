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
use crate::writers::{BufBatchWriter, PartitionWriter, RssPartitionPusher};
use crate::ShuffleBlockWriter;
use arrow::array::RecordBatch;

pub(crate) struct RssPartitionWriter {
    num_output_partitions: usize,
    partition_writers: Vec<BufBatchWriter<ShuffleBlockWriter, RssPartitionPusher>>,
}

impl RssPartitionWriter {
    pub fn try_new(
        pusher: &mut RssPartitionPusher,
        shuffle_block_writer: ShuffleBlockWriter,
        num_output_partitions: usize,
        batch_size: usize,
        write_buffer_size: usize,
    ) -> datafusion::common::Result<Self> {
        let partition_writers = (0..num_output_partitions)
            .map(|pid| {
                BufBatchWriter::new(
                    shuffle_block_writer.clone(),
                    pusher.clone_with_pid(pid as i32),
                    write_buffer_size,
                    batch_size,
                )
            })
            .collect::<Vec<_>>();
        Ok(Self {
            num_output_partitions,
            partition_writers,
        })
    }
}

impl PartitionWriter for RssPartitionWriter {
    fn write<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<()>
    where
        I: Iterator<Item = datafusion::common::Result<RecordBatch>>,
    {
        for batch in iter.by_ref() {
            let batch = batch?;
            self.partition_writers[pid].write(&batch, &metrics.encode_time, &metrics.write_time)?;
        }

        // Flush the partition writer if there are multiple output partitions.
        // This is to prevent partition writer buffers from holding too much memory.
        if self.num_output_partitions > 1 {
            self.partition_writers[pid].flush(&metrics.encode_time, &metrics.write_time)?;
        }

        Ok(())
    }

    fn finish_partition<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<()>
    where
        I: Iterator<Item = datafusion::common::Result<RecordBatch>>,
    {
        for batch in iter.by_ref() {
            let batch = batch?;
            self.partition_writers[pid].write(&batch, &metrics.encode_time, &metrics.write_time)?;
        }

        self.partition_writers[pid].flush(&metrics.encode_time, &metrics.write_time)
    }

    fn finish_all(
        &mut self,
        _metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<()> {
        Ok(())
    }
}
