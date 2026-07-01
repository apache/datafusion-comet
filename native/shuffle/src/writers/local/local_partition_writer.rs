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
use crate::writers::local::spill::SpillWriter;
use crate::writers::partition_writer::PartitionWriter;
use crate::writers::BufBatchWriter;
use crate::ShuffleBlockWriter;
use arrow::array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, Write};

pub(crate) struct LocalPartitionWriter {
    output_index_file: String,
    spill_writers: Vec<SpillWriter>,
    shuffle_block_writer: ShuffleBlockWriter,
    output_writer: BufWriter<File>,
    offsets: Vec<u64>,
    batch_size: usize,
    write_buffer_size: usize,
    num_output_partitions: usize,
    last_finish_pid: usize,
}

impl LocalPartitionWriter {
    pub(crate) fn try_new(
        output_data_file: String,
        output_index_file: String,
        shuffle_block_writer: ShuffleBlockWriter,
        num_output_partitions: usize,
        batch_size: usize,
        write_buffer_size: usize,
    ) -> datafusion::common::Result<Self> {
        let spill_writers = if num_output_partitions == 1 {
            vec![]
        } else {
            (0..num_output_partitions)
                .map(|_| {
                    SpillWriter::try_new(
                        shuffle_block_writer.clone(),
                        write_buffer_size,
                        batch_size,
                    )
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?
        };
        let output_writer = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_data_file.clone())
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;

        let output_writer = BufWriter::with_capacity(write_buffer_size, output_writer);
        Ok(Self {
            output_index_file,
            spill_writers,
            shuffle_block_writer,
            output_writer,
            offsets: vec![0u64; num_output_partitions + 1],
            batch_size,
            write_buffer_size,
            num_output_partitions,
            last_finish_pid: num_output_partitions - 1,
        })
    }

    #[cfg(test)]
    pub(crate) fn get_spill_writers(&self) -> &Vec<SpillWriter> {
        &self.spill_writers
    }
}

impl PartitionWriter for LocalPartitionWriter {
    fn spill<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        runtime: &RuntimeEnv,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<()>
    where
        I: Iterator<Item = datafusion::common::Result<RecordBatch>>,
    {
        self.spill_writers[pid]
            .write(iter, runtime, metrics)
            .map(|_| ())
    }

    fn write<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<()>
    where
        I: Iterator<Item = datafusion::common::Result<RecordBatch>>,
    {
        assert!(
            pid == 0 && self.spill_writers.is_empty(),
            "LocalPartitionWriter::write only for single shuffle partition."
        );

        let mut buf_batch_writer = BufBatchWriter::new(
            &mut self.shuffle_block_writer,
            &mut self.output_writer,
            self.write_buffer_size,
            self.batch_size,
        );

        while let Some(batch) = iter.next() {
            let batch = batch?;
            buf_batch_writer.write(&batch, &metrics.encode_time, &metrics.write_time)?;
        }
        buf_batch_writer.flush(&metrics.encode_time, &metrics.write_time)?;

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
        assert_eq!(
            (pid + self.num_output_partitions - self.last_finish_pid) % self.num_output_partitions,
            1,
            "LocalPartitionWriter::finish_partition must be called in order."
        );
        self.last_finish_pid = pid;

        self.offsets[pid] = self.output_writer.stream_position()?;

        // if we wrote a spill file for this partition then copy the
        // contents into the shuffle file
        if let Some(spill_path) = self.spill_writers[pid].path() {
            // Use raw File handle (not BufReader) so that std::io::copy
            // can use copy_file_range/sendfile for zero-copy on Linux.
            let mut spill_file = File::open(spill_path)?;
            let mut write_timer = metrics.write_time.timer();
            std::io::copy(&mut spill_file, &mut self.output_writer)?;
            write_timer.stop();
        }

        // Write in memory batches to output data file
        let mut buf_batch_writer = BufBatchWriter::new(
            &mut self.shuffle_block_writer,
            &mut self.output_writer,
            self.write_buffer_size,
            self.batch_size,
        );
        while let Some(batch) = iter.next() {
            let batch = batch?;
            buf_batch_writer.write(&batch, &metrics.encode_time, &metrics.write_time)?;
        }
        buf_batch_writer.flush(&metrics.encode_time, &metrics.write_time)?;
        Ok(())
    }

    fn finish_all(
        &mut self,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<()> {
        let mut write_timer = metrics.write_time.timer();
        self.output_writer.flush()?;
        write_timer.stop();

        // add one extra offset at last to ease partition length computation
        self.offsets[self.num_output_partitions] = self.output_writer.stream_position()?;

        let mut write_timer = metrics.write_time.timer();
        let mut output_index = BufWriter::new(
            File::create(self.output_index_file.clone())
                .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?,
        );

        self.offsets.iter().for_each(|offset| {
            output_index.write_all(&(offset.to_le_bytes()[..])).unwrap();
        });
        output_index.flush()?;
        write_timer.stop();

        Ok(())
    }
}
