use crate::execution::shuffle::metrics::ShufflePartitionerMetrics;
use crate::execution::shuffle::shuffle_writer::PartitionedBatchIterator;
use crate::execution::shuffle::writers::buf_batch_writer::BufBatchWriter;
use crate::execution::shuffle::ShuffleBlockWriter;
use datafusion::common::DataFusionError;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::runtime_env::RuntimeEnv;
use std::fs::{File, OpenOptions};

struct SpillFile {
    temp_file: RefCountedTempFile,
    file: File,
}

pub(crate) struct PartitionWriter {
    /// Spill file for intermediate shuffle output for this partition. Each spill event
    /// will append to this file and the contents will be copied to the shuffle file at
    /// the end of processing.
    spill_file: Option<SpillFile>,
    /// Writer that performs encoding and compression
    shuffle_block_writer: ShuffleBlockWriter,
}

impl PartitionWriter {
    pub(crate) fn try_new(
        shuffle_block_writer: ShuffleBlockWriter,
    ) -> datafusion::common::Result<Self> {
        Ok(Self {
            spill_file: None,
            shuffle_block_writer,
        })
    }

    fn ensure_spill_file_created(
        &mut self,
        runtime: &RuntimeEnv,
    ) -> datafusion::common::Result<()> {
        if self.spill_file.is_none() {
            // Spill file is not yet created, create it
            let spill_file = runtime
                .disk_manager
                .create_tmp_file("shuffle writer spill")?;
            let spill_data = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(spill_file.path())
                .map_err(|e| {
                    DataFusionError::Execution(format!("Error occurred while spilling {e}"))
                })?;
            self.spill_file = Some(SpillFile {
                temp_file: spill_file,
                file: spill_data,
            });
        }
        Ok(())
    }

    pub(crate) fn spill(
        &mut self,
        iter: &mut PartitionedBatchIterator,
        runtime: &RuntimeEnv,
        metrics: &ShufflePartitionerMetrics,
        write_buffer_size: usize,
    ) -> datafusion::common::Result<usize> {
        if let Some(batch) = iter.next() {
            self.ensure_spill_file_created(runtime)?;

            let total_bytes_written = {
                let mut buf_batch_writer = BufBatchWriter::new(
                    &mut self.shuffle_block_writer,
                    &mut self.spill_file.as_mut().unwrap().file,
                    write_buffer_size,
                );
                let mut bytes_written =
                    buf_batch_writer.write(&batch?, &metrics.encode_time, &metrics.write_time)?;
                for batch in iter {
                    let batch = batch?;
                    bytes_written += buf_batch_writer.write(
                        &batch,
                        &metrics.encode_time,
                        &metrics.write_time,
                    )?;
                }
                buf_batch_writer.flush(&metrics.write_time)?;
                bytes_written
            };

            Ok(total_bytes_written)
        } else {
            Ok(0)
        }
    }

    pub(crate) fn path(&self) -> Option<&std::path::Path> {
        self.spill_file
            .as_ref()
            .map(|spill_file| spill_file.temp_file.path())
    }

    #[cfg(test)]
    pub(crate) fn has_spill_file(&self) -> bool {
        self.spill_file.is_some()
    }
}
