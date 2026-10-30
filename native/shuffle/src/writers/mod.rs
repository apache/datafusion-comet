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

mod buf_batch_writer;
mod checksum;
mod local;
mod partition_writer;
pub(crate) mod rss;
mod shuffle_block_writer;

pub(crate) use buf_batch_writer::BufBatchWriter;
pub(crate) use checksum::Checksum;
pub(crate) use local::local_partition_writer::LocalPartitionWriter;
pub(crate) use partition_writer::PartitionWriter;
pub(crate) use rss::rss_partition_writer::RssPartitionWriter;
pub use shuffle_block_writer::{CompressionCodec, ShuffleBlockWriter};

/// Decodes every length-prefixed shuffle block in `bytes`, in order.
#[cfg(test)]
pub(crate) fn decode_blocks(bytes: &[u8]) -> Vec<arrow::array::RecordBatch> {
    let mut batches = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let len = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize;
        batches.push(crate::read_ipc_compressed(&bytes[pos + 16..pos + 8 + len]).unwrap());
        pos += 8 + len;
    }
    batches
}
