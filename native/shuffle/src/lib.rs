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

pub(crate) mod comet_partitioning;
pub mod ipc;
pub(crate) mod metrics;
pub(crate) mod partitioners;
mod shuffle_writer;
pub mod spark_unsafe;
pub(crate) mod writers;

pub use comet_partitioning::CometPartitioning;
pub use ipc::{read_ipc_compressed, read_ipc_stream};
pub use shuffle_writer::ShuffleWriterExec;
pub use writers::{CompressionCodec, IpcStreamWriter, ShuffleBlockWriter};

/// The format used for writing shuffle data.
#[derive(Debug, Clone)]
pub enum ShuffleFormat {
    /// Custom block format: each batch is a self-contained block with a
    /// length-prefix + field-count + codec header followed by compressed Arrow IPC.
    Block,
    /// Standard Arrow IPC stream format: schema written once, batches as IPC
    /// messages, with optional Arrow IPC body compression (LZ4_FRAME or ZSTD).
    IpcStream,
}
