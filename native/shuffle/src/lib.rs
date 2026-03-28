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
pub use ipc::read_ipc_compressed;
pub use shuffle_writer::ShuffleWriterExec;
pub use writers::{CompressionCodec, ShuffleBlockWriter};

/// Selects which shuffle implementation to use for multi-partition shuffles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShuffleMode {
    /// Buffered: buffer input batches, track per-partition row indices, and write
    /// all partitions at the end with memory-pressure-driven spilling.
    Buffered,
    /// Experimental: repartition each incoming batch immediately using `take` and
    /// write per-partition data directly to individual spill files, avoiding
    /// in-memory buffering of input batches.
    Immediate,
}
