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

mod codec_context;
pub(crate) mod comet_partitioning;
pub mod ipc;
pub(crate) mod metrics;
pub(crate) mod partitioners;
mod remote_schema;
#[cfg(test)]
mod remote_schema_tests;
#[cfg(test)]
mod rss_execution_tests;
mod schema_align;
mod shuffle_writer;
mod spark_crc32c_hasher;
pub mod spark_unsafe;
pub(crate) mod writers;

pub use codec_context::{ShuffleCodecContext, ShuffleDecodeContext};
pub use comet_partitioning::CometPartitioning;
pub use ipc::{
    read_ipc_compressed, read_ipc_compressed_validated, read_ipc_compressed_validated_with,
    read_ipc_compressed_with,
};
pub use remote_schema::{
    decode_remote_shuffle_batch, decode_remote_shuffle_batch_with, validate_remote_schema,
};
pub use schema_align::SchemaAlignExec;
pub use shuffle_writer::{ShuffleWriterDestination, ShuffleWriterExec};
pub use writers::{CompressionCodec, ShuffleBlockWriter};
