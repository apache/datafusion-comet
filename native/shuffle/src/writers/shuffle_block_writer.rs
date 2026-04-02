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

use arrow::ipc::writer::IpcWriteOptions;
use arrow::ipc::CompressionType;

/// Compression algorithm applied to shuffle IPC streams and Parquet output.
#[derive(Debug, Clone)]
pub enum CompressionCodec {
    None,
    Lz4Frame,
    Zstd(i32),
    /// Snappy is only used for Parquet output, not for shuffle IPC.
    Snappy,
}

impl CompressionCodec {
    pub fn ipc_write_options(&self) -> datafusion::error::Result<IpcWriteOptions> {
        let compression = match self {
            CompressionCodec::None | CompressionCodec::Snappy => None,
            CompressionCodec::Lz4Frame => Some(CompressionType::LZ4_FRAME),
            CompressionCodec::Zstd(_) => Some(CompressionType::ZSTD),
        };
        let options = IpcWriteOptions::try_new(8, false, arrow::ipc::MetadataVersion::V5)
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::from(e), None))?;
        options
            .try_with_compression(compression)
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::from(e), None))
    }
}
