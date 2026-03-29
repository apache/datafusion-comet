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

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use datafusion::common::DataFusionError;
use datafusion::error::Result;

/// Reads all record batches from a standard Arrow IPC stream.
///
/// This is the counterpart to [`crate::writers::IpcStreamWriter`]. The input
/// should be a complete Arrow IPC stream (schema + batch messages + EOS marker),
/// optionally with Arrow IPC body compression (LZ4_FRAME or ZSTD).
pub fn read_ipc_stream(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader = StreamReader::try_new(bytes, None)?;
    reader
        .into_iter()
        .map(|r| r.map_err(|e| e.into()))
        .collect()
}

pub fn read_ipc_compressed(bytes: &[u8]) -> Result<RecordBatch> {
    match &bytes[0..4] {
        b"SNAP" => {
            let decoder = snap::read::FrameDecoder::new(&bytes[4..]);
            let mut reader =
                unsafe { StreamReader::try_new(decoder, None)?.with_skip_validation(true) };
            reader.next().unwrap().map_err(|e| e.into())
        }
        b"LZ4_" => {
            let decoder = lz4_flex::frame::FrameDecoder::new(&bytes[4..]);
            let mut reader =
                unsafe { StreamReader::try_new(decoder, None)?.with_skip_validation(true) };
            reader.next().unwrap().map_err(|e| e.into())
        }
        b"ZSTD" => {
            let decoder = zstd::Decoder::new(&bytes[4..])?;
            let mut reader =
                unsafe { StreamReader::try_new(decoder, None)?.with_skip_validation(true) };
            reader.next().unwrap().map_err(|e| e.into())
        }
        b"NONE" => {
            let mut reader =
                unsafe { StreamReader::try_new(&bytes[4..], None)?.with_skip_validation(true) };
            reader.next().unwrap().map_err(|e| e.into())
        }
        other => Err(DataFusionError::Execution(format!(
            "Failed to decode batch: invalid compression codec: {other:?}"
        ))),
    }
}
