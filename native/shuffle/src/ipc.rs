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
use std::cell::RefCell;
use std::io::Read;

pub fn read_ipc_compressed(bytes: &[u8]) -> Result<RecordBatch> {
    match &bytes[0..4] {
        b"SNAP" => decode_ipc_stream(snap::read::FrameDecoder::new(&bytes[4..])),
        b"LZ4_" => decode_ipc_stream(lz4_flex::frame::FrameDecoder::new(&bytes[4..])),
        b"ZSTD" => ZSTD_DECODER.with(|decoder| decoder.borrow_mut().read_batch(&bytes[4..])),
        b"NONE" => decode_ipc_stream(&bytes[4..]),
        other => Err(DataFusionError::Execution(format!(
            "Failed to decode batch: invalid compression codec: {other:?}"
        ))),
    }
}

/// Read the single record batch from an uncompressed Arrow IPC stream.
fn decode_ipc_stream<R: Read>(reader: R) -> Result<RecordBatch> {
    let mut reader = unsafe { StreamReader::try_new(reader, None)?.with_skip_validation(true) };
    reader.next().unwrap().map_err(|e| e.into())
}

/// Per-thread reusable zstd decompression context, so decoding does not allocate a fresh
/// `ZSTD_DCtx` for every block.
struct ZstdBlockDecoder {
    context: zstd::zstd_safe::DCtx<'static>,
}

impl ZstdBlockDecoder {
    fn read_batch(&mut self, frame: &[u8]) -> Result<RecordBatch> {
        // The reader may stop before draining the whole frame, so reset the session state to leave
        // the reused context clean for the next block (as a fresh context would be).
        self.context
            .reset(zstd::zstd_safe::ResetDirective::SessionOnly)
            .map_err(|_| DataFusionError::Execution("failed to reset zstd context".to_string()))?;
        let decoder = zstd::stream::read::Decoder::with_context(frame, &mut self.context);
        decode_ipc_stream(decoder)
    }
}

thread_local! {
    static ZSTD_DECODER: RefCell<ZstdBlockDecoder> = RefCell::new(ZstdBlockDecoder {
        context: zstd::zstd_safe::DCtx::create(),
    });
}
