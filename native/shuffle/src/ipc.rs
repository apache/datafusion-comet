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

use crate::codec_context::ShuffleDecodeContext;
use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use std::cell::RefCell;
use std::io::{Error, ErrorKind, Read};

thread_local! {
    /// Backs the context-less entry points below. Their only production caller is the JVM's
    /// static decodeShuffleBlock JNI export, which has no native object that could own a
    /// context; everything else owns a [`ShuffleDecodeContext`] and uses the `_with`
    /// variants, so retention there ends with the owner instead of the thread.
    static DECODE_CONTEXT: RefCell<ShuffleDecodeContext> =
        RefCell::new(ShuffleDecodeContext::default());
}

/// Decode trusted local Comet output without revalidating every Arrow array value or offset.
pub fn read_ipc_compressed(bytes: &[u8]) -> Result<RecordBatch> {
    DECODE_CONTEXT.with(|context| read_ipc_compressed_impl(&mut context.borrow_mut(), bytes, false))
}

/// Decode remotely fetched Comet output, including Arrow buffer and offset validation.
pub fn read_ipc_compressed_validated(bytes: &[u8]) -> Result<RecordBatch> {
    DECODE_CONTEXT.with(|context| read_ipc_compressed_impl(&mut context.borrow_mut(), bytes, true))
}

/// [`read_ipc_compressed`] with a caller-owned decode context.
pub fn read_ipc_compressed_with(
    decode_context: &mut ShuffleDecodeContext,
    bytes: &[u8],
) -> Result<RecordBatch> {
    read_ipc_compressed_impl(decode_context, bytes, false)
}

/// [`read_ipc_compressed_validated`] with a caller-owned decode context.
pub fn read_ipc_compressed_validated_with(
    decode_context: &mut ShuffleDecodeContext,
    bytes: &[u8],
) -> Result<RecordBatch> {
    read_ipc_compressed_impl(decode_context, bytes, true)
}

fn read_ipc_compressed_impl(
    decode_context: &mut ShuffleDecodeContext,
    bytes: &[u8],
    validate: bool,
) -> Result<RecordBatch> {
    let result = decode_shuffle_frame(decode_context, bytes, validate);
    // Decoding a frame with a large advertised window grows the context past 100 MiB, and
    // contexts here are long-lived (operator-owned, or thread-local for the JNI entry
    // points). Bound what survives the frame, whether it decoded or not.
    decode_context.release_zstd_if_oversized();
    result
}

fn decode_shuffle_frame(
    decode_context: &mut ShuffleDecodeContext,
    bytes: &[u8],
    validate: bool,
) -> Result<RecordBatch> {
    let codec = bytes.get(..4).ok_or_else(|| {
        DataFusionError::Execution("Failed to decode batch: truncated compression codec".to_owned())
    })?;
    let mut encoded = &bytes[4..];
    let batch = match codec {
        b"SNAP" => read_single_batch(snap::read::FrameDecoder::new(&mut encoded), validate)?,
        b"LZ4_" => read_single_batch(
            lz4_flex::frame::FrameDecoder::new(RequireLz4EndMark(&mut encoded)),
            validate,
        )?,
        // The slice already implements BufRead. Adding another BufReader would let read-ahead
        // conceal compressed bytes left over after the decoder reaches its end marker.
        b"ZSTD" => read_single_batch(
            zstd::Decoder::with_context(&mut encoded, decode_context.zstd_dctx()?),
            validate,
        )?,
        b"NONE" => read_single_batch(&mut encoded, validate)?,
        other => {
            return Err(DataFusionError::Execution(format!(
                "Failed to decode batch: invalid compression codec: {other:?}"
            )))
        }
    };
    // LZ4 returns EOF at the end of one compressed frame without consuming the next one. Check
    // the encoded source as well as the decoded IPC tail so an oversized outer frame cannot
    // silently swallow another native frame's bytes.
    if !encoded.is_empty() {
        return Err(DataFusionError::Execution(
            "Failed to decode batch: trailing data after compressed stream".to_owned(),
        ));
    }
    Ok(batch)
}

// lz4_flex treats physical EOF (including a partial block header) as a clean end of frame.
// Comet always writes an explicit LZ4 EndMark, so a decoder trying to read past the supplied
// bytes has encountered a truncated frame. InvalidData is deliberate: UnexpectedEof is swallowed
// by lz4_flex::frame::FrameDecoder::read_block.
struct RequireLz4EndMark<R>(R);

impl<R: Read> Read for RequireLz4EndMark<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let count = self.0.read(buffer)?;
        if count == 0 && !buffer.is_empty() {
            Err(Error::new(
                ErrorKind::InvalidData,
                "Failed to decode batch: truncated LZ4 shuffle frame",
            ))
        } else {
            Ok(count)
        }
    }
}

fn read_single_batch<R: Read>(input: R, validate: bool) -> Result<RecordBatch> {
    let reader = StreamReader::try_new(input, None)?;
    let mut reader = if validate {
        // Remote data must not escape as unchecked arrays and fail later in a native operator.
        reader
    } else {
        // Preserve the existing local-shuffle fast path for trusted Comet-written arrays.
        unsafe { reader.with_skip_validation(true) }
    };
    let batch = reader.next().transpose()?.ok_or_else(|| {
        DataFusionError::Execution("Failed to decode batch: empty IPC stream".to_owned())
    })?;

    // Each Comet frame contains one complete IPC stream with exactly one record batch.
    // Stopping after that batch would skip codec footer/checksum validation and could silently
    // discard further frames swallowed by a corrupt outer length prefix.
    if reader.next().transpose()?.is_some() {
        return Err(DataFusionError::Execution(
            "Failed to decode batch: multiple record batches in one shuffle frame".to_owned(),
        ));
    }
    if reader.get_mut().read(&mut [0])? != 0 {
        return Err(DataFusionError::Execution(
            "Failed to decode batch: trailing data after IPC stream".to_owned(),
        ));
    }
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::{
        read_ipc_compressed, read_ipc_compressed_validated, read_ipc_compressed_validated_with,
        read_ipc_compressed_with,
    };
    use crate::codec_context::ShuffleDecodeContext;
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use std::io::Write;
    use std::sync::Arc;

    fn ipc_stream(batch_count: usize) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
        for _ in 0..batch_count {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
        bytes
    }

    fn encode(codec: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut bytes = codec.to_vec();
        match codec {
            b"NONE" => bytes.extend_from_slice(payload),
            b"SNAP" => {
                let mut writer = snap::write::FrameEncoder::new(&mut bytes);
                writer.write_all(payload).unwrap();
                writer.into_inner().unwrap();
            }
            b"LZ4_" => {
                let mut writer = lz4_flex::frame::FrameEncoder::new(&mut bytes);
                writer.write_all(payload).unwrap();
                writer.finish().unwrap();
            }
            b"ZSTD" => {
                let mut writer = zstd::Encoder::new(&mut bytes, 1).unwrap();
                writer.write_all(payload).unwrap();
                writer.finish().unwrap();
            }
            _ => unreachable!(),
        }
        bytes
    }

    #[test]
    fn malformed_codec_prefix_returns_error() {
        for prefix in [&b""[..], b"N", b"NO", b"NON", b"BAD!"] {
            assert!(read_ipc_compressed(prefix).is_err());
            assert!(read_ipc_compressed_validated(prefix).is_err());
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn empty_or_multiple_batch_stream_returns_error() {
        for codec in [b"NONE", b"SNAP", b"LZ4_", b"ZSTD"] {
            for batch_count in [0, 2] {
                let error = read_ipc_compressed(&encode(codec, &ipc_stream(batch_count)))
                    .unwrap_err()
                    .to_string();
                assert!(
                    error.contains(if batch_count == 0 {
                        "empty IPC stream"
                    } else {
                        "multiple record batches"
                    }),
                    "{codec:?}: {error}"
                );
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn trailing_data_after_ipc_stream_returns_error() {
        let mut payload = ipc_stream(1);
        payload.extend_from_slice(b"another shuffle frame");
        for codec in [b"NONE", b"SNAP", b"LZ4_", b"ZSTD"] {
            let error = read_ipc_compressed(&encode(codec, &payload))
                .unwrap_err()
                .to_string();
            assert!(error.contains("trailing data"), "{codec:?}: {error}");
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn trailing_data_after_compressed_stream_returns_error() {
        for codec in [b"NONE", b"SNAP", b"LZ4_", b"ZSTD"] {
            let mut frame = encode(codec, &ipc_stream(1));
            frame.extend_from_slice(&20_u64.to_le_bytes());
            frame.extend_from_slice(b"another native frame");
            assert!(read_ipc_compressed(&frame).is_err(), "{codec:?}");
        }
    }

    #[test]
    fn truncated_lz4_end_mark_returns_error() {
        let frame = encode(b"LZ4_", &ipc_stream(1));
        for truncated in 1..=4 {
            let error = read_ipc_compressed(&frame[..frame.len() - truncated])
                .unwrap_err()
                .to_string();
            assert!(error.contains("truncated LZ4"), "{truncated}: {error}");
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn invalid_array_offsets_return_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["abc", "def"]))],
        )
        .unwrap();
        let mut payload = Vec::new();
        let mut writer = StreamWriter::try_new(&mut payload, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let offsets: Vec<u8> = [0_i32, 3, 6]
            .into_iter()
            .flat_map(i32::to_le_bytes)
            .collect();
        let positions: Vec<usize> = payload
            .windows(offsets.len())
            .enumerate()
            .filter_map(|(position, bytes)| (bytes == offsets).then_some(position))
            .collect();
        assert_eq!(positions.len(), 1);
        // Change [0, 3, 6] to [0, 3, 2]: the second string now has decreasing offsets.
        payload[positions[0] + 8..positions[0] + 12].copy_from_slice(&2_i32.to_le_bytes());
        for codec in [b"NONE", b"SNAP", b"LZ4_", b"ZSTD"] {
            assert!(read_ipc_compressed_validated(&encode(codec, &payload)).is_err());
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn valid_single_batch_frames_decode_with_all_codecs() {
        for codec in [b"NONE", b"SNAP", b"LZ4_", b"ZSTD"] {
            let frame = encode(codec, &ipc_stream(1));
            let batch = read_ipc_compressed(&frame).unwrap();
            let validated = read_ipc_compressed_validated(&frame).unwrap();
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch, validated);
        }
    }

    /// One context across many frames, codecs changing between them, must decode exactly
    /// like a context created for each frame.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn decode_context_reused_across_frames_and_codecs() {
        let mut ctx = ShuffleDecodeContext::default();
        for _ in 0..3 {
            for codec in [b"ZSTD", b"NONE", b"SNAP", b"ZSTD", b"LZ4_", b"ZSTD"] {
                let frame = encode(codec, &ipc_stream(1));
                let fresh =
                    read_ipc_compressed_with(&mut ShuffleDecodeContext::default(), &frame).unwrap();
                let reused = read_ipc_compressed_with(&mut ctx, &frame).unwrap();
                let reused_validated =
                    read_ipc_compressed_validated_with(&mut ctx, &frame).unwrap();
                assert_eq!(reused, fresh);
                assert_eq!(reused_validated, fresh);
            }
        }
        assert_eq!(
            ctx.creation_count(),
            1,
            "every zstd frame must share one context"
        );
    }

    /// ZSTD shuffle frame written by a streaming encode with no pledged source size, so its
    /// header advertises `level`'s full default window and the decoder must allocate it.
    fn encode_zstd_at_level(payload: &[u8], level: i32) -> Vec<u8> {
        let mut bytes = b"ZSTD".to_vec();
        let mut writer = zstd::Encoder::new(&mut bytes, level).unwrap();
        writer.write_all(payload).unwrap();
        writer.finish().unwrap();
        bytes
    }

    /// Level 22's window makes decoding demand a >100 MiB workspace.
    fn encode_zstd_wide_window(payload: &[u8]) -> Vec<u8> {
        encode_zstd_at_level(payload, 22)
    }

    /// N small-window frames must cost one context creation, not N; that is the point of
    /// carrying a context at all. A level-19 frame decodes through the same context but
    /// inflates its workspace to ~8.47 MiB (zstd-sys 2.0.16+zstd.1.5.7), past the 8 MiB
    /// retention cap, so it must leave the context released and the next frame pays anew.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn decode_context_creations_track_retention_boundary() {
        let mut ctx = ShuffleDecodeContext::default();
        let small = encode(b"ZSTD", &ipc_stream(1));
        for _ in 0..3 {
            assert_eq!(
                read_ipc_compressed_with(&mut ctx, &small)
                    .unwrap()
                    .num_rows(),
                3
            );
        }
        assert_eq!(
            ctx.creation_count(),
            1,
            "small-window frames must share one context"
        );
        assert!(ctx.holds_zstd_dctx());

        let level19 = encode_zstd_at_level(&ipc_stream(1), 19);
        assert_eq!(
            read_ipc_compressed_with(&mut ctx, &level19)
                .unwrap()
                .num_rows(),
            3
        );
        assert_eq!(
            ctx.creation_count(),
            1,
            "the retained context serves the wide frame"
        );
        assert!(
            !ctx.holds_zstd_dctx(),
            "a level-19 frame's workspace exceeds the retention cap and must be dropped"
        );

        assert_eq!(
            read_ipc_compressed_with(&mut ctx, &small)
                .unwrap()
                .num_rows(),
            3
        );
        assert_eq!(
            ctx.creation_count(),
            2,
            "the dropped context is re-created lazily"
        );
        assert!(ctx.holds_zstd_dctx());
    }

    /// Small-window frames keep the context cached for reuse; a frame that inflates the
    /// workspace far past its usual size must not leave it pinned in a long-lived context.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn decode_context_drops_oversized_zstd_workspace() {
        let mut ctx = ShuffleDecodeContext::default();
        let small = encode(b"ZSTD", &ipc_stream(1));
        assert_eq!(
            read_ipc_compressed_with(&mut ctx, &small)
                .unwrap()
                .num_rows(),
            3
        );
        assert!(
            ctx.holds_zstd_dctx(),
            "a small-window frame must keep the context cached for reuse"
        );

        let wide = encode_zstd_wide_window(&ipc_stream(1));
        assert_eq!(
            read_ipc_compressed_with(&mut ctx, &wide)
                .unwrap()
                .num_rows(),
            3
        );
        assert!(
            !ctx.holds_zstd_dctx(),
            "a wide-window frame must not leave its workspace cached"
        );
    }

    /// The workspace bound must hold on the error path too: a failed decode of a
    /// wide-window frame cannot leave the inflated context behind.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn decode_error_drops_oversized_zstd_workspace() {
        let mut ctx = ShuffleDecodeContext::default();
        let wide = encode_zstd_wide_window(&ipc_stream(1));
        assert!(read_ipc_compressed_with(&mut ctx, &wide[..wide.len() - 7]).is_err());
        assert!(
            !ctx.holds_zstd_dctx(),
            "a failed wide-window decode must not leave its workspace cached"
        );
    }

    /// A truncated frame must not poison the context for the next valid one.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn decode_context_usable_after_error() {
        let mut ctx = ShuffleDecodeContext::default();
        let good = encode(b"ZSTD", &ipc_stream(1));
        let truncated = &good[..good.len() - 7];
        assert!(read_ipc_compressed_with(&mut ctx, truncated).is_err());
        let batch = read_ipc_compressed_with(&mut ctx, &good).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }
}
