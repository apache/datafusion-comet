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

//! LZ4 frames written with buffers and a hash table kept from one frame to the next.
//!
//! Every compressed shuffle block is one LZ4 frame. `lz4_flex::frame::FrameEncoder` owns the
//! writer it compresses into, and blocks go to different outputs, so `ShuffleBlockWriter` built
//! one per block, and with it a zeroed 16 KiB hash table, a 64 KiB input buffer and a 70 KiB
//! output buffer. [`Lz4FrameEncoder`] keeps those and writes the frame format itself around
//! lz4_flex's block compressor, compressing whole blocks straight from the caller's buffer.
//!
//! The frames have independent blocks of at most 64 KiB, no checksums, no content size and an
//! end mark. A block that does not get smaller is stored uncompressed, as `FrameEncoder` does.

use lz4_flex::block::{compress_into_with_table, get_maximum_output_size, CompressTable};
use std::io::{self, Write};

/// Uncompressed bytes per block: the frame format's smallest block maximum, which every LZ4
/// reader decodes with the least memory.
const BLOCK_SIZE: usize = 64 * 1024;

/// The magic number, then FLG 0x60 (version 01, independent blocks, no block checksums, no
/// content size, no content checksum), BD 0x40 (64 KiB block maximum), and the header checksum:
/// the second byte of the xxh32 of FLG and BD, seed 0.
const FRAME_HEADER: [u8; 7] = [0x04, 0x22, 0x4d, 0x18, 0x60, 0x40, 0x82];

/// Set in a block's size field when the block's bytes are stored uncompressed.
const UNCOMPRESSED_BLOCK: u32 = 1 << 31;

/// A zero block size ends the frame.
const END_MARK: [u8; 4] = [0; 4];

/// The buffers and hash table for writing LZ4 frames, up to about 150 KiB, reused by every frame.
pub(crate) struct Lz4FrameEncoder {
    /// Input of the block being filled, until it holds [`BLOCK_SIZE`] bytes.
    pending: Vec<u8>,
    /// Compressed output, grown to the worst case of the largest block so far rather than zeroed
    /// in full up front: a remote encode starts from a new encoder every time.
    compressed: Vec<u8>,
    /// 32-bit entries from the start, like `FrameEncoder`'s table, so that a block compresses the
    /// same whatever came before it. `compress_into_with_table` clears it for every block.
    table: CompressTable,
}

impl Default for Lz4FrameEncoder {
    fn default() -> Self {
        Self {
            pending: Vec::with_capacity(BLOCK_SIZE),
            compressed: Vec::new(),
            table: CompressTable::large(),
        }
    }
}

impl Lz4FrameEncoder {
    /// Writes a frame header to `output` and returns the writer for the frame's content. The
    /// frame is complete once [`Lz4Frame::finish`] returns.
    pub(crate) fn begin<W: Write>(&mut self, mut output: W) -> io::Result<Lz4Frame<'_, W>> {
        // a frame that failed part way may have left input behind
        self.pending.clear();
        output.write_all(&FRAME_HEADER)?;
        Ok(Lz4Frame {
            encoder: self,
            output,
        })
    }
}

/// The content of one LZ4 frame, compressed a block at a time as it is written.
pub(crate) struct Lz4Frame<'a, W: Write> {
    encoder: &'a mut Lz4FrameEncoder,
    output: W,
}

impl<W: Write> Lz4Frame<'_, W> {
    /// Writes the last block and the end mark.
    pub(crate) fn finish(mut self) -> io::Result<()> {
        self.write_pending()?;
        self.output.write_all(&END_MARK)
    }

    fn write_pending(&mut self) -> io::Result<()> {
        let Lz4FrameEncoder {
            pending,
            compressed,
            table,
        } = &mut *self.encoder;
        if !pending.is_empty() {
            write_block(&mut self.output, pending, compressed, table)?;
            pending.clear();
        }
        Ok(())
    }
}

impl<W: Write> Write for Lz4Frame<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let Lz4FrameEncoder {
            pending,
            compressed,
            table,
        } = &mut *self.encoder;
        let mut input = buf;
        if !pending.is_empty() {
            let fill = input.len().min(BLOCK_SIZE - pending.len());
            pending.extend_from_slice(&input[..fill]);
            input = &input[fill..];
            if pending.len() < BLOCK_SIZE {
                return Ok(buf.len());
            }
            write_block(&mut self.output, pending, compressed, table)?;
            pending.clear();
        }
        // Whole blocks are compressed straight from `buf`, without a copy into `pending`.
        let (blocks, rest) = input.as_chunks::<BLOCK_SIZE>();
        for block in blocks {
            write_block(&mut self.output, block, compressed, table)?;
        }
        pending.extend_from_slice(rest);
        Ok(buf.len())
    }

    /// Ends the current block early, as `FrameEncoder::flush` does, then flushes the output.
    fn flush(&mut self) -> io::Result<()> {
        self.write_pending()?;
        self.output.flush()
    }
}

/// Writes `block` to `output`, compressed if that makes it smaller and as it is otherwise.
fn write_block<W: Write>(
    output: &mut W,
    block: &[u8],
    compressed: &mut Vec<u8>,
    table: &mut CompressTable,
) -> io::Result<()> {
    debug_assert!(!block.is_empty() && block.len() <= BLOCK_SIZE);
    let bound = get_maximum_output_size(block.len());
    if compressed.len() < bound {
        // exact, so that growing from a short block does not double the capacity
        compressed.reserve_exact(bound - compressed.len());
        compressed.resize(bound, 0);
    }
    let len = compress_into_with_table(block, compressed, table).map_err(io::Error::other)?;
    let (size, data) = if len < block.len() {
        (len as u32, &compressed[..len])
    } else {
        (block.len() as u32 | UNCOMPRESSED_BLOCK, block)
    };
    output.write_all(&size.to_le_bytes())?;
    output.write_all(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lz4_flex::frame::{BlockSize, FrameDecoder, FrameEncoder, FrameInfo};
    use std::io::Read;

    /// Compressible text, or bytes from a xorshift generator that LZ4 cannot shrink.
    fn data(len: usize, compressible: bool) -> Vec<u8> {
        if compressible {
            return (0..len)
                .map(|i| b"shuffle block "[i % 14].wrapping_add((i / 4096) as u8))
                .collect();
        }
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 32) as u8
            })
            .collect()
    }

    /// Writes `input` as one frame, handing it over `chunk` bytes at a time.
    fn encode(encoder: &mut Lz4FrameEncoder, input: &[u8], chunk: usize) -> Vec<u8> {
        let mut out = vec![];
        let mut frame = encoder.begin(&mut out).unwrap();
        for piece in input.chunks(chunk.max(1)) {
            frame.write_all(piece).unwrap();
        }
        frame.finish().unwrap();
        out
    }

    /// What `FrameEncoder` writes with the block size pinned to 64 KiB.
    fn lz4_flex_frame(input: &[u8]) -> Vec<u8> {
        let info = FrameInfo::new().block_size(BlockSize::Max64KB);
        let mut encoder = FrameEncoder::with_frame_info(info, vec![]);
        encoder.write_all(input).unwrap();
        encoder.finish().unwrap()
    }

    fn decode(frame: &[u8]) -> Vec<u8> {
        let mut out = vec![];
        FrameDecoder::new(frame).read_to_end(&mut out).unwrap();
        out
    }

    /// The size field of every block, read by walking the frame.
    fn block_sizes(frame: &[u8]) -> Vec<u32> {
        assert_eq!(frame[..7], FRAME_HEADER);
        let mut sizes = vec![];
        let mut rest = &frame[7..];
        loop {
            let size = u32::from_le_bytes(rest[..4].try_into().unwrap());
            rest = &rest[4..];
            if size == 0 {
                assert!(rest.is_empty(), "bytes after the end mark");
                return sizes;
            }
            rest = &rest[(size & !UNCOMPRESSED_BLOCK) as usize..];
            sizes.push(size);
        }
    }

    /// An empty frame, and every frame that fits one block, is byte for byte what `FrameEncoder`
    /// writes, header included.
    #[test]
    fn single_block_frames_match_lz4_flex() {
        let mut encoder = Lz4FrameEncoder::default();
        for len in [0, 1, 12, 13, 1000, BLOCK_SIZE - 1, BLOCK_SIZE] {
            for compressible in [true, false] {
                let input = data(len, compressible);
                assert_eq!(
                    encode(&mut encoder, &input, input.len()),
                    lz4_flex_frame(&input),
                    "{len} bytes, compressible: {compressible}"
                );
            }
        }
    }

    /// Frames of any length decode with lz4_flex's reader, whatever the size of each write. Every
    /// block but the last holds 64 KiB, and a block that does not shrink is stored as it is.
    #[test]
    fn frames_round_trip_through_lz4_flex() {
        let mut encoder = Lz4FrameEncoder::default();
        for len in [BLOCK_SIZE + 1, 3 * BLOCK_SIZE + 17, 1 << 20] {
            for compressible in [true, false] {
                let input = data(len, compressible);
                let whole = encode(&mut encoder, &input, input.len());
                for chunk in [1, 7, 1000, BLOCK_SIZE - 3, BLOCK_SIZE + 5] {
                    // blocks are cut every 64 KiB of input, not where the writes end
                    assert_eq!(encode(&mut encoder, &input, chunk), whole, "{chunk}");
                }
                assert_eq!(decode(&whole), input);

                let sizes = block_sizes(&whole);
                assert_eq!(sizes.len(), len.div_ceil(BLOCK_SIZE));
                let raw = |size: &u32| size & UNCOMPRESSED_BLOCK != 0;
                if compressible {
                    // the last block can be too short to shrink
                    assert!(!sizes[..sizes.len() - 1].iter().any(raw));
                } else {
                    assert!(sizes.iter().all(raw));
                }
            }
        }
    }

    /// A flush ends the block early, and the frame still decodes.
    #[test]
    fn flush_ends_a_block() {
        let input = data(3000, true);
        let (mut encoder, mut out) = (Lz4FrameEncoder::default(), vec![]);
        let mut frame = encoder.begin(&mut out).unwrap();
        frame.write_all(&input[..1000]).unwrap();
        frame.flush().unwrap();
        frame.flush().unwrap();
        frame.write_all(&input[1000..]).unwrap();
        frame.finish().unwrap();
        assert_eq!(block_sizes(&out).len(), 2);
        assert_eq!(decode(&out), input);
    }

    /// The output buffer grows to the bound of the largest block and no further, so the encoder
    /// keeps at most 154,029 bytes of buffers and table.
    #[test]
    fn output_buffer_stays_within_one_block_bound() {
        let mut encoder = Lz4FrameEncoder::default();
        for len in [60_000, BLOCK_SIZE, 3 * BLOCK_SIZE] {
            encode(&mut encoder, &data(len, false), len);
            assert!(
                encoder.compressed.capacity() <= get_maximum_output_size(BLOCK_SIZE),
                "{len}: {}",
                encoder.compressed.capacity()
            );
        }
    }

    /// A frame abandoned part way leaves nothing behind for the next one.
    #[test]
    fn reused_encoder_writes_what_a_new_one_writes() {
        let input = data(2 * BLOCK_SIZE + 100, true);
        let expected = encode(&mut Lz4FrameEncoder::default(), &input, input.len());

        let mut encoder = Lz4FrameEncoder::default();
        encode(&mut encoder, &data(BLOCK_SIZE + 9, false), 1000);
        let mut abandoned = encoder.begin(vec![]).unwrap();
        abandoned.write_all(&input[..500]).unwrap();
        drop(abandoned);

        assert_eq!(encode(&mut encoder, &input, input.len()), expected);
    }
}
