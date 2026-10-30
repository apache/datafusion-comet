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

//! Decompressors kept from one shuffle block to the next, see [`Codecs`].

use crate::codec_context::MAX_RETAINED_ZSTD_CONTEXT_BYTES;
use crate::ipc::decode_error;
use datafusion::error::Result;
use lz4_flex::frame::FrameDecoder as Lz4FrameDecoder;
use snap::read::FrameDecoder as SnappyFrameDecoder;
use std::io::{Error, ErrorKind, Read};
use std::marker::PhantomData;
use std::ptr;
use zstd::zstd_safe::{DCtx, ResetDirective};

/// Decompressors kept from one block to the next.
///
/// Building a decoder per block was most of the fixed cost of decoding a small block. zstd
/// allocates a decompression context with its window buffers, lz4_flex allocates block-sized
/// input and output buffers as it reads the frame, and snap allocates and zero-fills about
/// 140 KiB of them up front. Kept on the thread, the lz4_flex and snap decoders reuse those
/// buffers for every following frame, and the zstd context keeps its workspace across the
/// session reset that readies it for the next frame.
///
/// Retention per thread is bounded. An lz4 decoder's buffers are sized to its frame's block
/// size, and lz4_flex expects them to stay that size, so one decoder is kept per block size the
/// thread has seen, 64 KiB to 4 MiB, at most about 11 MiB in all; Comet's writer picks the size
/// from the batch, so a shuffle normally uses one or two. A frame of any other layout (linked
/// blocks, or the legacy format) gets a decoder for that block only. snap's buffers are fixed at
/// about 140 KiB. A zstd context is dropped once it outgrows
/// [`MAX_RETAINED_ZSTD_CONTEXT_BYTES`], as the writer drops its compression context.
#[derive(Default)]
pub(crate) struct Codecs {
    /// One per block size code, see [`lz4_retention_slot`].
    lz4: [Option<Lz4Decoder>; LZ4_BLOCK_SIZES],
    /// For lz4 frames of any other layout; never kept past the block.
    lz4_other: Option<Lz4Decoder>,
    snappy: Option<SnappyDecoder>,
    zstd: Option<DCtx<'static>>,
    #[cfg(test)]
    pub(crate) creations: CodecCreations,
}

/// The LZ4 frame block maximum sizes lz4_flex decodes: codes 4 to 7, 64 KiB to 4 MiB.
const LZ4_BLOCK_SIZES: usize = 4;

pub(crate) type Lz4Decoder = Lz4FrameDecoder<RequireLz4EndMark<BlockInput>>;
pub(crate) type SnappyDecoder = SnappyFrameDecoder<BlockInput>;

impl Codecs {
    pub(crate) fn lend_lz4<'a, 'b>(&'a mut self, block: &'b [u8]) -> Loan<'a, 'b, Lz4Decoder> {
        let (slot, keep) = match lz4_retention_slot(block) {
            Some(index) => (&mut self.lz4[index], true),
            None => (&mut self.lz4_other, false),
        };
        #[cfg(test)]
        if slot.is_none() {
            self.creations.lz4 += 1;
        }
        Loan::new(slot, block, keep)
    }

    pub(crate) fn lend_snappy<'a, 'b>(
        &'a mut self,
        block: &'b [u8],
    ) -> Loan<'a, 'b, SnappyDecoder> {
        #[cfg(test)]
        if self.snappy.is_none() {
            self.creations.snappy += 1;
        }
        Loan::new(&mut self.snappy, block, true)
    }

    /// The shared zstd context, reset for one frame. The reset also clears whatever a failed
    /// decode left behind, so the context survives errors.
    pub(crate) fn zstd_dctx(&mut self) -> Result<&mut DCtx<'static>> {
        if self.zstd.is_none() {
            #[cfg(test)]
            {
                self.creations.zstd += 1;
            }
            let dctx = DCtx::try_create()
                .ok_or_else(|| decode_error("failed to allocate zstd decompression context"))?;
            self.zstd = Some(dctx);
        }
        let dctx = self.zstd.as_mut().expect("created above");
        dctx.reset(ResetDirective::SessionOnly)
            .map_err(|code| decode_error(zstd::zstd_safe::get_error_name(code)))?;
        Ok(dctx)
    }

    /// Drops the zstd context when its workspace, sized by the frames it has decoded, outgrew
    /// [`MAX_RETAINED_ZSTD_CONTEXT_BYTES`]; the next zstd block creates a new one.
    pub(crate) fn release_zstd_if_oversized(&mut self) {
        if self
            .zstd
            .as_ref()
            .is_some_and(|dctx| dctx.sizeof() > MAX_RETAINED_ZSTD_CONTEXT_BYTES)
        {
            self.zstd = None;
        }
    }
}

/// Whether a snappy block opens with the stream identifier chunk (type `0xff`), or is empty.
///
/// snap checks for the identifier only on a decoder's first chunk, so a retained decoder would
/// accept a later block that lacks it. A fresh decoder does not, and neither does the reader.
/// An empty block passes, to fail as an empty IPC stream the way it always has.
pub(crate) fn snappy_block_opens_with_stream_identifier(block: &[u8]) -> bool {
    const STREAM_IDENTIFIER_CHUNK: u8 = 0xff;
    block
        .first()
        .is_none_or(|&chunk_type| chunk_type == STREAM_IDENTIFIER_CHUNK)
}

/// Which retained decoder an LZ4 frame goes to: one per block maximum size, for a standard frame
/// with independent blocks, the layout Comet's writer produces. `None` for any other frame.
///
/// lz4_flex sizes a decoder's buffers to its frame's block size and asserts, in debug builds,
/// that they keep that size, so frames of different sizes cannot share a decoder. The header
/// is the 4-byte magic number, then `FLG`, whose bit 5 marks independent blocks, then `BD`,
/// whose bits 6 to 4 code the block size: 4 for 64 KiB through 7 for 4 MiB.
pub(crate) fn lz4_retention_slot(frame: &[u8]) -> Option<usize> {
    const MAGIC: [u8; 4] = 0x184D2204_u32.to_le_bytes();
    const INDEPENDENT_BLOCKS: u8 = 1 << 5;
    const BLOCK_SIZE_MASK: u8 = 0b0111_0000;
    const SMALLEST_CODE: u8 = 4;
    let (magic, header) = frame.split_at_checked(MAGIC.len())?;
    let &[flg, bd, ..] = header else {
        return None;
    };
    if magic != MAGIC || flg & INDEPENDENT_BLOCKS == 0 {
        return None;
    }
    let code = (bd & BLOCK_SIZE_MASK) >> 4;
    (SMALLEST_CODE..SMALLEST_CODE + LZ4_BLOCK_SIZES as u8)
        .contains(&code)
        .then(|| usize::from(code - SMALLEST_CODE))
}

/// A decoder kept in [`Codecs`], reading the block it is lent through a [`BlockInput`].
pub(crate) trait RetainedDecoder {
    fn new() -> Self;
    fn input(&mut self) -> &mut BlockInput;
}

impl RetainedDecoder for Lz4Decoder {
    fn new() -> Self {
        Lz4FrameDecoder::new(RequireLz4EndMark(BlockInput::default()))
    }

    fn input(&mut self) -> &mut BlockInput {
        &mut self.get_mut().0
    }
}

impl RetainedDecoder for SnappyDecoder {
    fn new() -> Self {
        SnappyFrameDecoder::new(BlockInput::default())
    }

    fn input(&mut self) -> &mut BlockInput {
        self.get_mut()
    }
}

/// The compressed bytes of the block being decoded, read by a decoder that outlives the block.
///
/// The lz4_flex and snap frame decoders own their reader. A reader that borrowed the block would
/// put the block's lifetime into the decoder's type, and a decoder kept in a thread-local has to
/// be `'static`. So the block is lent as a raw pointer instead, for the duration of one decode,
/// by a [`Loan`]: lending points the input at the block, and the loan ends by clearing it,
/// before the borrow it was made from can end. Outside a loan the input is empty, and a read
/// returns nothing without touching the pointer.
pub(crate) struct BlockInput {
    /// The unread rest of the lent block; dangling and empty outside a loan.
    unread: *const u8,
    len: usize,
}

impl Default for BlockInput {
    fn default() -> Self {
        Self {
            unread: ptr::null(),
            len: 0,
        }
    }
}

impl BlockInput {
    fn lend(&mut self, block: &[u8]) {
        self.unread = block.as_ptr();
        self.len = block.len();
    }

    /// Ends the loan, returning how many bytes of the block were left unread.
    fn revoke(&mut self) -> usize {
        self.unread = ptr::null();
        std::mem::take(&mut self.len)
    }
}

impl Read for BlockInput {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let count = buffer.len().min(self.len);
        if count == 0 {
            return Ok(0);
        }
        // SAFETY: `len` is nonzero only during a loan, when `unread..unread + len` is the unread
        // tail of the slice `lend` was given, and the `Loan` holding that slice's lifetime ends
        // the loan before the slice can go away.
        let bytes = unsafe { std::slice::from_raw_parts(self.unread, count) };
        buffer[..count].copy_from_slice(bytes);
        // SAFETY: `count <= len`, so the result stays within the same slice.
        self.unread = unsafe { self.unread.add(count) };
        self.len -= count;
        Ok(count)
    }
}

/// A block lent to a retained decoder for one decode, see [`BlockInput`].
///
/// The decoder is built on the first loan. [`Loan::settle`] ends the loan and says whether the
/// decode finished cleanly. A decoder that did not, because of an error, or of a panic that
/// unwinds through this guard's drop, is discarded, since it would carry the broken frame's
/// state into the next block. A decoder lent with `keep` false is discarded either way.
pub(crate) struct Loan<'a, 'b, D: RetainedDecoder> {
    slot: &'a mut Option<D>,
    keep: bool,
    settled: bool,
    block: PhantomData<&'b [u8]>,
}

impl<'a, 'b, D: RetainedDecoder> Loan<'a, 'b, D> {
    fn new(slot: &'a mut Option<D>, block: &'b [u8], keep: bool) -> Self {
        slot.get_or_insert_with(D::new).input().lend(block);
        Self {
            slot,
            keep,
            settled: false,
            block: PhantomData,
        }
    }

    pub(crate) fn decoder(&mut self) -> &mut D {
        self.slot.as_mut().expect("lent on construction")
    }

    /// Ends the loan, returning how many bytes of the block the decoder left unread.
    pub(crate) fn settle(mut self, finished: bool) -> usize {
        self.settled = true;
        let unread = self.decoder().input().revoke();
        if !finished || !self.keep {
            *self.slot = None;
        }
        unread
    }
}

impl<D: RetainedDecoder> Drop for Loan<'_, '_, D> {
    fn drop(&mut self) {
        if !self.settled {
            *self.slot = None;
        }
    }
}

// lz4_flex treats physical EOF (including a partial block header) as a clean end of frame.
// Comet always writes an explicit LZ4 EndMark, so a decoder trying to read past the supplied
// bytes has encountered a truncated frame. InvalidData is deliberate: UnexpectedEof is swallowed
// by lz4_flex::frame::FrameDecoder::read_block.
pub(crate) struct RequireLz4EndMark<R>(pub(crate) R);

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

/// Decompressors this thread has built since the last [`crate::ipc::reset_codecs`], per codec.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CodecCreations {
    pub(crate) lz4: usize,
    pub(crate) snappy: usize,
    pub(crate) zstd: usize,
}

#[cfg(test)]
mod tests {
    use super::{lz4_retention_slot, BlockInput, Loan, Lz4Decoder, RetainedDecoder};
    use lz4_flex::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};
    use std::io::{Read, Write};

    fn read_to_string(input: &mut BlockInput, len: usize) -> String {
        let mut buffer = vec![0; len];
        let count = input.read(&mut buffer).unwrap();
        String::from_utf8(buffer[..count].to_vec()).unwrap()
    }

    #[test]
    fn block_input_reads_the_lent_bytes_then_nothing_and_counts_the_rest() {
        let mut input = BlockInput::default();
        assert_eq!(read_to_string(&mut input, 4), "");
        input.lend(b"hello");
        assert_eq!(read_to_string(&mut input, 2), "he");
        assert_eq!(read_to_string(&mut input, 10), "llo");
        assert_eq!(read_to_string(&mut input, 10), "");
        assert_eq!(input.revoke(), 0);
        input.lend(b"hello");
        assert_eq!(read_to_string(&mut input, 1), "h");
        assert_eq!(input.revoke(), 4, "unread bytes");
        assert_eq!(read_to_string(&mut input, 4), "", "nothing outside a loan");
    }

    fn unread(decoder: &mut Lz4Decoder) -> usize {
        let mut sink = Vec::new();
        decoder.input().read_to_end(&mut sink).unwrap();
        sink.len()
    }

    #[test]
    fn loan_keeps_the_decoder_only_after_a_clean_decode_it_was_asked_to_keep() {
        let block = b"not a frame, never decoded";
        let mut slot: Option<Lz4Decoder> = None;

        let mut loan = Loan::new(&mut slot, block, true);
        assert_eq!(unread(loan.decoder()), block.len(), "the block is lent");
        assert_eq!(loan.settle(true), 0);
        let decoder = slot.as_mut().expect("kept");
        assert_eq!(unread(decoder), 0, "and revoked");

        let loan = Loan::new(&mut slot, block, true);
        assert_eq!(loan.settle(false), block.len(), "unread bytes come back");
        assert!(slot.is_none(), "a failed decode discards the decoder");

        let loan = Loan::new(&mut slot, block, true);
        drop(loan);
        assert!(slot.is_none(), "so does a loan dropped without settling");

        let loan = Loan::new(&mut slot, block, false);
        assert_eq!(loan.settle(true), block.len());
        assert!(slot.is_none(), "a decoder lent without keep is not kept");
    }

    #[test]
    fn lz4_retention_slot_reads_the_frame_header() {
        let encode_with = |frame_info: FrameInfo| {
            let mut frame = Vec::new();
            let mut writer = FrameEncoder::with_frame_info(frame_info, &mut frame);
            writer.write_all(b"payload").unwrap();
            writer.finish().unwrap();
            frame
        };
        let default = encode_with(FrameInfo::new());
        assert_eq!(lz4_retention_slot(&default), Some(0), "64 KiB blocks");
        let sizes = [
            BlockSize::Max64KB,
            BlockSize::Max256KB,
            BlockSize::Max1MB,
            BlockSize::Max4MB,
        ];
        for (index, size) in sizes.into_iter().enumerate() {
            let frame = encode_with(FrameInfo::new().block_size(size));
            assert_eq!(lz4_retention_slot(&frame), Some(index), "{size:?}");
        }
        let linked = encode_with(FrameInfo::new().block_mode(BlockMode::Linked));
        assert_eq!(lz4_retention_slot(&linked), None);
        assert_eq!(
            lz4_retention_slot(&default[..4]),
            None,
            "no FLG and BD bytes"
        );
        let mut wrong_magic = default.clone();
        wrong_magic[0] ^= 1;
        assert_eq!(lz4_retention_slot(&wrong_magic), None);
    }
}
