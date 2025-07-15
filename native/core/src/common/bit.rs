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

use std::{cmp::min, mem::size_of};

use crate::{
    errors::CometResult as Result,
    parquet::{data_type::AsBytes, util::bit_packing::unpack32},
};
use arrow::buffer::Buffer;
use datafusion_comet_spark_expr::utils::{likely, unlikely};

#[inline]
pub fn from_ne_slice<T: FromBytes>(bs: &[u8]) -> T {
    let mut b = T::Buffer::default();
    {
        let b = b.as_mut();
        let bs = &bs[..b.len()];
        b.copy_from_slice(bs);
    }
    T::from_ne_bytes(b)
}

pub trait FromBytes: Sized {
    type Buffer: AsMut<[u8]> + Default;
    fn from_le_bytes(bs: Self::Buffer) -> Self;
    fn from_be_bytes(bs: Self::Buffer) -> Self;
    fn from_ne_bytes(bs: Self::Buffer) -> Self;
    fn from(v: u64) -> Self;
}

macro_rules! from_le_bytes {
    ($($ty: ty),*) => {
        $(
        impl FromBytes for $ty {
            type Buffer = [u8; size_of::<Self>()];
            fn from_le_bytes(bs: Self::Buffer) -> Self {
                <$ty>::from_le_bytes(bs)
            }
            fn from_be_bytes(bs: Self::Buffer) -> Self {
                <$ty>::from_be_bytes(bs)
            }
            fn from_ne_bytes(bs: Self::Buffer) -> Self {
                <$ty>::from_ne_bytes(bs)
            }
            fn from(v: u64) -> Self {
                v as $ty
            }
        }
        )*
    };
}

impl FromBytes for bool {
    type Buffer = [u8; 1];
    fn from_le_bytes(bs: Self::Buffer) -> Self {
        Self::from_ne_bytes(bs)
    }
    fn from_be_bytes(bs: Self::Buffer) -> Self {
        Self::from_ne_bytes(bs)
    }
    fn from_ne_bytes(bs: Self::Buffer) -> Self {
        match bs[0] {
            0 => false,
            1 => true,
            _ => panic!("Invalid byte when reading bool"),
        }
    }
    fn from(v: u64) -> Self {
        (v & 1) == 1
    }
}

// TODO: support f32 and f64 in the future, but there is no use case right now
//       f32/f64::from(v: u64) will be like `from_ne_slice(v.as_bytes()))` and that is
//       expensive as it involves copying buffers
from_le_bytes! { u8, u16, u32, u64, i8, i16, i32, i64 }

/// Reads `$size` of bytes from `$src`, and reinterprets them as type `$ty`, in
/// little-endian order. `$ty` must implement the `Default` trait. Otherwise this won't
/// compile.
/// This is copied and modified from byteorder crate.
macro_rules! read_num_bytes {
    ($ty:ty, $size:expr, $src:expr) => {{
        debug_assert!($size <= $src.len());
        let mut buffer = <$ty as $crate::common::bit::FromBytes>::Buffer::default();
        buffer.as_mut()[..$size].copy_from_slice(&$src[..$size]);
        <$ty>::from_ne_bytes(buffer)
    }};
}

/// u64 specific version of read_num_bytes!
/// This is faster than read_num_bytes! because this method avoids buffer copies.
#[inline]
pub fn read_num_bytes_u64(size: usize, src: &[u8]) -> u64 {
    debug_assert!(size <= src.len());
    if unlikely(src.len() < 8) {
        return read_num_bytes!(u64, size, src);
    }
    let in_ptr = src as *const [u8] as *const u8 as *const u64;
    let v = unsafe { in_ptr.read_unaligned() };
    trailing_bits(v, size * 8)
}

/// u32 specific version of read_num_bytes!
/// This is faster than read_num_bytes! because this method avoids buffer copies.
#[inline]
pub fn read_num_bytes_u32(size: usize, src: &[u8]) -> u32 {
    debug_assert!(size <= src.len());
    if unlikely(src.len() < 4) {
        return read_num_bytes!(u32, size, src);
    }
    let in_ptr = src as *const [u8] as *const u8 as *const u32;
    let v = unsafe { in_ptr.read_unaligned() };
    trailing_bits(v as u64, size * 8) as u32
}

#[inline]
pub fn read_u64(src: &[u8]) -> u64 {
    let in_ptr = src.as_ptr() as *const u64;
    unsafe { in_ptr.read_unaligned() }
}

#[inline]
pub fn read_u32(src: &[u8]) -> u32 {
    let in_ptr = src.as_ptr() as *const u32;
    unsafe { in_ptr.read_unaligned() }
}

#[inline]
pub fn memcpy(source: &[u8], target: &mut [u8]) {
    debug_assert!(target.len() >= source.len(), "Copying from source to target is not possible. Source has {} bytes but target has {} bytes", source.len(), target.len());
    // Originally `target[..source.len()].copy_from_slice(source)`
    // We use the unsafe copy method to avoid some expensive bounds checking/
    unsafe { std::ptr::copy_nonoverlapping(source.as_ptr(), target.as_mut_ptr(), source.len()) }
}

#[inline]
pub fn memcpy_value<T>(source: &T, num_bytes: usize, target: &mut [u8])
where
    T: ?Sized + AsBytes,
{
    debug_assert!(
        target.len() >= num_bytes,
        "Not enough space. Only had {} bytes but need to put {} bytes",
        target.len(),
        num_bytes
    );
    memcpy(&source.as_bytes()[..num_bytes], target)
}

/// Returns ceil(log2(x))
#[inline]
pub fn log2(mut x: u64) -> u32 {
    if x == 1 {
        return 0;
    }
    x -= 1;
    64u32 - x.leading_zeros()
}

/// Returns the `num_bits` least-significant bits of `v`
#[inline]
pub fn trailing_bits(v: u64, num_bits: usize) -> u64 {
    if unlikely(num_bits == 0) {
        return 0;
    }
    if unlikely(num_bits >= 64) {
        return v;
    }
    v & ((1 << num_bits) - 1)
}

#[inline]
pub fn set_bit(bits: &mut [u8], i: usize) {
    bits[i / 8] |= 1 << (i % 8);
}

/// Set the bit value at index `i`, for buffer `bits`.
///
/// # Safety
/// This doesn't check bounds, the caller must ensure that `i` is in (0, bits.len() * 8)
#[inline]
pub unsafe fn set_bit_raw(bits: *mut u8, i: usize) {
    *bits.add(i / 8) |= 1 << (i % 8);
}

#[inline]
pub fn unset_bit(bits: &mut [u8], i: usize) {
    bits[i / 8] &= !(1 << (i % 8));
}

#[inline]
pub fn set_bits(bits: &mut [u8], offset: usize, length: usize) {
    let mut byte_i = offset / 8;
    let offset_r = offset % 8;
    let end = offset + length;
    let end_byte_i = end / 8;
    let end_r = end % 8;

    // if the offset starts in the middle of a byte, update the byte first
    if offset_r != 0 {
        let num_bits = min(length, 7);
        bits[byte_i] |= ((1u8 << num_bits) - 1) << offset_r;
        byte_i += 1;
    }

    // See if there is an opportunity to do a bulk byte write
    if byte_i < end_byte_i {
        unsafe {
            bits.as_mut_ptr()
                .add(byte_i)
                .write_bytes(255, end_byte_i - byte_i);
        }
        byte_i = end_byte_i;
    }

    // take care of the last byte
    if end_r > 0 && (byte_i == end_byte_i) {
        bits[byte_i] |= (1u8 << end_r) - 1;
    }
}

#[inline(always)]
pub fn mix_hash(lower: u64, upper: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(lower);
    hash.wrapping_mul(37).wrapping_add(upper)
}

static BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

/// Returns whether bit at position `i` in `data` is set or not
#[inline]
pub fn get_bit(data: &[u8], i: usize) -> bool {
    (data[i >> 3] & BIT_MASK[i & 7]) != 0
}

/// Returns the boolean value at index `i`.
///
/// # Safety
/// This doesn't check bounds, the caller must ensure that index < self.len()
#[inline]
pub unsafe fn get_bit_raw(ptr: *const u8, i: usize) -> bool {
    (*ptr.add(i >> 3) & BIT_MASK[i & 7]) != 0
}

/// Utility class for writing bit/byte streams. This class can write data in either
/// bit packed or byte aligned fashion.
pub struct BitWriter {
    buffer: Vec<u8>,
    max_bytes: usize,
    buffered_values: u64,
    byte_offset: usize,
    bit_offset: usize,
    start: usize,
}

impl BitWriter {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            buffer: vec![0; max_bytes],
            max_bytes,
            buffered_values: 0,
            byte_offset: 0,
            bit_offset: 0,
            start: 0,
        }
    }

    /// Initializes the writer from the existing buffer `buffer` and starting
    /// offset `start`.
    pub fn new_from_buf(buffer: Vec<u8>, start: usize) -> Self {
        debug_assert!(start < buffer.len());
        let len = buffer.len();
        Self {
            buffer,
            max_bytes: len,
            buffered_values: 0,
            byte_offset: start,
            bit_offset: 0,
            start,
        }
    }

    /// Extend buffer size by `increment` bytes
    #[inline]
    pub fn extend(&mut self, increment: usize) {
        self.max_bytes += increment;
        let extra = vec![0; increment];
        self.buffer.extend(extra);
    }

    /// Report buffer size, in bytes
    #[inline]
    pub fn capacity(&mut self) -> usize {
        self.max_bytes
    }

    /// Consumes and returns the current buffer.
    #[inline]
    pub fn consume(mut self) -> Vec<u8> {
        self.flush();
        self.buffer.truncate(self.byte_offset);
        self.buffer
    }

    /// Flushes the internal buffered bits and returns the buffer's content.
    /// This is a borrow equivalent of `consume` method.
    #[inline]
    pub fn flush_buffer(&mut self) -> &[u8] {
        self.flush();
        &self.buffer()[0..self.byte_offset]
    }

    /// Clears the internal state so the buffer can be reused.
    #[inline]
    pub fn clear(&mut self) {
        self.buffered_values = 0;
        self.byte_offset = self.start;
        self.bit_offset = 0;
    }

    /// Flushes the internal buffered bits and the align the buffer to the next byte.
    #[inline]
    pub fn flush(&mut self) {
        let num_bytes = self.bit_offset.div_ceil(8);
        debug_assert!(self.byte_offset + num_bytes <= self.max_bytes);
        memcpy_value(
            &self.buffered_values,
            num_bytes,
            &mut self.buffer[self.byte_offset..],
        );
        self.buffered_values = 0;
        self.bit_offset = 0;
        self.byte_offset += num_bytes;
    }

    /// Advances the current offset by skipping `num_bytes`, flushing the internal bit
    /// buffer first.
    /// This is useful when you want to jump over `num_bytes` bytes and come back later
    /// to fill these bytes.
    ///
    /// Returns error if `num_bytes` is beyond the boundary of the internal buffer.
    /// Otherwise, returns the old offset.
    #[inline]
    pub fn skip(&mut self, num_bytes: usize) -> Result<usize> {
        self.flush();
        debug_assert!(self.byte_offset <= self.max_bytes);
        if unlikely(self.byte_offset + num_bytes > self.max_bytes) {
            return Err(general_err!(
                "Not enough bytes left in BitWriter. Need {} but only have {}",
                self.byte_offset + num_bytes,
                self.max_bytes
            ));
        }
        let result = self.byte_offset;
        self.byte_offset += num_bytes;
        Ok(result)
    }

    /// Returns a slice containing the next `num_bytes` bytes starting from the current
    /// offset, and advances the underlying buffer by `num_bytes`.
    /// This is useful when you want to jump over `num_bytes` bytes and come back later
    /// to fill these bytes.
    #[inline]
    pub fn get_next_byte_ptr(&mut self, num_bytes: usize) -> Result<&mut [u8]> {
        let offset = self.skip(num_bytes)?;
        Ok(&mut self.buffer[offset..offset + num_bytes])
    }

    #[inline]
    pub fn bytes_written(&self) -> usize {
        self.byte_offset - self.start + self.bit_offset.div_ceil(8)
    }

    #[inline]
    pub fn buffer(&self) -> &[u8] {
        &self.buffer[self.start..]
    }

    #[inline]
    pub fn byte_offset(&self) -> usize {
        self.byte_offset
    }

    /// Returns the internal buffer length. This is the maximum number of bytes that this
    /// writer can write. User needs to call `consume` to consume the current buffer
    /// before more data can be written.
    #[inline]
    pub fn buffer_len(&self) -> usize {
        self.max_bytes
    }

    /// Writes the entire byte `value` at the byte `offset`
    pub fn write_at(&mut self, offset: usize, value: u8) {
        self.buffer[offset] = value;
    }

    /// Writes the `num_bits` LSB of value `v` to the internal buffer of this writer.
    /// The `num_bits` must not be greater than 64. This is bit packed.
    ///
    /// Returns false if there's not enough room left. True otherwise.
    #[inline]
    #[allow(clippy::unnecessary_cast)]
    pub fn put_value(&mut self, v: u64, num_bits: usize) -> bool {
        debug_assert!(num_bits <= 64);
        debug_assert_eq!(v.checked_shr(num_bits as u32).unwrap_or(0), 0); // covers case v >> 64

        let num_bytes = self.byte_offset * 8 + self.bit_offset + num_bits;
        if unlikely(num_bytes > self.max_bytes as usize * 8) {
            return false;
        }

        self.buffered_values |= v << self.bit_offset;
        self.bit_offset += num_bits;
        if self.bit_offset >= 64 {
            memcpy_value(
                &self.buffered_values,
                8,
                &mut self.buffer[self.byte_offset..],
            );
            self.byte_offset += 8;
            self.bit_offset -= 64;
            self.buffered_values = 0;
            // Perform checked right shift: v >> offset, where offset < 64, otherwise we
            // shift all bits
            self.buffered_values = v
                .checked_shr((num_bits - self.bit_offset) as u32)
                .unwrap_or(0);
        }
        debug_assert!(self.bit_offset < 64);
        true
    }

    /// Writes `val` of `num_bytes` bytes to the next aligned byte. If size of `T` is
    /// larger than `num_bytes`, extra higher ordered bytes will be ignored.
    ///
    /// Returns false if there's not enough room left. True otherwise.
    #[inline]
    pub fn put_aligned<T: AsBytes>(&mut self, val: T, num_bytes: usize) -> bool {
        let result = self.get_next_byte_ptr(num_bytes);
        if unlikely(result.is_err()) {
            // TODO: should we return `Result` for this func?
            return false;
        }
        let ptr = result.unwrap();
        memcpy_value(&val, num_bytes, ptr);
        true
    }

    /// Writes `val` of `num_bytes` bytes at the designated `offset`. The `offset` is the
    /// offset starting from the beginning of the internal buffer that this writer
    /// maintains. Note that this will overwrite any existing data between `offset` and
    /// `offset + num_bytes`. Also that if size of `T` is larger than `num_bytes`, extra
    /// higher ordered bytes will be ignored.
    ///
    /// Returns false if there's not enough room left, or the `pos` is not valid.
    /// True otherwise.
    #[inline]
    pub fn put_aligned_offset<T: AsBytes>(
        &mut self,
        val: T,
        num_bytes: usize,
        offset: usize,
    ) -> bool {
        if unlikely(num_bytes + offset > self.max_bytes) {
            return false;
        }
        memcpy_value(
            &val,
            num_bytes,
            &mut self.buffer[offset..offset + num_bytes],
        );
        true
    }

    /// Writes a VLQ encoded integer `v` to this buffer. The value is byte aligned.
    ///
    /// Returns false if there's not enough room left. True otherwise.
    #[inline]
    pub fn put_vlq_int(&mut self, mut v: u64) -> bool {
        let mut result = true;
        while v & 0xFFFFFFFFFFFFFF80 != 0 {
            result &= self.put_aligned::<u8>(((v & 0x7F) | 0x80) as u8, 1);
            v >>= 7;
        }
        result &= self.put_aligned::<u8>((v & 0x7F) as u8, 1);
        result
    }

    /// Writes a zigzag-VLQ encoded (in little endian order) int `v` to this buffer.
    /// Zigzag-VLQ is a variant of VLQ encoding where negative and positive
    /// numbers are encoded in a zigzag fashion.
    /// See: https://developers.google.com/protocol-buffers/docs/encoding
    ///
    /// Returns false if there's not enough room left. True otherwise.
    #[inline]
    pub fn put_zigzag_vlq_int(&mut self, v: i64) -> bool {
        let u: u64 = ((v << 1) ^ (v >> 63)) as u64;
        self.put_vlq_int(u)
    }
}

/// Maximum byte length for a VLQ encoded integer
/// MAX_VLQ_BYTE_LEN = 5 for i32, and MAX_VLQ_BYTE_LEN = 10 for i64
pub const MAX_VLQ_BYTE_LEN: usize = 10;

pub struct BitReader {
    /// The byte buffer to read from, passed in by client
    buffer: Buffer, // TODO: generalize this

    /// Bytes are memcpy'd from `buffer` and values are read from this variable.
    /// This is faster than reading values byte by byte directly from `buffer`
    buffered_values: u64,

    ///
    /// End                                         Start
    /// |............|B|B|B|B|B|B|B|B|..............|
    ///                   ^          ^
    ///                 bit_offset   byte_offset
    ///
    /// Current byte offset in `buffer`
    byte_offset: usize,

    /// Current bit offset in `buffered_values`
    bit_offset: usize,

    /// Total number of bytes in `buffer`
    total_bytes: usize,
}

/// Utility class to read bit/byte stream. This class can read bits or bytes that are
/// either byte aligned or not.
impl BitReader {
    pub fn new(buf: Buffer, len: usize) -> Self {
        let buffered_values = if size_of::<u64>() > len {
            read_num_bytes_u64(len, buf.as_slice())
        } else {
            read_u64(buf.as_slice())
        };
        BitReader {
            buffer: buf,
            buffered_values,
            byte_offset: 0,
            bit_offset: 0,
            total_bytes: len,
        }
    }

    pub fn new_all(buf: Buffer) -> Self {
        let len = buf.len();
        Self::new(buf, len)
    }

    pub fn reset(&mut self, buf: Buffer) {
        self.buffer = buf;
        self.total_bytes = self.buffer.len();
        self.buffered_values = if size_of::<u64>() > self.total_bytes {
            read_num_bytes_u64(self.total_bytes, self.buffer.as_slice())
        } else {
            read_u64(self.buffer.as_slice())
        };
        self.byte_offset = 0;
        self.bit_offset = 0;
    }

    /// Gets the current byte offset
    #[inline]
    pub fn get_byte_offset(&self) -> usize {
        self.byte_offset + self.bit_offset.div_ceil(8)
    }

    /// Reads a value of type `T` and of size `num_bits`.
    ///
    /// Returns `None` if there's not enough data available. `Some` otherwise.
    pub fn get_value<T: FromBytes>(&mut self, num_bits: usize) -> Option<T> {
        debug_assert!(num_bits <= 64);
        debug_assert!(num_bits <= size_of::<T>() * 8);

        if unlikely(self.byte_offset * 8 + self.bit_offset + num_bits > self.total_bytes * 8) {
            return None;
        }

        let v = self.get_u64_value(num_bits);
        Some(T::from(v))
    }

    /// Reads a `u32` value encoded using `num_bits` of bits.
    ///
    /// # Safety
    ///
    /// This method asusumes the following:
    ///
    /// - the `num_bits` is <= 64
    /// - the remaining number of bits to read in this reader is >= `num_bits`.
    ///
    /// Undefined behavior will happen if any of the above assumptions is violated.
    #[inline]
    pub fn get_u32_value(&mut self, num_bits: usize) -> u32 {
        self.get_u64_value(num_bits) as u32
    }

    #[inline(always)]
    fn get_u64_value(&mut self, num_bits: usize) -> u64 {
        if unlikely(num_bits == 0) {
            0
        } else {
            let v = self.buffered_values >> self.bit_offset;
            let mask = u64::MAX >> (64 - num_bits);
            self.bit_offset += num_bits;
            if self.bit_offset < 64 {
                v & mask
            } else {
                self.byte_offset += 8;
                self.bit_offset -= 64;
                self.reload_buffer_values();
                ((self.buffered_values << (num_bits - self.bit_offset)) | v) & mask
            }
        }
    }

    /// Gets at most `num` bits from this reader, and append them to the `dst` byte slice, starting
    /// at bit offset `offset`.
    ///
    /// Returns the actual number of bits appended. In case either the `dst` slice doesn't have
    /// enough space or the current reader doesn't have enough bits to consume, the returned value
    /// will be less than the input `num_bits`.
    ///
    /// # Preconditions
    /// * `offset` MUST < dst.len() * 8
    pub fn get_bits(&mut self, dst: &mut [u8], offset: usize, num_bits: usize) -> usize {
        debug_assert!(offset < dst.len() * 8);

        let remaining_bits = (self.total_bytes - self.byte_offset) * 8 - self.bit_offset;
        let num_bits_to_read = min(remaining_bits, min(num_bits, dst.len() * 8 - offset));
        let mut i = 0;

        // First consume all the remaining bits from the `buffered_values` if there're any.
        if likely(self.bit_offset != 0) {
            i += self.get_bits_buffered(dst, offset, num_bits_to_read);
        }

        debug_assert!(self.bit_offset == 0 || i == num_bits_to_read);

        // Check if there's opportunity to directly copy bytes using `memcpy`.
        if (offset + i) % 8 == 0 && i < num_bits_to_read {
            let num_bytes = (num_bits_to_read - i) / 8;
            let dst_byte_offset = (offset + i) / 8;
            if num_bytes > 0 {
                memcpy(
                    &self.buffer[self.byte_offset..self.byte_offset + num_bytes],
                    &mut dst[dst_byte_offset..],
                );
                i += num_bytes * 8;
                self.byte_offset += num_bytes;
                self.reload_buffer_values();
            }
        }

        debug_assert!((offset + i) % 8 != 0 || num_bits_to_read - i < 8);

        // Now copy the remaining bits if there's any.
        while i < num_bits_to_read {
            i += self.get_bits_buffered(dst, offset + i, num_bits_to_read - i);
        }

        num_bits_to_read
    }

    /// Consume at most `n` bits from `buffered_values`. Returns the actual number of bits consumed.
    ///
    /// # Postcondition
    /// - either bits from `buffered_values` are completely drained (i.e., `bit_offset` == 0)
    /// - OR the `num_bits` is < the number of remaining bits in `buffered_values` and thus the
    ///   returned value is < `num_bits`.
    ///
    /// Either way, the returned value is in range [0, 64].
    #[inline]
    fn get_bits_buffered(&mut self, dst: &mut [u8], offset: usize, num_bits: usize) -> usize {
        if unlikely(num_bits == 0) {
            return 0;
        }

        let n = min(num_bits, 64 - self.bit_offset);
        let offset_i = offset / 8;
        let offset_r = offset % 8;

        // Extract the value to read out of the buffer
        let mut v = trailing_bits(self.buffered_values >> self.bit_offset, n);

        // Read the first byte always because n > 0
        dst[offset_i] |= (v << offset_r) as u8;
        v >>= 8 - offset_r;

        // Read the rest of the bytes
        ((offset_i + 1)..(offset_i + usize::div_ceil(n + offset_r, 8))).for_each(|i| {
            dst[i] |= v as u8;
            v >>= 8;
        });

        self.bit_offset += n;
        if self.bit_offset == 64 {
            self.byte_offset += 8;
            self.bit_offset -= 64;
            self.reload_buffer_values();
        }

        n
    }

    /// Skips at most `num` bits from this reader.
    ///
    /// Returns the actual number of bits skipped.
    pub fn skip_bits(&mut self, num_bits: usize) -> usize {
        let remaining_bits = (self.total_bytes - self.byte_offset) * 8 - self.bit_offset;
        let num_bits_to_read = min(remaining_bits, num_bits);
        let mut i = 0;

        // First skip all the remaining bits by updating the offsets of `buffered_values`.
        if likely(self.bit_offset != 0) {
            let n = 64 - self.bit_offset;
            if num_bits_to_read < n {
                self.bit_offset += num_bits_to_read;
                i = num_bits_to_read;
            } else {
                self.byte_offset += 8;
                self.bit_offset = 0;
                i = n;
            }
        }

        // Check if there's opportunity to skip by byte
        if i + 7 < num_bits_to_read {
            let num_bytes = (num_bits_to_read - i) / 8;
            i += num_bytes * 8;
            self.byte_offset += num_bytes;
        }

        if self.bit_offset == 0 {
            self.reload_buffer_values();
        }

        // Now skip the remaining bits if there's any.
        if i < num_bits_to_read {
            self.bit_offset += num_bits_to_read - i;
        }

        num_bits_to_read
    }

    /// Reads a batch of `u32` values encoded using `num_bits` of bits, into `dst`.
    ///
    /// # Safety
    ///
    /// This method asusumes the following:
    ///
    /// - the `num_bits` is <= 64
    /// - the remaining number of bits to read in this reader is >= `total * num_bits`.
    ///
    /// Undefined behavior will happen if any of the above assumptions is violated.
    ///
    /// Unlike `[get_batch]`, this method removes a few checks such as checking the remaining number
    /// of bits as well as checking the bit width for the element type in `dst`. Therefore, it is
    /// more efficient.
    pub unsafe fn get_u32_batch(&mut self, mut dst: *mut u32, total: usize, num_bits: usize) {
        let mut i = 0;

        // First align bit offset to byte offset
        if likely(self.bit_offset != 0) {
            while i < total && self.bit_offset != 0 {
                *dst = self.get_u32_value(num_bits);
                dst = dst.offset(1);
                i += 1;
            }
        }

        let in_buf = &self.buffer.as_slice()[self.byte_offset..];
        let mut in_ptr = in_buf as *const [u8] as *const u8 as *const u32;
        while total - i >= 32 {
            in_ptr = unpack32(in_ptr, dst, num_bits);
            self.byte_offset += 4 * num_bits;
            dst = dst.offset(32);
            i += 32;
        }

        self.reload_buffer_values();
        while i < total {
            *dst = self.get_u32_value(num_bits);
            dst = dst.offset(1);
            i += 1;
        }
    }

    pub fn get_batch<T: FromBytes>(&mut self, batch: &mut [T], num_bits: usize) -> usize {
        debug_assert!(num_bits <= 32);
        debug_assert!(num_bits <= size_of::<T>() * 8);

        let mut values_to_read = batch.len();
        let needed_bits = num_bits * values_to_read;
        let remaining_bits = (self.total_bytes - self.byte_offset) * 8 - self.bit_offset;
        if remaining_bits < needed_bits {
            values_to_read = remaining_bits / num_bits;
        }

        let mut i = 0;

        // First align bit offset to byte offset
        if likely(self.bit_offset != 0) {
            while i < values_to_read && self.bit_offset != 0 {
                batch[i] = self
                    .get_value(num_bits)
                    .expect("expected to have more data");
                i += 1;
            }
        }

        unsafe {
            let in_buf = &self.buffer.as_slice()[self.byte_offset..];
            let mut in_ptr = in_buf as *const [u8] as *const u8 as *const u32;
            // FIXME assert!(memory::is_ptr_aligned(in_ptr));
            if size_of::<T>() == 4 {
                while values_to_read - i >= 32 {
                    let out_ptr = &mut batch[i..] as *mut [T] as *mut T as *mut u32;
                    in_ptr = unpack32(in_ptr, out_ptr, num_bits);
                    self.byte_offset += 4 * num_bits;
                    i += 32;
                }
            } else {
                let mut out_buf = [0u32; 32];
                let out_ptr = &mut out_buf as &mut [u32] as *mut [u32] as *mut u32;
                while values_to_read - i >= 32 {
                    in_ptr = unpack32(in_ptr, out_ptr, num_bits);
                    self.byte_offset += 4 * num_bits;
                    for n in 0..32 {
                        // We need to copy from smaller size to bigger size to avoid
                        // overwriting other memory regions.
                        if size_of::<T>() > size_of::<u32>() {
                            std::ptr::copy_nonoverlapping(
                                out_buf[n..].as_ptr(),
                                &mut batch[i] as *mut T as *mut u32,
                                1,
                            );
                        } else {
                            std::ptr::copy_nonoverlapping(
                                out_buf[n..].as_ptr() as *const T,
                                &mut batch[i] as *mut T,
                                1,
                            );
                        }
                        i += 1;
                    }
                }
            }
        }

        debug_assert!(values_to_read - i < 32);

        self.reload_buffer_values();
        while i < values_to_read {
            batch[i] = self
                .get_value(num_bits)
                .expect("expected to have more data");
            i += 1;
        }

        values_to_read
    }

    /// Reads a `num_bytes`-sized value from this buffer and return it.
    /// `T` needs to be a little-endian native type. The value is assumed to be byte
    /// aligned so the bit reader will be advanced to the start of the next byte before
    /// reading the value.
    /// Returns `Some` if there's enough bytes left to form a value of `T`.
    /// Otherwise `None`.
    pub fn get_aligned<T: FromBytes>(&mut self, num_bytes: usize) -> Option<T> {
        debug_assert!(8 >= size_of::<T>());
        debug_assert!(num_bytes <= size_of::<T>());

        let bytes_read = self.bit_offset.div_ceil(8);
        if unlikely(self.byte_offset + bytes_read + num_bytes > self.total_bytes) {
            return None;
        }

        if bytes_read + num_bytes > 8 {
            // There may be still unread bytes in buffered_values; however, just reloading seems to
            // be faster than stitching the buffer with the next buffer based on micro benchmarks
            // because reloading logic can be simpler

            // Advance byte_offset to next unread byte
            self.byte_offset += bytes_read;
            // Reset buffered_values
            self.reload_buffer_values();
            self.bit_offset = 0
        } else {
            // Advance bit_offset to next unread byte
            self.bit_offset = bytes_read * 8;
        }

        let v = T::from(trailing_bits(
            self.buffered_values >> self.bit_offset,
            num_bytes * 8,
        ));
        self.bit_offset += num_bytes * 8;

        if self.bit_offset == 64 {
            self.byte_offset += 8;
            self.bit_offset -= 64;
            self.reload_buffer_values();
        }

        Some(v)
    }

    /// Reads a VLQ encoded (in little endian order) int from the stream.
    /// The encoded int must start at the beginning of a byte.
    ///
    /// Returns `None` if there's not enough bytes in the stream. `Some` otherwise.
    pub fn get_vlq_int(&mut self) -> Option<i64> {
        let mut shift = 0;
        let mut v: i64 = 0;
        while let Some(byte) = self.get_aligned::<u8>(1) {
            v |= ((byte & 0x7F) as i64) << shift;
            shift += 7;
            debug_assert!(
                shift <= MAX_VLQ_BYTE_LEN * 7,
                "Num of bytes exceed MAX_VLQ_BYTE_LEN ({MAX_VLQ_BYTE_LEN})"
            );
            if likely(byte & 0x80 == 0) {
                return Some(v);
            }
        }
        None
    }

    /// Reads a zigzag-VLQ encoded (in little endian order) int from the stream
    /// Zigzag-VLQ is a variant of VLQ encoding where negative and positive numbers are
    /// encoded in a zigzag fashion.
    /// See: https://developers.google.com/protocol-buffers/docs/encoding
    ///
    /// Note: the encoded int must start at the beginning of a byte.
    ///
    /// Returns `None` if the number of bytes there's not enough bytes in the stream.
    /// `Some` otherwise.
    #[inline]
    pub fn get_zigzag_vlq_int(&mut self) -> Option<i64> {
        self.get_vlq_int().map(|v| {
            let u = v as u64;
            (u >> 1) as i64 ^ -((u & 1) as i64)
        })
    }

    fn reload_buffer_values(&mut self) {
        let bytes_to_read = self.total_bytes - self.byte_offset;
        self.buffered_values = if 8 > bytes_to_read {
            read_num_bytes_u64(bytes_to_read, &self.buffer.as_slice()[self.byte_offset..])
        } else {
            read_u64(&self.buffer.as_slice()[self.byte_offset..])
        };
    }
}

impl From<Vec<u8>> for BitReader {
    #[inline]
    fn from(vec: Vec<u8>) -> Self {
        let len = vec.len();
        BitReader::new(Buffer::from(vec.as_slice()), len)
    }
}

/// Returns the nearest multiple of `factor` that is `>=` than `num`. Here `factor` must
/// be a power of 2.
///
/// Copied from the arrow crate to make arrow optional
pub fn round_upto_power_of_2(num: usize, factor: usize) -> usize {
    debug_assert!(factor > 0 && (factor & (factor - 1)) == 0);
    (num + (factor - 1)) & !(factor - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet::util::test_common::*;

    use rand::{
        distr::{Distribution, StandardUniform},
        Rng,
    };
    use std::fmt::Debug;

    #[test]
    fn test_read_num_bytes_u64() {
        let buffer: Vec<u8> = vec![0, 1, 2, 3, 4, 5, 6, 7];
        for size in 0..buffer.len() {
            assert_eq!(
                read_num_bytes_u64(size, &buffer),
                read_num_bytes!(u64, size, &buffer),
            );
        }
    }

    #[test]
    fn test_read_u64() {
        let buffer: Vec<u8> = vec![0, 1, 2, 3, 4, 5, 6, 7];
        assert_eq!(read_u64(&buffer), read_num_bytes!(u64, 8, &buffer),);
    }

    #[test]
    fn test_read_num_bytes_u32() {
        let buffer: Vec<u8> = vec![0, 1, 2, 3];
        for size in 0..buffer.len() {
            assert_eq!(
                read_num_bytes_u32(size, &buffer),
                read_num_bytes!(u32, size, &buffer),
            );
        }
    }

    #[test]
    fn test_read_u32() {
        let buffer: Vec<u8> = vec![0, 1, 2, 3];
        assert_eq!(read_u32(&buffer), read_num_bytes!(u32, 4, &buffer),);
    }

    #[test]
    fn test_bit_reader_get_byte_offset() {
        let buffer = vec![255; 10];
        let mut bit_reader = BitReader::from(buffer);
        assert_eq!(bit_reader.get_byte_offset(), 0); // offset (0 bytes, 0 bits)
        bit_reader.get_value::<i32>(6);
        assert_eq!(bit_reader.get_byte_offset(), 1); // offset (0 bytes, 6 bits)
        bit_reader.get_value::<i32>(10);
        assert_eq!(bit_reader.get_byte_offset(), 2); // offset (0 bytes, 16 bits)
        bit_reader.get_value::<i32>(20);
        assert_eq!(bit_reader.get_byte_offset(), 5); // offset (0 bytes, 36 bits)
        bit_reader.get_value::<i32>(30);
        assert_eq!(bit_reader.get_byte_offset(), 9); // offset (8 bytes, 2 bits)
    }

    #[test]
    fn test_bit_reader_get_value() {
        let buffer = vec![255, 0];
        let mut bit_reader = BitReader::from(buffer);
        assert_eq!(bit_reader.get_value::<i32>(1), Some(1));
        assert_eq!(bit_reader.get_value::<i32>(2), Some(3));
        assert_eq!(bit_reader.get_value::<i32>(3), Some(7));
        assert_eq!(bit_reader.get_value::<i32>(4), Some(3));
    }

    #[test]
    fn test_bit_reader_get_value_boundary() {
        let buffer = vec![10, 0, 0, 0, 20, 0, 30, 0, 0, 0, 40, 0];
        let mut bit_reader = BitReader::from(buffer);
        assert_eq!(bit_reader.get_value::<i64>(32), Some(10));
        assert_eq!(bit_reader.get_value::<i64>(16), Some(20));
        assert_eq!(bit_reader.get_value::<i64>(32), Some(30));
        assert_eq!(bit_reader.get_value::<i64>(16), Some(40));
    }

    #[test]
    fn test_bit_reader_get_aligned() {
        // 01110101 11001011
        let buffer = Buffer::from(&[0x75, 0xCB]);
        let mut bit_reader = BitReader::new_all(buffer.clone());
        assert_eq!(bit_reader.get_value::<i32>(3), Some(5));
        assert_eq!(bit_reader.get_aligned::<i32>(1), Some(203));
        assert_eq!(bit_reader.get_value::<i32>(1), None);
        bit_reader.reset(buffer);
        assert_eq!(bit_reader.get_aligned::<i32>(3), None);
    }

    #[test]
    fn test_bit_reader_get_vlq_int() {
        // 10001001 00000001 11110010 10110101 00000110
        let buffer: Vec<u8> = vec![0x89, 0x01, 0xF2, 0xB5, 0x06];
        let mut bit_reader = BitReader::from(buffer);
        assert_eq!(bit_reader.get_vlq_int(), Some(137));
        assert_eq!(bit_reader.get_vlq_int(), Some(105202));
    }

    #[test]
    fn test_bit_reader_get_zigzag_vlq_int() {
        let buffer: Vec<u8> = vec![0, 1, 2, 3];
        let mut bit_reader = BitReader::from(buffer);
        assert_eq!(bit_reader.get_zigzag_vlq_int(), Some(0));
        assert_eq!(bit_reader.get_zigzag_vlq_int(), Some(-1));
        assert_eq!(bit_reader.get_zigzag_vlq_int(), Some(1));
        assert_eq!(bit_reader.get_zigzag_vlq_int(), Some(-2));
    }

    #[test]
    fn test_set_bit() {
        let mut buffer = vec![0, 0, 0];
        set_bit(&mut buffer[..], 1);
        assert_eq!(buffer, vec![2, 0, 0]);
        set_bit(&mut buffer[..], 4);
        assert_eq!(buffer, vec![18, 0, 0]);
        unset_bit(&mut buffer[..], 1);
        assert_eq!(buffer, vec![16, 0, 0]);
        set_bit(&mut buffer[..], 10);
        assert_eq!(buffer, vec![16, 4, 0]);
        set_bit(&mut buffer[..], 10);
        assert_eq!(buffer, vec![16, 4, 0]);
        set_bit(&mut buffer[..], 11);
        assert_eq!(buffer, vec![16, 12, 0]);
        unset_bit(&mut buffer[..], 10);
        assert_eq!(buffer, vec![16, 8, 0]);
    }

    #[test]
    fn test_set_bits() {
        for offset in 0..=16 {
            for length in 0..=16 {
                let mut actual = vec![0, 0, 0, 0];
                set_bits(&mut actual[..], offset, length);
                let mut expected = vec![0, 0, 0, 0];
                for i in 0..length {
                    set_bit(&mut expected, offset + i);
                }
                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    fn test_get_bit() {
        // 00001101
        assert!(get_bit(&[0b00001101], 0));
        assert!(!get_bit(&[0b00001101], 1));
        assert!(get_bit(&[0b00001101], 2));
        assert!(get_bit(&[0b00001101], 3));

        // 01001001 01010010
        assert!(get_bit(&[0b01001001, 0b01010010], 0));
        assert!(!get_bit(&[0b01001001, 0b01010010], 1));
        assert!(!get_bit(&[0b01001001, 0b01010010], 2));
        assert!(get_bit(&[0b01001001, 0b01010010], 3));
        assert!(!get_bit(&[0b01001001, 0b01010010], 4));
        assert!(!get_bit(&[0b01001001, 0b01010010], 5));
        assert!(get_bit(&[0b01001001, 0b01010010], 6));
        assert!(!get_bit(&[0b01001001, 0b01010010], 7));
        assert!(!get_bit(&[0b01001001, 0b01010010], 8));
        assert!(get_bit(&[0b01001001, 0b01010010], 9));
        assert!(!get_bit(&[0b01001001, 0b01010010], 10));
        assert!(!get_bit(&[0b01001001, 0b01010010], 11));
        assert!(get_bit(&[0b01001001, 0b01010010], 12));
        assert!(!get_bit(&[0b01001001, 0b01010010], 13));
        assert!(get_bit(&[0b01001001, 0b01010010], 14));
        assert!(!get_bit(&[0b01001001, 0b01010010], 15));
    }

    #[test]
    fn test_log2() {
        assert_eq!(log2(1), 0);
        assert_eq!(log2(2), 1);
        assert_eq!(log2(3), 2);
        assert_eq!(log2(4), 2);
        assert_eq!(log2(5), 3);
        assert_eq!(log2(5), 3);
        assert_eq!(log2(6), 3);
        assert_eq!(log2(7), 3);
        assert_eq!(log2(8), 3);
        assert_eq!(log2(9), 4);
    }

    #[test]
    fn test_skip() {
        let mut writer = BitWriter::new(5);
        let old_offset = writer.skip(1).expect("skip() should return OK");
        writer.put_aligned(42, 4);
        writer.put_aligned_offset(0x10, 1, old_offset);
        let result = writer.consume();
        assert_eq!(result.as_ref(), [0x10, 42, 0, 0, 0]);

        writer = BitWriter::new(4);
        let result = writer.skip(5);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_next_byte_ptr() {
        let mut writer = BitWriter::new(5);
        {
            let first_byte = writer
                .get_next_byte_ptr(1)
                .expect("get_next_byte_ptr() should return OK");
            first_byte[0] = 0x10;
        }
        writer.put_aligned(42, 4);
        let result = writer.consume();
        assert_eq!(result.as_ref(), [0x10, 42, 0, 0, 0]);
    }

    #[test]
    fn test_consume_flush_buffer() {
        let mut writer1 = BitWriter::new(3);
        let mut writer2 = BitWriter::new(3);
        for i in 1..10 {
            writer1.put_value(i, 4);
            writer2.put_value(i, 4);
        }
        let res1 = writer1.flush_buffer();
        let res2 = writer2.consume();
        assert_eq!(res1, &res2[..]);
    }

    #[test]
    fn test_put_get_bool() {
        let len = 8;
        let mut writer = BitWriter::new(len);

        for i in 0..8 {
            let result = writer.put_value(i % 2, 1);
            assert!(result);
        }

        writer.flush();
        {
            let buffer = writer.buffer();
            assert_eq!(buffer[0], 0b10101010);
        }

        // Write 00110011
        for i in 0..8 {
            let result = match i {
                0 | 1 | 4 | 5 => writer.put_value(false as u64, 1),
                _ => writer.put_value(true as u64, 1),
            };
            assert!(result);
        }
        writer.flush();
        {
            let buffer = writer.buffer();
            assert_eq!(buffer[0], 0b10101010);
            assert_eq!(buffer[1], 0b11001100);
        }

        let mut reader = BitReader::from(writer.consume());

        for i in 0..8 {
            let val = reader
                .get_value::<u8>(1)
                .expect("get_value() should return OK");
            assert_eq!(val, i % 2);
        }

        for i in 0..8 {
            let val = reader
                .get_value::<bool>(1)
                .expect("get_value() should return OK");
            match i {
                0 | 1 | 4 | 5 => assert!(!val),
                _ => assert!(val),
            }
        }
    }

    #[test]
    fn test_put_value_roundtrip() {
        test_put_value_rand_numbers(32, 2);
        test_put_value_rand_numbers(32, 3);
        test_put_value_rand_numbers(32, 4);
        test_put_value_rand_numbers(32, 5);
        test_put_value_rand_numbers(32, 6);
        test_put_value_rand_numbers(32, 7);
        test_put_value_rand_numbers(32, 8);
        test_put_value_rand_numbers(64, 16);
        test_put_value_rand_numbers(64, 24);
        test_put_value_rand_numbers(64, 32);
    }

    fn test_put_value_rand_numbers(total: usize, num_bits: usize) {
        assert!(num_bits < 64);
        let num_bytes = num_bits.div_ceil(8);
        let mut writer = BitWriter::new(num_bytes * total);
        let values: Vec<u64> = random_numbers::<u64>(total)
            .iter()
            .map(|v| v & ((1 << num_bits) - 1))
            .collect();
        (0..total).for_each(|i| {
            assert!(
                writer.put_value(values[i], num_bits),
                "[{i}]: put_value() failed"
            );
        });

        let mut reader = BitReader::from(writer.consume());
        (0..total).for_each(|i| {
            let v = reader
                .get_value::<u64>(num_bits)
                .expect("get_value() should return OK");
            assert_eq!(
                v, values[i],
                "[{}]: expected {} but got {}",
                i, values[i], v
            );
        });
    }

    #[test]
    fn test_get_bits() {
        const NUM_BYTES: usize = 100;

        let mut vec = vec![0; NUM_BYTES];
        let total_num_bits = NUM_BYTES * 8;
        let v = random_bools(total_num_bits);
        (0..total_num_bits).for_each(|i| {
            if v[i] {
                set_bit(&mut vec, i);
            } else {
                unset_bit(&mut vec, i);
            }
        });

        let expected = vec.clone();

        // test reading the first time from a buffer
        for &(offset, num_bits) in [(0, 10), (2, 10), (8, 16), (25, 40), (7, 64)].iter() {
            let mut reader = BitReader::from(vec.clone());
            let mut buffer = vec![0; NUM_BYTES];

            let actual_bits_read = reader.get_bits(&mut buffer, offset, num_bits);
            let expected_bits_read = ::std::cmp::min(buffer.len() * 8 - offset, num_bits);
            assert_eq!(expected_bits_read, actual_bits_read);

            for i in 0..actual_bits_read {
                assert_eq!(get_bit(&expected, i), get_bit(&buffer, offset + i));
            }
        }

        // test reading consecutively from a buffer
        let mut reader = BitReader::from(vec);
        let mut buffer = vec![0; NUM_BYTES];
        let mut rng = rand::rng();
        let mut bits_read = 0;

        loop {
            if bits_read >= total_num_bits {
                break;
            }
            let n: u64 = rng.random();
            let num_bits = n % 20;
            bits_read += reader.get_bits(&mut buffer, bits_read, num_bits as usize);
        }

        assert_eq!(total_num_bits, bits_read);
        assert_eq!(&expected, &buffer);
    }

    #[test]
    fn test_skip_bits() {
        const NUM_BYTES: usize = 100;

        let mut vec = vec![0; NUM_BYTES];
        let total_num_bits = NUM_BYTES * 8;
        let v = random_bools(total_num_bits);
        (0..total_num_bits).for_each(|i| {
            if v[i] {
                set_bit(&mut vec, i);
            } else {
                unset_bit(&mut vec, i);
            }
        });

        let expected = vec.clone();

        // test skipping and check the next value
        let mut reader = BitReader::from(vec);
        let mut bits_read = 0;
        for &num_bits in [10, 60, 8].iter() {
            let actual_bits_read = reader.skip_bits(num_bits);
            assert_eq!(num_bits, actual_bits_read);

            bits_read += num_bits;
            assert_eq!(Some(get_bit(&expected, bits_read)), reader.get_value(1));
            bits_read += 1;
        }

        // test skipping consecutively
        let mut rng = rand::rng();
        loop {
            if bits_read >= total_num_bits {
                break;
            }
            let n: u64 = rng.random();
            let num_bits = n % 20;
            bits_read += reader.skip_bits(num_bits as usize);
        }

        assert_eq!(total_num_bits, bits_read);
    }

    #[test]
    fn test_get_batch() {
        const SIZE: &[usize] = &[1, 31, 32, 33, 128, 129];
        for s in SIZE {
            for i in 0..33 {
                match i {
                    0..=8 => test_get_batch_helper::<u8>(*s, i),
                    9..=16 => test_get_batch_helper::<u16>(*s, i),
                    _ => test_get_batch_helper::<u32>(*s, i),
                }
            }
        }
    }

    fn test_get_batch_helper<T>(total: usize, num_bits: usize)
    where
        T: FromBytes + Default + Clone + Debug + Eq,
    {
        assert!(num_bits <= 32);
        let num_bytes = num_bits.div_ceil(8);
        let mut writer = BitWriter::new(num_bytes * total);

        let values: Vec<u32> = random_numbers::<u32>(total)
            .iter()
            .map(|v| v & ((1u64 << num_bits) - 1) as u32)
            .collect();

        // Generic values used to check against actual values read from `get_batch`.
        let expected_values: Vec<T> = values.iter().map(|v| from_ne_slice(v.as_bytes())).collect();

        (0..total).for_each(|i| {
            assert!(writer.put_value(values[i] as u64, num_bits));
        });

        let buf = writer.consume();
        let mut reader = BitReader::from(buf);
        let mut batch = vec![T::default(); values.len()];
        let values_read = reader.get_batch::<T>(&mut batch, num_bits);
        assert_eq!(values_read, values.len());
        for i in 0..batch.len() {
            assert_eq!(
                batch[i], expected_values[i],
                "num_bits = {num_bits}, index = {i}"
            );
        }
    }

    #[test]
    fn test_get_u32_batch() {
        const SIZE: &[usize] = &[1, 31, 32, 33, 128, 129];
        for total in SIZE {
            for num_bits in 1..33 {
                let num_bytes = usize::div_ceil(num_bits, 8);
                let mut writer = BitWriter::new(num_bytes * total);

                let values: Vec<u32> = random_numbers::<u32>(*total)
                    .iter()
                    .map(|v| v & ((1u64 << num_bits) - 1) as u32)
                    .collect();

                (0..*total).for_each(|i| {
                    assert!(writer.put_value(values[i] as u64, num_bits));
                });

                let buf = writer.consume();
                let mut reader = BitReader::from(buf);
                let mut batch = vec![0u32; values.len()];
                unsafe {
                    reader.get_u32_batch(batch.as_mut_ptr(), *total, num_bits);
                }
                for i in 0..batch.len() {
                    assert_eq!(batch[i], values[i], "num_bits = {num_bits}, index = {i}");
                }
            }
        }
    }

    #[test]
    fn test_put_aligned_roundtrip() {
        test_put_aligned_rand_numbers::<u8>(4, 3);
        test_put_aligned_rand_numbers::<u8>(16, 5);
        test_put_aligned_rand_numbers::<i16>(32, 7);
        test_put_aligned_rand_numbers::<i16>(32, 9);
        test_put_aligned_rand_numbers::<i32>(32, 11);
        test_put_aligned_rand_numbers::<i32>(32, 13);
        test_put_aligned_rand_numbers::<i64>(32, 17);
        test_put_aligned_rand_numbers::<i64>(32, 23);
    }

    fn test_put_aligned_rand_numbers<T>(total: usize, num_bits: usize)
    where
        T: Copy + FromBytes + AsBytes + Debug + PartialEq,
        StandardUniform: Distribution<T>,
    {
        assert!(num_bits <= 32);
        assert_eq!(total % 2, 0);

        let aligned_value_byte_width = std::mem::size_of::<T>();
        let value_byte_width = num_bits.div_ceil(8);
        let mut writer =
            BitWriter::new((total / 2) * (aligned_value_byte_width + value_byte_width));
        let values: Vec<u32> = random_numbers::<u32>(total / 2)
            .iter()
            .map(|v| v & ((1 << num_bits) - 1))
            .collect();
        let aligned_values = random_numbers::<T>(total / 2);

        for i in 0..total {
            let j = i / 2;
            if i % 2 == 0 {
                assert!(
                    writer.put_value(values[j] as u64, num_bits),
                    "[{i}]: put_value() failed"
                );
            } else {
                assert!(
                    writer.put_aligned::<T>(aligned_values[j], aligned_value_byte_width),
                    "[{i}]: put_aligned() failed"
                );
            }
        }

        let mut reader = BitReader::from(writer.consume());
        for i in 0..total {
            let j = i / 2;
            if i % 2 == 0 {
                let v = reader
                    .get_value::<u64>(num_bits)
                    .expect("get_value() should return OK");
                assert_eq!(
                    v, values[j] as u64,
                    "[{}]: expected {} but got {}",
                    i, values[j], v
                );
            } else {
                let v = reader
                    .get_aligned::<T>(aligned_value_byte_width)
                    .expect("get_aligned() should return OK");
                assert_eq!(
                    v, aligned_values[j],
                    "[{}]: expected {:?} but got {:?}",
                    i, aligned_values[j], v
                );
            }
        }
    }

    #[test]
    fn test_put_vlq_int() {
        let total = 64;
        let mut writer = BitWriter::new(total * 32);
        let values = random_numbers::<u32>(total);
        (0..total).for_each(|i| {
            assert!(
                writer.put_vlq_int(values[i] as u64),
                "[{i}]; put_vlq_int() failed"
            );
        });

        let mut reader = BitReader::from(writer.consume());
        (0..total).for_each(|i| {
            let v = reader
                .get_vlq_int()
                .expect("get_vlq_int() should return OK");
            assert_eq!(
                v as u32, values[i],
                "[{}]: expected {} but got {}",
                i, values[i], v
            );
        });
    }

    #[test]
    fn test_put_zigzag_vlq_int() {
        let total = 64;
        let mut writer = BitWriter::new(total * 32);
        let values = random_numbers::<i32>(total);
        (0..total).for_each(|i| {
            assert!(
                writer.put_zigzag_vlq_int(values[i] as i64),
                "[{i}]; put_zigzag_vlq_int() failed"
            );
        });

        let mut reader = BitReader::from(writer.consume());
        (0..total).for_each(|i| {
            let v = reader
                .get_zigzag_vlq_int()
                .expect("get_zigzag_vlq_int() should return OK");
            assert_eq!(
                v as i32, values[i],
                "[{}]: expected {} but got {}",
                i, values[i], v
            );
        });
    }
}
