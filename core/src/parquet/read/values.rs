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

use std::{marker::PhantomData, mem, ptr::copy_nonoverlapping};

use arrow::buffer::Buffer;
use bytes::Buf;
use log::debug;
use parquet::{basic::Encoding, schema::types::ColumnDescPtr};

use super::{PlainDecoderInner, PlainDecoding, PlainDictDecoding, ReadOptions};
use crate::{
    common::bit::{self, BitReader},
    parquet::{data_type::*, read::DECIMAL_BYTE_WIDTH, ParquetMutableVector},
    unlikely,
};

pub fn get_decoder<T: DataType>(
    value_data: Buffer,
    num_values: usize,
    encoding: Encoding,
    desc: ColumnDescPtr,
    read_options: ReadOptions,
) -> Box<dyn Decoder> {
    let decoder: Box<dyn Decoder> = match encoding {
        Encoding::PLAIN | Encoding::PLAIN_DICTIONARY => Box::new(PlainDecoder::<T>::new(
            value_data,
            num_values,
            desc,
            read_options,
        )),
        // This is for dictionary indices
        Encoding::RLE_DICTIONARY => Box::new(DictDecoder::new(value_data, num_values)),
        _ => panic!("Unsupported encoding: {}", encoding),
    };
    decoder
}

/// A Parquet decoder for values within a Parquet data page.
pub trait Decoder {
    /// Consumes a single value from the decoder and stores it into `dst`.
    ///
    /// # Preconditions
    ///
    /// * `dst` have enough length to hold at least one value.
    /// * `data` of this decoder should have enough bytes left to be decoded.
    fn read(&mut self, dst: &mut ParquetMutableVector);

    /// Consumes a batch of `num` values from the data and stores  them to `dst`.
    ///
    /// # Preconditions
    ///
    /// * `dst` should have length >= `num * T::type_size()` .
    /// * `data` of this decoder should have >= `num * T::type_size()` bytes left to be decoded.
    fn read_batch(&mut self, dst: &mut ParquetMutableVector, num: usize);

    /// Skips a batch of `num` values from the data.
    ///
    /// # Preconditions
    ///
    /// * `data` of this decoder should have >= `num * T::type_size()` bytes left to be decoded.
    fn skip_batch(&mut self, num: usize);

    /// Returns the encoding for this decoder.
    fn encoding(&self) -> Encoding;
}

/// The switch off date between Julian and Gregorian calendar. See
///   https://docs.oracle.com/javase/7/docs/api/java/util/GregorianCalendar.html
const JULIAN_GREGORIAN_SWITCH_OFF_DAY: i32 = -141427;

/// The switch off timestamp (in micros) between Julian and Gregorian calendar. See
///   https://docs.oracle.com/javase/7/docs/api/java/util/GregorianCalendar.html
const JULIAN_GREGORIAN_SWITCH_OFF_TS: i64 = -2208988800000000;

/// See http://stackoverflow.com/questions/466321/convert-unix-timestamp-to-julian
/// Also see Spark's `DateTimeUtils.JULIAN_DAY_OF_EPOCH`
const JULIAN_DAY_OF_EPOCH: i32 = 2440588;

/// Number of micro seconds per milli second.
const MICROS_PER_MILLIS: i64 = 1000;

const MICROS_PER_DAY: i64 = 24_i64 * 60 * 60 * 1000 * 1000;

pub struct PlainDecoder<T: DataType> {
    /// Internal states for this decoder.
    inner: PlainDecoderInner,

    /// Marker to allow `T` in the generic parameter of the struct.
    _phantom: PhantomData<T>,
}

impl<T: DataType> PlainDecoder<T> {
    pub fn new(
        value_data: Buffer,
        num_values: usize,
        desc: ColumnDescPtr,
        read_options: ReadOptions,
    ) -> Self {
        let len = value_data.len();
        let inner = PlainDecoderInner {
            data: value_data.clone(),
            offset: 0,
            value_count: num_values,
            bit_reader: BitReader::new(value_data, len),
            read_options,
            desc,
        };
        Self {
            inner,
            _phantom: PhantomData,
        }
    }
}

macro_rules! make_plain_default_impl {
    ($($ty: ident), *) => {
        $(
            impl PlainDecoding for $ty {
                /// Default implementation for PLAIN encoding. Uses `mempcy` when the physical
                /// layout is the same between Parquet and Arrow.
                fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
                    let src_data = &src.data;
                    let byte_width = src.desc.type_length() as usize;
                    let num_bytes = byte_width * num;
                    let dst_offset = byte_width * dst.num_values;

                    bit::memcpy(
                        &src_data[src.offset..src.offset + num_bytes],
                        &mut dst.value_buffer[dst_offset..]);
                    src.offset += num_bytes;
                }

                fn skip(src: &mut PlainDecoderInner, num: usize) {
                    let num_bytes = src.desc.type_length() as usize * num;
                    src.offset += num_bytes;
                }
            }
        )*
    };
}

make_plain_default_impl! { Int32Type, Int64Type, FloatType, DoubleType, FLBAType }

macro_rules! make_plain_dict_impl {
    ($($ty: ident), *) => {
        $(
            impl PlainDictDecoding for $ty {
                fn decode_dict_one(
                    idx: usize,
                    val_idx: usize,
                    src: &ParquetMutableVector,
                    dst: &mut ParquetMutableVector,
                    bit_width: usize,
                ) {
                    let byte_width = bit_width / 8;
                    bit::memcpy(
                        &src.value_buffer[val_idx * byte_width..(val_idx+1) * byte_width],
                        &mut dst.value_buffer[idx * byte_width..],
                    );
                }
            }
        )*
    };
}

make_plain_dict_impl! { Int8Type, UInt8Type, Int16Type, UInt16Type, Int32Type, UInt32Type }
make_plain_dict_impl! { Int32DateType, Int64Type, FloatType, FLBAType }
make_plain_dict_impl! { DoubleType, Int64TimestampMillisType, Int64TimestampMicrosType }

impl PlainDecoding for Int32To64Type {
    fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
        let src_ptr = src.data.as_ptr() as *const i32;
        let dst_ptr = dst.value_buffer.as_mut_ptr() as *mut i64;
        unsafe {
            for i in 0..num {
                dst_ptr
                    .add(dst.num_values + i)
                    .write_unaligned(src_ptr.add(src.offset + i).read_unaligned() as i64);
            }
        }
        src.offset += 4 * num;
    }

    fn skip(src: &mut PlainDecoderInner, num: usize) {
        src.offset += 4 * num;
    }
}

impl PlainDictDecoding for Int32To64Type {
    fn decode_dict_one(
        idx: usize,
        val_idx: usize,
        src: &ParquetMutableVector,
        dst: &mut ParquetMutableVector,
        _: usize,
    ) {
        let src_ptr = src.value_buffer.as_ptr() as *const i32;
        let dst_ptr = dst.value_buffer.as_mut_ptr() as *mut i64;
        unsafe {
            dst_ptr
                .add(idx)
                .write_unaligned(src_ptr.add(val_idx).read_unaligned() as i64);
        }
    }
}

impl PlainDecoding for FloatToDoubleType {
    fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
        let src_ptr = src.data.as_ptr() as *const f32;
        let dst_ptr = dst.value_buffer.as_mut_ptr() as *mut f64;
        unsafe {
            for i in 0..num {
                dst_ptr
                    .add(dst.num_values + i)
                    .write_unaligned(src_ptr.add(src.offset + i).read_unaligned() as f64);
            }
        }
        src.offset += 4 * num;
    }

    fn skip(src: &mut PlainDecoderInner, num: usize) {
        src.offset += 4 * num;
    }
}

impl PlainDictDecoding for FloatToDoubleType {
    fn decode_dict_one(
        idx: usize,
        val_idx: usize,
        src: &ParquetMutableVector,
        dst: &mut ParquetMutableVector,
        _: usize,
    ) {
        let src_ptr = src.value_buffer.as_ptr() as *const f32;
        let dst_ptr = dst.value_buffer.as_mut_ptr() as *mut f64;
        unsafe {
            dst_ptr
                .add(idx)
                .write_unaligned(src_ptr.add(val_idx).read_unaligned() as f64);
        }
    }
}

impl PlainDecoding for Int32DateType {
    fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
        let src_data = &src.data;
        let byte_width = src.desc.type_length() as usize;
        let num_bytes = byte_width * num;
        let dst_offset = byte_width * dst.num_values;

        if !src.read_options.use_legacy_date_timestamp_or_ntz {
            // By default we panic if the date value is before the switch date between Julian
            // calendar and Gregorian calendar, which is 1582-10-15, and -141727 days
            // before the unix epoch date 1970-01-01.
            let mut offset = src.offset;
            for _ in 0..num {
                let v = &src_data[offset..offset + byte_width] as *const [u8] as *const u8
                    as *const i32;

                // TODO: optimize this further as checking value one by one is not very efficient
                unsafe {
                    if unlikely(v.read_unaligned() < JULIAN_GREGORIAN_SWITCH_OFF_DAY) {
                        panic!(
                        "Encountered date value {}, which is before 1582-10-15 (counting backwards \
                         from Unix eopch date 1970-01-01), and could be ambigous depending on \
                         whether a legacy Julian/Gregorian hybrid calendar is used, or a Proleptic \
                         Gregorian calendar is used.",
                        *v
                    );
                    }
                }

                offset += byte_width;
            }
        }

        bit::memcpy(
            &src_data[src.offset..src.offset + num_bytes],
            &mut dst.value_buffer[dst_offset..],
        );

        src.offset += num_bytes;
    }

    fn skip(src: &mut PlainDecoderInner, num: usize) {
        let num_bytes = src.desc.type_length() as usize * num;
        src.offset += num_bytes;
    }
}

impl PlainDecoding for Int64TimestampMillisType {
    #[inline]
    fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
        let src_data = &src.data;
        let byte_width = src.desc.type_length() as usize;
        let num_bytes = byte_width * num;

        if !src.read_options.use_legacy_date_timestamp_or_ntz {
            let mut offset = src.offset;
            for _ in 0..num {
                unsafe {
                    let v = &src_data[offset..offset + byte_width] as *const [u8] as *const u8
                        as *const i64;
                    let v = v.read_unaligned() * MICROS_PER_MILLIS;

                    // TODO: optimize this further as checking value one by one is not very
                    // efficient
                    if unlikely(v < JULIAN_GREGORIAN_SWITCH_OFF_TS) {
                        panic!(
                            "Encountered timestamp value {}, which is before 1582-10-15 (counting \
                         backwards from Unix eopch date 1970-01-01), and could be ambigous \
                         depending on whether a legacy Julian/Gregorian hybrid calendar is used, \
                         or a Proleptic Gregorian calendar is used.",
                            v
                        );
                    }

                    offset += byte_width;
                }
            }
        }

        unsafe {
            let mut offset = src.offset;
            let mut dst_offset = byte_width * dst.num_values;
            for _ in 0..num {
                let v = &src_data[offset..offset + byte_width] as *const [u8] as *const u8
                    as *const i64;
                let v = v.read_unaligned() * MICROS_PER_MILLIS;
                bit::memcpy_value(&v, byte_width, &mut dst.value_buffer[dst_offset..]);
                offset += byte_width;
                dst_offset += byte_width;
            }
        }

        src.offset += num_bytes;
    }

    #[inline]
    fn skip(src: &mut PlainDecoderInner, num: usize) {
        let num_bytes = src.desc.type_length() as usize * num;
        src.offset += num_bytes;
    }
}

impl PlainDecoding for Int64TimestampMicrosType {
    #[inline]
    fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
        let src_data = &src.data;
        let byte_width = src.desc.type_length() as usize;
        let num_bytes = byte_width * num;
        let dst_offset = byte_width * dst.num_values;

        if !src.read_options.use_legacy_date_timestamp_or_ntz {
            let mut offset = src.offset;
            for _ in 0..num {
                unsafe {
                    let v = &src_data[offset..offset + byte_width] as *const [u8] as *const u8
                        as *const i64;

                    // TODO: optimize this further as checking value one by one is not very
                    // efficient
                    if unlikely(v.read_unaligned() < JULIAN_GREGORIAN_SWITCH_OFF_TS) {
                        panic!(
                            "Encountered timestamp value {}, which is before 1582-10-15 (counting \
                         backwards from Unix eopch date 1970-01-01), and could be ambigous \
                         depending on whether a legacy Julian/Gregorian hybrid calendar is used, \
                         or a Proleptic Gregorian calendar is used.",
                            *v
                        );
                    }

                    offset += byte_width;
                }
            }
        }

        bit::memcpy(
            &src_data[src.offset..src.offset + num_bytes],
            &mut dst.value_buffer[dst_offset..],
        );

        src.offset += num_bytes;
    }

    #[inline]
    fn skip(src: &mut PlainDecoderInner, num: usize) {
        let num_bytes = src.desc.type_length() as usize * num;
        src.offset += num_bytes;
    }
}

impl PlainDecoding for BoolType {
    /// Specific implementation for PLAIN encoding of boolean type. Even though both Parquet and
    /// Arrow share the same physical layout for the type (which is 1 bit for each value), we'll
    /// need to treat the number of bytes specifically.
    #[inline]
    fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
        src.bit_reader
            .get_bits(&mut dst.value_buffer, dst.num_values, num);
    }

    #[inline]
    fn skip(src: &mut PlainDecoderInner, num: usize) {
        src.bit_reader.skip_bits(num);
    }
}

// Does it make sense to encode booleans with dictionary?
impl PlainDictDecoding for BoolType {
    #[inline]
    fn decode_dict_one(
        idx: usize,
        val_idx: usize,
        src: &ParquetMutableVector,
        dst: &mut ParquetMutableVector,
        _: usize,
    ) {
        let v = bit::get_bit(src.value_buffer.as_slice(), val_idx);
        if v {
            bit::set_bit(dst.value_buffer.as_slice_mut(), idx);
        } // `dst` should be zero initialized so no need to call `unset_bit`.
    }
}

// Shared implementation for int variants such as Int8 and Int16
macro_rules! make_int_variant_impl {
    ($ty: ident, $native_ty: ty, $type_size: expr) => {
        impl PlainDecoding for $ty {
            fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
                let num_bytes = 4 * num; // Parquet stores Int8/Int16 using 4 bytes

                let src_data = &src.data;
                let mut src_offset = src.offset;
                let dst_slice = dst.value_buffer.as_slice_mut();
                let mut dst_offset = dst.num_values * $type_size;

                let mut i = 0;
                let mut in_ptr = &src_data[src_offset..] as *const [u8] as *const u8 as *const u32;

                while num - i >= 32 {
                    unsafe {
                        let in_slice = std::slice::from_raw_parts(in_ptr, 32);

                        for n in 0..32 {
                            copy_nonoverlapping(
                                in_slice[n..].as_ptr() as *const $native_ty,
                                &mut dst_slice[dst_offset] as *mut u8 as *mut $native_ty,
                                1,
                            );
                            i += 1;
                            dst_offset += $type_size;
                        }
                        in_ptr = in_ptr.offset(32);
                    }
                }

                src_offset += i * 4;

                (0..(num - i)).for_each(|_| {
                    unsafe {
                        copy_nonoverlapping(
                            &src_data[src_offset..] as *const [u8] as *const u8
                                as *const $native_ty,
                            &mut dst_slice[dst_offset] as *mut u8 as *mut $native_ty,
                            1,
                        );
                    }
                    src_offset += 4;
                    dst_offset += $type_size;
                });

                src.offset += num_bytes;
            }

            fn skip(src: &mut PlainDecoderInner, num: usize) {
                let num_bytes = 4 * num; // Parquet stores Int8/Int16 using 4 bytes
                src.offset += num_bytes;
            }
        }
    };
}

make_int_variant_impl!(Int8Type, i8, 1);
make_int_variant_impl!(UInt8Type, u8, 2);
make_int_variant_impl!(Int16Type, i16, 2);
make_int_variant_impl!(UInt16Type, u16, 4);
make_int_variant_impl!(UInt32Type, u32, 8);

// Shared implementation for variants of Binary type
macro_rules! make_plain_binary_impl {
    ($($ty: ident), *) => {
        $(
            impl PlainDecoding for $ty {
                fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
                    let src_data = &src.data;
                    let mut src_offset = src.offset;

                    let mut offset_offset = dst.num_values * 4;
                    let offset_buf = &mut dst.value_buffer.as_slice_mut();
                    let mut offset_value = read_num_bytes!(i32, 4, &offset_buf[offset_offset..]);
                    offset_offset += 4;

                    // The actual content of a byte array is stored contiguously in the child vector
                    let child = &mut dst.children[0];
                    let mut value_offset = child.num_values; // num_values == num of bytes

                    (0..num).for_each(|_| {
                        let len = read_num_bytes!(i32, 4, &src_data[src_offset..]) as usize;
                        offset_value += len as i32;

                        // Copy offset for the current string value into the offset buffer
                        bit::memcpy_value(&offset_value, 4, &mut offset_buf[offset_offset..]);

                        // Reserve additional space in child value buffer if not enough
                        let value_buf_len = child.value_buffer.len();

                        if unlikely(value_buf_len < value_offset + len) {
                            let new_capacity = ::std::cmp::max(value_offset + len, value_buf_len * 2);
                            debug!("Reserving additional space ({} -> {} bytes) for value buffer",
                                   value_buf_len, new_capacity);
                            child.value_buffer.resize(new_capacity);
                        }

                        // Copy the actual string content into the value buffer
                        src_offset += mem::size_of::<u32>();
                        bit::memcpy(
                            &src_data[src_offset..src_offset + len],
                            &mut child.value_buffer.as_slice_mut()[value_offset..],
                        );

                        value_offset += len;
                        src_offset += len;
                        offset_offset += 4;
                    });

                    src.offset = src_offset;
                    child.num_values = value_offset;
                }

                fn skip(src: &mut PlainDecoderInner, num: usize) {
                    let src_data = &src.data;
                    let mut src_offset = src.offset;

                    (0..num).for_each(|_| {
                        let len = read_num_bytes!(i32, 4, &src_data[src_offset..]) as usize;
                        src_offset += mem::size_of::<u32>();
                        src_offset += len;
                    });

                    src.offset = src_offset;
                }
            }
        )*
    };
}

make_plain_binary_impl! { ByteArrayType, StringType }

macro_rules! make_plain_dict_binary_impl {
    ($($ty: ident), *) => {
        $(
            impl PlainDictDecoding for $ty {
                #[inline]
                fn decode_dict_one(
                    idx: usize,
                    val_idx: usize,
                    src: &ParquetMutableVector,
                    dst: &mut ParquetMutableVector,
                    _: usize,
                ) {
                    debug_assert!(src.children.len() == 1);
                    debug_assert!(dst.children.len() == 1);

                    let src_child = &src.children[0];
                    let dst_child = &mut dst.children[0];

                    // get the offset & data for the binary value at index `val_idx`
                    let mut start_slice = &src.value_buffer[val_idx * 4..];
                    let start = start_slice.get_u32_le() as usize;
                    let mut end_slice = &src.value_buffer[(val_idx + 1) * 4..];
                    let end = end_slice.get_u32_le() as usize;

                    debug_assert!(end >= start);

                    let len = end - start;
                    let curr_offset = read_num_bytes!(u32, 4, &dst.value_buffer[idx * 4..]) as usize;

                    // Reserve additional space in child value buffer if not enough
                    let value_buf_len = dst_child.value_buffer.len();

                    if unlikely(value_buf_len < curr_offset + len) {
                        let new_capacity = ::std::cmp::max(curr_offset + len, value_buf_len * 2);
                        debug!("Reserving additional space ({} -> {} bytes) for value buffer \
                                during dictionary fallback", value_buf_len,
                               new_capacity);
                        dst_child.value_buffer.resize(new_capacity);
                    }

                    bit::memcpy(
                        &src_child.value_buffer[start..end],
                        &mut dst_child.value_buffer[curr_offset..],
                    );

                    bit::memcpy_value(
                        &((curr_offset + len) as u32),
                        4,
                        &mut dst.value_buffer[(idx + 1) * 4..],
                    );

                    dst_child.num_values += len;
                }
            }
        )*
    };
}

make_plain_dict_binary_impl! { ByteArrayType, StringType }

macro_rules! make_plain_decimal_impl {
    ($is_signed: expr, $($ty: ident; $need_convert: expr), *) => {
        $(
            impl PlainDecoding for $ty {
                fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
                    let byte_width = src.desc.type_length() as usize;

                    let src_data = &src.data[src.offset..];
                    let dst_data = &mut dst.value_buffer[dst.num_values * DECIMAL_BYTE_WIDTH..];

                    let mut src_offset = 0;
                    let mut dst_offset = 0;

                    debug_assert!(byte_width <= DECIMAL_BYTE_WIDTH);

                    for _ in 0..num {
                        let s = &mut dst_data[dst_offset..];

                        bit::memcpy(
                            &src_data[src_offset..src_offset + byte_width],
                            s,
                        );

                        // Swap the order of bytes to make it little-endian.
                        if $need_convert {
                            for i in 0..byte_width / 2 {
                                s.swap(i, byte_width - i - 1);
                            }
                        }

                        if $is_signed {
                            // Check if the most significant bit is 1 (negative in 2's complement).
                            // If so, also fill pad the remaining bytes with 0xff.
                            if s[byte_width - 1] & 0x80 == 0x80 {
                                s[byte_width..DECIMAL_BYTE_WIDTH].fill(0xff);
                            }
                        }

                        src_offset += byte_width;
                        dst_offset += DECIMAL_BYTE_WIDTH;
                    }

                    src.offset += num * byte_width;
                }

                #[inline]
                fn skip(src: &mut PlainDecoderInner, num: usize) {
                    let num_bytes_to_skip = num * src.desc.type_length() as usize;
                    src.offset += num_bytes_to_skip;
                }
            }

            impl PlainDictDecoding for $ty {
                fn decode_dict_one(_: usize, val_idx: usize, src: &ParquetMutableVector, dst: &mut ParquetMutableVector, _: usize) {
                    let src_offset = val_idx * DECIMAL_BYTE_WIDTH;
                    let dst_offset = dst.num_values * DECIMAL_BYTE_WIDTH;

                    bit::memcpy(
                        &src.value_buffer[src_offset..src_offset + DECIMAL_BYTE_WIDTH],
                        &mut dst.value_buffer[dst_offset..dst_offset + DECIMAL_BYTE_WIDTH],
                    );
                }
            }
        )*
    }
}

make_plain_decimal_impl!(true, Int32DecimalType; false, Int64DecimalType; false, FLBADecimalType; true);
make_plain_decimal_impl!(false, UInt64Type; false);

macro_rules! make_plain_decimal_int_impl {
    ($($ty: ident; $num_bytes: expr), *) => {
        $(
            impl PlainDecoding for $ty {
                fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
                    let byte_width = src.desc.type_length() as usize;
                    let num_bits = 8 * byte_width;

                    let src_data = &src.data[src.offset..];
                    let dst_data = &mut dst.value_buffer[dst.num_values * $num_bytes..];

                    let mut src_offset = 0;

                    for i in 0..num {
                        let mut unscaled: i64 = 0;
                        for _ in 0..byte_width {
                            unscaled = unscaled << 8 | src_data[src_offset] as i64;
                            src_offset += 1;
                        }
                        unscaled = (unscaled << (64 - num_bits)) >> (64 - num_bits);
                        bit::memcpy_value(&unscaled, $num_bytes, &mut dst_data[i *
                        $num_bytes..]);
                    }

                    src.offset += num * byte_width;
                }

                fn skip(src: &mut PlainDecoderInner, num: usize) {
                    let num_bytes_to_skip = num * src.desc.type_length() as usize;
                    src.offset += num_bytes_to_skip;
                }
            }

            impl PlainDictDecoding for $ty {
                #[inline]
                fn decode_dict_one(_: usize, val_idx: usize, src: &ParquetMutableVector, dst: &mut ParquetMutableVector, _: usize) {
                    bit::memcpy(
                        &src.value_buffer[val_idx * $num_bytes..(val_idx + 1) * $num_bytes],
                        &mut dst.value_buffer[dst.num_values * $num_bytes..],
                    );
                }
            }
        )*
    };
}

make_plain_decimal_int_impl!(FLBADecimal32Type; 4, FLBADecimal64Type; 8);

// Int96 contains 12 bytes
const INT96_SRC_BYTE_WIDTH: usize = 12;
// We convert INT96 to micros and store using i64
const INT96_DST_BYTE_WIDTH: usize = 8;

/// Decode timestamps represented as INT96 into i64 with micros precision
impl PlainDecoding for Int96TimestampMicrosType {
    #[inline]
    fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
        let src_data = &src.data;

        if !src.read_options.use_legacy_date_timestamp_or_ntz {
            let mut offset = src.offset;
            for _ in 0..num {
                let v = &src_data[offset..offset + INT96_SRC_BYTE_WIDTH];
                let nanos = &v[..INT96_DST_BYTE_WIDTH] as *const [u8] as *const u8 as *const i64;
                let day = &v[INT96_DST_BYTE_WIDTH..] as *const [u8] as *const u8 as *const i32;

                // TODO: optimize this further as checking value one by one is not very efficient
                unsafe {
                    let micros = (day.read_unaligned() - JULIAN_DAY_OF_EPOCH) as i64
                        * MICROS_PER_DAY
                        + nanos.read_unaligned() / 1000;

                    if unlikely(micros < JULIAN_GREGORIAN_SWITCH_OFF_TS) {
                        panic!(
                            "Encountered timestamp value {}, which is before 1582-10-15 (counting \
                         backwards from Unix eopch date 1970-01-01), and could be ambigous \
                         depending on whether a legacy Julian/Gregorian hybrid calendar is used, \
                         or a Proleptic Gregorian calendar is used.",
                            micros
                        );
                    }

                    offset += INT96_SRC_BYTE_WIDTH;
                }
            }
        }

        let mut offset = src.offset;
        let mut dst_offset = INT96_DST_BYTE_WIDTH * dst.num_values;
        unsafe {
            for _ in 0..num {
                let v = &src_data[offset..offset + INT96_SRC_BYTE_WIDTH];
                let nanos = &v[..INT96_DST_BYTE_WIDTH] as *const [u8] as *const u8 as *const i64;
                let day = &v[INT96_DST_BYTE_WIDTH..] as *const [u8] as *const u8 as *const i32;

                let micros = (day.read_unaligned() - JULIAN_DAY_OF_EPOCH) as i64 * MICROS_PER_DAY
                    + nanos.read_unaligned() / 1000;

                bit::memcpy_value(
                    &micros,
                    INT96_DST_BYTE_WIDTH,
                    &mut dst.value_buffer[dst_offset..],
                );

                offset += INT96_SRC_BYTE_WIDTH;
                dst_offset += INT96_DST_BYTE_WIDTH;
            }
        }

        src.offset = offset;
    }

    #[inline]
    fn skip(src: &mut PlainDecoderInner, num: usize) {
        src.offset += INT96_SRC_BYTE_WIDTH * num;
    }
}

impl PlainDictDecoding for Int96TimestampMicrosType {
    fn decode_dict_one(
        _: usize,
        val_idx: usize,
        src: &ParquetMutableVector,
        dst: &mut ParquetMutableVector,
        _: usize,
    ) {
        let src_offset = val_idx * INT96_DST_BYTE_WIDTH;
        let dst_offset = dst.num_values * INT96_DST_BYTE_WIDTH;

        bit::memcpy(
            &src.value_buffer[src_offset..src_offset + INT96_DST_BYTE_WIDTH],
            &mut dst.value_buffer[dst_offset..dst_offset + INT96_DST_BYTE_WIDTH],
        );
    }
}

impl<T: DataType> Decoder for PlainDecoder<T> {
    #[inline]
    fn read(&mut self, dst: &mut ParquetMutableVector) {
        self.read_batch(dst, 1)
    }

    /// Default implementation for PLAIN encoding, which uses a `memcpy` to copy from Parquet to the
    /// Arrow vector. NOTE: this only works if the Parquet physical type has the same type width as
    /// the Arrow's physical type (e.g., Parquet INT32 vs Arrow INT32). For other cases, we should
    /// have special implementations.
    #[inline]
    fn read_batch(&mut self, dst: &mut ParquetMutableVector, num: usize) {
        T::decode(&mut self.inner, dst, num);
    }

    #[inline]
    fn skip_batch(&mut self, num: usize) {
        T::skip(&mut self.inner, num);
    }

    #[inline]
    fn encoding(&self) -> Encoding {
        Encoding::PLAIN
    }
}

/// A decoder for Parquet dictionary indices, which is always of integer type, and encoded with
/// RLE/BitPacked encoding.
pub struct DictDecoder {
    /// Number of bits used to represent dictionary indices. Must be between `[0, 64]`.
    bit_width: usize,

    /// The number of total values in `data`
    value_count: usize,

    /// Bit reader
    bit_reader: BitReader,

    /// Number of values left in the current RLE run
    rle_left: usize,

    /// Number of values left in the current BIT_PACKED run
    bit_packed_left: usize,

    /// Current value in the RLE run. Unused if BIT_PACKED
    current_value: u32,
}

impl DictDecoder {
    pub fn new(buf: Buffer, num_values: usize) -> Self {
        let bit_width = buf.as_bytes()[0] as usize;

        Self {
            bit_width,
            value_count: num_values,
            bit_reader: BitReader::new_all(buf.slice(1)),
            rle_left: 0,
            bit_packed_left: 0,
            current_value: 0,
        }
    }
}

impl DictDecoder {
    /// Reads the header of the next RLE/BitPacked run, and update the internal state such as # of
    /// values for the next run, as well as the current value in case it's RLE.
    fn reload(&mut self) {
        if let Some(indicator_value) = self.bit_reader.get_vlq_int() {
            if indicator_value & 1 == 1 {
                self.bit_packed_left = ((indicator_value >> 1) * 8) as usize;
            } else {
                self.rle_left = (indicator_value >> 1) as usize;
                let value_width = bit::ceil(self.bit_width, 8);
                self.current_value = self.bit_reader.get_aligned::<u32>(value_width).unwrap();
            }
        } else {
            panic!("Can't read VLQ int from BitReader");
        }
    }
}

impl Decoder for DictDecoder {
    fn read(&mut self, dst: &mut ParquetMutableVector) {
        let dst_slice = dst.value_buffer.as_slice_mut();
        let dst_offset = dst.num_values * 4;

        // We've finished the current run. Now load the next.
        if self.rle_left == 0 && self.bit_packed_left == 0 {
            self.reload();
        }

        let value = if self.rle_left > 0 {
            self.rle_left -= 1;
            self.current_value
        } else {
            self.bit_packed_left -= 1;
            self.bit_reader.get_u32_value(self.bit_width)
        };

        // Directly copy the value into the destination buffer
        unsafe {
            let dst = &mut dst_slice[dst_offset..] as *mut [u8] as *mut u8 as *mut u32;
            *dst = value;
        }
    }

    fn read_batch(&mut self, dst: &mut ParquetMutableVector, num: usize) {
        let mut values_read = 0;
        let dst_slice = dst.value_buffer.as_slice_mut();
        let mut dst_offset = dst.num_values * 4;

        while values_read < num {
            let num_to_read = num - values_read;
            let mut dst = &mut dst_slice[dst_offset..] as *mut [u8] as *mut u8 as *mut u32;

            if self.rle_left > 0 {
                let n = std::cmp::min(num_to_read, self.rle_left);
                unsafe {
                    // Copy the current RLE value into the destination buffer.
                    for _ in 0..n {
                        *dst = self.current_value;
                        dst = dst.offset(1);
                    }
                    dst_offset += 4 * n;
                }
                self.rle_left -= n;
                values_read += n;
            } else if self.bit_packed_left > 0 {
                let n = std::cmp::min(num_to_read, self.bit_packed_left);
                unsafe {
                    // Decode the next `n` BitPacked values into u32 and put the result directly to
                    // `dst`.
                    self.bit_reader.get_u32_batch(dst, n, self.bit_width);
                }
                dst_offset += 4 * n;
                self.bit_packed_left -= n;
                values_read += n;
            } else {
                self.reload();
            }
        }
    }

    fn skip_batch(&mut self, num: usize) {
        let mut values_skipped = 0;

        while values_skipped < num {
            let num_to_skip = num - values_skipped;

            if self.rle_left > 0 {
                let n = std::cmp::min(num_to_skip, self.rle_left);
                self.rle_left -= n;
                values_skipped += n;
            } else if self.bit_packed_left > 0 {
                let n = std::cmp::min(num_to_skip, self.bit_packed_left);
                self.bit_reader.skip_bits(n * self.bit_width);
                self.bit_packed_left -= n;
                values_skipped += n;
            } else {
                self.reload();
            }
        }
    }

    fn encoding(&self) -> Encoding {
        Encoding::RLE_DICTIONARY
    }
}
