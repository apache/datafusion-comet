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

use std::{marker::PhantomData, mem};

use arrow::buffer::Buffer;
use bytes::Buf;
use log::debug;
use parquet::{basic::Encoding, schema::types::ColumnDescPtr};

use super::{PlainDecoderInner, PlainDecoding, PlainDictDecoding, ReadOptions};
use crate::write_null;
use crate::write_val_or_null;
use crate::{
    common::bit::{self, BitReader},
    parquet::{data_type::*, ParquetMutableVector},
};
use arrow::datatypes::DataType as ArrowDataType;
use datafusion_comet_spark_expr::utils::unlikely;

pub fn get_decoder<T: DataType>(
    value_data: Buffer,
    encoding: Encoding,
    desc: ColumnDescPtr,
    read_options: ReadOptions,
) -> Box<dyn Decoder> {
    let decoder: Box<dyn Decoder> = match encoding {
        Encoding::PLAIN | Encoding::PLAIN_DICTIONARY => {
            Box::new(PlainDecoder::<T>::new(value_data, desc, read_options))
        }
        // This is for dictionary indices
        Encoding::RLE_DICTIONARY => Box::new(DictDecoder::new(value_data)),
        _ => panic!("Unsupported encoding: {encoding}"),
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
    pub fn new(value_data: Buffer, desc: ColumnDescPtr, read_options: ReadOptions) -> Self {
        let len = value_data.len();
        let inner = PlainDecoderInner {
            data: value_data.clone(),
            offset: 0,
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
make_plain_dict_impl! { Int32DateType, Int64Type, FloatType, Int64ToDecimal64Type, FLBAType }
make_plain_dict_impl! { DoubleType, Int64TimestampMillisType, Int64TimestampMicrosType }

macro_rules! make_int_variant_dict_impl {
    ($ty:ty, $src_ty:ty, $dst_ty:ty) => {
        impl PlainDictDecoding for $ty {
            fn decode_dict_one(
                idx: usize,
                val_idx: usize,
                src: &ParquetMutableVector,
                dst: &mut ParquetMutableVector,
                _: usize,
            ) {
                let src_ptr = src.value_buffer.as_ptr() as *const $src_ty;
                let dst_ptr = dst.value_buffer.as_mut_ptr() as *mut $dst_ty;
                unsafe {
                    // SAFETY the caller must ensure `idx`th pointer is in bounds
                    dst_ptr
                        .add(idx)
                        .write_unaligned(src_ptr.add(val_idx).read_unaligned() as $dst_ty);
                }
            }
        }
    };
}

make_int_variant_dict_impl!(Int16ToDoubleType, i16, f64);
make_int_variant_dict_impl!(Int32To64Type, i32, i64);
make_int_variant_dict_impl!(Int32ToDecimal64Type, i32, i64);
make_int_variant_dict_impl!(Int32ToDoubleType, i32, f64);
make_int_variant_dict_impl!(Int32TimestampMicrosType, i32, i64);
make_int_variant_dict_impl!(FloatToDoubleType, f32, f64);
make_int_variant_dict_impl!(Int32DecimalType, i128, i128);
make_int_variant_dict_impl!(Int64DecimalType, i128, i128);
make_int_variant_dict_impl!(UInt64Type, u128, u128);

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
                         from Unix epoch date 1970-01-01), and could be ambigous depending on \
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

impl PlainDecoding for Int32TimestampMicrosType {
    #[inline]
    fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
        let src_data = &src.data;
        let byte_width = src.desc.type_length() as usize;
        let num_bytes = byte_width * num;

        {
            let mut offset = src.offset;
            for _ in 0..num {
                let v = src_data[offset..offset + byte_width].as_ptr() as *const i32;

                // TODO: optimize this further as checking value one by one is not very efficient
                unsafe {
                    if unlikely(v.read_unaligned() < JULIAN_GREGORIAN_SWITCH_OFF_DAY) {
                        panic!(
                            "Encountered timestamp value {}, which is before 1582-10-15 (counting \
                        backwards from Unix epoch date 1970-01-01), and could be ambigous \
                        depending on whether a legacy Julian/Gregorian hybrid calendar is used, \
                        or a Proleptic Gregorian calendar is used.",
                            *v
                        );
                    }
                }

                offset += byte_width;
            }
        }

        let mut offset = src.offset;
        let dst_byte_width = byte_width * 2;
        let mut dst_offset = dst_byte_width * dst.num_values;
        for _ in 0..num {
            let v = src_data[offset..offset + byte_width].as_ptr() as *const i32;
            let v = unsafe { v.read_unaligned() };
            let v = (v as i64).wrapping_mul(MICROS_PER_DAY);
            bit::memcpy_value(&v, dst_byte_width, &mut dst.value_buffer[dst_offset..]);
            offset += byte_width;
            dst_offset += dst_byte_width;
        }

        src.offset += num_bytes;
    }

    #[inline]
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
                            "Encountered timestamp value {v}, which is before 1582-10-15 (counting \
                         backwards from Unix epoch date 1970-01-01), and could be ambigous \
                         depending on whether a legacy Julian/Gregorian hybrid calendar is used, \
                         or a Proleptic Gregorian calendar is used."
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
                         backwards from Unix epoch date 1970-01-01), and could be ambigous \
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

macro_rules! make_int_variant_impl {
    ($dst_type:ty, $copy_fn:ident, $type_width:expr) => {
        impl PlainDecoding for $dst_type {
            fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
                let dst_slice = dst.value_buffer.as_slice_mut();
                let dst_offset = dst.num_values * $type_width;
                $copy_fn(&src.data[src.offset..], &mut dst_slice[dst_offset..], num);
                src.offset += 4 * num; // Parquet stores Int8/Int16 using 4 bytes
            }

            fn skip(src: &mut PlainDecoderInner, num: usize) {
                src.offset += 4 * num; // Parquet stores Int8/Int16 using 4 bytes
            }
        }
    };
}

make_int_variant_impl!(Int8Type, copy_i32_to_i8, 1);
make_int_variant_impl!(Int16Type, copy_i32_to_i16, 2);
make_int_variant_impl!(Int16ToDoubleType, copy_i32_to_f64, 8); // Parquet uses Int16 using 4 bytes
make_int_variant_impl!(Int32To64Type, copy_i32_to_i64, 8);
make_int_variant_impl!(Int32ToDoubleType, copy_i32_to_f64, 8);
make_int_variant_impl!(FloatToDoubleType, copy_f32_to_f64, 8);

// unsigned type require double the width and zeroes are written for the second half
// because they are implemented as the next size up signed type
make_int_variant_impl!(UInt8Type, copy_i32_to_u8, 2);
make_int_variant_impl!(UInt16Type, copy_i32_to_u16, 4);
make_int_variant_impl!(UInt32Type, copy_i32_to_u32, 8);

macro_rules! make_int_decimal_variant_impl {
    ($ty:ty, $copy_fn:ident, $type_width:expr, $dst_type:ty) => {
        impl PlainDecoding for $ty {
            fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
                let dst_slice = dst.value_buffer.as_slice_mut();
                let dst_offset = dst.num_values * std::mem::size_of::<$dst_type>();
                $copy_fn(&src.data[src.offset..], &mut dst_slice[dst_offset..], num);

                let src_precision = src.desc.type_precision() as u32;
                let src_scale = ::std::cmp::max(src.desc.type_scale(), 0) as u32;
                let (dst_precision, dst_scale) = match dst.arrow_type {
                    ArrowDataType::Decimal128(p, s) if s >= 0 => (p as u32, s as u32),
                    _ => unreachable!(),
                };
                let upper = (10 as $dst_type).pow(dst_precision);
                let v = dst_slice[dst_offset..].as_mut_ptr() as *mut $dst_type;
                if dst_scale > src_scale {
                    let mul = (10 as $dst_type).pow(dst_scale - src_scale);
                    for i in 0..num {
                        unsafe {
                            // SAFETY the caller must ensure `i`th pointer is in bounds
                            let v = v.add(i);
                            write_val_or_null!(v, v.read_unaligned() * mul, upper, dst, i);
                        }
                    }
                } else if dst_scale < src_scale {
                    let div = (10 as $dst_type).pow(src_scale - dst_scale);
                    for i in 0..num {
                        unsafe {
                            // SAFETY the caller must ensure `i`th pointer is in bounds
                            let v = v.add(i);
                            write_val_or_null!(v, v.read_unaligned() / div, upper, dst, i);
                        }
                    }
                } else if src_precision > dst_precision {
                    for i in 0..num {
                        unsafe {
                            // SAFETY the caller must ensure `i`th pointer is in bounds
                            let v = v.add(i);
                            write_null!(v.read_unaligned(), upper, dst, i);
                        }
                    }
                }

                src.offset += $type_width * num;
            }

            fn skip(src: &mut PlainDecoderInner, num: usize) {
                src.offset += $type_width * num;
            }
        }
    };
}
make_int_decimal_variant_impl!(Int32ToDecimal64Type, copy_i32_to_i64, 4, i64);
make_int_decimal_variant_impl!(Int32DecimalType, copy_i32_to_i128, 4, i128);
make_int_decimal_variant_impl!(Int64ToDecimal64Type, copy_i64_to_i64, 8, i64);
make_int_decimal_variant_impl!(Int64DecimalType, copy_i64_to_i128, 8, i128);
make_int_decimal_variant_impl!(UInt64Type, copy_u64_to_u128, 8, u128);

#[macro_export]
macro_rules! write_val_or_null {
    ($v: expr, $adjusted: expr, $upper: expr, $dst: expr, $i: expr) => {
        let adjusted = $adjusted;
        $v.write_unaligned(adjusted);
        write_null!(adjusted, $upper, $dst, $i);
    };
}

#[macro_export]
macro_rules! write_null {
    ($val: expr, $upper: expr, $dst: expr, $i: expr) => {
        if $upper <= $val {
            bit::unset_bit($dst.validity_buffer.as_slice_mut(), $dst.num_values + $i);
            $dst.num_nulls += 1;
        }
    };
}

macro_rules! generate_cast_to_unsigned {
    ($name: ident, $src_type:ty, $dst_type:ty, $zero_value:expr) => {
        pub fn $name(src: &[u8], dst: &mut [u8], num: usize) {
            debug_assert!(
                src.len() >= num * std::mem::size_of::<$src_type>(),
                "Source slice is too small"
            );
            debug_assert!(
                dst.len() >= num * std::mem::size_of::<$dst_type>() * 2,
                "Destination slice is too small"
            );

            let src_ptr = src.as_ptr() as *const $src_type;
            let dst_ptr = dst.as_mut_ptr() as *mut $dst_type;
            unsafe {
                for i in 0..num {
                    dst_ptr
                        .add(2 * i)
                        .write_unaligned(src_ptr.add(i).read_unaligned() as $dst_type);
                    // write zeroes
                    dst_ptr.add(2 * i + 1).write_unaligned($zero_value);
                }
            }
        }
    };
}

generate_cast_to_unsigned!(copy_i32_to_u32, i32, u32, 0_u32);

macro_rules! generate_cast_to_signed {
    ($name: ident, $src_type:ty, $dst_type:ty) => {
        pub fn $name(src: &[u8], dst: &mut [u8], num: usize) {
            debug_assert!(
                src.len() >= num * std::mem::size_of::<$src_type>(),
                "Source slice is too small"
            );
            debug_assert!(
                dst.len() >= num * std::mem::size_of::<$dst_type>(),
                "Destination slice is too small"
            );

            let src_ptr = src.as_ptr() as *const $src_type;
            let dst_ptr = dst.as_mut_ptr() as *mut $dst_type;
            unsafe {
                for i in 0..num {
                    dst_ptr
                        .add(i)
                        .write_unaligned(src_ptr.add(i).read_unaligned() as $dst_type);
                }
            }
        }
    };
}

generate_cast_to_signed!(copy_i32_to_i8, i32, i8);
generate_cast_to_signed!(copy_i32_to_i16, i32, i16);
generate_cast_to_signed!(copy_i32_to_i64, i32, i64);
generate_cast_to_signed!(copy_i32_to_i128, i32, i128);
generate_cast_to_signed!(copy_i32_to_f64, i32, f64);
generate_cast_to_signed!(copy_i64_to_i64, i64, i64);
generate_cast_to_signed!(copy_i64_to_i128, i64, i128);
generate_cast_to_signed!(copy_u64_to_u128, u64, u128);
generate_cast_to_signed!(copy_f32_to_f64, f32, f64);
// even for u8/u16, need to copy full i16/i32 width for Spark compatibility
generate_cast_to_signed!(copy_i32_to_u8, i32, i16);
generate_cast_to_signed!(copy_i32_to_u16, i32, i32);

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

macro_rules! make_plain_decimal_int_impl {
    ($($ty: ident; $dst_type:ty), *) => {
        $(
            impl PlainDecoding for $ty {
                fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
                    let num_bytes = std::mem::size_of::<$dst_type>();
                    let byte_width = src.desc.type_length() as usize;
                    let num_bits = num_bytes.saturating_sub(byte_width) * 8;

                    let src_data = &src.data[src.offset..];
                    let dst_data = &mut dst.value_buffer[dst.num_values * num_bytes..];

                    let mut src_offset = 0;

                    let src_precision = src.desc.type_precision() as u32;
                    let src_scale = ::std::cmp::max(src.desc.type_scale(), 0) as u32;
                    let (dst_precision, dst_scale) = match dst.arrow_type {
                        ArrowDataType::Decimal128(p, s) if s >= 0 => (p as u32, s as u32),
                        _ => (src_precision, src_scale),
                    };
                    let upper = (10 as $dst_type).pow(dst_precision);
                    let mul_div = (10 as $dst_type).pow(dst_scale.abs_diff(src_scale));

                    for i in 0..num {
                        let mut unscaled: $dst_type = 0;
                        for _ in 0..byte_width {
                            unscaled = unscaled << 8 | src_data[src_offset] as $dst_type;
                            src_offset += 1;
                        }
                        unscaled = (unscaled << num_bits) >> num_bits;
                        if dst_scale > src_scale {
                            unscaled *= mul_div;
                        } else if dst_scale < src_scale {
                            unscaled /= mul_div;
                        }
                        bit::memcpy_value(&unscaled, num_bytes, &mut dst_data[i * num_bytes..]);
                        if src_precision > dst_precision {
                            write_null!(unscaled, upper, dst, i);
                        }
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
                    let num_bytes = std::mem::size_of::<$dst_type>();
                    bit::memcpy(
                        &src.value_buffer[val_idx * num_bytes..(val_idx + 1) * num_bytes],
                        &mut dst.value_buffer[dst.num_values * num_bytes..],
                    );
                }
            }
        )*
    };
}

make_plain_decimal_int_impl!(FLBADecimal32Type; i32, FLBADecimal64Type; i64, FLBADecimalType; i128);

// Int96 contains 12 bytes
const INT96_SRC_BYTE_WIDTH: usize = 12;
// We convert INT96 to micros and store using i64
const INT96_DST_BYTE_WIDTH: usize = 8;

fn int96_to_microsecond(v: &[u8]) -> i64 {
    let nanos = &v[..INT96_DST_BYTE_WIDTH] as *const [u8] as *const u8 as *const i64;
    let day = &v[INT96_DST_BYTE_WIDTH..] as *const [u8] as *const u8 as *const i32;

    unsafe {
        ((day.read_unaligned() - JULIAN_DAY_OF_EPOCH) as i64)
            .wrapping_mul(MICROS_PER_DAY)
            .wrapping_add(nanos.read_unaligned() / 1000)
    }
}

/// Decode timestamps represented as INT96 into i64 with micros precision
impl PlainDecoding for Int96TimestampMicrosType {
    #[inline]
    fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize) {
        let src_data = &src.data;

        if !src.read_options.use_legacy_date_timestamp_or_ntz {
            let mut offset = src.offset;
            for _ in 0..num {
                // TODO: optimize this further as checking value one by one is not very efficient
                let micros = int96_to_microsecond(&src_data[offset..offset + INT96_SRC_BYTE_WIDTH]);

                if unlikely(micros < JULIAN_GREGORIAN_SWITCH_OFF_TS) {
                    panic!(
                        "Encountered timestamp value {micros}, which is before 1582-10-15 (counting \
                         backwards from Unix epoch date 1970-01-01), and could be ambigous \
                         depending on whether a legacy Julian/Gregorian hybrid calendar is used, \
                         or a Proleptic Gregorian calendar is used."
                    );
                }

                offset += INT96_SRC_BYTE_WIDTH;
            }
        }

        let mut offset = src.offset;
        let mut dst_offset = INT96_DST_BYTE_WIDTH * dst.num_values;
        for _ in 0..num {
            let micros = int96_to_microsecond(&src_data[offset..offset + INT96_SRC_BYTE_WIDTH]);

            bit::memcpy_value(
                &micros,
                INT96_DST_BYTE_WIDTH,
                &mut dst.value_buffer[dst_offset..],
            );

            offset += INT96_SRC_BYTE_WIDTH;
            dst_offset += INT96_DST_BYTE_WIDTH;
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
    pub fn new(buf: Buffer) -> Self {
        let bit_width = buf.as_bytes()[0] as usize;

        Self {
            bit_width,
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

#[cfg(test)]
mod test {
    use super::*;
    use parquet::data_type::AsBytes;

    #[test]
    fn test_i32_to_i8() {
        let source =
            hex::decode("8a000000dbffffff1800000034ffffff300000001d000000abffffff37fffffff1000000")
                .unwrap();
        let expected = hex::decode("8adb1834301dab37f1").unwrap();
        let num = source.len() / 4;
        let mut dest: Vec<u8> = vec![b' '; num];
        copy_i32_to_i8(source.as_bytes(), dest.as_mut_slice(), num);
        assert_eq!(expected.as_bytes(), dest.as_bytes());
    }

    #[test]
    fn test_i32_to_u8() {
        let source =
            hex::decode("8a000000dbffffff1800000034ffffff300000001d000000abffffff37fffffff1000000")
                .unwrap();
        let expected = hex::decode("8a00dbff180034ff30001d00abff37fff100").unwrap();
        let num = source.len() / 4;
        let mut dest: Vec<u8> = vec![b' '; num * 2];
        copy_i32_to_u8(source.as_bytes(), dest.as_mut_slice(), num);
        assert_eq!(expected.as_bytes(), dest.as_bytes());
    }

    #[test]
    fn test_i32_to_i16() {
        let source =
            hex::decode("8a0e0000db93ffff1826000034f4ffff300200001d2b0000abe3ffff378dfffff1470000")
                .unwrap();
        let expected = hex::decode("8a0edb93182634f430021d2babe3378df147").unwrap();
        let num = source.len() / 4;
        let mut dest: Vec<u8> = vec![b' '; num * 2];
        copy_i32_to_i16(source.as_bytes(), dest.as_mut_slice(), num);
        assert_eq!(expected.as_bytes(), dest.as_bytes());
    }

    #[test]
    fn test_i32_to_u16() {
        let source = hex::decode(
            "ff7f0000008000000180000002800000038000000480000005800000068000000780000008800000",
        )
        .unwrap();
        let expected = hex::decode(
            "ff7f0000008000000180000002800000038000000480000005800000068000000780000008800000",
        )
        .unwrap();
        let num = source.len() / 4;
        let mut dest: Vec<u8> = vec![b' '; num * 4];
        copy_i32_to_u16(source.as_bytes(), dest.as_mut_slice(), num);
        assert_eq!(expected.as_bytes(), dest.as_bytes());
    }

    #[test]
    fn test_i32_to_u32() {
        let source = hex::decode(
            "ffffff7f000000800100008002000080030000800400008005000080060000800700008008000080",
        )
        .unwrap();
        let expected = hex::decode("ffffff7f00000000000000800000000001000080000000000200008000000000030000800000000004000080000000000500008000000000060000800000000007000080000000000800008000000000").unwrap();
        let num = source.len() / 4;
        let mut dest: Vec<u8> = vec![b' '; num * 8];
        copy_i32_to_u32(source.as_bytes(), dest.as_mut_slice(), num);
        assert_eq!(expected.as_bytes(), dest.as_bytes());
    }
}
