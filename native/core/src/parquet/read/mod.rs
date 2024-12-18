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

pub mod column;
pub mod levels;
pub mod values;

pub use column::ColumnReader;
use parquet::schema::types::ColumnDescPtr;

use super::ParquetMutableVector;
use crate::common::bit::{self, BitReader};
use arrow::buffer::Buffer;
use bytes::Buf;

/// Number of bytes to store a decimal value in Arrow value vector
pub(crate) const DECIMAL_BYTE_WIDTH: usize = 16;

#[derive(Clone, Copy)]
pub struct ReadOptions {
    // Whether to read legacy dates/timestamps as it is. If false, throw exceptions.
    pub(crate) use_legacy_date_timestamp_or_ntz: bool,
}

/// Internal states for PLAIN decoder. Used in combination of `PlainDecoding`.
pub struct PlainDecoderInner {
    /// The input buffer containing values to be decoded
    data: Buffer,

    /// The current offset in `data`, in bytes.
    offset: usize,

    /// Reads `data` bit by bit, used if `T` is [`BoolType`].
    bit_reader: BitReader,

    /// Options for reading Parquet
    read_options: ReadOptions,

    /// The Parquet column descriptor
    desc: ColumnDescPtr,
}

/// A trait for [`super::DataType`] to implement how PLAIN encoded data is to be decoded into Arrow
/// format given an input and output buffer.
///
/// The actual implementations of this trait is in `read/values.rs`.
pub trait PlainDecoding {
    /// Decodes `num` of items from `src`, and store the result into `dst`, in Arrow format.
    ///
    /// Note: this assumes the `src` has data for at least `num` elements, and won't do any
    /// bound checking. The condition MUST be guaranteed from the caller side.
    fn decode(src: &mut PlainDecoderInner, dst: &mut ParquetMutableVector, num: usize);

    /// Skip `num` of items from `src`
    ///
    /// Note: this assumes the `src` has data for at least `num` elements, and won't do any
    /// bound checking. The condition MUST be guaranteed from the caller side.
    fn skip(src: &mut PlainDecoderInner, num: usize);
}

pub trait PlainDictDecoding {
    /// Eagerly decode vector `src` which must have dictionary associated. The decoded values are
    /// appended into `dst`.
    fn decode_dict(src: ParquetMutableVector, dst: &mut ParquetMutableVector, bit_width: usize) {
        assert!(dst.dictionary.is_none());
        assert!(src.dictionary.is_some());

        let mut value_buf = src.value_buffer.as_slice();
        let validity_buf = src.validity_buffer.as_slice();
        let dictionary = src.dictionary.as_ref().unwrap();

        for i in 0..src.num_values {
            if bit::get_bit(validity_buf, i) {
                // non-null value: lookup the value position and copy its value into `dst`
                let val_idx = value_buf.get_u32_le();
                Self::decode_dict_one(i, val_idx as usize, dictionary, dst, bit_width);
                dst.num_values += 1;
            } else {
                value_buf.advance(4);
                dst.put_null();
            }
        }

        dst.validity_buffer = src.validity_buffer;
    }

    /// Decode a single value from `src`, whose position in the dictionary indices (i.e., keys)
    /// is `idx` and the positions in the dictionary values is `val_idx`. The decoded value is
    /// appended to `dst`.
    fn decode_dict_one(
        idx: usize,
        val_idx: usize,
        src: &ParquetMutableVector,
        dst: &mut ParquetMutableVector,
        bit_width: usize,
    );
}
