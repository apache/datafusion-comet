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

use std::mem;

use super::values::Decoder;
use crate::{
    common::bit::{self, read_u32, BitReader},
    parquet::ParquetMutableVector,
};
use arrow::buffer::Buffer;
use datafusion_comet_spark_expr::utils::unlikely;
use parquet::schema::types::ColumnDescPtr;

const INITIAL_BUF_LEN: usize = 16;

enum Mode {
    RLE,
    BitPacked,
}

/// A decoder for Parquet definition & repetition levels.
pub struct LevelDecoder {
    /// The descriptor of the column that this level decoder is associated to.
    desc: ColumnDescPtr,
    /// Number of bits used to represent the levels.
    bit_width: u8,
    /// Mode for the current run.
    mode: Mode,
    /// Total number of values (including both null and non-null) to be decoded.
    num_values: usize,
    /// The current value in a RLE run. Unused if BitPacked.
    current_value: i32,
    /// The number of total values in the current RLE run. Unused if BitPacked.
    current_count: usize,
    /// The current buffer used in a BitPacked run. Unused if RLE.
    /// This will be resized if the total number of values in the BitPacked run is larger than its
    /// capacity.
    current_buffer: Vec<i32>, // TODO: double check this
    /// The index into `current_buffer` in a BitPacked run. Unused if RLE.
    current_buffer_idx: usize,
    /// A bit reader wrapping the input buffer for levels.
    bit_reader: Option<BitReader>,
    /// Whether we need to read the length of data. This is typically true for Parquet page V1, but
    /// not for V2, since it uses separate buffer for definition & repetition levels.
    need_length: bool,
}

impl LevelDecoder {
    pub fn new(desc: ColumnDescPtr, bit_width: u8, need_length: bool) -> Self {
        Self {
            desc,
            bit_width,
            mode: Mode::RLE,
            num_values: 0,
            current_value: 0,
            current_count: 0,
            current_buffer: vec![0; INITIAL_BUF_LEN],
            current_buffer_idx: 0,
            bit_reader: None,
            need_length,
        }
    }

    /// Sets data for this level decoder, and returns total number of bytes consumed. This is used
    /// for reading DataPage v1 levels.
    pub fn set_data(&mut self, page_value_count: usize, page_data: &Buffer) -> usize {
        self.num_values = page_value_count;
        if self.bit_width == 0 {
            // Special case where the page doesn't have encoded rl/dl data. Here we'll treat it as
            // an RLE run of `page_value_count` number of 0s.
            self.mode = Mode::RLE;
            self.current_count = page_value_count;
            0
        } else if self.need_length {
            let u32_size = mem::size_of::<u32>();
            let data_size = read_u32(page_data.as_slice()) as usize;
            self.bit_reader = Some(BitReader::new(page_data.slice(u32_size), data_size));
            u32_size + data_size
        } else {
            // No need to read length, just read the whole buffer
            self.bit_reader = Some(BitReader::new_all(page_data.to_owned()));
            0
        }
    }

    /// Reads a batch of `total` values into `vector`. The value decoding is done by
    /// `value_decoder`.
    pub fn read_batch(
        &mut self,
        total: usize,
        vector: &mut ParquetMutableVector,
        value_decoder: &mut dyn Decoder,
    ) {
        let mut left = total;
        while left > 0 {
            if unlikely(self.current_count == 0) {
                self.read_next_group();
            }

            debug_assert!(self.current_count > 0);

            let n = ::std::cmp::min(left, self.current_count);
            let max_def_level = self.desc.max_def_level();

            match self.mode {
                Mode::RLE => {
                    if self.current_value as i16 == max_def_level {
                        bit::set_bits(vector.validity_buffer.as_slice_mut(), vector.num_values, n);
                        value_decoder.read_batch(vector, n);
                        vector.num_values += n;
                    } else {
                        vector.put_nulls(n);
                    }
                }
                Mode::BitPacked => {
                    for i in 0..n {
                        if self.current_buffer[self.current_buffer_idx + i] == max_def_level as i32
                        {
                            bit::set_bit(vector.validity_buffer.as_slice_mut(), vector.num_values);
                            value_decoder.read(vector);
                            vector.num_values += 1;
                        } else {
                            vector.put_null();
                        }
                    }
                    self.current_buffer_idx += n;
                }
            }

            left -= n;
            self.current_count -= n;
        }
    }

    /// Skips a batch of `total` values. The value decoding is done by `value_decoder`.
    pub fn skip_batch(
        &mut self,
        total: usize,
        vector: &mut ParquetMutableVector,
        value_decoder: &mut dyn Decoder,
        put_nulls: bool,
    ) {
        let mut skip = total;
        while skip > 0 {
            if unlikely(self.current_count == 0) {
                self.read_next_group();
            }

            debug_assert!(self.current_count > 0);

            let n = ::std::cmp::min(skip, self.current_count);
            let max_def_level = self.desc.max_def_level();

            match self.mode {
                Mode::RLE => {
                    if self.current_value as i16 == max_def_level {
                        value_decoder.skip_batch(n);
                    }
                }
                Mode::BitPacked => {
                    let mut num_skips = 0;
                    for i in 0..n {
                        if self.current_buffer[self.current_buffer_idx + i] == max_def_level as i32
                        {
                            num_skips += 1;
                        }
                    }
                    value_decoder.skip_batch(num_skips);
                    self.current_buffer_idx += n;
                }
            }
            if put_nulls {
                vector.put_nulls(n);
            }

            skip -= n;
            self.current_count -= n;
        }
    }

    /// Loads the next group from this RLE/BitPacked hybrid reader.
    fn read_next_group(&mut self) {
        let bit_reader = self.bit_reader.as_mut().expect("bit_reader should be set");
        if let Some(indicator_value) = bit_reader.get_vlq_int() {
            self.mode = if indicator_value & 1 == 1 {
                Mode::BitPacked
            } else {
                Mode::RLE
            };

            match self.mode {
                Mode::BitPacked => {
                    self.current_count = ((indicator_value >> 1) * 8) as usize;
                    if self.current_buffer.len() < self.current_count {
                        self.current_buffer.resize(self.current_count, 0);
                    }
                    self.current_buffer_idx = 0;
                    bit_reader.get_batch(
                        &mut self.current_buffer[..self.current_count],
                        self.bit_width as usize,
                    );
                }
                Mode::RLE => {
                    // RLE
                    self.current_count = (indicator_value >> 1) as usize;
                    let value_width = (self.bit_width as usize).div_ceil(8);
                    self.current_value = bit_reader
                        .get_aligned::<i32>(value_width)
                        .expect("current value should be set");
                }
            }
        }
    }
}
