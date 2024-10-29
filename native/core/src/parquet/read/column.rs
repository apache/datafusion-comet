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

use std::{marker::PhantomData, sync::Arc};

use arrow::{
    array::ArrayData,
    buffer::{Buffer, MutableBuffer},
    datatypes::{ArrowNativeType, DataType as ArrowDataType, TimeUnit},
};

use parquet::{
    basic::{Encoding, LogicalType, TimeUnit as ParquetTimeUnit, Type as PhysicalType},
    schema::types::{ColumnDescPtr, ColumnDescriptor},
};

use crate::parquet::{
    data_type::*, read::DECIMAL_BYTE_WIDTH, util::jni::TypePromotionInfo, ParquetMutableVector,
};

use super::{
    levels::LevelDecoder,
    values::{get_decoder, Decoder},
    ReadOptions,
};

use crate::common::{bit, bit::log2};
use crate::execution::operators::ExecutionError;

/// Maximum number of decimal digits an i32 can represent
const DECIMAL_MAX_INT_DIGITS: i32 = 9;

/// Maximum number of decimal digits an i64 can represent
const DECIMAL_MAX_LONG_DIGITS: i32 = 18;

pub enum ColumnReader {
    BoolColumnReader(TypedColumnReader<BoolType>),
    Int8ColumnReader(TypedColumnReader<Int8Type>),
    UInt8ColumnReader(TypedColumnReader<UInt8Type>),
    Int16ColumnReader(TypedColumnReader<Int16Type>),
    Int16ToDoubleColumnReader(TypedColumnReader<Int16ToDoubleType>),
    UInt16ColumnReader(TypedColumnReader<UInt16Type>),
    Int32ColumnReader(TypedColumnReader<Int32Type>),
    Int32To64ColumnReader(TypedColumnReader<Int32To64Type>),
    Int32ToDecimal64ColumnReader(TypedColumnReader<Int32ToDecimal64Type>),
    Int32ToDoubleColumnReader(TypedColumnReader<Int32ToDoubleType>),
    UInt32ColumnReader(TypedColumnReader<UInt32Type>),
    Int32DecimalColumnReader(TypedColumnReader<Int32DecimalType>),
    Int32DateColumnReader(TypedColumnReader<Int32DateType>),
    Int32TimestampMicrosColumnReader(TypedColumnReader<Int32TimestampMicrosType>),
    Int64ColumnReader(TypedColumnReader<Int64Type>),
    Int64ToDecimal64ColumnReader(TypedColumnReader<Int64ToDecimal64Type>),
    UInt64DecimalColumnReader(TypedColumnReader<UInt64Type>),
    Int64DecimalColumnReader(TypedColumnReader<Int64DecimalType>),
    Int64TimestampMillisColumnReader(TypedColumnReader<Int64TimestampMillisType>),
    Int64TimestampMicrosColumnReader(TypedColumnReader<Int64TimestampMicrosType>),
    Int64TimestampNanosColumnReader(TypedColumnReader<Int64Type>),
    Int96ColumnReader(TypedColumnReader<Int96TimestampMicrosType>),
    FloatColumnReader(TypedColumnReader<FloatType>),
    FloatToDoubleColumnReader(TypedColumnReader<FloatToDoubleType>),
    DoubleColumnReader(TypedColumnReader<DoubleType>),
    ByteArrayColumnReader(TypedColumnReader<ByteArrayType>),
    StringColumnReader(TypedColumnReader<StringType>),
    FLBADecimalColumnReader(TypedColumnReader<FLBADecimalType>),
    FLBADecimal32ColumnReader(TypedColumnReader<FLBADecimal32Type>),
    FLBADecimal64ColumnReader(TypedColumnReader<FLBADecimal64Type>),
    FLBAColumnReader(TypedColumnReader<FLBAType>),
}

impl ColumnReader {
    /// Creates a new column reader according to the input `desc`.
    ///
    /// - `desc`: The actual descriptor for the underlying Parquet files
    /// - `promotion_info`: Extra information about type promotion. This is passed in to support
    ///   schema evolution, e.g., int -> long, where Parquet type is int but Spark type is long.
    /// - `use_decimal_128`: Whether to read small precision decimals as `i128` instead of as `i32`
    ///   or `i64` as Spark does
    /// - `use_legacy_date_timestamp_or_ntz`: Whether to read dates/timestamps that were written
    ///   using the legacy Julian/Gregorian hybrid calendar as it is. If false, exceptions will be
    ///   thrown. If the spark type is TimestampNTZ, this should be true.
    pub fn get(
        desc: ColumnDescriptor,
        promotion_info: TypePromotionInfo,
        capacity: usize,
        use_decimal_128: bool,
        use_legacy_date_timestamp_or_ntz: bool,
    ) -> Self {
        let read_options = ReadOptions {
            use_legacy_date_timestamp_or_ntz,
        };
        macro_rules! typed_reader {
            ($reader_ty:ident, $arrow_ty:ident) => {
                Self::$reader_ty(TypedColumnReader::new(
                    desc,
                    capacity,
                    ArrowDataType::$arrow_ty,
                    read_options,
                ))
            };
            ($reader_ty:ident, $arrow_ty:expr) => {
                Self::$reader_ty(TypedColumnReader::new(
                    desc,
                    capacity,
                    $arrow_ty,
                    read_options,
                ))
            };
        }

        match desc.physical_type() {
            PhysicalType::BOOLEAN => typed_reader!(BoolColumnReader, Boolean),
            PhysicalType::INT32 => {
                if let Some(ref logical_type) = desc.logical_type() {
                    match logical_type {
                        lt @ LogicalType::Integer {
                            bit_width,
                            is_signed,
                        } => match (bit_width, is_signed) {
                            (8, true) => match promotion_info.physical_type {
                                PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                                    if promotion_info.precision <= DECIMAL_MAX_INT_DIGITS
                                        && promotion_info.scale < 1
                                    {
                                        typed_reader!(Int32ColumnReader, Int32)
                                    } else if promotion_info.precision <= DECIMAL_MAX_LONG_DIGITS {
                                        typed_reader!(
                                            Int32ToDecimal64ColumnReader,
                                            ArrowDataType::Decimal128(
                                                promotion_info.precision as u8,
                                                promotion_info.scale as i8
                                            )
                                        )
                                    } else {
                                        typed_reader!(
                                            Int32DecimalColumnReader,
                                            ArrowDataType::Decimal128(
                                                promotion_info.precision as u8,
                                                promotion_info.scale as i8
                                            )
                                        )
                                    }
                                }
                                _ => typed_reader!(Int8ColumnReader, Int8),
                            },
                            (8, false) => typed_reader!(UInt8ColumnReader, Int16),
                            (16, true) => match promotion_info.physical_type {
                                PhysicalType::DOUBLE => {
                                    typed_reader!(Int16ToDoubleColumnReader, Float64)
                                }
                                PhysicalType::INT32 if promotion_info.bit_width == 32 => {
                                    typed_reader!(Int32ColumnReader, Int32)
                                }
                                PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                                    if promotion_info.precision <= DECIMAL_MAX_INT_DIGITS
                                        && promotion_info.scale < 1
                                    {
                                        typed_reader!(Int32ColumnReader, Int32)
                                    } else if promotion_info.precision <= DECIMAL_MAX_LONG_DIGITS {
                                        typed_reader!(
                                            Int32ToDecimal64ColumnReader,
                                            ArrowDataType::Decimal128(
                                                promotion_info.precision as u8,
                                                promotion_info.scale as i8
                                            )
                                        )
                                    } else {
                                        typed_reader!(
                                            Int32DecimalColumnReader,
                                            ArrowDataType::Decimal128(
                                                promotion_info.precision as u8,
                                                promotion_info.scale as i8
                                            )
                                        )
                                    }
                                }
                                _ => typed_reader!(Int16ColumnReader, Int16),
                            },
                            (16, false) => typed_reader!(UInt16ColumnReader, Int32),
                            (32, true) => match promotion_info.physical_type {
                                PhysicalType::INT32 if promotion_info.bit_width == 16 => {
                                    typed_reader!(Int16ColumnReader, Int16)
                                }
                                _ => typed_reader!(Int32ColumnReader, Int32),
                            },
                            (32, false) => typed_reader!(UInt32ColumnReader, Int64),
                            _ => unimplemented!("Unsupported INT32 annotation: {:?}", lt),
                        },
                        LogicalType::Decimal {
                            scale: _,
                            precision: _,
                        } => {
                            if use_decimal_128 || promotion_info.precision > DECIMAL_MAX_LONG_DIGITS
                            {
                                typed_reader!(
                                    Int32DecimalColumnReader,
                                    ArrowDataType::Decimal128(
                                        promotion_info.precision as u8,
                                        promotion_info.scale as i8
                                    )
                                )
                            } else {
                                typed_reader!(
                                    Int32ToDecimal64ColumnReader,
                                    ArrowDataType::Decimal128(
                                        promotion_info.precision as u8,
                                        promotion_info.scale as i8
                                    )
                                )
                            }
                        }
                        LogicalType::Date => match promotion_info.physical_type {
                            PhysicalType::INT64 => typed_reader!(
                                Int32TimestampMicrosColumnReader,
                                ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
                            ),
                            _ => typed_reader!(Int32DateColumnReader, Date32),
                        },
                        lt => unimplemented!("Unsupported logical type for INT32: {:?}", lt),
                    }
                } else {
                    // We support type promotion from int to long
                    match promotion_info.physical_type {
                        PhysicalType::INT32 if promotion_info.bit_width == 16 => {
                            typed_reader!(Int16ColumnReader, Int16)
                        }
                        PhysicalType::INT32 => typed_reader!(Int32ColumnReader, Int32),
                        PhysicalType::INT64 => typed_reader!(Int32To64ColumnReader, Int64),
                        PhysicalType::DOUBLE => typed_reader!(Int32ToDoubleColumnReader, Float64),
                        PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                            if promotion_info.precision <= DECIMAL_MAX_INT_DIGITS
                                && promotion_info.scale < 1
                            {
                                typed_reader!(Int32ColumnReader, Int32)
                            } else if promotion_info.precision <= DECIMAL_MAX_LONG_DIGITS {
                                typed_reader!(
                                    Int32ToDecimal64ColumnReader,
                                    ArrowDataType::Decimal128(
                                        promotion_info.precision as u8,
                                        promotion_info.scale as i8
                                    )
                                )
                            } else {
                                typed_reader!(
                                    Int32DecimalColumnReader,
                                    ArrowDataType::Decimal128(
                                        promotion_info.precision as u8,
                                        promotion_info.scale as i8
                                    )
                                )
                            }
                        }
                        t => unimplemented!("Unsupported read physical type for INT32: {}", t),
                    }
                }
            }
            PhysicalType::INT64 => {
                if let Some(ref logical_type) = desc.logical_type() {
                    match logical_type {
                        lt @ LogicalType::Integer {
                            bit_width,
                            is_signed,
                        } => match (bit_width, is_signed) {
                            (64, true) => typed_reader!(Int64ColumnReader, Int64),
                            (64, false) => typed_reader!(
                                UInt64DecimalColumnReader,
                                ArrowDataType::Decimal128(20u8, 0i8)
                            ),
                            _ => panic!("Unsupported INT64 annotation: {:?}", lt),
                        },
                        LogicalType::Decimal {
                            scale: _,
                            precision: _,
                        } => {
                            if use_decimal_128 || promotion_info.precision > DECIMAL_MAX_LONG_DIGITS
                            {
                                typed_reader!(
                                    Int64DecimalColumnReader,
                                    ArrowDataType::Decimal128(
                                        promotion_info.precision as u8,
                                        promotion_info.scale as i8
                                    )
                                )
                            } else {
                                typed_reader!(
                                    Int64ToDecimal64ColumnReader,
                                    ArrowDataType::Decimal128(
                                        promotion_info.precision as u8,
                                        promotion_info.scale as i8
                                    )
                                )
                            }
                        }
                        LogicalType::Timestamp {
                            is_adjusted_to_u_t_c,
                            unit,
                        } => {
                            // To be consistent with Spark, we always store as micro-second and
                            // convert milli-second to it.
                            let time_unit = TimeUnit::Microsecond;
                            let time_zone = if *is_adjusted_to_u_t_c {
                                Some("UTC".to_string().into())
                            } else {
                                None
                            };
                            match unit {
                                ParquetTimeUnit::MILLIS(_) => {
                                    typed_reader!(
                                        Int64TimestampMillisColumnReader,
                                        ArrowDataType::Timestamp(time_unit, time_zone)
                                    )
                                }
                                ParquetTimeUnit::MICROS(_) => {
                                    typed_reader!(
                                        Int64TimestampMicrosColumnReader,
                                        ArrowDataType::Timestamp(time_unit, time_zone)
                                    )
                                }
                                ParquetTimeUnit::NANOS(_) => {
                                    typed_reader!(
                                        Int64TimestampNanosColumnReader,
                                        ArrowDataType::Int64
                                    )
                                }
                            }
                        }
                        lt => panic!("Unsupported logical type for INT64: {:?}", lt),
                    }
                } else {
                    match promotion_info.physical_type {
                        PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                            if promotion_info.precision <= DECIMAL_MAX_LONG_DIGITS
                                && promotion_info.scale < 1
                            {
                                typed_reader!(Int64ColumnReader, Int64)
                            } else {
                                typed_reader!(
                                    Int64DecimalColumnReader,
                                    ArrowDataType::Decimal128(
                                        promotion_info.precision as u8,
                                        promotion_info.scale as i8
                                    )
                                )
                            }
                        }
                        // By default it is INT(64, true)
                        _ => typed_reader!(Int64ColumnReader, Int64),
                    }
                }
            }
            PhysicalType::INT96 => {
                typed_reader!(
                    Int96ColumnReader,
                    ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".to_string().into()))
                )
            }
            PhysicalType::FLOAT => match promotion_info.physical_type {
                // We support type promotion from float to double
                PhysicalType::FLOAT => typed_reader!(FloatColumnReader, Float32),
                PhysicalType::DOUBLE => typed_reader!(FloatToDoubleColumnReader, Float64),
                t => panic!("Unsupported read physical type: {} for FLOAT", t),
            },

            PhysicalType::DOUBLE => typed_reader!(DoubleColumnReader, Float64),
            PhysicalType::BYTE_ARRAY => {
                if let Some(logical_type) = desc.logical_type() {
                    match logical_type {
                        LogicalType::String => typed_reader!(StringColumnReader, Utf8),
                        // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
                        // "enum type should interpret ENUM annotated field as a UTF-8"
                        LogicalType::Enum => typed_reader!(StringColumnReader, Utf8),
                        lt => panic!("Unsupported logical type for BYTE_ARRAY: {:?}", lt),
                    }
                } else {
                    typed_reader!(ByteArrayColumnReader, Binary)
                }
            }
            PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                if let Some(logical_type) = desc.logical_type() {
                    match logical_type {
                        LogicalType::Decimal {
                            precision,
                            scale: _,
                        } => {
                            if !use_decimal_128 && precision <= DECIMAL_MAX_INT_DIGITS {
                                typed_reader!(FLBADecimal32ColumnReader, Int32)
                            } else if !use_decimal_128
                                && promotion_info.precision <= DECIMAL_MAX_LONG_DIGITS
                            {
                                typed_reader!(
                                    FLBADecimal64ColumnReader,
                                    ArrowDataType::Decimal128(
                                        promotion_info.precision as u8,
                                        promotion_info.scale as i8
                                    )
                                )
                            } else {
                                typed_reader!(
                                    FLBADecimalColumnReader,
                                    ArrowDataType::Decimal128(
                                        promotion_info.precision as u8,
                                        promotion_info.scale as i8
                                    )
                                )
                            }
                        }
                        LogicalType::Uuid => {
                            let type_length = desc.type_length();
                            typed_reader!(
                                FLBAColumnReader,
                                ArrowDataType::FixedSizeBinary(type_length)
                            )
                        }
                        t => panic!("Unsupported logical type for FIXED_LEN_BYTE_ARRAY: {:?}", t),
                    }
                } else {
                    let type_length = desc.type_length();
                    typed_reader!(
                        FLBAColumnReader,
                        ArrowDataType::FixedSizeBinary(type_length)
                    )
                }
            }
        }
    }
}

macro_rules! make_func {
    ($self:ident, $func:ident $(,$args:ident)*) => ({
        match *$self {
            Self::BoolColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int8ColumnReader(ref typed) => typed.$func($($args),*),
            Self::UInt8ColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int16ColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int16ToDoubleColumnReader(ref typed) => typed.$func($($args), *),
            Self::UInt16ColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int32ColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int32To64ColumnReader(ref typed) => typed.$func($($args), *),
            Self::Int32ToDecimal64ColumnReader(ref typed) => typed.$func($($args), *),
            Self::Int32ToDoubleColumnReader(ref typed) => typed.$func($($args), *),
            Self::UInt32ColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int32DateColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int32DecimalColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int32TimestampMicrosColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int64ColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int64ToDecimal64ColumnReader(ref typed) => typed.$func($($args), *),
            Self::UInt64DecimalColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int64DecimalColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int64TimestampMillisColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int64TimestampMicrosColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int64TimestampNanosColumnReader(ref typed) => typed.$func($($args),*),
            Self::FloatColumnReader(ref typed) => typed.$func($($args),*),
            Self::DoubleColumnReader(ref typed) => typed.$func($($args),*),
            Self::FloatToDoubleColumnReader(ref typed) => typed.$func($($args),*),
            Self::ByteArrayColumnReader(ref typed) => typed.$func($($args),*),
            Self::StringColumnReader(ref typed) => typed.$func($($args),*),
            Self::FLBADecimalColumnReader(ref typed) => typed.$func($($args),*),
            Self::FLBADecimal32ColumnReader(ref typed) => typed.$func($($args),*),
            Self::FLBADecimal64ColumnReader(ref typed) => typed.$func($($args),*),
            Self::FLBAColumnReader(ref typed) => typed.$func($($args),*),
            Self::Int96ColumnReader(ref typed) => typed.$func($($args),*),
        }
    });
}

macro_rules! make_func_mut {
    ($self:ident, $func:ident $(,$args:ident)*) => ({
        match *$self {
            Self::BoolColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int8ColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::UInt8ColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int16ColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int16ToDoubleColumnReader(ref mut typed) => typed.$func($($args), *),
            Self::UInt16ColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int32ColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int32To64ColumnReader(ref mut typed) => typed.$func($($args), *),
            Self::Int32ToDecimal64ColumnReader(ref mut typed) => typed.$func($($args), *),
            Self::Int32ToDoubleColumnReader(ref mut typed) => typed.$func($($args), *),
            Self::UInt32ColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int32DateColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int32DecimalColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int32TimestampMicrosColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int64ColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int64ToDecimal64ColumnReader(ref mut typed) => typed.$func($($args), *),
            Self::UInt64DecimalColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int64DecimalColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int64TimestampMillisColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int64TimestampMicrosColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int64TimestampNanosColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::FloatColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::DoubleColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::FloatToDoubleColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::ByteArrayColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::StringColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::FLBADecimalColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::FLBADecimal32ColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::FLBADecimal64ColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::FLBAColumnReader(ref mut typed) => typed.$func($($args),*),
            Self::Int96ColumnReader(ref mut typed) => typed.$func($($args),*),
        }
    });
}

impl ColumnReader {
    #[inline]
    pub fn get_descriptor(&self) -> &ColumnDescriptor {
        make_func!(self, get_descriptor)
    }

    #[inline]
    pub fn set_dictionary_page(
        &mut self,
        page_value_count: usize,
        page_data: Buffer,
        encoding: Encoding,
    ) {
        make_func_mut!(
            self,
            set_dictionary_page,
            page_value_count,
            page_data,
            encoding
        )
    }

    #[inline]
    pub fn set_page_v1(&mut self, page_value_count: usize, page_data: Buffer, encoding: Encoding) {
        make_func_mut!(self, set_page_v1, page_value_count, page_data, encoding)
    }

    #[inline]
    pub fn set_page_v2(
        &mut self,
        page_value_count: usize,
        def_level_data: Buffer,
        rep_level_data: Buffer,
        value_data: Buffer,
        encoding: Encoding,
    ) {
        make_func_mut!(
            self,
            set_page_v2,
            page_value_count,
            def_level_data,
            rep_level_data,
            value_data,
            encoding
        )
    }

    #[inline]
    pub fn set_null(&mut self) {
        make_func_mut!(self, set_null)
    }

    #[inline]
    pub fn set_boolean(&mut self, value: bool) {
        make_func_mut!(self, set_boolean, value)
    }

    #[inline]
    pub fn set_fixed<U: ArrowNativeType + AsBytes>(&mut self, value: U) {
        make_func_mut!(self, set_fixed, value)
    }

    #[inline]
    pub fn set_binary(&mut self, value: MutableBuffer) {
        make_func_mut!(self, set_binary, value)
    }

    #[inline]
    pub fn set_decimal_flba(&mut self, value: MutableBuffer) {
        make_func_mut!(self, set_decimal_flba, value)
    }

    #[inline]
    pub fn set_position(&mut self, value: i64, size: usize) {
        make_func_mut!(self, set_position, value, size)
    }

    #[inline]
    pub fn set_is_deleted(&mut self, value: MutableBuffer) {
        make_func_mut!(self, set_is_deleted, value)
    }

    #[inline]
    pub fn reset_batch(&mut self) {
        make_func_mut!(self, reset_batch)
    }

    #[inline]
    pub fn current_batch(&mut self) -> Result<ArrayData, ExecutionError> {
        make_func_mut!(self, current_batch)
    }

    #[inline]
    pub fn read_batch(&mut self, total: usize, null_pad_size: usize) -> (usize, usize) {
        make_func_mut!(self, read_batch, total, null_pad_size)
    }

    #[inline]
    pub fn skip_batch(&mut self, total: usize, put_nulls: bool) -> usize {
        make_func_mut!(self, skip_batch, total, put_nulls)
    }
}

/// A batched reader for a primitive Parquet column.
pub struct TypedColumnReader<T: DataType> {
    desc: ColumnDescPtr,
    arrow_type: ArrowDataType,
    rep_level_decoder: Option<LevelDecoder>,
    def_level_decoder: Option<LevelDecoder>,
    value_decoder: Option<Box<dyn Decoder>>,

    /// The remaining number of values to read in the current page
    num_values_in_page: usize,
    /// The value vector for this column reader; reused across batches.
    vector: ParquetMutableVector,
    /// The batch size for this column vector.
    capacity: usize,
    /// Number of bits used to represent one value in Parquet.
    bit_width: usize,
    /// Whether this is a constant column reader (always return constant vector).
    is_const: bool,

    // Options for reading Parquet
    read_options: ReadOptions,

    /// Marker to allow `T` in the generic parameter of the struct.
    _phantom: PhantomData<T>,
}

impl<T: DataType> TypedColumnReader<T> {
    pub fn new(
        desc: ColumnDescriptor,
        capacity: usize,
        arrow_type: ArrowDataType,
        read_options: ReadOptions,
    ) -> Self {
        let vector = ParquetMutableVector::new(capacity, &arrow_type);
        let bit_width = ParquetMutableVector::bit_width(&arrow_type);
        Self {
            desc: Arc::new(desc),
            arrow_type,
            rep_level_decoder: None,
            def_level_decoder: None,
            value_decoder: None,
            num_values_in_page: 0,
            vector,
            capacity,
            bit_width,
            is_const: false,
            read_options,
            _phantom: PhantomData,
        }
    }

    #[inline]
    pub fn get_descriptor(&self) -> &ColumnDescriptor {
        &self.desc
    }

    /// Reset the current batch. This will clear all the content of the current columnar vector as
    /// well as reset all of its internal states.
    #[inline]
    pub fn reset_batch(&mut self) {
        self.vector.reset()
    }

    /// Returns the current batch that's been constructed.
    ///
    /// Note: the caller must make sure the returned Arrow vector is fully consumed before calling
    /// `read_batch` again.
    #[inline]
    pub fn current_batch(&mut self) -> Result<ArrayData, ExecutionError> {
        self.vector.get_array_data()
    }

    /// Reads a batch of at most `total` values from the current page this reader has. Returns a
    /// tuple where the first element is the actual number of values read (including both nulls and
    /// non-nulls), and the second element is the actual number of nulls read.
    ///
    /// Pad nulls for the amount of `null_pad_size` before reading.
    ///
    /// If the return number of values is < `total`, it means the current page is drained and the
    /// caller should call `set_page_v1` or `set_page_v2` before calling next `read_batch`.
    pub fn read_batch(&mut self, total: usize, null_pad_size: usize) -> (usize, usize) {
        debug_assert!(
            self.value_decoder.is_some() && self.def_level_decoder.is_some(),
            "set_page_v1/v2 should have been called"
        );

        let n = ::std::cmp::min(self.num_values_in_page, total);
        self.num_values_in_page -= n;
        let value_decoder = self.value_decoder.as_mut().unwrap();
        let dl_decoder = self.def_level_decoder.as_mut().unwrap();

        let previous_num_nulls = self.vector.num_nulls;
        self.vector.put_nulls(null_pad_size);
        dl_decoder.read_batch(n, &mut self.vector, value_decoder.as_mut());

        (n, self.vector.num_nulls - previous_num_nulls)
    }

    /// Skips a batch of at most `total` values from the current page this reader has, and returns
    /// the actual number of values skipped.
    ///
    /// If the return value is < `total`, it means the current page is drained and the caller should
    /// call `set_page_v1` or `set_page_v2` before calling next `skip_batch`.
    pub fn skip_batch(&mut self, total: usize, put_nulls: bool) -> usize {
        debug_assert!(
            self.value_decoder.is_some() && self.def_level_decoder.is_some(),
            "set_page_v1/v2 should have been called"
        );

        let n = ::std::cmp::min(self.num_values_in_page, total);
        self.num_values_in_page -= n;
        let value_decoder = self.value_decoder.as_mut().unwrap();
        let dl_decoder = self.def_level_decoder.as_mut().unwrap();

        dl_decoder.skip_batch(n, &mut self.vector, value_decoder.as_mut(), put_nulls);

        n
    }

    /// Sets the dictionary page for this column reader and eagerly reads it.
    ///
    /// # Panics
    ///
    /// - If being called more than once during the lifetime of this column reader. A Parquet column
    ///   chunk should only contain a single dictionary page.
    /// - If the input `encoding` is neither `PLAIN`, `PLAIN_DICTIONARY` nor `RLE_DICTIONARY`.
    pub fn set_dictionary_page(
        &mut self,
        page_value_count: usize,
        page_data: Buffer,
        mut encoding: Encoding,
    ) {
        // In Parquet format v1, both dictionary page and data page use the same encoding
        // `PLAIN_DICTIONARY`, while in v2, dictioanry page uses `PLAIN` and data page uses
        // `RLE_DICTIONARY`.
        //
        // Here, we convert `PLAIN` from v2 dictionary page to `PLAIN_DICTIONARY`, so that v1 and v2
        // shares the same encoding. Later on, `get_decoder` will use the `PlainDecoder` for this
        // case.
        if encoding == Encoding::PLAIN {
            encoding = Encoding::PLAIN_DICTIONARY;
        }

        if encoding != Encoding::PLAIN_DICTIONARY {
            panic!("Invalid encoding type for Parquet dictionary: {}", encoding);
        }

        if self.vector.dictionary.is_some() {
            panic!("Parquet column cannot have more than one dictionary");
        }

        // Create a new vector for dictionary values
        let mut value_vector = ParquetMutableVector::new(page_value_count, &self.arrow_type);

        let mut dictionary = self.get_decoder(page_data, page_value_count, encoding);
        dictionary.read_batch(&mut value_vector, page_value_count);
        value_vector.num_values = page_value_count;

        // Re-create the parent vector since it is initialized with the dictionary value type, not
        // the key type (which is always integer).
        self.vector = ParquetMutableVector::new(self.capacity, &ArrowDataType::Int32);
        self.vector.set_dictionary(value_vector);
    }

    /// Resets the Parquet data page for this column reader.
    pub fn set_page_v1(
        &mut self,
        page_value_count: usize,
        page_data: Buffer,
        mut encoding: Encoding,
    ) {
        // In v1, when data is encoded with dictionary, data page uses `PLAIN_DICTIONARY`, while v2
        // uses  `RLE_DICTIONARY`. To consolidate the two, here we convert `PLAIN_DICTIONARY` to
        // `RLE_DICTIONARY` following v2. Later on, `get_decoder` will use `DictDecoder` for this
        // case.
        if encoding == Encoding::PLAIN_DICTIONARY {
            encoding = Encoding::RLE_DICTIONARY;
        }

        self.num_values_in_page = page_value_count;
        self.check_dictionary(&encoding);

        let mut page_buffer = page_data;

        let bit_width = log2(self.desc.max_rep_level() as u64 + 1) as u8;
        let mut rl_decoder = LevelDecoder::new(Arc::clone(&self.desc), bit_width, true);
        let offset = rl_decoder.set_data(page_value_count, &page_buffer);
        self.rep_level_decoder = Some(rl_decoder);
        page_buffer = page_buffer.slice(offset);

        let bit_width = log2(self.desc.max_def_level() as u64 + 1) as u8;
        let mut dl_decoder = LevelDecoder::new(Arc::clone(&self.desc), bit_width, true);
        let offset = dl_decoder.set_data(page_value_count, &page_buffer);
        self.def_level_decoder = Some(dl_decoder);
        page_buffer = page_buffer.slice(offset);

        let value_decoder = self.get_decoder(page_buffer, page_value_count, encoding);
        self.value_decoder = Some(value_decoder);
    }

    /// Resets the Parquet data page for this column reader.
    pub fn set_page_v2(
        &mut self,
        page_value_count: usize,
        def_level_data: Buffer,
        rep_level_data: Buffer,
        value_data: Buffer,
        encoding: Encoding,
    ) {
        self.num_values_in_page = page_value_count;
        self.check_dictionary(&encoding);

        let bit_width = log2(self.desc.max_rep_level() as u64 + 1) as u8;
        let mut rl_decoder = LevelDecoder::new(Arc::clone(&self.desc), bit_width, false);
        rl_decoder.set_data(page_value_count, &rep_level_data);
        self.rep_level_decoder = Some(rl_decoder);

        let bit_width = log2(self.desc.max_def_level() as u64 + 1) as u8;
        let mut dl_decoder = LevelDecoder::new(Arc::clone(&self.desc), bit_width, false);
        dl_decoder.set_data(page_value_count, &def_level_data);
        self.def_level_decoder = Some(dl_decoder);

        let value_decoder = self.get_decoder(value_data, page_value_count, encoding);
        self.value_decoder = Some(value_decoder);
    }

    /// Sets all values in the vector of this column reader to be null.
    pub fn set_null(&mut self) {
        self.check_const("set_null");
        self.vector.put_nulls(self.capacity);
    }

    /// Sets all values in the vector of this column reader to be `value`.
    pub fn set_boolean(&mut self, value: bool) {
        self.check_const("set_boolean");
        if value {
            let dst = self.vector.value_buffer.as_slice_mut();
            bit::set_bits(dst, 0, self.capacity);
        }
        self.vector.num_values += self.capacity;
    }

    /// Sets all values in the vector of this column reader to be `value`.
    pub fn set_fixed<U: ArrowNativeType + AsBytes>(&mut self, value: U) {
        self.check_const("set_fixed");
        let type_size = std::mem::size_of::<U>();

        let mut offset = 0;
        for _ in 0..self.capacity {
            bit::memcpy_value(&value, type_size, &mut self.vector.value_buffer[offset..]);
            offset += type_size;
        }
        self.vector.num_values += self.capacity;
    }

    /// Sets all values in the vector of this column reader to be binary represented by `buffer`.
    pub fn set_binary(&mut self, buffer: MutableBuffer) {
        self.check_const("set_binary");

        // TODO: consider using dictionary here

        let len = buffer.len();
        let total_len = len * self.capacity;
        let offset_buf = self.vector.value_buffer.as_slice_mut();
        let child_vector = &mut self.vector.children[0];
        let value_buf = &mut child_vector.value_buffer;

        value_buf.resize(total_len);

        let mut value_buf_offset = 0;
        let mut offset_buf_offset = 4;
        for _ in 0..self.capacity {
            bit::memcpy(&buffer, &mut value_buf.as_slice_mut()[value_buf_offset..]);
            value_buf_offset += len;

            bit::memcpy_value(
                &(value_buf_offset as i32),
                4,
                &mut offset_buf[offset_buf_offset..],
            );
            offset_buf_offset += 4;
        }
        self.vector.num_values += self.capacity;
    }

    /// Sets all values in the vector of this column reader to be decimal represented by `buffer`.
    pub fn set_decimal_flba(&mut self, buffer: MutableBuffer) {
        self.check_const("set_decimal_flba");

        // TODO: consider using dictionary here

        let len = buffer.len();
        let mut bytes: [u8; DECIMAL_BYTE_WIDTH] = [0; DECIMAL_BYTE_WIDTH];

        for i in 0..len {
            bytes[len - i - 1] = buffer[i];
        }
        if bytes[len - 1] & 0x80 == 0x80 {
            bytes[len..DECIMAL_BYTE_WIDTH].fill(0xff);
        }

        let mut offset = 0;
        for _ in 0..self.capacity {
            bit::memcpy(&bytes, &mut self.vector.value_buffer[offset..]);
            offset += DECIMAL_BYTE_WIDTH;
        }
        self.vector.num_values += self.capacity;
    }

    /// Sets position values of this column reader to the vector starting from `value`.
    pub fn set_position(&mut self, value: i64, size: usize) {
        let i64_size = std::mem::size_of::<i64>();

        let mut offset = self.vector.num_values * i64_size;
        for i in value..(value + size as i64) {
            // TODO: is it better to convert self.value_buffer to &mut [i64] and for-loop update?
            bit::memcpy_value(&i, i64_size, &mut self.vector.value_buffer[offset..]);
            offset += i64_size;
        }
        self.vector.num_values += size;
    }

    /// Sets the values in the vector of this column reader to be a boolean array represented
    /// by `buffer`.
    pub fn set_is_deleted(&mut self, buffer: MutableBuffer) {
        let len = buffer.len();
        let dst = self.vector.value_buffer.as_slice_mut();
        for i in 0..len {
            if buffer[i] == 1 {
                bit::set_bit(dst, i);
            } else if buffer[i] == 0 {
                bit::unset_bit(dst, i);
            }
        }
        self.vector.num_values += len;
    }

    /// Check a few pre-conditions for setting constants, as well as setting
    /// that `is_const` to true for the particular column reader.
    fn check_const(&mut self, method_name: &str) {
        assert!(
            self.value_decoder.is_none(),
            "{} cannot be called after set_page_v1/set_page_v2!",
            method_name
        );
        assert!(!self.is_const, "can only set constant once!");
        self.is_const = true;
    }

    fn check_dictionary(&mut self, encoding: &Encoding) {
        // The column has a dictionary while the new page is of PLAIN encoding. In this case, we
        // should eagerly decode all the dictionary indices and convert the underlying vector to a
        // plain encoded vector.
        if self.vector.dictionary.is_some() && *encoding == Encoding::PLAIN {
            let new_vector = ParquetMutableVector::new(self.capacity, &self.arrow_type);
            let old_vector = std::mem::replace(&mut self.vector, new_vector);
            T::decode_dict(old_vector, &mut self.vector, self.bit_width);
            debug_assert!(self.vector.dictionary.is_none());
        }
    }

    fn get_decoder(
        &self,
        value_data: Buffer,
        page_value_count: usize,
        encoding: Encoding,
    ) -> Box<dyn Decoder> {
        get_decoder::<T>(
            value_data,
            page_value_count,
            encoding,
            Arc::clone(&self.desc),
            self.read_options,
        )
    }
}
