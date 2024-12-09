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

use arrow::datatypes::DataType as ArrowDataType;
use arrow_schema::TimeUnit;
use std::{cmp, fmt::Debug};

#[derive(Debug, PartialEq)]
pub enum DataType {
    Boolean,
    Byte,
    Short,
    Integer,
    Long,
    Float,
    Double,
    Decimal(u8, i8),
    String,
    Binary,
    Timestamp,
    Date,
}

impl From<&ArrowDataType> for DataType {
    fn from(dt: &ArrowDataType) -> Self {
        match dt {
            ArrowDataType::Boolean => DataType::Boolean,
            ArrowDataType::Int8 => DataType::Byte,
            ArrowDataType::Int16 => DataType::Short,
            ArrowDataType::Int32 => DataType::Integer,
            ArrowDataType::Int64 => DataType::Long,
            ArrowDataType::Float32 => DataType::Float,
            ArrowDataType::Float64 => DataType::Double,
            ArrowDataType::Decimal128(precision, scale) => DataType::Decimal(*precision, *scale),
            ArrowDataType::Utf8 => DataType::String,
            ArrowDataType::Binary => DataType::Binary,
            // Spark always store timestamp in micro seconds
            ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => DataType::Timestamp,
            ArrowDataType::Date32 => DataType::Date,
            ArrowDataType::Dictionary(key_dt, value_dt) if is_valid_key_type(key_dt) => {
                Self::from(value_dt.as_ref())
            }
            dt => panic!("unsupported Arrow data type: {:?}", dt),
        }
    }
}

impl DataType {
    pub fn kind(&self) -> TypeKind {
        match self {
            DataType::Boolean => TypeKind::Boolean,
            DataType::Byte => TypeKind::Byte,
            DataType::Short => TypeKind::Short,
            DataType::Integer => TypeKind::Integer,
            DataType::Long => TypeKind::Long,
            DataType::Float => TypeKind::Float,
            DataType::Double => TypeKind::Double,
            DataType::Decimal(_, _) => TypeKind::Decimal,
            DataType::String => TypeKind::String,
            DataType::Binary => TypeKind::Binary,
            DataType::Timestamp => TypeKind::Timestamp,
            DataType::Date => TypeKind::Date,
        }
    }
}

/// Comet only use i32 as dictionary key
fn is_valid_key_type(dt: &ArrowDataType) -> bool {
    matches!(dt, ArrowDataType::Int32)
}

/// Unlike [`DataType`], [`TypeKind`] doesn't carry extra information about the type itself, such as
/// decimal precision & scale. Instead, it is merely a token that is used to do runtime case
/// analysis depending on the actual type. It can be obtained from a `TypeTrait` generic parameter.
#[derive(Debug, PartialEq)]
pub enum TypeKind {
    Boolean,
    Byte,
    Short,
    Integer,
    Long,
    Float,
    Double,
    Decimal,
    String,
    Binary,
    Timestamp,
    Date,
}

pub const BITS_PER_BYTE: usize = 8;

impl TypeKind {
    /// Returns the size of this type, in number of bits.
    pub fn type_size(&self) -> usize {
        match self {
            TypeKind::Boolean => 1,
            TypeKind::Byte => BITS_PER_BYTE,
            TypeKind::Short => BITS_PER_BYTE * 2,
            TypeKind::Integer | TypeKind::Float => BITS_PER_BYTE * 4,
            TypeKind::Long | TypeKind::Double => BITS_PER_BYTE * 8,
            TypeKind::Decimal => BITS_PER_BYTE * 16,
            TypeKind::String | TypeKind::Binary => BITS_PER_BYTE * 16,
            TypeKind::Timestamp => BITS_PER_BYTE * 8,
            TypeKind::Date => BITS_PER_BYTE * 4,
        }
    }
}

pub const STRING_VIEW_LEN: usize = 16; // StringView is stored using 16 bytes
pub const STRING_VIEW_PREFIX_LEN: usize = 4; // String prefix in StringView is stored using 4 bytes

#[repr(C, align(16))]
#[derive(Clone, Copy, Debug)]
pub struct StringView {
    pub len: u32,
    pub prefix: [u8; STRING_VIEW_PREFIX_LEN],
    pub ptr: usize,
}

impl StringView {
    pub fn as_utf8_str(&self) -> &str {
        unsafe {
            let slice = std::slice::from_raw_parts(self.ptr as *const u8, self.len as usize);
            std::str::from_utf8_unchecked(slice)
        }
    }
}

impl Default for StringView {
    fn default() -> Self {
        Self {
            len: 0,
            prefix: [0; STRING_VIEW_PREFIX_LEN],
            ptr: 0,
        }
    }
}

impl PartialEq for StringView {
    fn eq(&self, other: &Self) -> bool {
        if self.len != other.len {
            return false;
        }
        if self.prefix != other.prefix {
            return false;
        }
        self.as_utf8_str() == other.as_utf8_str()
    }
}

pub trait NativeEqual {
    fn is_equal(&self, other: &Self) -> bool;
}

macro_rules! make_native_equal {
    ($native_ty:ty) => {
        impl NativeEqual for $native_ty {
            fn is_equal(&self, other: &Self) -> bool {
                self == other
            }
        }
    };
}

make_native_equal!(bool);
make_native_equal!(i8);
make_native_equal!(i16);
make_native_equal!(i32);
make_native_equal!(i64);
make_native_equal!(i128);
make_native_equal!(StringView);

impl NativeEqual for f32 {
    fn is_equal(&self, other: &Self) -> bool {
        self.total_cmp(other) == cmp::Ordering::Equal
    }
}

impl NativeEqual for f64 {
    fn is_equal(&self, other: &Self) -> bool {
        self.total_cmp(other) == cmp::Ordering::Equal
    }
}
pub trait NativeType: Debug + Default + Copy + NativeEqual {}

impl NativeType for bool {}
impl NativeType for i8 {}
impl NativeType for i16 {}
impl NativeType for i32 {}
impl NativeType for i64 {}
impl NativeType for i128 {}
impl NativeType for f32 {}
impl NativeType for f64 {}
impl NativeType for StringView {}

/// A trait for Comet data type. This should only be used as generic parameter during method
/// invocations.
pub trait TypeTrait: 'static {
    type Native: NativeType;
    fn type_kind() -> TypeKind;
}

macro_rules! make_type_trait {
    ($name:ident, $native_ty:ty, $kind:path) => {
        pub struct $name {}
        impl TypeTrait for $name {
            type Native = $native_ty;
            fn type_kind() -> TypeKind {
                $kind
            }
        }
    };
}

make_type_trait!(BoolType, bool, TypeKind::Boolean);
make_type_trait!(ByteType, i8, TypeKind::Byte);
make_type_trait!(ShortType, i16, TypeKind::Short);
make_type_trait!(IntegerType, i32, TypeKind::Integer);
make_type_trait!(LongType, i64, TypeKind::Long);
make_type_trait!(FloatType, f32, TypeKind::Float);
make_type_trait!(DoubleType, f64, TypeKind::Double);
make_type_trait!(DecimalType, i128, TypeKind::Decimal);
make_type_trait!(StringType, StringView, TypeKind::String);
make_type_trait!(BinaryType, StringView, TypeKind::Binary);
make_type_trait!(TimestampType, i64, TypeKind::Timestamp);
make_type_trait!(DateType, i32, TypeKind::Date);
