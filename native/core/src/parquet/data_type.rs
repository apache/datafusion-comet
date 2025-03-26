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

use super::read::{PlainDecoding, PlainDictDecoding};

pub trait DataType: PlainDecoding + PlainDictDecoding + 'static {}

macro_rules! make_type {
    ($name:ident) => {
        pub struct $name {}
        impl DataType for $name {}
    };
}

make_type!(BoolType);
make_type!(Int8Type);
make_type!(UInt8Type);
make_type!(Int16Type);
make_type!(Int16ToDoubleType);
make_type!(UInt16Type);
make_type!(Int32Type);
make_type!(Int32To64Type);
make_type!(Int32ToDecimal64Type);
make_type!(Int32ToDoubleType);
make_type!(UInt32Type);
make_type!(Int64Type);
make_type!(Int64ToDecimal64Type);
make_type!(UInt64Type);
make_type!(FloatType);
make_type!(DoubleType);
make_type!(FloatToDoubleType);
make_type!(ByteArrayType);
make_type!(StringType);
make_type!(Int32DecimalType);
make_type!(Int64DecimalType);
make_type!(FLBADecimalType);
make_type!(FLBADecimal32Type);
make_type!(FLBADecimal64Type);
make_type!(FLBAType);
make_type!(Int32DateType);
make_type!(Int32TimestampMicrosType);
make_type!(Int64TimestampMillisType);
make_type!(Int64TimestampMicrosType);
make_type!(Int96TimestampMicrosType);

pub trait AsBytes {
    /// Returns the slice of bytes for an instance of this data type.
    fn as_bytes(&self) -> &[u8];
}

impl AsBytes for Vec<u8> {
    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsBytes for &str {
    fn as_bytes(&self) -> &[u8] {
        (self as &str).as_bytes()
    }
}

impl AsBytes for [u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

impl AsBytes for str {
    fn as_bytes(&self) -> &[u8] {
        (self as &str).as_bytes()
    }
}

macro_rules! make_as_bytes {
    ($source_ty:ident) => {
        impl AsBytes for $source_ty {
            #[allow(clippy::size_of_in_element_count)]
            fn as_bytes(&self) -> &[u8] {
                unsafe {
                    ::std::slice::from_raw_parts(
                        self as *const $source_ty as *const u8,
                        ::std::mem::size_of::<$source_ty>(),
                    )
                }
            }
        }
    };
}

make_as_bytes!(bool);
make_as_bytes!(i8);
make_as_bytes!(u8);
make_as_bytes!(i16);
make_as_bytes!(u16);
make_as_bytes!(i32);
make_as_bytes!(u32);
make_as_bytes!(i64);
make_as_bytes!(u64);
make_as_bytes!(f32);
make_as_bytes!(f64);
make_as_bytes!(i128);
