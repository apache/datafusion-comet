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

/// Similar to the `read_num_bytes` but read nums from bytes in big-endian order
/// This is used to read bytes from Java's OutputStream which writes bytes in big-endian
/// Reads `$size` of bytes from `$src`, and reinterprets them as type `$ty`, in
/// big-endian order.
/// This is copied and modified datafusion_comet::common::bit.
macro_rules! read_num_be_bytes {
    ($ty:ty, $size:expr, $src:expr) => {{
        debug_assert!($size <= $src.len());
        let mut buffer = [0u8; std::mem::size_of::<$ty>()];
        buffer.as_mut()[..$size].copy_from_slice(&$src[..$size]);
        <$ty>::from_be_bytes(buffer)
    }};
}
