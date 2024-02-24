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

//! unit tests for the shuffle writer

use super::*;

#[test]
fn test_slot_size() {
    let batch_size = 1usize;
    // not inclusive of all supported types, but enough to test the function
    let supported_primitive_types = [
        DataType::Int32,
        DataType::Int64,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Boolean,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::FixedSizeBinary(16),
    ];
    let expected_slot_size = [4, 8, 4, 8, 4, 8, 1, 104, 108, 104, 108, 16];
    supported_primitive_types
        .iter()
        .zip(expected_slot_size.iter())
        .for_each(|(data_type, expected)| {
            let slot_size = slot_size(batch_size, data_type);
            assert_eq!(slot_size, *expected);
        })
}
