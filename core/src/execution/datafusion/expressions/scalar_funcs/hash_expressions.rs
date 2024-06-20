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

use crate::execution::datafusion::spark_hash::{create_murmur3_hashes, create_xxhash64_hashes};
use arrow_array::{ArrayRef, Int32Array, Int64Array};
use datafusion_common::{internal_err, DataFusionError, ScalarValue};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

pub fn spark_murmur3_hash(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let length = args.len();
    let seed = &args[length - 1];
    match seed {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(seed))) => {
            // iterate over the arguments to find out the length of the array
            let num_rows = args[0..args.len() - 1]
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            let mut hashes: Vec<u32> = vec![0_u32; num_rows];
            hashes.fill(*seed as u32);
            let arrays = args[0..args.len() - 1]
                .iter()
                .map(|arg| match arg {
                    ColumnarValue::Array(array) => array.clone(),
                    ColumnarValue::Scalar(scalar) => {
                        scalar.clone().to_array_of_size(num_rows).unwrap()
                    }
                })
                .collect::<Vec<ArrayRef>>();
            create_murmur3_hashes(&arrays, &mut hashes)?;
            if num_rows == 1 {
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(
                    hashes[0] as i32,
                ))))
            } else {
                let hashes: Vec<i32> = hashes.into_iter().map(|x| x as i32).collect();
                Ok(ColumnarValue::Array(Arc::new(Int32Array::from(hashes))))
            }
        }
        _ => {
            internal_err!(
                "The seed of function murmur3_hash must be an Int32 scalar value, but got: {:?}.",
                seed
            )
        }
    }
}

pub fn spark_xxhash64(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let length = args.len();
    let seed = &args[length - 1];
    match seed {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(seed))) => {
            // iterate over the arguments to find out the length of the array
            let num_rows = args[0..args.len() - 1]
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            let mut hashes: Vec<u64> = vec![0_u64; num_rows];
            hashes.fill(*seed as u64);
            let arrays = args[0..args.len() - 1]
                .iter()
                .map(|arg| match arg {
                    ColumnarValue::Array(array) => array.clone(),
                    ColumnarValue::Scalar(scalar) => {
                        scalar.clone().to_array_of_size(num_rows).unwrap()
                    }
                })
                .collect::<Vec<ArrayRef>>();
            create_xxhash64_hashes(&arrays, &mut hashes)?;
            if num_rows == 1 {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                    hashes[0] as i64,
                ))))
            } else {
                let hashes: Vec<i64> = hashes.into_iter().map(|x| x as i64).collect();
                Ok(ColumnarValue::Array(Arc::new(Int64Array::from(hashes))))
            }
        }
        _ => {
            internal_err!(
                "The seed of function xxhash64 must be an Int64 scalar value, but got: {:?}.",
                seed
            )
        }
    }
}
