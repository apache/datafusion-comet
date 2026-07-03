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

use crate::agg_funcs::estimate_from_bytes;
use arrow::array::{Array, BinaryArray, Int64Array};
use datafusion::common::Result;
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Spark hll_sketch_estimate: Binary sketch -> Long distinct-count estimate.
pub fn spark_hll_sketch_estimate(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let input = arrays[0].as_any().downcast_ref::<BinaryArray>().unwrap();
    let mut out = Int64Array::builder(input.len());
    for i in 0..input.len() {
        if input.is_null(i) {
            out.append_null();
        } else {
            out.append_value(estimate_from_bytes(input.value(i))?);
        }
    }
    Ok(ColumnarValue::Array(Arc::new(out.finish())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agg_funcs::SparkHllSketch;

    #[test]
    fn estimates_from_sketch_column() {
        let mut s = SparkHllSketch::new(12);
        for i in 0..1000i64 {
            s.update_i64(i);
        }
        let arr = Arc::new(BinaryArray::from(vec![Some(
            s.to_sketch_bytes().as_slice(),
        )]));
        let out = spark_hll_sketch_estimate(&[ColumnarValue::Array(arr)]).unwrap();
        let ColumnarValue::Array(a) = out else {
            panic!()
        };
        let est = a.as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        assert!((est - 1000).abs() <= 30, "estimate {est}");
    }
}
