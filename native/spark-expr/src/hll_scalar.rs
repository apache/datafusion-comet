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
use datafusion::common::{DataFusionError, Result};
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

// Spark's HllUnion is a TernaryExpression (first, second, third=allowDifferentLgConfigK).
// It builds `new Union(min(k1, k2))`, throws when the two sketches have different
// lgConfigK and the flag is false, and returns an HLL_8 sketch.
/// Spark hll_union(first, second, allowDifferentLgConfigK): union two sketch columns.
pub fn spark_hll_union(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    use crate::agg_funcs::{SparkHllSketch, SparkHllUnion};
    use arrow::array::BooleanArray;
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let a = arrays[0].as_any().downcast_ref::<BinaryArray>().unwrap();
    let b = arrays[1].as_any().downcast_ref::<BinaryArray>().unwrap();
    let allow = arrays[2].as_any().downcast_ref::<BooleanArray>().unwrap();
    let mut out = arrow::array::BinaryBuilder::new();
    for i in 0..a.len() {
        if a.is_null(i) || b.is_null(i) {
            out.append_null();
            continue;
        }
        let sa = SparkHllSketch::from_bytes(a.value(i))?;
        let sb = SparkHllSketch::from_bytes(b.value(i))?;
        let allow_i = !allow.is_null(i) && allow.value(i);
        if !allow_i && sa.lg_config_k() != sb.lg_config_k() {
            return Err(DataFusionError::Execution(format!(
                "Sketches have different lgConfigK values: {} and {}. \
                 Set allowDifferentLgConfigK to true to enable unions of different lgConfigK.",
                sa.lg_config_k(),
                sb.lg_config_k()
            )));
        }
        // Spark builds `new Union(min(k1, k2))`.
        let mut u = SparkHllUnion::new(sa.lg_config_k().min(sb.lg_config_k()));
        u.merge(&sa);
        u.merge(&sb);
        out.append_value(u.to_sketch_bytes());
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

#[cfg(test)]
mod union_tests {
    use super::*;
    use crate::agg_funcs::SparkHllSketch;
    use arrow::array::BooleanArray;

    #[test]
    fn unions_two_sketch_columns() {
        let mut a = SparkHllSketch::new(12);
        for i in 0..1000i64 {
            a.update_i64(i);
        }
        let mut b = SparkHllSketch::new(12);
        for i in 500..1500i64 {
            b.update_i64(i);
        }
        let aa = Arc::new(BinaryArray::from(vec![Some(
            a.to_sketch_bytes().as_slice(),
        )]));
        let bb = Arc::new(BinaryArray::from(vec![Some(
            b.to_sketch_bytes().as_slice(),
        )]));
        let allow = Arc::new(BooleanArray::from(vec![false]));
        let out = spark_hll_union(&[
            ColumnarValue::Array(aa),
            ColumnarValue::Array(bb),
            ColumnarValue::Array(allow),
        ])
        .unwrap();
        let ColumnarValue::Array(arr) = out else {
            panic!()
        };
        let est = estimate_from_bytes(arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(0))
            .unwrap();
        assert!((est - 1500).abs() <= 45, "estimate {est}");
    }
}
