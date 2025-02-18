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

use arrow_array::{Array, ArrayRef, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow_schema::DataType;
use datafusion_common::DataFusionError;
use datafusion_expr_common::columnar_value::ColumnarValue;
use std::ops::Rem;
use std::sync::Arc;

macro_rules! signed_integer_rem {
    ($left:expr, $right:expr, $array_type:ty, $min_val:expr) => {{
        let left = $left.as_any().downcast_ref::<$array_type>().unwrap();
        let right = $right.as_any().downcast_ref::<$array_type>().unwrap();
        let result: $array_type = arrow::compute::kernels::arity::binary(left, right, |l, r| {
            if l == $min_val && r == -1 {
                0
            } else {
                l.rem(r)
            }
        })?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

// spark compatible `remainder` function for signed integers.
// COMET-1412: if the left is the minimum integer value and the right is -1, the result is 0.
pub fn spark_signed_integer_remainder(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let left = &args[0];
    let right = &args[1];

    let (left, right): (ArrayRef, ArrayRef) = match (left, right) {
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => (Arc::clone(l), Arc::clone(r)),
        (ColumnarValue::Scalar(l), ColumnarValue::Array(r)) => {
            (l.to_array_of_size(r.len())?, Arc::clone(r))
        }
        (ColumnarValue::Array(l), ColumnarValue::Scalar(r)) => {
            (Arc::clone(l), r.to_array_of_size(l.len())?)
        }
        (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => (l.to_array()?, r.to_array()?),
    };
    match (left.data_type(), right.data_type(), data_type) {
        (DataType::Int8, DataType::Int8, DataType::Int8) => {
            signed_integer_rem!(left, right, Int8Array, i8::MIN)
        }
        (DataType::Int16, DataType::Int16, DataType::Int16) => {
            signed_integer_rem!(left, right, Int16Array, i16::MIN)
        }
        (DataType::Int32, DataType::Int32, DataType::Int32) => {
            signed_integer_rem!(left, right, Int32Array, i32::MIN)
        }
        (DataType::Int64, DataType::Int64, DataType::Int64) => {
            signed_integer_rem!(left, right, Int64Array, i64::MIN)
        }
        _ => Err(DataFusionError::Internal(format!(
            "Invalid data type for spark_signed_integer_remainder operation: {:?} {:?}",
            left.data_type(),
            right.data_type()
        ))),
    }
}

#[cfg(test)]
mod test {
    use crate::math_funcs::remainder::spark_signed_integer_remainder;
    use arrow_array::Int8Array;
    use arrow_schema::DataType;
    use datafusion_common::cast::as_int8_array;
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use std::sync::Arc;

    #[test]
    fn test_spark_signed_integer_remainder() -> datafusion_common::Result<()> {
        let args = vec![
            ColumnarValue::Array(Arc::new(Int8Array::from(vec![9i8, i8::MIN]))),
            ColumnarValue::Array(Arc::new(Int8Array::from(vec![5i8, -1]))),
        ];
        let ColumnarValue::Array(result) = spark_signed_integer_remainder(&args, &DataType::Int8)?
        else {
            unreachable!()
        };
        let results = as_int8_array(&result)?;
        let expected = Int8Array::from(vec![4, 0]);
        assert_eq!(results, &expected);
        Ok(())
    }
}
