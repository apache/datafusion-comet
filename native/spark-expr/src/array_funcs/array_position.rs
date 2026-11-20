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

use std::sync::Arc;

use super::array_match::{invoke_match, MatchOperation, MatchRows};
use crate::float_semantics::spark_equality_by_ordinal;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Date32Array, Decimal128Array, GenericListArray,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, OffsetSizeTrait, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

fn array_position(array: &ArrayRef, needle: &ColumnarValue) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::List(_) => position(array.as_list::<i32>(), needle),
        DataType::LargeList(_) => position(array.as_list::<i64>(), needle),
        other => exec_err!("array_position does not support type '{other:?}'"),
    }
}

fn position<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    needle: &ColumnarValue,
) -> Result<ArrayRef> {
    let rows = MatchRows::new(array, needle)?;
    let values = array.values();

    // Only position needs typed non-float comparisons: contains and remove use DataFusion here.
    macro_rules! typed_position {
        ($ty:ty) => {
            if let (Some(values), Some(needles)) = (
                values.as_any().downcast_ref::<$ty>(),
                rows.needles.as_any().downcast_ref::<$ty>(),
            ) {
                return SparkArrayPositionFunc::apply(&rows, |i, j| {
                    values.value(i) == needles.value(j)
                });
            }
        };
    }
    match values.data_type() {
        DataType::Boolean => typed_position!(BooleanArray),
        DataType::Int8 => typed_position!(Int8Array),
        DataType::Int16 => typed_position!(Int16Array),
        DataType::Int32 => typed_position!(Int32Array),
        DataType::Int64 => typed_position!(Int64Array),
        DataType::Decimal128(_, _) => typed_position!(Decimal128Array),
        DataType::Date32 => typed_position!(Date32Array),
        DataType::Timestamp(TimeUnit::Microsecond, _) => typed_position!(TimestampMicrosecondArray),
        DataType::Utf8 => typed_position!(StringArray),
        DataType::LargeUtf8 => typed_position!(LargeStringArray),
        _ => {}
    }
    rows.evaluate::<SparkArrayPositionFunc>(spark_equality_by_ordinal)
}

impl<O: OffsetSizeTrait> MatchOperation<O> for SparkArrayPositionFunc {
    fn apply(rows: &MatchRows<'_, O>, equal: impl Fn(usize, usize) -> bool) -> Result<ArrayRef> {
        let positions: Vec<_> = rows
            .first_matches(equal)
            .map(|(position, _)| position)
            .collect();
        Ok(Arc::new(Int64Array::new(
            positions.into(),
            rows.nulls.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::float_semantics::NEGATIVE_NAN;
    use arrow::array::{LargeListArray, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Float64Type};
    use datafusion::common::ScalarValue;

    #[test]
    fn scalar_needle_in_sliced_large_list() -> Result<()> {
        let array = LargeListArray::from_iter_primitive::<Float64Type, _, _>([
            Some(vec![Some(f64::NAN)]),
            Some(vec![None, Some(NEGATIVE_NAN), Some(f64::NAN)]),
            None,
            Some(vec![]),
            Some(vec![Some(1.0)]),
        ]);
        let array: ArrayRef = Arc::new(array.slice(1, 4));
        let needle = ColumnarValue::Scalar(ScalarValue::Float64(Some(f64::NAN)));
        let result = array_position(&array, &needle)?;
        assert_eq!(
            result.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![Some(2), None, Some(0), Some(0)])
        );
        Ok(())
    }

    #[test]
    fn test_nested_float_and_null_position() -> Result<()> {
        let values = ListArray::from_iter_primitive::<Float64Type, _, _>([
            Some(vec![Some(1.0)]),
            Some(vec![Some(f64::NAN)]),
            Some(vec![Some(-0.0)]),
            Some(vec![Some(0.0)]),
            Some(vec![Some(1.0), None]),
        ]);
        let array = ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 2, 4, 5].into()),
            Arc::new(values),
            None,
        );
        let element = ListArray::from_iter_primitive::<Float64Type, _, _>([
            Some(vec![Some(f64::NAN)]),
            Some(vec![Some(0.0)]),
            Some(vec![Some(1.0), None]),
        ]);

        let result = position(&array, &ColumnarValue::Array(Arc::new(element)))?;
        let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result, &Int64Array::from(vec![2, 1, 1]));
        Ok(())
    }

    // ArraysBase.isTypeSupported rejects structs, so this case is only reachable from Rust.
    #[test]
    fn struct_position_compares_signed_zeros_by_field_ordinal() -> Result<()> {
        use arrow::array::{Float64Builder, StructArray, StructBuilder};

        let fields = vec![Arc::new(Field::new("a", DataType::Float64, true))];
        let mut values_builder = StructBuilder::new(fields, vec![Box::new(Float64Builder::new())]);
        for v in [-0.0, 1.0] {
            values_builder
                .field_builder::<Float64Builder>(0)
                .unwrap()
                .append_value(v);
            values_builder.append(true);
        }
        let values = Arc::new(values_builder.finish());
        let array = ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 2].into()),
            values,
            None,
        );

        let fields = vec![Arc::new(Field::new("A", DataType::Float64, true))];
        let mut element_builder = StructBuilder::new(fields, vec![Box::new(Float64Builder::new())]);
        element_builder
            .field_builder::<Float64Builder>(0)
            .unwrap()
            .append_value(0.0);
        element_builder.append(true);
        let element = element_builder.finish();

        let result = position(&array, &ColumnarValue::Array(Arc::new(element)))?;
        let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result, &Int64Array::from(vec![1]));

        let empty_struct = StructArray::new_empty_fields(1, None);
        assert!(position(&array, &ColumnarValue::Array(Arc::new(empty_struct))).is_err());
        Ok(())
    }
}

/// Returns the 1-based position of the first matching element, or 0 if none matches.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkArrayPositionFunc {
    signature: Signature,
}

impl Default for SparkArrayPositionFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayPositionFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArrayPositionFunc {
    fn name(&self) -> &str {
        "spark_array_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_match(self, args, array_position)
    }
}
