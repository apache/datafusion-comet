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

use crate::float_semantics::{float_gt, float_lt, has_float_leaf, spark_comparator};
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use arrow::compute::cast;
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Float32Type, Float64Type};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use num::Float;
use std::sync::Arc;

/// Spark's `greatest` or `least` over Float32 or Float64 values, or over arrays or structs with a
/// float leaf.
///
/// Spark orders floats with `SQLOrderingUtil.compareDoubles`, in which NaN is larger than every
/// other value and `-0.0` equals `0.0`, at any depth. It walks the arguments in order and replaces
/// its result only with a strictly greater (or smaller) value, skipping nulls, so of equal
/// arguments the first one wins: `greatest(-0.0, 0.0)` is `-0.0`. DataFusion's `greatest` and
/// `least` order floats by IEEE 754 total order, handle constant arguments before the others, and
/// let a later argument win a tie.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkGreatestLeast {
    signature: Signature,
    greatest: bool,
}

impl SparkGreatestLeast {
    pub fn new(greatest: bool) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            greatest,
        }
    }

    /// Whether arguments of this type need Spark's float ordering. DataFusion's `greatest` and
    /// `least` already match Spark for every other type, since equal values are identical there.
    pub fn handles(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Float32 | DataType::Float64)
            || (data_type.is_nested() && has_float_leaf(data_type))
    }

    fn validate_arg_count(&self, count: usize) -> Result<()> {
        if count < 2 {
            return exec_err!("{} requires at least two arguments", self.name());
        }
        Ok(())
    }

    /// The result of `current` and then `candidate`, row by row: `candidate` replaces `current`
    /// when it is not null and either `current` is null or `candidate` ranks strictly before it.
    fn pick(&self, current: &ArrayRef, candidate: &ArrayRef) -> Result<ArrayRef> {
        match current.data_type() {
            DataType::Float32 => Ok(Arc::new(
                self.pick_floats::<Float32Type>(current.as_primitive(), candidate.as_primitive()),
            )),
            DataType::Float64 => Ok(Arc::new(
                self.pick_floats::<Float64Type>(current.as_primitive(), candidate.as_primitive()),
            )),
            _ => {
                let compare = spark_comparator(candidate.as_ref(), current.as_ref())?;
                let take = BooleanBuffer::collect_bool(current.len(), |row| {
                    candidate.is_valid(row)
                        && (current.is_null(row) || {
                            let ordering = compare(row, row);
                            if self.greatest {
                                ordering.is_gt()
                            } else {
                                ordering.is_lt()
                            }
                        })
                });
                Ok(zip(&BooleanArray::new(take, None), candidate, current)?)
            }
        }
    }

    fn pick_floats<T>(
        &self,
        current: &PrimitiveArray<T>,
        candidate: &PrimitiveArray<T>,
    ) -> PrimitiveArray<T>
    where
        T: ArrowPrimitiveType,
        T::Native: Float,
    {
        if self.greatest {
            pick_floats_with(current, candidate, float_gt)
        } else {
            pick_floats_with(current, candidate, float_lt)
        }
    }
}

/// [`SparkGreatestLeast::pick`] for floats, where `replaces(candidate, current)` ranks the two.
fn pick_floats_with<T, F>(
    current: &PrimitiveArray<T>,
    candidate: &PrimitiveArray<T>,
    replaces: F,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native, T::Native) -> bool,
{
    let (current_values, candidate_values) = (current.values(), candidate.values());
    if current.nulls().is_none() && candidate.nulls().is_none() {
        // One select per row, which vectorizes.
        let values: ScalarBuffer<T::Native> = current_values
            .iter()
            .zip(candidate_values.iter())
            .map(|(&current, &candidate)| {
                if replaces(candidate, current) {
                    candidate
                } else {
                    current
                }
            })
            .collect();
        return PrimitiveArray::new(values, None);
    }
    let values: ScalarBuffer<T::Native> = (0..current.len())
        .map(|row| {
            let take = candidate.is_valid(row)
                && (current.is_null(row) || replaces(candidate_values[row], current_values[row]));
            if take {
                candidate_values[row]
            } else {
                current_values[row]
            }
        })
        .collect();
    // A row is null only where both are.
    let nulls = match (current.nulls(), candidate.nulls()) {
        (Some(current), Some(candidate)) => {
            Some(NullBuffer::new(current.inner() | candidate.inner()))
        }
        _ => None,
    };
    PrimitiveArray::new(values, nulls)
}

impl ScalarUDFImpl for SparkGreatestLeast {
    fn name(&self) -> &str {
        if self.greatest {
            "greatest"
        } else {
            "least"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.validate_arg_count(arg_types.len())?;
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.validate_arg_count(args.args.len())?;
        let all_scalars = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let rows = if all_scalars { 1 } else { args.number_rows };
        // Spark gives every argument the same type up to nullability, but Arrow also compares
        // field names and nullability, which the row selection below needs to match.
        let data_type = args.return_field.data_type();
        let to_array = |arg: &ColumnarValue| -> Result<ArrayRef> {
            let array = arg.to_array(rows)?;
            Ok(if array.data_type() == data_type {
                array
            } else {
                cast(&array, data_type)?
            })
        };
        let mut result = to_array(&args.args[0])?;
        for arg in &args.args[1..] {
            result = self.pick(&result, &to_array(arg)?)?;
        }
        if all_scalars {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::float_semantics::{spark_extreme, EDGE_VALUES};
    use arrow::array::{Float32Array, Float64Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, FieldRef, Fields};
    use datafusion::config::ConfigOptions;
    use std::sync::Arc;

    fn invoke(
        greatest: bool,
        args: Vec<ColumnarValue>,
        rows: usize,
        return_type: DataType,
    ) -> Result<ColumnarValue> {
        let return_field: FieldRef = Arc::new(Field::new("result", return_type, true));
        SparkGreatestLeast::new(greatest).invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    /// Every combination of three edge values, with the middle argument also given as a constant.
    /// The combinations without a null run again on their own, which takes the faster path.
    #[test]
    fn floats_follow_spark_order() -> Result<()> {
        let all_rows: Vec<[Option<f64>; 3]> = EDGE_VALUES
            .iter()
            .flat_map(|&a| {
                EDGE_VALUES
                    .iter()
                    .flat_map(move |&b| EDGE_VALUES.iter().map(move |&c| [a, b, c]))
            })
            .collect();
        let rows_without_nulls: Vec<[Option<f64>; 3]> = all_rows
            .iter()
            .copied()
            .filter(|row| row.iter().all(Option::is_some))
            .collect();
        for rows in [all_rows, rows_without_nulls] {
            check_floats_follow_spark_order(&rows)?;
        }
        Ok(())
    }

    fn check_floats_follow_spark_order(rows: &[[Option<f64>; 3]]) -> Result<()> {
        let column = |i: usize| -> ColumnarValue {
            ColumnarValue::Array(Arc::new(Float64Array::from(
                rows.iter().map(|row| row[i]).collect::<Vec<_>>(),
            )))
        };
        for greatest in [true, false] {
            let args = vec![column(0), column(1), column(2)];
            let result = invoke(greatest, args, rows.len(), DataType::Float64)?;
            let result = result.into_array(rows.len())?;
            let result = result.as_primitive::<Float64Type>();
            for (i, row) in rows.iter().enumerate() {
                let expected = spark_extreme(row, greatest).map(f64::to_bits);
                let actual = result.is_valid(i).then(|| result.value(i).to_bits());
                assert_eq!(actual, expected, "{row:?} greatest={greatest}");
            }

            // A constant in the middle still takes its turn in argument order.
            for middle in EDGE_VALUES {
                let args = vec![
                    column(0),
                    ColumnarValue::Scalar(ScalarValue::Float64(middle)),
                    column(2),
                ];
                let result = invoke(greatest, args, rows.len(), DataType::Float64)?;
                let result = result.into_array(rows.len())?;
                let result = result.as_primitive::<Float64Type>();
                for (i, row) in rows.iter().enumerate() {
                    let expected =
                        spark_extreme(&[row[0], middle, row[2]], greatest).map(f64::to_bits);
                    let actual = result.is_valid(i).then(|| result.value(i).to_bits());
                    assert_eq!(
                        actual, expected,
                        "{row:?} middle={middle:?} greatest={greatest}"
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn float32_ties_keep_the_first_argument() -> Result<()> {
        let zero = ColumnarValue::Array(Arc::new(Float32Array::from(vec![0.0f32])));
        let negative_zero = ColumnarValue::Array(Arc::new(Float32Array::from(vec![-0.0f32])));
        for greatest in [true, false] {
            for (args, expected) in [
                (vec![negative_zero.clone(), zero.clone()], -0.0f32),
                (vec![zero.clone(), negative_zero.clone()], 0.0f32),
            ] {
                let result = invoke(greatest, args, 1, DataType::Float32)?.into_array(1)?;
                let value = result.as_primitive::<Float32Type>().value(0);
                assert_eq!(value.to_bits(), expected.to_bits(), "greatest={greatest}");
            }
        }
        Ok(())
    }

    #[test]
    fn constants_only_return_a_constant() -> Result<()> {
        let scalar = |v: Option<f64>| ColumnarValue::Scalar(ScalarValue::Float64(v));
        for (greatest, args, expected) in [
            (
                true,
                vec![scalar(Some(-0.0)), scalar(Some(0.0))],
                Some(-0.0),
            ),
            (
                false,
                vec![scalar(Some(0.0)), scalar(Some(-0.0))],
                Some(0.0),
            ),
            (
                true,
                vec![
                    scalar(None),
                    scalar(Some(f64::NAN)),
                    scalar(Some(f64::INFINITY)),
                ],
                Some(f64::NAN),
            ),
            (false, vec![scalar(None), scalar(None)], None),
        ] {
            match invoke(greatest, args, 5, DataType::Float64)? {
                ColumnarValue::Scalar(ScalarValue::Float64(actual)) => {
                    assert_eq!(actual.map(f64::to_bits), expected.map(f64::to_bits))
                }
                other => panic!("expected a double scalar, got {other:?}"),
            }
        }
        Ok(())
    }

    #[test]
    fn rejects_fewer_than_two_arguments() {
        let scalar = ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0)));
        for greatest in [true, false] {
            let function = SparkGreatestLeast::new(greatest);
            for arg_types in [vec![], vec![DataType::Float64]] {
                let error = function.return_type(&arg_types).unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("requires at least two arguments"),
                    "unexpected error: {error}"
                );
            }
            for args in [vec![], vec![scalar.clone()]] {
                let error = invoke(greatest, args, 1, DataType::Float64).unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("requires at least two arguments"),
                    "unexpected error: {error}"
                );
            }
        }
    }

    /// Lists compare their elements in Spark's order too. The second argument's element field has
    /// a different name and nullability, which the result type decides.
    #[test]
    fn nested_arguments() -> Result<()> {
        let list = |name: &str, nullable: bool, values: Vec<f64>| -> ColumnarValue {
            let field = Arc::new(Field::new(name, DataType::Float64, nullable));
            ColumnarValue::Array(Arc::new(ListArray::new(
                field,
                OffsetBuffer::from_lengths(vec![1; values.len()]),
                Arc::new(Float64Array::from(values)),
                None,
            )))
        };
        let return_type = DataType::List(Arc::new(Field::new("item", DataType::Float64, true)));
        for (greatest, expected) in [(true, [-0.0, f64::NAN]), (false, [-0.0, f64::INFINITY])] {
            let args = vec![
                list("item", true, vec![-0.0, f64::INFINITY]),
                list("element", false, vec![0.0, f64::NAN]),
            ];
            let result = invoke(greatest, args, 2, return_type.clone())?.into_array(2)?;
            assert_eq!(result.data_type(), &return_type);
            let values = result
                .as_list::<i32>()
                .values()
                .as_primitive::<Float64Type>()
                .clone();
            let actual: Vec<u64> = values.values().iter().map(|v| v.to_bits()).collect();
            let expected: Vec<u64> = expected.iter().map(|v| v.to_bits()).collect();
            assert_eq!(actual, expected, "greatest={greatest}");
        }
        Ok(())
    }

    #[test]
    fn handles_floats_and_nested_floats_only() {
        let float_list = DataType::List(Arc::new(Field::new("item", DataType::Float32, true)));
        let int_list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let float_struct =
            DataType::Struct(Fields::from(vec![Field::new("v", DataType::Float64, true)]));
        assert!(SparkGreatestLeast::handles(&DataType::Float32));
        assert!(SparkGreatestLeast::handles(&DataType::Float64));
        assert!(SparkGreatestLeast::handles(&float_list));
        assert!(SparkGreatestLeast::handles(&float_struct));
        assert!(!SparkGreatestLeast::handles(&DataType::Int32));
        assert!(!SparkGreatestLeast::handles(&int_list));
        assert!(!SparkGreatestLeast::handles(&DataType::Utf8));
    }
}
