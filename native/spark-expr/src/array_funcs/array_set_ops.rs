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

use crate::float_semantics::normalize_nested_floats;
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::functions_nested::set_ops::{array_distinct_udf, array_union_udf};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};

/// Spark's `array_distinct` and `array_union` for elements that hold a float at any depth.
///
/// Spark 4.2.0 normalizes the arguments of these functions in the plan (SPARK-54918), and 4.0.5,
/// 4.1.4 and 4.2.1 normalize while evaluating them (SPARK-59602). Either way, Spark treats `-0.0`
/// and `0.0`, and every NaN representation, as one value at any depth, and returns the normalized
/// value. DataFusion folds `-0.0` into `0.0` only in a flat float array and compares NaNs by their
/// bits, so normalize the input before delegating.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkArraySetOp {
    name: &'static str,
    datafusion_udf: Arc<ScalarUDF>,
}

impl SparkArraySetOp {
    pub fn distinct() -> Self {
        Self {
            name: "spark_array_distinct",
            datafusion_udf: array_distinct_udf(),
        }
    }

    pub fn union() -> Self {
        Self {
            name: "spark_array_union",
            datafusion_udf: array_union_udf(),
        }
    }
}

impl ScalarUDFImpl for SparkArraySetOp {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        self.datafusion_udf.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.datafusion_udf.return_type(arg_types)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args = args
            .args
            .into_iter()
            .map(|arg| match arg {
                ColumnarValue::Array(array) => {
                    Ok(ColumnarValue::Array(normalize_nested_floats(&array)))
                }
                ColumnarValue::Scalar(value) => {
                    let array = normalize_nested_floats(&value.to_array()?);
                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                        &array, 0,
                    )?))
                }
            })
            .collect::<Result<_>>()?;
        self.datafusion_udf.invoke_with_args(args)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::{bits, first_row_field_bits, list};
    use super::*;
    use crate::float_semantics::{NEGATIVE_NAN, PAYLOAD_NAN};
    use arrow::array::{Array, ArrayRef, AsArray, Float64Array, ListArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Fields, Float64Type};
    use datafusion::config::ConfigOptions;

    const ZERO: u64 = 0;
    const NAN: u64 = 0x7ff8_0000_0000_0000;

    fn invoke(
        udf: SparkArraySetOp,
        args: Vec<ColumnarValue>,
        rows: usize,
    ) -> Result<ColumnarValue> {
        let arg_fields = args
            .iter()
            .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
            .collect();
        let return_field = Arc::new(Field::new("result", args[0].data_type(), true));
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn invoke_arrays(udf: SparkArraySetOp, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        let rows = args[0].len();
        let args = args.into_iter().map(ColumnarValue::Array).collect();
        invoke(udf, args, rows)?.into_array(rows)
    }

    /// One row holding a list of `{x: DOUBLE}` structs.
    fn structs(values: &[f64]) -> ArrayRef {
        let fields = Fields::from(vec![Field::new("x", DataType::Float64, true)]);
        let x: ArrayRef = Arc::new(Float64Array::from(values.to_vec()));
        let structs: ArrayRef = Arc::new(StructArray::new(fields, vec![x], None));
        let field = Arc::new(Field::new("item", structs.data_type().clone(), true));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::from_lengths([values.len()]),
            structs,
            None,
        ))
    }

    /// One row holding a list of single-element `DOUBLE` lists.
    fn lists(values: &[f64]) -> ArrayRef {
        let inner = list(
            &values
                .iter()
                .map(|v| Some(vec![Some(*v)]))
                .collect::<Vec<_>>(),
        );
        let field = Arc::new(Field::new("item", inner.data_type().clone(), true));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::from_lengths([values.len()]),
            inner,
            None,
        ))
    }

    #[test]
    fn distinct_merges_zeros_and_nans() -> Result<()> {
        let input = list(&[
            Some(vec![
                Some(f64::NAN),
                Some(NEGATIVE_NAN),
                Some(PAYLOAD_NAN),
                Some(1.0),
                Some(-0.0),
                Some(0.0),
                None,
                None,
            ]),
            None,
        ]);
        let result = invoke_arrays(SparkArraySetOp::distinct(), vec![input])?;
        let expected = vec![
            Some(vec![Some(NAN), Some(1.0f64.to_bits()), Some(ZERO), None]),
            None,
        ];
        assert_eq!(bits(&result), expected);
        Ok(())
    }

    #[test]
    fn union_merges_across_sides_in_order() -> Result<()> {
        let left = list(&[Some(vec![Some(1.0), Some(NEGATIVE_NAN), Some(-0.0)])]);
        let right = list(&[Some(vec![Some(f64::NAN), Some(0.0), Some(2.0)])]);
        let result = invoke_arrays(SparkArraySetOp::union(), vec![left, right])?;
        let expected = vec![Some(vec![
            Some(1.0f64.to_bits()),
            Some(NAN),
            Some(ZERO),
            Some(2.0f64.to_bits()),
        ])];
        assert_eq!(bits(&result), expected);
        Ok(())
    }

    #[test]
    fn nested_floats_are_normalized() -> Result<()> {
        let input = structs(&[-0.0, 0.0, NEGATIVE_NAN, f64::NAN]);
        let result = invoke_arrays(SparkArraySetOp::distinct(), vec![input])?;
        assert_eq!(first_row_field_bits(&result), vec![ZERO, NAN]);

        let result = invoke_arrays(
            SparkArraySetOp::union(),
            vec![structs(&[-0.0]), structs(&[0.0, PAYLOAD_NAN])],
        )?;
        assert_eq!(first_row_field_bits(&result), vec![ZERO, NAN]);

        let result = invoke_arrays(
            SparkArraySetOp::distinct(),
            vec![lists(&[0.0, -0.0, f64::NAN, NEGATIVE_NAN])],
        )?;
        let row = result.as_list::<i32>().value(0);
        let inner = row.as_list::<i32>().values().as_primitive::<Float64Type>();
        let inner_bits: Vec<u64> = inner.values().iter().map(|v| v.to_bits()).collect();
        assert_eq!(inner_bits, vec![ZERO, NAN]);
        Ok(())
    }

    #[test]
    fn scalar_input_is_normalized() -> Result<()> {
        let input = list(&[Some(vec![Some(-0.0), Some(0.0), Some(NEGATIVE_NAN)])]);
        let scalar = ScalarValue::try_from_array(&input, 0)?;
        let result = invoke(
            SparkArraySetOp::distinct(),
            vec![ColumnarValue::Scalar(scalar)],
            1,
        )?;
        let ColumnarValue::Scalar(result) = result else {
            panic!("expected a scalar result");
        };
        assert_eq!(
            bits(&result.to_array()?),
            vec![Some(vec![Some(ZERO), Some(NAN)])]
        );
        Ok(())
    }
}
