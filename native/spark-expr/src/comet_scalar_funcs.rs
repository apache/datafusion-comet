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

use crate::hash_funcs::*;
use crate::map_funcs::spark_map_sort;
use crate::math_funcs::abs::abs;
use crate::math_funcs::checked_arithmetic::{checked_add, checked_div, checked_mul, checked_sub};
use crate::math_funcs::log::spark_log;
use crate::math_funcs::modulo_expr::spark_modulo;
use crate::{
    spark_ceil, spark_decimal_div, spark_decimal_integral_div, spark_floor, spark_isnan,
    spark_lpad, spark_make_decimal, spark_read_side_padding, spark_round, spark_rpad, spark_unhex,
    spark_unscaled_value, EvalMode, SparkArrayCompact, SparkArrayPositionFunc, SparkArraysOverlap,
    SparkContains, SparkDateDiff, SparkDateFromUnixDate, SparkDateTrunc, SparkMakeDate,
    SparkSecondsToTimestamp, SparkSizeFunc,
};
use arrow::array::{Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ScalarFunctionArgs, ScalarFunctionImplementation, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::physical_plan::ColumnarValue;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

macro_rules! make_comet_scalar_udf {
    ($name:expr, $func:ident, $data_type:ident) => {{
        let scalar_func = CometScalarFunction::new(
            $name.to_string(),
            Signature::variadic_any(Volatility::Immutable),
            $data_type.clone(),
            Arc::new(move |args| $func(args, &$data_type)),
        );
        Ok(Arc::new(ScalarUDF::new_from_impl(scalar_func)))
    }};
    ($name:expr, $func:expr, without $data_type:ident) => {{
        let scalar_func = CometScalarFunction::new(
            $name.to_string(),
            Signature::variadic_any(Volatility::Immutable),
            $data_type,
            $func,
        );
        Ok(Arc::new(ScalarUDF::new_from_impl(scalar_func)))
    }};
    ($name:expr, $func:ident, without $data_type:ident, $fail_on_error:ident) => {{
        let scalar_func = CometScalarFunction::new(
            $name.to_string(),
            Signature::variadic_any(Volatility::Immutable),
            $data_type,
            Arc::new(move |args| $func(args, $fail_on_error)),
        );
        Ok(Arc::new(ScalarUDF::new_from_impl(scalar_func)))
    }};
    ($name:expr, $func:ident, $data_type:ident, $eval_mode:ident) => {{
        let scalar_func = CometScalarFunction::new(
            $name.to_string(),
            Signature::variadic_any(Volatility::Immutable),
            $data_type.clone(),
            Arc::new(move |args| $func(args, &$data_type, $eval_mode)),
        );
        Ok(Arc::new(ScalarUDF::new_from_impl(scalar_func)))
    }};
}

/// Create a physical scalar function.
pub fn create_comet_physical_fun(
    fun_name: &str,
    data_type: DataType,
    registry: &dyn FunctionRegistry,
    fail_on_error: Option<bool>,
) -> Result<Arc<ScalarUDF>, DataFusionError> {
    create_comet_physical_fun_with_eval_mode(
        fun_name,
        data_type,
        registry,
        fail_on_error,
        EvalMode::Legacy,
    )
}

/// Create a physical scalar function with eval mode. Goal is to deprecate above function once all the operators have ANSI support
pub fn create_comet_physical_fun_with_eval_mode(
    fun_name: &str,
    data_type: DataType,
    registry: &dyn FunctionRegistry,
    fail_on_error: Option<bool>,
    eval_mode: EvalMode,
) -> Result<Arc<ScalarUDF>, DataFusionError> {
    let fail_on_error = fail_on_error.unwrap_or(false);
    match fun_name {
        "ceil" => {
            make_comet_scalar_udf!("ceil", spark_ceil, data_type)
        }
        "floor" => {
            make_comet_scalar_udf!("floor", spark_floor, data_type)
        }
        "read_side_padding" => {
            let func = Arc::new(spark_read_side_padding);
            make_comet_scalar_udf!("read_side_padding", func, without data_type)
        }
        "rpad" => {
            let func = Arc::new(spark_rpad);
            make_comet_scalar_udf!("rpad", func, without data_type)
        }
        "lpad" => {
            let func = Arc::new(spark_lpad);
            make_comet_scalar_udf!("lpad", func, without data_type)
        }
        "round" => {
            make_comet_scalar_udf!("round", spark_round, data_type, fail_on_error)
        }
        "unscaled_value" => {
            let func = Arc::new(spark_unscaled_value);
            make_comet_scalar_udf!("unscaled_value", func, without data_type)
        }
        "make_decimal" => {
            make_comet_scalar_udf!("make_decimal", spark_make_decimal, data_type)
        }
        "unhex" => {
            let func = Arc::new(spark_unhex);
            make_comet_scalar_udf!("unhex", func, without data_type)
        }
        "decimal_div" => {
            make_comet_scalar_udf!("decimal_div", spark_decimal_div, data_type, eval_mode)
        }
        "decimal_integral_div" => {
            make_comet_scalar_udf!(
                "decimal_integral_div",
                spark_decimal_integral_div,
                data_type,
                eval_mode
            )
        }
        "checked_add" => {
            make_comet_scalar_udf!("checked_add", checked_add, data_type, eval_mode)
        }
        "checked_sub" => {
            make_comet_scalar_udf!("checked_sub", checked_sub, data_type, eval_mode)
        }
        "checked_mul" => {
            make_comet_scalar_udf!("checked_mul", checked_mul, data_type, eval_mode)
        }
        "checked_div" => {
            make_comet_scalar_udf!("checked_div", checked_div, data_type, eval_mode)
        }
        "murmur3_hash" => {
            let func = Arc::new(spark_murmur3_hash);
            make_comet_scalar_udf!("murmur3_hash", func, without data_type)
        }
        "xxhash64" => {
            let func = Arc::new(spark_xxhash64);
            make_comet_scalar_udf!("xxhash64", func, without data_type)
        }
        "isnan" => {
            let func = Arc::new(spark_isnan);
            make_comet_scalar_udf!("isnan", func, without data_type)
        }
        "spark_modulo" => {
            let func = Arc::new(spark_modulo);
            make_comet_scalar_udf!("spark_modulo", func, without data_type, fail_on_error)
        }
        "abs" => {
            let func = Arc::new(abs);
            make_comet_scalar_udf!("abs", func, without data_type)
        }
        "spark_log" => {
            let func = Arc::new(spark_log);
            make_comet_scalar_udf!("spark_log", func, without data_type)
        }
        "split" => {
            let func = Arc::new(crate::string_funcs::spark_split);
            make_comet_scalar_udf!("split", func, without data_type)
        }
        "get_json_object" => {
            let func = Arc::new(crate::string_funcs::spark_get_json_object);
            make_comet_scalar_udf!("get_json_object", func, without data_type)
        }
        "map_sort" => {
            let func = Arc::new(spark_map_sort);
            make_comet_scalar_udf!("spark_map_sort", func, without data_type)
        }
        "array_except" => {
            let df_udf = registry.udf("array_except")?;
            let signature = df_udf.signature().clone();
            let wrapper = NormalizingArrayExcept {
                delegate: df_udf,
                data_type,
                signature,
            };
            Ok(Arc::new(ScalarUDF::new_from_impl(wrapper)))
        }
        _ => registry.udf(fun_name).map_err(|e| {
            DataFusionError::Execution(format!(
                "Function {fun_name} not found in the registry: {e}",
            ))
        }),
    }
}

/// Normalize inner field nullability of list types to `true`.
///
/// Spark can produce arrays with different `containsNull` values for the same
/// element type (e.g., `array(not_null_col)` yields `containsNull=false` while
/// `array(nullable_col)` yields `containsNull=true`). DataFusion's array set
/// functions (e.g., `array_except`) use strict `equals_datatype()` which compares
/// inner field nullability, causing errors like:
///   "array_except received incompatible types: List(Int32), List(non-null Int32)"
///
/// This helper normalizes `false → true` so both inputs have compatible types.
fn normalize_list_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) => DataType::List(Arc::new(Field::new(
            field.name(),
            normalize_list_data_type(field.data_type()),
            true,
        ))),
        DataType::LargeList(field) => DataType::LargeList(Arc::new(Field::new(
            field.name(),
            normalize_list_data_type(field.data_type()),
            true,
        ))),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(
            Arc::new(Field::new(
                field.name(),
                normalize_list_data_type(field.data_type()),
                true,
            )),
            *size,
        ),
        _ => data_type.clone(),
    }
}

fn normalize_list_inner_nullability(arr: ArrayRef) -> ArrayRef {
    match arr.data_type() {
        DataType::List(field)
            if !field.is_nullable()
                || normalize_list_data_type(field.data_type()) != field.data_type().clone() =>
        {
            let list_arr = arr
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("Expected ListArray");
            let values = normalize_list_inner_nullability(list_arr.values().clone());
            let new_field = Arc::new(Field::new(field.name(), values.data_type().clone(), true));
            Arc::new(ListArray::new(
                new_field,
                list_arr.offsets().clone(),
                values,
                list_arr.nulls().cloned(),
            ))
        }
        DataType::LargeList(field)
            if !field.is_nullable()
                || normalize_list_data_type(field.data_type()) != field.data_type().clone() =>
        {
            let list_arr = arr
                .as_any()
                .downcast_ref::<LargeListArray>()
                .expect("Expected LargeListArray");
            let values = normalize_list_inner_nullability(list_arr.values().clone());
            let new_field = Arc::new(Field::new(field.name(), values.data_type().clone(), true));
            Arc::new(LargeListArray::new(
                new_field,
                list_arr.offsets().clone(),
                values,
                list_arr.nulls().cloned(),
            ))
        }
        DataType::FixedSizeList(field, size)
            if !field.is_nullable()
                || normalize_list_data_type(field.data_type()) != field.data_type().clone() =>
        {
            let list_arr = arr
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("Expected FixedSizeListArray");
            let values = normalize_list_inner_nullability(list_arr.values().clone());
            let new_field = Arc::new(Field::new(field.name(), values.data_type().clone(), true));
            Arc::new(FixedSizeListArray::new(
                new_field,
                *size,
                values,
                list_arr.nulls().cloned(),
            ))
        }
        _ => arr,
    }
}

fn normalize_list_scalar(scalar: ScalarValue) -> ScalarValue {
    match scalar {
        ScalarValue::List(arr) => {
            let normalized = normalize_list_inner_nullability(arr);
            ScalarValue::List(Arc::new(
                normalized
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("Expected ListArray")
                    .clone(),
            ))
        }
        ScalarValue::LargeList(arr) => {
            let normalized = normalize_list_inner_nullability(arr);
            ScalarValue::LargeList(Arc::new(
                normalized
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .expect("Expected LargeListArray")
                    .clone(),
            ))
        }
        ScalarValue::FixedSizeList(arr) => {
            let normalized = normalize_list_inner_nullability(arr);
            ScalarValue::FixedSizeList(Arc::new(
                normalized
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .expect("Expected FixedSizeListArray")
                    .clone(),
            ))
        }
        _ => scalar,
    }
}

/// Strip non-nullable markers from inner list fields in a `DataType` so the
/// return type matches what `normalize_list_inner_nullability` produces at runtime.
fn normalize_return_data_type(dt: DataType) -> DataType {
    normalize_list_data_type(&dt)
}

/// Wraps the DataFusion `array_except` UDF, normalizing inner list field nullability
/// on inputs to avoid `check_datatypes()` type compatibility errors.
struct NormalizingArrayExcept {
    delegate: Arc<ScalarUDF>,
    data_type: DataType,
    signature: Signature,
}

impl PartialEq for NormalizingArrayExcept {
    fn eq(&self, other: &Self) -> bool {
        self.data_type == other.data_type && self.signature == other.signature
    }
}

impl Eq for NormalizingArrayExcept {}

impl std::hash::Hash for NormalizingArrayExcept {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.data_type.hash(state);
        self.signature.hash(state);
    }
}

impl Debug for NormalizingArrayExcept {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NormalizingArrayExcept")
            .field("name", &"array_except")
            .field("data_type", &self.data_type)
            .finish()
    }
}

impl ScalarUDFImpl for NormalizingArrayExcept {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_except"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        let dt = self.delegate.inner().return_type(arg_types)?;
        // Normalize the return type so it matches the runtime normalization
        // applied to inputs. Without this, the planner might declare a return
        // type like List(Int32, false) while the actual output is List(Int32, true).
        Ok(normalize_return_data_type(dt))
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        for arg in args.args.iter_mut() {
            match arg {
                ColumnarValue::Array(arr) => {
                    *arg = ColumnarValue::Array(normalize_list_inner_nullability(Arc::clone(arr)));
                }
                ColumnarValue::Scalar(scalar) => {
                    *arg = ColumnarValue::Scalar(normalize_list_scalar(scalar.clone()));
                }
            }
        }
        self.delegate.invoke_with_args(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        self.delegate.inner().coerce_types(arg_types)
    }

    fn aliases(&self) -> &[String] {
        &[]
    }
}

fn all_scalar_functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(ScalarUDF::new_from_impl(SparkArrayCompact::default())),
        Arc::new(ScalarUDF::new_from_impl(SparkArrayPositionFunc::default())),
        Arc::new(ScalarUDF::new_from_impl(SparkArraysOverlap::default())),
        Arc::new(ScalarUDF::new_from_impl(SparkContains::default())),
        Arc::new(ScalarUDF::new_from_impl(SparkDateDiff::default())),
        Arc::new(ScalarUDF::new_from_impl(SparkDateFromUnixDate::default())),
        Arc::new(ScalarUDF::new_from_impl(SparkDateTrunc::default())),
        Arc::new(ScalarUDF::new_from_impl(SparkMakeDate::default())),
        Arc::new(ScalarUDF::new_from_impl(SparkSecondsToTimestamp::default())),
        Arc::new(ScalarUDF::new_from_impl(SparkSizeFunc::default())),
    ]
}

/// Registers all custom UDFs
pub fn register_all_comet_functions(registry: &mut dyn FunctionRegistry) -> DataFusionResult<()> {
    // This will override existing UDFs with the same name
    all_scalar_functions()
        .into_iter()
        .try_for_each(|udf| registry.register_udf(udf).map(|_| ()))?;

    Ok(())
}

struct CometScalarFunction {
    name: String,
    signature: Signature,
    data_type: DataType,
    func: ScalarFunctionImplementation,
}

impl PartialEq for CometScalarFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.signature == other.signature
            && self.data_type == other.data_type
        // Note: we do not test ScalarFunctionImplementation equality, relying on function metadata.
    }
}

impl Eq for CometScalarFunction {}

impl std::hash::Hash for CometScalarFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        self.data_type.hash(state);
        // Note: we do not hash ScalarFunctionImplementation, relying on function metadata.
    }
}

impl Debug for CometScalarFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CometScalarFunction")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("data_type", &self.data_type)
            .finish()
    }
}

impl CometScalarFunction {
    fn new(
        name: String,
        signature: Signature,
        data_type: DataType,
        func: ScalarFunctionImplementation,
    ) -> Self {
        Self {
            name,
            signature,
            data_type,
            func,
        }
    }
}

impl ScalarUDFImpl for CometScalarFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> {
        Ok(self.data_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        (self.func)(&args.args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::buffer::OffsetBuffer;

    #[test]
    fn normalizes_scalar_list_nullability() {
        let values = Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef;
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            OffsetBuffer::new(vec![0, 2].into()),
            values,
            None,
        ));

        let normalized = normalize_list_scalar(ScalarValue::List(list));
        let ScalarValue::List(normalized) = normalized else {
            panic!("Expected list scalar");
        };
        let DataType::List(field) = normalized.data_type() else {
            panic!("Expected list type");
        };
        assert!(field.is_nullable());
    }

    #[test]
    fn normalizes_nested_list_data_type() {
        let data_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
            false,
        )));

        let normalized = normalize_list_data_type(&data_type);
        let DataType::List(outer_field) = normalized else {
            panic!("Expected outer list type");
        };
        assert!(outer_field.is_nullable());

        let DataType::List(inner_field) = outer_field.data_type() else {
            panic!("Expected inner list type");
        };
        assert!(inner_field.is_nullable());
    }
}
