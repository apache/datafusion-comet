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
use crate::map_funcs::{map_from_list, map_to_list, spark_map_sort};
use crate::math_funcs::checked_arithmetic::{checked_add, checked_div, checked_mul, checked_sub};
use crate::math_funcs::modulo_expr::spark_modulo;
use crate::{
    spark_array_repeat, spark_ceil, spark_date_add, spark_date_sub, spark_decimal_div,
    spark_decimal_integral_div, spark_floor, spark_hex, spark_isnan, spark_make_decimal,
    spark_read_side_padding, spark_round, spark_rpad, spark_unhex, spark_unscaled_value,
    SparkBitwiseCount, SparkBitwiseGet, SparkBitwiseNot, SparkDateTrunc, SparkStringSpace,
};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
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
}

/// Create a physical scalar function.
pub fn create_comet_physical_fun(
    fun_name: &str,
    data_type: DataType,
    registry: &dyn FunctionRegistry,
    fail_on_error: Option<bool>,
) -> Result<Arc<ScalarUDF>, DataFusionError> {
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
        "round" => {
            make_comet_scalar_udf!("round", spark_round, data_type)
        }
        "unscaled_value" => {
            let func = Arc::new(spark_unscaled_value);
            make_comet_scalar_udf!("unscaled_value", func, without data_type)
        }
        "make_decimal" => {
            make_comet_scalar_udf!("make_decimal", spark_make_decimal, data_type)
        }
        "hex" => {
            let func = Arc::new(spark_hex);
            make_comet_scalar_udf!("hex", func, without data_type)
        }
        "unhex" => {
            let func = Arc::new(spark_unhex);
            make_comet_scalar_udf!("unhex", func, without data_type)
        }
        "decimal_div" => {
            make_comet_scalar_udf!("decimal_div", spark_decimal_div, data_type)
        }
        "decimal_integral_div" => {
            make_comet_scalar_udf!(
                "decimal_integral_div",
                spark_decimal_integral_div,
                data_type
            )
        }
        "checked_add" => {
            make_comet_scalar_udf!("checked_add", checked_add, data_type)
        }
        "checked_sub" => {
            make_comet_scalar_udf!("checked_sub", checked_sub, data_type)
        }
        "checked_mul" => {
            make_comet_scalar_udf!("checked_mul", checked_mul, data_type)
        }
        "checked_div" => {
            make_comet_scalar_udf!("checked_div", checked_div, data_type)
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
        "date_add" => {
            let func = Arc::new(spark_date_add);
            make_comet_scalar_udf!("date_add", func, without data_type)
        }
        "date_sub" => {
            let func = Arc::new(spark_date_sub);
            make_comet_scalar_udf!("date_sub", func, without data_type)
        }
        "array_repeat" => {
            let func = Arc::new(spark_array_repeat);
            make_comet_scalar_udf!("array_repeat", func, without data_type)
        }
        "spark_modulo" => {
            let func = Arc::new(spark_modulo);
            let fail_on_error = fail_on_error.unwrap_or(false);
            make_comet_scalar_udf!("spark_modulo", func, without data_type, fail_on_error)
        }
        "map_sort" => {
            let func = Arc::new(spark_map_sort);
            make_comet_scalar_udf!("spark_map_sort", func, without data_type)
        }
        "map_to_list" => {
            let func = Arc::new(map_to_list);
            make_comet_scalar_udf!("map_to_list", func, without data_type)
        }
        "map_from_list" => {
            let func = Arc::new(map_from_list);
            make_comet_scalar_udf!("map_from_list", func, without data_type)
        }
        _ => registry.udf(fun_name).map_err(|e| {
            DataFusionError::Execution(format!(
                "Function {fun_name} not found in the registry: {e}",
            ))
        }),
    }
}

fn all_scalar_functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(ScalarUDF::new_from_impl(SparkBitwiseNot::default())),
        Arc::new(ScalarUDF::new_from_impl(SparkBitwiseCount::default())),
        Arc::new(ScalarUDF::new_from_impl(SparkBitwiseGet::default())),
        Arc::new(ScalarUDF::new_from_impl(SparkDateTrunc::default())),
        Arc::new(ScalarUDF::new_from_impl(SparkStringSpace::default())),
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
