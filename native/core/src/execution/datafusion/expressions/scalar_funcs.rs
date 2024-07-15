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

use std::{any::Any, cmp::min, fmt::Debug, sync::Arc};

use arrow::{
    array::{
        ArrayRef, AsArray, Decimal128Builder, Float32Array, Float64Array, GenericStringArray,
        Int16Array, Int32Array, Int64Array, Int64Builder, Int8Array, OffsetSizeTrait,
    },
    datatypes::{validate_decimal_precision, Decimal128Type, Int64Type},
};
use arrow_array::{Array, ArrowNativeTypeOp, BooleanArray, Decimal128Array};
use arrow_schema::DataType;
use datafusion::{
    execution::FunctionRegistry,
    functions::math::round::round,
    logical_expr::{ScalarFunctionImplementation, ScalarUDFImpl, Signature, Volatility},
    physical_plan::ColumnarValue,
};
use datafusion_common::{
    cast::as_generic_string_array, exec_err, internal_err, DataFusionError,
    Result as DataFusionResult, ScalarValue,
};
use datafusion_expr::ScalarUDF;
use num::{
    integer::{div_ceil, div_floor},
    BigInt, Signed, ToPrimitive,
};
use unicode_segmentation::UnicodeSegmentation;

mod unhex;
use unhex::spark_unhex;

mod hex;
use hex::spark_hex;

mod chr;
use chr::spark_chr;

pub mod hash_expressions;
// exposed for benchmark only
use hash_expressions::wrap_digest_result_as_hex_string;
pub use hash_expressions::{spark_murmur3_hash, spark_xxhash64};

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
}

/// Create a physical scalar function.
pub fn create_comet_physical_fun(
    fun_name: &str,
    data_type: DataType,
    registry: &dyn FunctionRegistry,
) -> Result<Arc<ScalarUDF>, DataFusionError> {
    let sha2_functions = ["sha224", "sha256", "sha384", "sha512"];
    match fun_name {
        "ceil" => {
            make_comet_scalar_udf!("ceil", spark_ceil, data_type)
        }
        "floor" => {
            make_comet_scalar_udf!("floor", spark_floor, data_type)
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
        "murmur3_hash" => {
            let func = Arc::new(spark_murmur3_hash);
            make_comet_scalar_udf!("murmur3_hash", func, without data_type)
        }
        "xxhash64" => {
            let func = Arc::new(spark_xxhash64);
            make_comet_scalar_udf!("xxhash64", func, without data_type)
        }
        "chr" => {
            let func = Arc::new(spark_chr);
            make_comet_scalar_udf!("chr", func, without data_type)
        }
        "isnan" => {
            let func = Arc::new(spark_isnan);
            make_comet_scalar_udf!("isnan", func, without data_type)
        }
        sha if sha2_functions.contains(&sha) => {
            // Spark requires hex string as the result of sha2 functions, we have to wrap the
            // result of digest functions as hex string
            let func = registry.udf(sha)?;
            let wrapped_func = Arc::new(move |args: &[ColumnarValue]| {
                wrap_digest_result_as_hex_string(args, func.fun())
            });
            let spark_func_name = "spark".to_owned() + sha;
            make_comet_scalar_udf!(spark_func_name, wrapped_func, without data_type)
        }
        _ => registry.udf(fun_name).map_err(|e| {
            DataFusionError::Execution(format!(
                "Function {fun_name} not found in the registry: {e}",
            ))
        }),
    }
}

#[inline]
fn get_precision_scale(data_type: &DataType) -> (u8, i8) {
    let DataType::Decimal128(precision, scale) = data_type else {
        unreachable!()
    };
    (*precision, *scale)
}

macro_rules! downcast_compute_op {
    ($ARRAY:expr, $NAME:expr, $FUNC:ident, $TYPE:ident, $RESULT:ident) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => {
                let res: $RESULT =
                    arrow::compute::kernels::arity::unary(array, |x| x.$FUNC() as i64);
                Ok(Arc::new(res))
            }
            _ => Err(DataFusionError::Internal(format!(
                "Invalid data type for {}",
                $NAME
            ))),
        }
    }};
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

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        (self.func)(args)
    }
}

/// `ceil` function that simulates Spark `ceil` expression
pub fn spark_ceil(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float32 => {
                let result = downcast_compute_op!(array, "ceil", ceil, Float32Array, Int64Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Float64 => {
                let result = downcast_compute_op!(array, "ceil", ceil, Float64Array, Int64Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Int64 => {
                let result = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(ColumnarValue::Array(Arc::new(result.clone())))
            }
            DataType::Decimal128(_, scale) if *scale > 0 => {
                let f = decimal_ceil_f(scale);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_array(array, precision, scale, &f)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function ceil",
                other,
            ))),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x.ceil() as i64),
            ))),
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x.ceil() as i64),
            ))),
            ScalarValue::Int64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(a.map(|x| x)))),
            ScalarValue::Decimal128(a, _, scale) if *scale > 0 => {
                let f = decimal_ceil_f(scale);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_scalar(a, precision, scale, &f)
            }
            _ => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function ceil",
                value.data_type(),
            ))),
        },
    }
}

/// `floor` function that simulates Spark `floor` expression
pub fn spark_floor(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float32 => {
                let result = downcast_compute_op!(array, "floor", floor, Float32Array, Int64Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Float64 => {
                let result = downcast_compute_op!(array, "floor", floor, Float64Array, Int64Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Int64 => {
                let result = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(ColumnarValue::Array(Arc::new(result.clone())))
            }
            DataType::Decimal128(_, scale) if *scale > 0 => {
                let f = decimal_floor_f(scale);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_array(array, precision, scale, &f)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function floor",
                other,
            ))),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x.floor() as i64),
            ))),
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x.floor() as i64),
            ))),
            ScalarValue::Int64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(a.map(|x| x)))),
            ScalarValue::Decimal128(a, _, scale) if *scale > 0 => {
                let f = decimal_floor_f(scale);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_scalar(a, precision, scale, &f)
            }
            _ => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function floor",
                value.data_type(),
            ))),
        },
    }
}

pub fn spark_unscaled_value(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Scalar(v) => match v {
            ScalarValue::Decimal128(d, _, _) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                d.map(|n| n as i64),
            ))),
            dt => internal_err!("Expected Decimal128 but found {dt:}"),
        },
        ColumnarValue::Array(a) => {
            let arr = a.as_primitive::<Decimal128Type>();
            let mut result = Int64Builder::new();
            for v in arr.into_iter() {
                result.append_option(v.map(|v| v as i64));
            }
            Ok(ColumnarValue::Array(Arc::new(result.finish())))
        }
    }
}

pub fn spark_make_decimal(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> DataFusionResult<ColumnarValue> {
    let (precision, scale) = get_precision_scale(data_type);
    match &args[0] {
        ColumnarValue::Scalar(v) => match v {
            ScalarValue::Int64(n) => Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                long_to_decimal(n, precision),
                precision,
                scale,
            ))),
            sv => internal_err!("Expected Int64 but found {sv:?}"),
        },
        ColumnarValue::Array(a) => {
            let arr = a.as_primitive::<Int64Type>();
            let mut result = Decimal128Builder::new();
            for v in arr.into_iter() {
                result.append_option(long_to_decimal(&v, precision))
            }
            let result_type = DataType::Decimal128(precision, scale);

            Ok(ColumnarValue::Array(Arc::new(
                result.finish().with_data_type(result_type),
            )))
        }
    }
}

/// Convert the input long to decimal with the given maximum precision. If overflows, returns null
/// instead.
#[inline]
fn long_to_decimal(v: &Option<i64>, precision: u8) -> Option<i128> {
    match v {
        Some(v) if validate_decimal_precision(*v as i128, precision).is_ok() => Some(*v as i128),
        _ => None,
    }
}

#[inline]
fn decimal_ceil_f(scale: &i8) -> impl Fn(i128) -> i128 {
    let div = 10_i128.pow_wrapping(*scale as u32);
    move |x: i128| div_ceil(x, div)
}

#[inline]
fn decimal_floor_f(scale: &i8) -> impl Fn(i128) -> i128 {
    let div = 10_i128.pow_wrapping(*scale as u32);
    move |x: i128| div_floor(x, div)
}

// Spark uses BigDecimal. See RoundBase implementation in Spark. Instead, we do the same by
// 1) add the half of divisor, 2) round down by division, 3) adjust precision by multiplication
#[inline]
fn decimal_round_f(scale: &i8, point: &i64) -> Box<dyn Fn(i128) -> i128> {
    if *point < 0 {
        if let Some(div) = 10_i128.checked_pow((-(*point) as u32) + (*scale as u32)) {
            let half = div / 2;
            let mul = 10_i128.pow_wrapping((-(*point)) as u32);
            // i128 can hold 39 digits of a base 10 number, adding half will not cause overflow
            Box::new(move |x: i128| (x + x.signum() * half) / div * mul)
        } else {
            Box::new(move |_: i128| 0)
        }
    } else {
        let div = 10_i128.pow_wrapping((*scale as u32) - min(*scale as u32, *point as u32));
        let half = div / 2;
        Box::new(move |x: i128| (x + x.signum() * half) / div)
    }
}

#[inline]
fn make_decimal_array(
    array: &ArrayRef,
    precision: u8,
    scale: i8,
    f: &dyn Fn(i128) -> i128,
) -> Result<ColumnarValue, DataFusionError> {
    let array = array.as_primitive::<Decimal128Type>();
    let result: Decimal128Array = arrow::compute::kernels::arity::unary(array, f);
    let result = result.with_data_type(DataType::Decimal128(precision, scale));
    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[inline]
fn make_decimal_scalar(
    a: &Option<i128>,
    precision: u8,
    scale: i8,
    f: &dyn Fn(i128) -> i128,
) -> Result<ColumnarValue, DataFusionError> {
    let result = ScalarValue::Decimal128(a.map(f), precision, scale);
    Ok(ColumnarValue::Scalar(result))
}

macro_rules! integer_round {
    ($X:expr, $DIV:expr, $HALF:expr) => {{
        let rem = $X % $DIV;
        if rem <= -$HALF {
            ($X - rem).sub_wrapping($DIV)
        } else if rem >= $HALF {
            ($X - rem).add_wrapping($DIV)
        } else {
            $X - rem
        }
    }};
}

macro_rules! round_integer_array {
    ($ARRAY:expr, $POINT:expr, $TYPE:ty, $NATIVE:ty) => {{
        let array = $ARRAY.as_any().downcast_ref::<$TYPE>().unwrap();
        let ten: $NATIVE = 10;
        let result: $TYPE = if let Some(div) = ten.checked_pow((-(*$POINT)) as u32) {
            let half = div / 2;
            arrow::compute::kernels::arity::unary(array, |x| integer_round!(x, div, half))
        } else {
            arrow::compute::kernels::arity::unary(array, |_| 0)
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

macro_rules! round_integer_scalar {
    ($SCALAR:expr, $POINT:expr, $TYPE:expr, $NATIVE:ty) => {{
        let ten: $NATIVE = 10;
        if let Some(div) = ten.checked_pow((-(*$POINT)) as u32) {
            let half = div / 2;
            Ok(ColumnarValue::Scalar($TYPE(
                $SCALAR.map(|x| integer_round!(x, div, half)),
            )))
        } else {
            Ok(ColumnarValue::Scalar($TYPE(Some(0))))
        }
    }};
}

/// `round` function that simulates Spark `round` expression
fn spark_round(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    let point = &args[1];
    let ColumnarValue::Scalar(ScalarValue::Int64(Some(point))) = point else {
        return internal_err!("Invalid point argument for Round(): {:#?}", point);
    };
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Int64 if *point < 0 => round_integer_array!(array, point, Int64Array, i64),
            DataType::Int32 if *point < 0 => round_integer_array!(array, point, Int32Array, i32),
            DataType::Int16 if *point < 0 => round_integer_array!(array, point, Int16Array, i16),
            DataType::Int8 if *point < 0 => round_integer_array!(array, point, Int8Array, i8),
            DataType::Decimal128(_, scale) if *scale > 0 => {
                let f = decimal_round_f(scale, point);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_array(array, precision, scale, &f)
            }
            DataType::Float32 | DataType::Float64 => {
                Ok(ColumnarValue::Array(round(&[array.clone()])?))
            }
            dt => exec_err!("Not supported datatype for ROUND: {dt}"),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Int64(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int64, i64)
            }
            ScalarValue::Int32(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int32, i32)
            }
            ScalarValue::Int16(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int16, i16)
            }
            ScalarValue::Int8(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int8, i8)
            }
            ScalarValue::Decimal128(a, _, scale) if *scale >= 0 => {
                let f = decimal_round_f(scale, point);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_scalar(a, precision, scale, &f)
            }
            ScalarValue::Float32(_) | ScalarValue::Float64(_) => Ok(ColumnarValue::Scalar(
                ScalarValue::try_from_array(&round(&[a.to_array()?])?, 0)?,
            )),
            dt => exec_err!("Not supported datatype for ROUND: {dt}"),
        },
    }
}

/// Similar to DataFusion `rpad`, but not to truncate when the string is already longer than length
fn spark_rpad(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    match args {
        [ColumnarValue::Array(array), ColumnarValue::Scalar(ScalarValue::Int32(Some(length)))] => {
            match args[0].data_type() {
                DataType::Utf8 => spark_rpad_internal::<i32>(array, *length),
                DataType::LargeUtf8 => spark_rpad_internal::<i64>(array, *length),
                // TODO: handle Dictionary types
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {other:?} for function rpad",
                ))),
            }
        }
        other => Err(DataFusionError::Internal(format!(
            "Unsupported arguments {other:?} for function rpad",
        ))),
    }
}

fn spark_rpad_internal<T: OffsetSizeTrait>(
    array: &ArrayRef,
    length: i32,
) -> Result<ColumnarValue, DataFusionError> {
    let string_array = as_generic_string_array::<T>(array)?;

    let result = string_array
        .iter()
        .map(|string| match string {
            Some(string) => {
                let length = if length < 0 { 0 } else { length as usize };
                if length == 0 {
                    Ok(Some("".to_string()))
                } else {
                    let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                    if length < graphemes.len() {
                        Ok(Some(string.to_string()))
                    } else {
                        let mut s = string.to_string();
                        s.push_str(" ".repeat(length - graphemes.len()).as_str());
                        Ok(Some(s))
                    }
                }
            }
            _ => Ok(None),
        })
        .collect::<Result<GenericStringArray<T>, DataFusionError>>()?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

// Let Decimal(p3, s3) as return type i.e. Decimal(p1, s1) / Decimal(p2, s2) = Decimal(p3, s3).
// Conversely, Decimal(p1, s1) = Decimal(p2, s2) * Decimal(p3, s3). This means that, in order to
// get enough scale that matches with Spark behavior, it requires to widen s1 to s2 + s3 + 1. Since
// both s2 and s3 are 38 at max., s1 is 77 at max. DataFusion division cannot handle such scale >
// Decimal256Type::MAX_SCALE. Therefore, we need to implement this decimal division using BigInt.
fn spark_decimal_div(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let left = &args[0];
    let right = &args[1];
    let (p3, s3) = get_precision_scale(data_type);

    let (left, right): (ArrayRef, ArrayRef) = match (left, right) {
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => (l.clone(), r.clone()),
        (ColumnarValue::Scalar(l), ColumnarValue::Array(r)) => {
            (l.to_array_of_size(r.len())?, r.clone())
        }
        (ColumnarValue::Array(l), ColumnarValue::Scalar(r)) => {
            (l.clone(), r.to_array_of_size(l.len())?)
        }
        (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => (l.to_array()?, r.to_array()?),
    };
    let left = left.as_primitive::<Decimal128Type>();
    let right = right.as_primitive::<Decimal128Type>();
    let (_, s1) = get_precision_scale(left.data_type());
    let (_, s2) = get_precision_scale(right.data_type());

    let ten = BigInt::from(10);
    let l_exp = ((s2 + s3 + 1) as u32).saturating_sub(s1 as u32);
    let r_exp = (s1 as u32).saturating_sub((s2 + s3 + 1) as u32);
    let l_mul = ten.pow(l_exp);
    let r_mul = ten.pow(r_exp);
    let five = BigInt::from(5);
    let zero = BigInt::from(0);
    let result: Decimal128Array = arrow::compute::kernels::arity::binary(left, right, |l, r| {
        let l = BigInt::from(l) * &l_mul;
        let r = BigInt::from(r) * &r_mul;
        let div = if r.eq(&zero) { zero.clone() } else { &l / &r };
        let res = if div.is_negative() {
            div - &five
        } else {
            div + &five
        } / &ten;
        res.to_i128().unwrap_or(i128::MAX)
    })?;
    let result = result.with_data_type(DataType::Decimal128(p3, s3));
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn spark_isnan(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    fn set_nulls_to_false(is_nan: BooleanArray) -> ColumnarValue {
        match is_nan.nulls() {
            Some(nulls) => {
                let is_not_null = nulls.inner();
                ColumnarValue::Array(Arc::new(BooleanArray::new(
                    is_nan.values() & is_not_null,
                    None,
                )))
            }
            None => ColumnarValue::Array(Arc::new(is_nan)),
        }
    }
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let is_nan = BooleanArray::from_unary(array, |x| x.is_nan());
                Ok(set_nulls_to_false(is_nan))
            }
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let is_nan = BooleanArray::from_unary(array, |x| x.is_nan());
                Ok(set_nulls_to_false(is_nan))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function isnan",
                other,
            ))),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                a.map(|x| x.is_nan()).unwrap_or(false),
            )))),
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                a.map(|x| x.is_nan()).unwrap_or(false),
            )))),
            _ => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function isnan",
                value.data_type(),
            ))),
        },
    }
}
