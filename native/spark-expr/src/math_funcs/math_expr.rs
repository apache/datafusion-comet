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

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int32Array, Int64Array};
use arrow::datatypes::DataType;

use datafusion::scalar::ScalarValue;
use std::f64::consts::{E, PI};

use datafusion::common::{exec_err, Result as DataFusionResult};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

const FACTORIALS: [i64; 21] = [
    1,
    1,
    2,
    6,
    24,
    120,
    720,
    5040,
    40320,
    362880,
    3628800,
    39916800,
    479001600,
    6227020800,
    87178291200,
    1307674368000,
    20922789888000,
    355687428096000,
    6402373705728000,
    121645100408832000,
    2432902008176640000,
];

fn unary_f64_op<F>(arg: ColumnarValue, num_rows: usize, op: F) -> DataFusionResult<ColumnarValue>
where
    F: Fn(f64) -> f64,
{
    let arr = arg.into_array(num_rows)?;
    let arr = arr
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| DataFusionError::Execution("expected Float64Array".to_string()))?;

    let out: Float64Array = arr.iter().map(|v| v.map(&op)).collect();
    Ok(ColumnarValue::Array(Arc::new(out)))
}

#[derive(Clone)]
pub struct SecFunc;

impl Debug for SecFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecFunc")
    }
}
impl PartialEq for SecFunc {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for SecFunc {}
impl Hash for SecFunc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "sec".hash(state);
    }
}

impl ScalarUDFImpl for SecFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sec"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIG.get_or_init(|| Signature::exact(vec![DataType::Float64], Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        unary_f64_op(args.args[0].clone(), args.number_rows, |x| 1.0f64 / x.cos())
    }
}

#[derive(Clone)]
pub struct CscFunc;

impl Debug for CscFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CscFunc")
    }
}
impl PartialEq for CscFunc {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for CscFunc {}
impl Hash for CscFunc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "csc".hash(state);
    }
}

impl ScalarUDFImpl for CscFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "csc"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIG.get_or_init(|| Signature::exact(vec![DataType::Float64], Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        unary_f64_op(args.args[0].clone(), args.number_rows, |x| 1.0f64 / x.sin())
    }
}

#[derive(Clone)]
pub struct CbrtFunc;

impl Debug for CbrtFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CbrtFunc")
    }
}
impl PartialEq for CbrtFunc {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for CbrtFunc {}
impl Hash for CbrtFunc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "cbrt".hash(state);
    }
}

impl ScalarUDFImpl for CbrtFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "cbrt"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIG.get_or_init(|| Signature::exact(vec![DataType::Float64], Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        unary_f64_op(args.args[0].clone(), args.number_rows, |x| x.cbrt())
    }
}

#[derive(Clone)]
pub struct HypotFunc;

impl Debug for HypotFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HypotFunc")
    }
}
impl PartialEq for HypotFunc {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for HypotFunc {}
impl Hash for HypotFunc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "hypot".hash(state);
    }
}

impl ScalarUDFImpl for HypotFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "hypot"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIG.get_or_init(|| {
            Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            )
        })
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let x = args.args[0].clone().into_array(args.number_rows)?;
        let y = args.args[1].clone().into_array(args.number_rows)?;

        let x = x
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("expected Float64Array".to_string()))?;
        let y = y
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("expected Float64Array".to_string()))?;

        let out: Float64Array = (0..x.len())
            .map(|i| {
                if x.is_null(i) || y.is_null(i) {
                    None
                } else {
                    Some(x.value(i).hypot(y.value(i)))
                }
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

#[derive(Clone)]
pub struct FactorialFunc;

impl Debug for FactorialFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FactorialFunc")
    }
}
impl PartialEq for FactorialFunc {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for FactorialFunc {}
impl Hash for FactorialFunc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "factorial".hash(state);
    }
}

impl ScalarUDFImpl for FactorialFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "factorial"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIG.get_or_init(|| Signature::exact(vec![DataType::Int32], Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arr = args.args[0].clone().into_array(args.number_rows)?;
        let arr = arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| DataFusionError::Execution("expected Int32Array".to_string()))?;

        let out: Int64Array = arr
            .iter()
            .map(|v| {
                v.and_then(|x| {
                    if !(0..=20).contains(&x) {
                        None
                    } else {
                        Some(FACTORIALS[x as usize])
                    }
                })
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

#[derive(Clone)]
pub struct ShiftRightUnsignedFunc;

impl Debug for ShiftRightUnsignedFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShiftRightUnsignedFunc")
    }
}
impl PartialEq for ShiftRightUnsignedFunc {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for ShiftRightUnsignedFunc {}
impl Hash for ShiftRightUnsignedFunc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "shiftrightunsigned".hash(state);
    }
}

impl ScalarUDFImpl for ShiftRightUnsignedFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "shiftrightunsigned"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIG.get_or_init(|| {
            Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int32, DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int32]),
                ],
                Volatility::Immutable,
            )
        })
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        match arg_types.first() {
            Some(DataType::Int32) => Ok(DataType::Int32),
            Some(DataType::Int64) => Ok(DataType::Int64),
            other => exec_err!("unsupported left type for shiftrightunsigned: {other:?}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let left = args.args[0].clone().into_array(args.number_rows)?;
        let right = args.args[1].clone().into_array(args.number_rows)?;

        if let Some(left) = left.as_any().downcast_ref::<Int32Array>() {
            let right = right.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Execution("expected Int32Array shift amount".to_string())
            })?;

            let out: Int32Array = (0..left.len())
                .map(|i| {
                    if left.is_null(i) || right.is_null(i) {
                        None
                    } else {
                        let x = left.value(i) as u32;
                        let s = (right.value(i) & 31) as u32;
                        Some((x >> s) as i32)
                    }
                })
                .collect();

            return Ok(ColumnarValue::Array(Arc::new(out)));
        }

        if let Some(left) = left.as_any().downcast_ref::<Int64Array>() {
            let right = right.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Execution("expected Int32Array shift amount".to_string())
            })?;

            let out: Int64Array = (0..left.len())
                .map(|i| {
                    if left.is_null(i) || right.is_null(i) {
                        None
                    } else {
                        let x = left.value(i) as u64;
                        let s = (right.value(i) & 63) as u32;
                        Some((x >> s) as i64)
                    }
                })
                .collect();

            return Ok(ColumnarValue::Array(Arc::new(out)));
        }

        exec_err!("unsupported input arrays for shiftrightunsigned")
    }
}

#[derive(Clone)]
pub struct PiFunc;

impl Debug for PiFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PiFunc")
    }
}
impl PartialEq for PiFunc {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for PiFunc {}
impl Hash for PiFunc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "pi".hash(state);
    }
}

impl ScalarUDFImpl for PiFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "pi"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIG.get_or_init(|| Signature::exact(vec![], Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(PI))))
    }
}

#[derive(Clone)]
pub struct EulerNumberFunc;

impl Debug for EulerNumberFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EulerNumberFunc")
    }
}
impl PartialEq for EulerNumberFunc {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for EulerNumberFunc {}
impl Hash for EulerNumberFunc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "e".hash(state);
    }
}

impl ScalarUDFImpl for EulerNumberFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "e"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIG.get_or_init(|| Signature::exact(vec![], Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(E))))
    }
}

#[derive(Clone)]
pub struct ToDegreesFunc;

impl Debug for ToDegreesFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ToDegreesFunc")
    }
}
impl PartialEq for ToDegreesFunc {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for ToDegreesFunc {}
impl Hash for ToDegreesFunc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "degrees".hash(state);
    }
}

impl ScalarUDFImpl for ToDegreesFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "degrees"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIG.get_or_init(|| Signature::exact(vec![DataType::Float64], Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        unary_f64_op(args.args[0].clone(), args.number_rows, |x| x.to_degrees())
    }
}

#[derive(Clone)]
pub struct ToRadiansFunc;

impl Debug for ToRadiansFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ToRadiansFunc")
    }
}
impl PartialEq for ToRadiansFunc {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for ToRadiansFunc {}
impl Hash for ToRadiansFunc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "radians".hash(state);
    }
}

impl ScalarUDFImpl for ToRadiansFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "radians"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIG.get_or_init(|| Signature::exact(vec![DataType::Float64], Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        unary_f64_op(args.args[0].clone(), args.number_rows, |x| x.to_radians())
    }
}
