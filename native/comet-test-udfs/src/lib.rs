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

//! Test UDF cdylib for Comet's Rust UDF host tests.
//!
//! Exports the same `add_one` semantics through both ABI flavors so the
//! host can verify them side-by-side:
//!
//! - `add_one_c` — exposed via the C ABI (arrow-only FFI, sedona-style)
//! - `add_one_df` — exposed via the datafusion-ffi ABI (`FFI_ScalarUDF`)
//!
//! Built as `libcomet_test_udfs.{so,dylib}`.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use comet_udf_sdk::c_abi::CometCScalarUdf;
use comet_udf_sdk::{comet_c_udf_export, comet_df_udf_export};

// ---------- C ABI implementation ------------------------------------

/// `add_one` exposed via the C ABI.
pub struct AddOneC;

impl Default for AddOneC {
    fn default() -> Self {
        AddOneC
    }
}

impl CometCScalarUdf for AddOneC {
    fn name(&self) -> &str {
        "add_one_c"
    }

    fn return_field(&self, args: &[Field]) -> Result<Field, String> {
        if args.len() != 1 {
            return Err(format!("add_one_c expects 1 arg, got {}", args.len()));
        }
        if args[0].data_type() != &DataType::Int64 {
            return Err(format!(
                "add_one_c expects Int64, got {}",
                args[0].data_type()
            ));
        }
        Ok(Field::new("add_one_c", DataType::Int64, true))
    }

    fn invoke(&self, args: &[ArrayRef], _n_rows: usize) -> Result<ArrayRef, String> {
        let arr = args[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "expected Int64Array".to_string())?;
        let out: Int64Array = arr.iter().map(|v| v.map(|x| x + 1)).collect();
        Ok(Arc::new(out))
    }
}

// ---------- datafusion-ffi implementation ---------------------------

/// `add_one` exposed via datafusion-ffi.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AddOneDf {
    signature: Signature,
}

impl AddOneDf {
    fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Int64]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for AddOneDf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "add_one_df"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let arr: ArrayRef = match &args.args[0] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
        };
        let a = arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("expected Int64".into()))?;
        let out: Int64Array = a.iter().map(|v| v.map(|x| x + 1)).collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// Factory used by the discovery macro.
pub fn make_add_one_df() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(AddOneDf::new()))
}

// ---------- discovery exports --------------------------------------

// `comet_c_udf_export!` emits both `comet_c_udf_list_v1` and
// `comet_udf_abi_version`. `comet_df_udf_export!` would emit
// `comet_udf_abi_version` again, so we use the `no_abi_version` form to
// avoid the duplicate symbol.
comet_c_udf_export!(AddOneC);
comet_df_udf_export!(no_abi_version => make_add_one_df);
