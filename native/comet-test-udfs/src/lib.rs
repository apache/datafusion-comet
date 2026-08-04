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
//! Exports, through the Comet UDF C ABI:
//!
//! - `add_one_c` — `(Int64) -> Int64`, the basic compute path
//! - `echo_c` — identity over any type, used to check that each supported
//!   Spark type survives the round trip through the ABI with its nulls
//! - `stringify_c` — `(any) -> Utf8`, which forces the UDF to actually
//!   decode the values rather than hand the array straight back
//! - `panics_on_invoke` / `panics_on_return_field` — panic containment
//!
//! Note that this crate depends only on `arrow` and `comet-udf-sdk` — no
//! DataFusion dependency — which is the point of the ABI.
//!
//! Built as `libcomet_test_udfs.{so,dylib}`.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::util::display::{ArrayFormatter, FormatOptions};

use comet_udf_sdk::c_abi::CometCScalarUdf;
use comet_udf_sdk::comet_c_udf_export;

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

/// Identity over any input type: declares the argument's own type as the
/// return type and hands the array back.
///
/// Used to check that every supported Spark type survives the trip out to
/// the UDF and back through the Arrow C Data Interface, nulls included.
#[derive(Default)]
pub struct EchoC;

impl CometCScalarUdf for EchoC {
    fn name(&self) -> &str {
        "echo_c"
    }

    fn return_field(&self, args: &[Field]) -> Result<Field, String> {
        if args.len() != 1 {
            return Err(format!("echo_c expects 1 arg, got {}", args.len()));
        }
        // Echo the argument's own type, so this works for every type
        // including the parameterized ones (decimal, timestamp with tz).
        Ok(Field::new("echo_c", args[0].data_type().clone(), true))
    }

    fn invoke(&self, args: &[ArrayRef], _n_rows: usize) -> Result<ArrayRef, String> {
        Ok(Arc::clone(&args[0]))
    }
}

/// Renders any input array as strings, one per row, preserving nulls.
///
/// Where `echo_c` only proves the array survives the round trip, this
/// forces the UDF to decode each value, so it catches a type that arrives
/// with the right `DataType` but an unreadable layout.
#[derive(Default)]
pub struct StringifyC;

impl CometCScalarUdf for StringifyC {
    fn name(&self) -> &str {
        "stringify_c"
    }

    fn return_field(&self, args: &[Field]) -> Result<Field, String> {
        if args.len() != 1 {
            return Err(format!("stringify_c expects 1 arg, got {}", args.len()));
        }
        Ok(Field::new("stringify_c", DataType::Utf8, true))
    }

    fn invoke(&self, args: &[ArrayRef], _n_rows: usize) -> Result<ArrayRef, String> {
        let array = &args[0];
        let options = FormatOptions::default().with_null("__NULL__");
        let formatter = ArrayFormatter::try_new(array.as_ref(), &options)
            .map_err(|e| format!("stringify_c cannot format {}: {e}", array.data_type()))?;
        let values: StringArray = (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    Some(formatter.value(i).to_string())
                }
            })
            .collect();
        Ok(Arc::new(values))
    }
}

/// Panics unconditionally when invoked, so host tests can verify that a
/// panic inside user code is caught at the FFI boundary and surfaced as a
/// query error rather than unwinding into the host.
#[derive(Default)]
pub struct PanicsOnInvoke;

impl CometCScalarUdf for PanicsOnInvoke {
    fn name(&self) -> &str {
        "panics_on_invoke"
    }

    fn return_field(&self, _args: &[Field]) -> Result<Field, String> {
        Ok(Field::new("panics_on_invoke", DataType::Int64, true))
    }

    fn invoke(&self, _args: &[ArrayRef], _n_rows: usize) -> Result<ArrayRef, String> {
        panic!("deliberate panic from user UDF code")
    }
}

/// Panics inside `return_field`, i.e. during planning rather than
/// execution, exercising the other side of the FFI panic boundary.
#[derive(Default)]
pub struct PanicsOnReturnField;

impl CometCScalarUdf for PanicsOnReturnField {
    fn name(&self) -> &str {
        "panics_on_return_field"
    }

    fn return_field(&self, _args: &[Field]) -> Result<Field, String> {
        panic!("deliberate panic from user return_field")
    }

    fn invoke(&self, _args: &[ArrayRef], _n_rows: usize) -> Result<ArrayRef, String> {
        unreachable!("return_field panics first")
    }
}

comet_c_udf_export!(
    AddOneC,
    EchoC,
    StringifyC,
    PanicsOnInvoke,
    PanicsOnReturnField
);
