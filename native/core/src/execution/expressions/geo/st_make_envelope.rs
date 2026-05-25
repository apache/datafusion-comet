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
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array, Float64Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StMakeEnvelope {
    signature: Signature,
}

impl Default for StMakeEnvelope {
    fn default() -> Self {
        Self {
            signature: Signature::any(4, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StMakeEnvelope {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_makeenvelope"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let xmins = extract_f64_col(&arrays[0]);
        let ymins = extract_f64_col(&arrays[1]);
        let xmaxs = extract_f64_col(&arrays[2]);
        let ymaxs = extract_f64_col(&arrays[3]);

        let result: StringArray = (0..xmins.len())
            .map(|i| {
                let (xmin, ymin, xmax, ymax) = (xmins[i]?, ymins[i]?, xmaxs[i]?, ymaxs[i]?);
                Some(format!(
                    "POLYGON(({xmin} {ymin},{xmax} {ymin},{xmax} {ymax},{xmin} {ymax},{xmin} {ymin}))"
                ))
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

fn extract_f64_col(arr: &dyn Array) -> Vec<Option<f64>> {
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return a.iter().collect();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Decimal128Array>() {
        let scale = match arr.data_type() {
            DataType::Decimal128(_, s) => *s as i32,
            _ => 0,
        };
        return a
            .iter()
            .map(|v| v.map(|n| (n as f64) / 10f64.powi(scale)))
            .collect();
    }
    vec![None; arr.len()]
}
