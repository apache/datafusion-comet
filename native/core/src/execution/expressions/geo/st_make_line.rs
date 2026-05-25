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

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use wkt::TryFromWkt;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StMakeLine {
    signature: Signature,
}

impl Default for StMakeLine {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StMakeLine {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { "st_makeline" }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let p1s = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
        let p2s = arrays[1].as_any().downcast_ref::<StringArray>().unwrap();

        let result: StringArray = p1s
            .iter()
            .zip(p2s.iter())
            .map(|(w1, w2)| {
                let g1 = geo::Geometry::<f64>::try_from_wkt_str(w1?).ok()?;
                let g2 = geo::Geometry::<f64>::try_from_wkt_str(w2?).ok()?;
                let c1 = match g1 {
                    geo::Geometry::Point(p) => p.0,
                    _ => return None,
                };
                let c2 = match g2 {
                    geo::Geometry::Point(p) => p.0,
                    _ => return None,
                };
                Some(format!("LINESTRING({} {},{} {})", c1.x, c1.y, c2.x, c2.y))
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}
