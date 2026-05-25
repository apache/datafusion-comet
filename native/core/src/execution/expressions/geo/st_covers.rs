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

use arrow::array::{ArrayRef, BooleanArray, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use geo::relate::Relate;
use wkt::TryFromWkt;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StCovers {
    signature: Signature,
}

impl Default for StCovers {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StCovers {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { "st_covers" }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let g1s = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
        let g2s = arrays[1].as_any().downcast_ref::<StringArray>().unwrap();

        let result: BooleanArray = g1s
            .iter()
            .zip(g2s.iter())
            .map(|(w1, w2)| match (w1, w2) {
                (Some(w1), Some(w2)) => {
                    let a = geo::Geometry::<f64>::try_from_wkt_str(w1).ok()?;
                    let b = geo::Geometry::<f64>::try_from_wkt_str(w2).ok()?;
                    // DE-9IM T*****FF* or *T****FF* or ***T**FF* or ****T*FF*
                    Some(a.relate(&b).is_covers())
                }
                _ => None,
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}
