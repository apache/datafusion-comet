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

use arrow::array::{ArrayRef, Float64Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use geo::EuclideanDistance;
use wkt::TryFromWkt;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StDistance {
    signature: Signature,
}

impl Default for StDistance {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StDistance {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_distance"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let geom1 = args[0].as_any().downcast_ref::<StringArray>().unwrap();
        let geom2 = args[1].as_any().downcast_ref::<StringArray>().unwrap();

        let result: Float64Array = geom1
            .iter()
            .zip(geom2.iter())
            .map(|(g1, g2)| match (g1, g2) {
                (Some(g1), Some(g2)) => {
                    let a = geo::Geometry::<f64>::try_from_wkt_str(g1).ok()?;
                    let b = geo::Geometry::<f64>::try_from_wkt_str(g2).ok()?;
                    Some(a.euclidean_distance(&b))
                }
                _ => None,
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}
