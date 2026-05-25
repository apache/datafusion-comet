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
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geo::Simplify;
use wkt::{ToWkt, TryFromWkt};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StSimplify {
    signature: Signature,
}

impl Default for StSimplify {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StSimplify {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_simplify"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        // Extract tolerance — may be a scalar literal or a column.
        let tolerance = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => *v,
            ColumnarValue::Scalar(ScalarValue::Float32(Some(v))) => *v as f64,
            _ => {
                let arr = ColumnarValue::values_to_arrays(std::slice::from_ref(&args.args[1]))?;
                arr[0].as_any().downcast_ref::<arrow::array::Float64Array>()
                    .and_then(|a| a.iter().next().flatten())
                    .unwrap_or(0.0)
            }
        };
        let geom_arrays = ColumnarValue::values_to_arrays(std::slice::from_ref(&args.args[0]))?;
        let geom_col = geom_arrays[0].as_any().downcast_ref::<StringArray>().unwrap();

        let result: StringArray = geom_col
            .iter()
            .map(|g| {
                let wkt = g?;
                let geom = geo::Geometry::<f64>::try_from_wkt_str(wkt).ok()?;
                let simplified = match geom {
                    geo::Geometry::LineString(ls) => {
                        geo::Geometry::LineString(ls.simplify(&tolerance))
                    }
                    geo::Geometry::MultiLineString(ml) => {
                        geo::Geometry::MultiLineString(ml.simplify(&tolerance))
                    }
                    geo::Geometry::Polygon(p) => {
                        geo::Geometry::Polygon(p.simplify(&tolerance))
                    }
                    geo::Geometry::MultiPolygon(mp) => {
                        geo::Geometry::MultiPolygon(mp.simplify(&tolerance))
                    }
                    other => other,
                };
                Some(simplified.wkt_string())
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}
