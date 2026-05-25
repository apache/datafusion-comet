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
use wkt::{ToWkt, TryFromWkt};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StBoundary {
    signature: Signature,
}

impl Default for StBoundary {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StBoundary {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_boundary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let col = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();

        let result: StringArray = col
            .iter()
            .map(|v| {
                let wkt_str = v?;
                let geom = geo::Geometry::<f64>::try_from_wkt_str(wkt_str).ok()?;
                // Boundary of a polygon = its exterior ring as a LineString
                // Boundary of a LineString = its two endpoints as a MultiPoint
                let boundary: geo::Geometry<f64> = match geom {
                    geo::Geometry::Polygon(p) => geo::Geometry::LineString(p.exterior().clone()),
                    geo::Geometry::MultiPolygon(mp) => {
                        let rings: Vec<geo::LineString<f64>> =
                            mp.iter().map(|p| p.exterior().clone()).collect();
                        geo::Geometry::MultiLineString(geo::MultiLineString(rings))
                    }
                    geo::Geometry::LineString(ls) => {
                        let coords = ls.0.clone();
                        if coords.len() < 2 {
                            return Some("GEOMETRYCOLLECTION EMPTY".to_string());
                        }
                        let pts = vec![
                            geo::Point::from(*coords.first()?),
                            geo::Point::from(*coords.last()?),
                        ];
                        geo::Geometry::MultiPoint(geo::MultiPoint(pts))
                    }
                    _ => return Some("GEOMETRYCOLLECTION EMPTY".to_string()),
                };
                Some(boundary.wkt_string())
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}
