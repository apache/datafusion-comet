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
use std::f64::consts::PI;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use wkt::TryFromWkt;

const EARTH_RADIUS_METERS: f64 = 6_371_008.8;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StDistanceSphere {
    signature: Signature,
}

impl Default for StDistanceSphere {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StDistanceSphere {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { "st_distancesphere" }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let g1s = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
        let g2s = arrays[1].as_any().downcast_ref::<StringArray>().unwrap();

        let result: Float64Array = g1s
            .iter()
            .zip(g2s.iter())
            .map(|(w1, w2)| {
                let a = geo::Geometry::<f64>::try_from_wkt_str(w1?).ok()?;
                let b = geo::Geometry::<f64>::try_from_wkt_str(w2?).ok()?;
                let (lon1, lat1) = centroid_coords(&a)?;
                let (lon2, lat2) = centroid_coords(&b)?;
                Some(haversine(lon1, lat1, lon2, lat2))
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

fn centroid_coords(geom: &geo::Geometry<f64>) -> Option<(f64, f64)> {
    use geo::Centroid;
    let c = geom.centroid()?;
    Some((c.x(), c.y()))
}

fn haversine(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let to_rad = PI / 180.0;
    let dlat = (lat2 - lat1) * to_rad;
    let dlon = (lon2 - lon1) * to_rad;
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_METERS * a.sqrt().asin()
}
