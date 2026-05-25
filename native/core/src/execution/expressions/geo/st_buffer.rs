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

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geo::{Coord, LineString, Point, Polygon};
use wkt::TryFromWkt;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StBuffer {
    signature: Signature,
}

impl Default for StBuffer {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StBuffer {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_buffer"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        // Extract distance — may be a scalar literal or a column.
        let distance = scalar_to_f64(&args.args[1]);
        let geom_arrays = ColumnarValue::values_to_arrays(std::slice::from_ref(&args.args[0]))?;
        let geom_col = geom_arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let result: StringArray = geom_col
            .iter()
            .map(|g| {
                let wkt = g?;
                let geom = geo::Geometry::<f64>::try_from_wkt_str(wkt).ok()?;
                Some(geom_to_wkt(&buffer_geometry(&geom, distance, 32)))
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

fn scalar_to_f64(val: &ColumnarValue) -> f64 {
    match val {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => *v,
        ColumnarValue::Scalar(ScalarValue::Float32(Some(v))) => *v as f64,
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => *v as f64,
        ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => *v as f64,
        ColumnarValue::Scalar(ScalarValue::Decimal128(Some(v), _p, s)) => {
            (*v as f64) / 10f64.powi(*s as i32)
        }
        _ => 0.0,
    }
}

fn point_circle(cx: f64, cy: f64, radius: f64, segments: usize) -> Polygon<f64> {
    let coords: Vec<Coord<f64>> = (0..=segments)
        .map(|i| {
            let angle = 2.0 * PI * (i as f64) / (segments as f64);
            Coord {
                x: cx + radius * angle.cos(),
                y: cy + radius * angle.sin(),
            }
        })
        .collect();
    Polygon::new(LineString::from(coords), vec![])
}

fn coords_to_wkt(coords: &[Coord<f64>]) -> String {
    let pts: Vec<String> = coords.iter().map(|c| format!("{} {}", c.x, c.y)).collect();
    format!("({})", pts.join(","))
}

fn geom_to_wkt(geom: &geo::Geometry<f64>) -> String {
    match geom {
        geo::Geometry::Polygon(p) => {
            format!("POLYGON({})", coords_to_wkt(p.exterior().0.as_slice()))
        }
        geo::Geometry::MultiPolygon(mp) => {
            let parts: Vec<String> = mp
                .iter()
                .map(|p| coords_to_wkt(p.exterior().0.as_slice()))
                .collect();
            format!("MULTIPOLYGON(({}))", parts.join("),("))
        }
        other => {
            use wkt::ToWkt;
            other.wkt_string()
        }
    }
}

fn buffer_geometry(
    geom: &geo::Geometry<f64>,
    distance: f64,
    segments: usize,
) -> geo::Geometry<f64> {
    match geom {
        geo::Geometry::Point(Point(c)) => {
            geo::Geometry::Polygon(point_circle(c.x, c.y, distance, segments))
        }
        geo::Geometry::MultiPoint(mp) => {
            let polys: Vec<Polygon<f64>> = mp
                .iter()
                .map(|Point(c)| point_circle(c.x, c.y, distance, segments))
                .collect();
            geo::Geometry::MultiPolygon(geo::MultiPolygon(polys))
        }
        other => other.clone(),
    }
}
