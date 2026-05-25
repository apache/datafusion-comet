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
use geo::SimplifyVwPreserve;
use wkt::{ToWkt, TryFromWkt};

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

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StSimplifyPreserveTopology {
    signature: Signature,
}

impl Default for StSimplifyPreserveTopology {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StSimplifyPreserveTopology {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_simplifypreservetopology"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let tolerance = scalar_to_f64(&args.args[1]);
        let geom_arrays = ColumnarValue::values_to_arrays(std::slice::from_ref(&args.args[0]))?;
        let col = geom_arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let result: StringArray = col
            .iter()
            .map(|v| {
                let wkt_str = v?;
                let geom = geo::Geometry::<f64>::try_from_wkt_str(wkt_str).ok()?;
                let simplified = match geom {
                    geo::Geometry::LineString(ls) => {
                        geo::Geometry::LineString(ls.simplify_vw_preserve(&tolerance))
                    }
                    geo::Geometry::MultiLineString(ml) => {
                        geo::Geometry::MultiLineString(ml.simplify_vw_preserve(&tolerance))
                    }
                    geo::Geometry::Polygon(p) => {
                        geo::Geometry::Polygon(p.simplify_vw_preserve(&tolerance))
                    }
                    geo::Geometry::MultiPolygon(mp) => {
                        geo::Geometry::MultiPolygon(mp.simplify_vw_preserve(&tolerance))
                    }
                    other => other,
                };
                Some(simplified.wkt_string())
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}
