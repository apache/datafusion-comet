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
use geo::BooleanOps;
use wkt::{ToWkt, TryFromWkt};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StUnion {
    signature: Signature,
}

impl Default for StUnion {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StUnion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_union"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let g1s = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
        let g2s = arrays[1].as_any().downcast_ref::<StringArray>().unwrap();

        let result: StringArray = g1s
            .iter()
            .zip(g2s.iter())
            .map(|(w1, w2)| {
                let a = as_multipolygon(w1?)?;
                let b = as_multipolygon(w2?)?;
                Some(a.union(&b).wkt_string())
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

fn as_multipolygon(wkt: &str) -> Option<geo::MultiPolygon<f64>> {
    match geo::Geometry::<f64>::try_from_wkt_str(wkt).ok()? {
        geo::Geometry::Polygon(p) => Some(geo::MultiPolygon(vec![p])),
        geo::Geometry::MultiPolygon(mp) => Some(mp),
        _ => None,
    }
}
