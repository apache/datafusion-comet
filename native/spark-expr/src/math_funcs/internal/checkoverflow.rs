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

use crate::utils::is_valid_decimal_precision;
use arrow::datatypes::{DataType, Schema};
use arrow::{
    array::{as_primitive_array, Array},
    datatypes::Decimal128Type,
    record_batch::RecordBatch,
};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::hash::Hash;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    sync::Arc,
};

/// This is from Spark `CheckOverflow` expression. Spark `CheckOverflow` expression rounds decimals
/// to given scale and check if the decimals can fit in given precision. As `cast` kernel rounds
/// decimals already, Comet `CheckOverflow` expression only checks if the decimals can fit in the
/// precision.
#[derive(Debug, Eq)]
pub struct CheckOverflow {
    pub child: Arc<dyn PhysicalExpr>,
    pub data_type: DataType,
    pub fail_on_error: bool,
}

impl Hash for CheckOverflow {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.data_type.hash(state);
        self.fail_on_error.hash(state);
    }
}

impl PartialEq for CheckOverflow {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.data_type.eq(&other.data_type)
            && self.fail_on_error.eq(&other.fail_on_error)
    }
}

impl CheckOverflow {
    pub fn new(child: Arc<dyn PhysicalExpr>, data_type: DataType, fail_on_error: bool) -> Self {
        Self {
            child,
            data_type,
            fail_on_error,
        }
    }
}

impl Display for CheckOverflow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CheckOverflow [datatype: {}, fail_on_error: {}, child: {}]",
            self.data_type, self.fail_on_error, self.child
        )
    }
}

impl PhysicalExpr for CheckOverflow {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }

    fn data_type(&self, _: &Schema) -> datafusion::common::Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _: &Schema) -> datafusion::common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array)
                if matches!(array.data_type(), DataType::Decimal128(_, _)) =>
            {
                let (target_precision, target_scale) = match &self.data_type {
                    DataType::Decimal128(p, s) => (*p, *s),
                    dt => {
                        return Err(DataFusionError::Execution(format!(
                            "CheckOverflow expects only Decimal128, but got {dt:?}"
                        )))
                    }
                };

                let decimal_array = as_primitive_array::<Decimal128Type>(&array);

                let result_array = if self.fail_on_error {
                    // ANSI mode: validate and return error on overflow
                    // Use optimized validation that avoids error string allocation until needed
                    for i in 0..decimal_array.len() {
                        if decimal_array.is_valid(i) {
                            let value = decimal_array.value(i);
                            if !is_valid_decimal_precision(value, target_precision) {
                                return Err(DataFusionError::ArrowError(
                                    Box::new(arrow::error::ArrowError::InvalidArgumentError(
                                        format!(
                                            "{} is not a valid Decimal128 value with precision {}",
                                            value, target_precision
                                        ),
                                    )),
                                    None,
                                ));
                            }
                        }
                    }
                    // Validation passed - just update metadata without copying data
                    decimal_array
                        .clone()
                        .with_precision_and_scale(target_precision, target_scale)?
                } else {
                    // Legacy/Try mode: convert overflows to null
                    // Use Arrow's optimized null_if_overflow_precision which does a single pass
                    let result = decimal_array.null_if_overflow_precision(target_precision);
                    result.with_precision_and_scale(target_precision, target_scale)?
                };

                Ok(ColumnarValue::Array(Arc::new(result_array)))
            }
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, _, _)) => {
                let (target_precision, target_scale) = match &self.data_type {
                    DataType::Decimal128(p, s) => (*p, *s),
                    dt => {
                        return Err(DataFusionError::Execution(format!(
                            "CheckOverflow expects only Decimal128 for scalar, but got {dt:?}"
                        )))
                    }
                };

                if self.fail_on_error {
                    if let Some(value) = v {
                        if !is_valid_decimal_precision(value, target_precision) {
                            return Err(DataFusionError::ArrowError(
                                Box::new(arrow::error::ArrowError::InvalidArgumentError(format!(
                                    "{} is not a valid Decimal128 value with precision {}",
                                    value, target_precision
                                ))),
                                None,
                            ));
                        }
                    }
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        v,
                        target_precision,
                        target_scale,
                    )))
                } else {
                    // Use optimized bool check instead of Result-returning validation
                    let new_v = v.filter(|&val| is_valid_decimal_precision(val, target_precision));
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        new_v,
                        target_precision,
                        target_scale,
                    )))
                }
            }
            v => Err(DataFusionError::Execution(format!(
                "CheckOverflow's child expression should be decimal array, but found {v:?}"
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(CheckOverflow::new(
            Arc::clone(&children[0]),
            self.data_type.clone(),
            self.fail_on_error,
        )))
    }
}
