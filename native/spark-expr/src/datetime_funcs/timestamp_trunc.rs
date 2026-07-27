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

use crate::utils::array_with_timezone;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Schema, TimeUnit::Microsecond};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, ScalarValue, ScalarValue::Utf8};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::hash::Hash;
use std::{
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

use crate::kernels::temporal::{timestamp_trunc_array_fmt_dyn, timestamp_trunc_dyn};

#[derive(Debug, Eq)]
pub struct TimestampTruncExpr {
    /// An array with DataType::Timestamp(TimeUnit::Microsecond, None)
    child: Arc<dyn PhysicalExpr>,
    /// Scalar UTF8 string matching the valid values in Spark SQL: https://spark.apache.org/docs/latest/api/sql/index.html#date_trunc
    format: Arc<dyn PhysicalExpr>,
    /// IANA timezone name (e.g. `America/Los_Angeles`) or fixed offset (`+HH:MM`). Stored as
    /// `Arc<str>` so it can be cheaply cloned onto Arrow `Timestamp` data types without
    /// reallocating, and parsed once into a `chrono::TimeZone` per batch.
    timezone: Arc<str>,
}

impl Hash for TimestampTruncExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.format.hash(state);
        self.timezone.hash(state);
    }
}
impl PartialEq for TimestampTruncExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.format.eq(&other.format)
            && self.timezone.eq(&other.timezone)
    }
}

impl TimestampTruncExpr {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        format: Arc<dyn PhysicalExpr>,
        timezone: String,
    ) -> Self {
        TimestampTruncExpr {
            child,
            format,
            timezone: Arc::from(timezone),
        }
    }
}

impl Display for TimestampTruncExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TimestampTrunc [child:{}, format:{}, timezone: {}]",
            self.child, self.format, self.timezone
        )
    }
}

impl PhysicalExpr for TimestampTruncExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion::common::Result<DataType> {
        // The kernel converts the input to the session timezone and emits an array stamped with
        // that timezone (NTZ stays NTZ). The declared `data_type` has to match or
        // shuffle/sort/`RowConverter` will fail with a schema-mismatch error.
        let session_tz = || DataType::Timestamp(Microsecond, Some(Arc::clone(&self.timezone)));
        match self.child.data_type(input_schema)? {
            DataType::Timestamp(_, None) => Ok(DataType::Timestamp(Microsecond, None)),
            DataType::Dictionary(key_type, inner) => {
                let inner_out = match inner.as_ref() {
                    DataType::Timestamp(_, None) => DataType::Timestamp(Microsecond, None),
                    _ => session_tz(),
                };
                Ok(DataType::Dictionary(key_type, Box::new(inner_out)))
            }
            _ => Ok(session_tz()),
        }
    }

    fn nullable(&self, _: &Schema) -> datafusion::common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let timestamp = self.child.evaluate(batch)?;
        let format = self.format.evaluate(batch)?;
        let tz = &self.timezone;
        let resolve_tz = |ts: ArrayRef| -> datafusion::common::Result<ArrayRef> {
            // For TimestampNTZ (Timestamp(Microsecond, None)), skip timezone conversion.
            // NTZ values are timezone-independent and truncation should operate directly on the
            // naive microsecond values without any timezone resolution.
            if matches!(ts.data_type(), DataType::Timestamp(Microsecond, None)) {
                Ok(ts)
            } else {
                Ok(array_with_timezone(
                    ts,
                    tz.to_string(),
                    Some(&DataType::Timestamp(Microsecond, Some(Arc::clone(tz)))),
                )?)
            }
        };
        match (timestamp, format) {
            (ColumnarValue::Array(ts), ColumnarValue::Scalar(Utf8(Some(format)))) => {
                let result = timestamp_trunc_dyn(&resolve_tz(ts)?, format)?;
                Ok(ColumnarValue::Array(result))
            }
            (ColumnarValue::Array(ts), ColumnarValue::Array(formats)) => {
                let result = timestamp_trunc_array_fmt_dyn(&resolve_tz(ts)?, &formats)?;
                Ok(ColumnarValue::Array(result))
            }
            (ColumnarValue::Scalar(ts_scalar), ColumnarValue::Scalar(Utf8(Some(format)))) => {
                let result = timestamp_trunc_dyn(&resolve_tz(ts_scalar.to_array()?)?, format)?;
                let scalar = ScalarValue::try_from_array(&result, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
            _ => Err(DataFusionError::Execution(
                "Invalid input to function TimestampTrunc. \
                    Expected (Timestamp, Utf8)"
                    .to_string(),
            )),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
        Ok(Arc::new(TimestampTruncExpr::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.format),
            self.timezone.to_string(),
        )))
    }
}
