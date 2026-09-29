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
use arrow::compute::cast;
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
        Ok(output_type(&self.child.data_type(input_schema)?))
    }

    fn nullable(&self, _: &Schema) -> datafusion::common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let timestamp = self.child.evaluate(batch)?;
        let format = self.format.evaluate(batch)?;
        let output_type = output_type(&timestamp.data_type());
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
        // The kernels label their output with the session timezone they truncated in. Relabel it
        // with the input's timezone, which leaves the values unchanged.
        let relabel = |result: ArrayRef| -> datafusion::common::Result<ArrayRef> {
            Ok(cast(&result, &output_type)?)
        };
        match (timestamp, format) {
            (ColumnarValue::Array(ts), ColumnarValue::Scalar(Utf8(Some(format)))) => {
                let result = timestamp_trunc_dyn(&resolve_tz(ts)?, format)?;
                Ok(ColumnarValue::Array(relabel(result)?))
            }
            (ColumnarValue::Array(ts), ColumnarValue::Array(formats)) => {
                let result = timestamp_trunc_array_fmt_dyn(&resolve_tz(ts)?, &formats)?;
                Ok(ColumnarValue::Array(relabel(result)?))
            }
            (ColumnarValue::Scalar(ts_scalar), ColumnarValue::Scalar(Utf8(Some(format)))) => {
                let result = timestamp_trunc_dyn(&resolve_tz(ts_scalar.to_array()?)?, format)?;
                let scalar = ScalarValue::try_from_array(&relabel(result)?, 0)?;
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

/// The result type for an input of type `input`. The kernels truncate in the session timezone, but
/// the result keeps the input's timezone label. Every `TimestampType` value in a native plan is
/// labelled "UTC", and a result labelled with the session timezone could not be compared with
/// other timestamps or mixed with them in `CASE` and `coalesce`.
fn output_type(input: &DataType) -> DataType {
    match input {
        DataType::Dictionary(key_type, value_type) => {
            DataType::Dictionary(key_type.clone(), Box::new(output_type(value_type)))
        }
        DataType::Timestamp(_, tz) => DataType::Timestamp(Microsecond, tz.clone()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray, DictionaryArray, Int32Array, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, Int32Type, TimestampMicrosecondType};
    use datafusion::physical_expr::expressions::{Column, Literal};

    /// 2024-01-15 18:30:45 UTC, which is 2024-01-16 00:00:45 in Asia/Kolkata (+05:30).
    const MICROS: i64 = 1_705_343_445_000_000;
    /// `MICROS` truncated to the hour in Asia/Kolkata: 2024-01-15 18:30:00 UTC.
    const HOUR_IN_KOLKATA: i64 = 1_705_343_400_000_000;

    fn utc_timestamp() -> DataType {
        DataType::Timestamp(Microsecond, Some("UTC".into()))
    }

    /// Truncates `input` to the hour in Asia/Kolkata, returning the declared and actual types.
    fn trunc_to_hour_in_kolkata(input: ArrayRef) -> (DataType, ArrayRef) {
        let schema = Schema::new(vec![Field::new("ts", input.data_type().clone(), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![input]).unwrap();
        let expr = TimestampTruncExpr::new(
            Arc::new(Column::new("ts", 0)),
            Arc::new(Literal::new(Utf8(Some("HOUR".to_string())))),
            "Asia/Kolkata".to_string(),
        );
        let declared = expr.data_type(&schema).unwrap();
        let ColumnarValue::Array(result) = expr.evaluate(&batch).unwrap() else {
            panic!("expected an array");
        };
        (declared, result)
    }

    #[test]
    fn result_keeps_the_input_label() {
        let input = TimestampMicrosecondArray::from(vec![Some(MICROS), None]).with_timezone("UTC");
        let (declared, result) = trunc_to_hour_in_kolkata(Arc::new(input));
        assert_eq!(declared, utc_timestamp());
        assert_eq!(result.data_type(), &utc_timestamp());
        let result = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(result.value(0), HOUR_IN_KOLKATA);
        assert!(result.is_null(1));
    }

    #[test]
    fn dictionary_result_keeps_the_input_label() {
        let values = TimestampMicrosecondArray::from(vec![MICROS]).with_timezone("UTC");
        let input =
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 0]), Arc::new(values))
                .unwrap();
        let expected = DataType::Dictionary(Box::new(DataType::Int32), Box::new(utc_timestamp()));
        let (declared, result) = trunc_to_hour_in_kolkata(Arc::new(input));
        assert_eq!(declared, expected);
        assert_eq!(result.data_type(), &expected);
        let values = result.as_any_dictionary().values();
        assert_eq!(
            values.as_primitive::<TimestampMicrosecondType>().value(0),
            HOUR_IN_KOLKATA
        );
    }
}
