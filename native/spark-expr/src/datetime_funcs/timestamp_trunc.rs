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
use arrow::array::{new_null_array, ArrayRef};
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
    /// Whether SECOND and MILLISECOND truncation wraps below the smallest timestamp, as Spark
    /// did before 4.2.0, instead of raising `long overflow` as 4.2.0 and later do (SPARK-56663).
    wrap_second_millisecond_overflow: bool,
}

impl Hash for TimestampTruncExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.format.hash(state);
        self.timezone.hash(state);
        self.wrap_second_millisecond_overflow.hash(state);
    }
}
impl PartialEq for TimestampTruncExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.format.eq(&other.format)
            && self.timezone.eq(&other.timezone)
            && self.wrap_second_millisecond_overflow == other.wrap_second_millisecond_overflow
    }
}

impl TimestampTruncExpr {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        format: Arc<dyn PhysicalExpr>,
        timezone: String,
        wrap_second_millisecond_overflow: bool,
    ) -> Self {
        TimestampTruncExpr {
            child,
            format,
            timezone: Arc::from(timezone),
            wrap_second_millisecond_overflow,
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
        let wrap = self.wrap_second_millisecond_overflow;
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
                let result = timestamp_trunc_dyn(&resolve_tz(ts)?, format, wrap)?;
                Ok(ColumnarValue::Array(relabel(result)?))
            }
            (ColumnarValue::Array(ts), ColumnarValue::Array(formats)) => {
                let result = timestamp_trunc_array_fmt_dyn(&resolve_tz(ts)?, &formats, wrap)?;
                Ok(ColumnarValue::Array(relabel(result)?))
            }
            (ColumnarValue::Scalar(ts_scalar), ColumnarValue::Scalar(Utf8(Some(format)))) => {
                let result =
                    timestamp_trunc_dyn(&resolve_tz(ts_scalar.to_array()?)?, format, wrap)?;
                let scalar = ScalarValue::try_from_array(&relabel(result)?, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
            (ColumnarValue::Scalar(ts_scalar), ColumnarValue::Array(formats)) => {
                let ts = ts_scalar.to_array_of_size(formats.len())?;
                let result = timestamp_trunc_array_fmt_dyn(&resolve_tz(ts)?, &formats, wrap)?;
                Ok(ColumnarValue::Array(relabel(result)?))
            }
            // A NULL format gives NULL, as in Spark.
            (ColumnarValue::Array(ts), ColumnarValue::Scalar(Utf8(None))) => {
                Ok(ColumnarValue::Array(new_null_array(&output_type, ts.len())))
            }
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(Utf8(None))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from(&output_type)?))
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
            self.wrap_second_millisecond_overflow,
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
    use arrow::array::{
        Array, AsArray, DictionaryArray, Int32Array, StringArray, TimestampMicrosecondArray,
    };
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
            false,
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

    /// Evaluates `date_trunc` in America/Los_Angeles with the given timestamp and format inputs.
    fn trunc_in_los_angeles(
        timestamp: Arc<dyn PhysicalExpr>,
        format: Arc<dyn PhysicalExpr>,
        batch: &RecordBatch,
    ) -> ColumnarValue {
        TimestampTruncExpr::new(timestamp, format, "America/Los_Angeles".to_string(), false)
            .evaluate(batch)
            .unwrap()
    }

    #[test]
    fn literal_timestamp_with_a_format_column() {
        // 2024-11-03 01:30 PDT, inside the overlap. DAY keeps the input's offset, YEAR does not
        // depend on it, and a NULL format gives NULL.
        let micros = 1_730_622_600_000_000;
        let formats = StringArray::from(vec![Some("DAY"), Some("YEAR"), None]);
        let schema = Schema::new(vec![Field::new("fmt", DataType::Utf8, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(formats)]).unwrap();
        let timestamp = Literal::new(ScalarValue::TimestampMicrosecond(
            Some(micros),
            Some("UTC".into()),
        ));
        let ColumnarValue::Array(result) =
            trunc_in_los_angeles(Arc::new(timestamp), Arc::new(Column::new("fmt", 0)), &batch)
        else {
            panic!("expected an array");
        };
        assert_eq!(result.data_type(), &utc_timestamp());
        let result = result.as_primitive::<TimestampMicrosecondType>();
        // 2024-11-03 00:00 PDT and 2024-01-01 00:00 PST.
        assert_eq!(result.value(0), 1_730_617_200_000_000);
        assert_eq!(result.value(1), 1_704_096_000_000_000);
        assert!(result.is_null(2));
    }

    #[test]
    fn null_literal_format_gives_null() {
        let input = TimestampMicrosecondArray::from(vec![Some(MICROS), None]).with_timezone("UTC");
        let schema = Schema::new(vec![Field::new("ts", utc_timestamp(), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(input)]).unwrap();
        let null_format = || Arc::new(Literal::new(Utf8(None)));
        let ColumnarValue::Array(result) =
            trunc_in_los_angeles(Arc::new(Column::new("ts", 0)), null_format(), &batch)
        else {
            panic!("expected an array");
        };
        assert_eq!(result.data_type(), &utc_timestamp());
        assert_eq!(result.null_count(), 2);

        let timestamp = Arc::new(Literal::new(ScalarValue::TimestampMicrosecond(
            Some(MICROS),
            Some("UTC".into()),
        )));
        let ColumnarValue::Scalar(result) = trunc_in_los_angeles(timestamp, null_format(), &batch)
        else {
            panic!("expected a scalar");
        };
        assert_eq!(
            result,
            ScalarValue::TimestampMicrosecond(None, Some("UTC".into()))
        );
    }
}
