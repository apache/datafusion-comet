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

use std::{
    any::Any,
    fmt::{Debug, Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    compute::{date_part, DatePart},
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Schema, TimeUnit::Microsecond};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{DataFusionError, ScalarValue::Utf8};
use datafusion_physical_expr::PhysicalExpr;

use crate::execution::{
    datafusion::expressions::utils::{array_with_timezone, down_cast_any_ref},
    kernels::temporal::{
        date_trunc_array_fmt_dyn, date_trunc_dyn, timestamp_trunc_array_fmt_dyn,
        timestamp_trunc_dyn,
    },
};

#[derive(Debug, Hash)]
pub struct HourExec {
    /// An array with DataType::Timestamp(TimeUnit::Microsecond, None)
    child: Arc<dyn PhysicalExpr>,
    timezone: String,
}

impl HourExec {
    pub fn new(child: Arc<dyn PhysicalExpr>, timezone: String) -> Self {
        HourExec { child, timezone }
    }
}

impl Display for HourExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Hour [timezone:{}, child: {}]",
            self.timezone, self.child
        )
    }
}

impl PartialEq<dyn Any> for HourExec {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.child.eq(&x.child) && self.timezone.eq(&x.timezone))
            .unwrap_or(false)
    }
}

impl PhysicalExpr for HourExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion_common::Result<DataType> {
        match self.child.data_type(input_schema).unwrap() {
            DataType::Dictionary(key_type, _) => {
                Ok(DataType::Dictionary(key_type, Box::new(DataType::Int32)))
            }
            _ => Ok(DataType::Int32),
        }
    }

    fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let array = array_with_timezone(
                    array,
                    self.timezone.clone(),
                    Some(&DataType::Timestamp(
                        Microsecond,
                        Some(self.timezone.clone().into()),
                    )),
                )?;
                let result = date_part(&array, DatePart::Hour)?;

                Ok(ColumnarValue::Array(result))
            }
            _ => Err(DataFusionError::Execution(
                "Hour(scalar) should be fold in Spark JVM side.".to_string(),
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
        Ok(Arc::new(HourExec::new(
            children[0].clone(),
            self.timezone.clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.child.hash(&mut s);
        self.timezone.hash(&mut s);
        self.hash(&mut s);
    }
}

#[derive(Debug, Hash)]
pub struct MinuteExec {
    /// An array with DataType::Timestamp(TimeUnit::Microsecond, None)
    child: Arc<dyn PhysicalExpr>,
    timezone: String,
}

impl MinuteExec {
    pub fn new(child: Arc<dyn PhysicalExpr>, timezone: String) -> Self {
        MinuteExec { child, timezone }
    }
}

impl Display for MinuteExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Minute [timezone:{}, child: {}]",
            self.timezone, self.child
        )
    }
}

impl PartialEq<dyn Any> for MinuteExec {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.child.eq(&x.child) && self.timezone.eq(&x.timezone))
            .unwrap_or(false)
    }
}

impl PhysicalExpr for MinuteExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion_common::Result<DataType> {
        match self.child.data_type(input_schema).unwrap() {
            DataType::Dictionary(key_type, _) => {
                Ok(DataType::Dictionary(key_type, Box::new(DataType::Int32)))
            }
            _ => Ok(DataType::Int32),
        }
    }

    fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let array = array_with_timezone(
                    array,
                    self.timezone.clone(),
                    Some(&DataType::Timestamp(
                        Microsecond,
                        Some(self.timezone.clone().into()),
                    )),
                )?;
                let result = date_part(&array, DatePart::Minute)?;

                Ok(ColumnarValue::Array(result))
            }
            _ => Err(DataFusionError::Execution(
                "Minute(scalar) should be fold in Spark JVM side.".to_string(),
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
        Ok(Arc::new(MinuteExec::new(
            children[0].clone(),
            self.timezone.clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.child.hash(&mut s);
        self.timezone.hash(&mut s);
        self.hash(&mut s);
    }
}

#[derive(Debug, Hash)]
pub struct SecondExec {
    /// An array with DataType::Timestamp(TimeUnit::Microsecond, None)
    child: Arc<dyn PhysicalExpr>,
    timezone: String,
}

impl SecondExec {
    pub fn new(child: Arc<dyn PhysicalExpr>, timezone: String) -> Self {
        SecondExec { child, timezone }
    }
}

impl Display for SecondExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Second (timezone:{}, child: {}]",
            self.timezone, self.child
        )
    }
}

impl PartialEq<dyn Any> for SecondExec {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.child.eq(&x.child) && self.timezone.eq(&x.timezone))
            .unwrap_or(false)
    }
}

impl PhysicalExpr for SecondExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion_common::Result<DataType> {
        match self.child.data_type(input_schema).unwrap() {
            DataType::Dictionary(key_type, _) => {
                Ok(DataType::Dictionary(key_type, Box::new(DataType::Int32)))
            }
            _ => Ok(DataType::Int32),
        }
    }

    fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let array = array_with_timezone(
                    array,
                    self.timezone.clone(),
                    Some(&DataType::Timestamp(
                        Microsecond,
                        Some(self.timezone.clone().into()),
                    )),
                )?;
                let result = date_part(&array, DatePart::Second)?;

                Ok(ColumnarValue::Array(result))
            }
            _ => Err(DataFusionError::Execution(
                "Second(scalar) should be fold in Spark JVM side.".to_string(),
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
        Ok(Arc::new(SecondExec::new(
            children[0].clone(),
            self.timezone.clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.child.hash(&mut s);
        self.timezone.hash(&mut s);
        self.hash(&mut s);
    }
}

#[derive(Debug, Hash)]
pub struct DateTruncExec {
    /// An array with DataType::Date32
    child: Arc<dyn PhysicalExpr>,
    /// Scalar UTF8 string matching the valid values in Spark SQL: https://spark.apache.org/docs/latest/api/sql/index.html#trunc
    format: Arc<dyn PhysicalExpr>,
}

impl DateTruncExec {
    pub fn new(child: Arc<dyn PhysicalExpr>, format: Arc<dyn PhysicalExpr>) -> Self {
        DateTruncExec { child, format }
    }
}

impl Display for DateTruncExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DateTrunc [child:{}, format: {}]",
            self.child, self.format
        )
    }
}

impl PartialEq<dyn Any> for DateTruncExec {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.child.eq(&x.child) && self.format.eq(&x.format))
            .unwrap_or(false)
    }
}

impl PhysicalExpr for DateTruncExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion_common::Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let date = self.child.evaluate(batch)?;
        let format = self.format.evaluate(batch)?;
        match (date, format) {
            (ColumnarValue::Array(date), ColumnarValue::Scalar(Utf8(Some(format)))) => {
                let result = date_trunc_dyn(&date, format)?;
                Ok(ColumnarValue::Array(result))
            }
            (ColumnarValue::Array(date), ColumnarValue::Array(formats)) => {
                let result = date_trunc_array_fmt_dyn(&date, &formats)?;
                Ok(ColumnarValue::Array(result))
            }
            _ => Err(DataFusionError::Execution(
                "Invalid input to function DateTrunc. Expected (PrimitiveArray<Date32>, Scalar) or \
                    (PrimitiveArray<Date32>, StringArray)".to_string(),
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
        Ok(Arc::new(DateTruncExec::new(
            children[0].clone(),
            self.format.clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.child.hash(&mut s);
        self.format.hash(&mut s);
        self.hash(&mut s);
    }
}

#[derive(Debug, Hash)]
pub struct TimestampTruncExec {
    /// An array with DataType::Timestamp(TimeUnit::Microsecond, None)
    child: Arc<dyn PhysicalExpr>,
    /// Scalar UTF8 string matching the valid values in Spark SQL: https://spark.apache.org/docs/latest/api/sql/index.html#date_trunc
    format: Arc<dyn PhysicalExpr>,
    /// String containing a timezone name. The name must be found in the standard timezone
    /// database (https://en.wikipedia.org/wiki/List_of_tz_database_time_zones). The string is
    /// later parsed into a chrono::TimeZone.
    /// Timestamp arrays in this implementation are kept in arrays of UTC timestamps (in micros)
    /// along with a single value for the associated TimeZone. The timezone offset is applied
    /// just before any operations on the timestamp
    timezone: String,
}

impl TimestampTruncExec {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        format: Arc<dyn PhysicalExpr>,
        timezone: String,
    ) -> Self {
        TimestampTruncExec {
            child,
            format,
            timezone,
        }
    }
}

impl Display for TimestampTruncExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TimestampTrunc [child:{}, format:{}, timezone: {}]",
            self.child, self.format, self.timezone
        )
    }
}

impl PartialEq<dyn Any> for TimestampTruncExec {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.child.eq(&x.child)
                    && self.format.eq(&x.format)
                    && self.timezone.eq(&x.timezone)
            })
            .unwrap_or(false)
    }
}

impl PhysicalExpr for TimestampTruncExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion_common::Result<DataType> {
        match self.child.data_type(input_schema)? {
            DataType::Dictionary(key_type, _) => Ok(DataType::Dictionary(
                key_type,
                Box::new(DataType::Timestamp(Microsecond, None)),
            )),
            _ => Ok(DataType::Timestamp(Microsecond, None)),
        }
    }

    fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let timestamp = self.child.evaluate(batch)?;
        let format = self.format.evaluate(batch)?;
        let tz = self.timezone.clone();
        match (timestamp, format) {
            (ColumnarValue::Array(ts), ColumnarValue::Scalar(Utf8(Some(format)))) => {
                let ts = array_with_timezone(
                    ts,
                    tz.clone(),
                    Some(&DataType::Timestamp(Microsecond, Some(tz.into()))),
                )?;
                let result = timestamp_trunc_dyn(&ts, format)?;
                Ok(ColumnarValue::Array(result))
            }
            (ColumnarValue::Array(ts), ColumnarValue::Array(formats)) => {
                let ts = array_with_timezone(
                    ts,
                    tz.clone(),
                    Some(&DataType::Timestamp(Microsecond, Some(tz.into()))),
                )?;
                let result = timestamp_trunc_array_fmt_dyn(&ts, &formats)?;
                Ok(ColumnarValue::Array(result))
            }
            _ => Err(DataFusionError::Execution(
                "Invalid input to function TimestampTrunc. \
                    Expected (PrimitiveArray<TimestampMicrosecondType>, Scalar, String) or \
                    (PrimitiveArray<TimestampMicrosecondType>, StringArray, String)"
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
        Ok(Arc::new(TimestampTruncExec::new(
            children[0].clone(),
            self.format.clone(),
            self.timezone.clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.child.hash(&mut s);
        self.format.hash(&mut s);
        self.timezone.hash(&mut s);
        self.hash(&mut s);
    }
}
