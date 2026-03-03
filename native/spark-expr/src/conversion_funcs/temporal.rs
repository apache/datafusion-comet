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

use crate::{timezone, SparkCastOptions, SparkResult};
use arrow::array::{ArrayRef, AsArray, TimestampMicrosecondBuilder};
use arrow::datatypes::{DataType, Date32Type};
use chrono::{NaiveDate, TimeZone};
use std::str::FromStr;
use std::sync::Arc;

pub(crate) fn is_df_cast_from_date_spark_compatible(to_type: &DataType) -> bool {
    matches!(to_type, DataType::Int32 | DataType::Utf8)
}

pub(crate) fn is_df_cast_from_timestamp_spark_compatible(to_type: &DataType) -> bool {
    matches!(
        to_type,
        DataType::Int64 | DataType::Date32 | DataType::Utf8 | DataType::Timestamp(_, _)
    )
}

pub(crate) fn cast_date_to_timestamp(
    array_ref: &ArrayRef,
    cast_options: &SparkCastOptions,
    target_tz: &Option<Arc<str>>,
) -> SparkResult<ArrayRef> {
    let tz_str = if cast_options.timezone.is_empty() {
        "UTC"
    } else {
        cast_options.timezone.as_str()
    };
    // safe to unwrap since we are falling back to UTC above
    let tz = timezone::Tz::from_str(tz_str)?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date_array = array_ref.as_primitive::<Date32Type>();

    let mut builder = TimestampMicrosecondBuilder::with_capacity(date_array.len());

    for date in date_array.iter() {
        match date {
            Some(date) => {
                // safe to unwrap since chrono's range ( 262,143 yrs) is higher than
                // number of years possible with days as i32 (~ 6 mil yrs)
                // convert date in session timezone to timestamp in UTC
                let naive_date = epoch + chrono::Duration::days(date as i64);
                let local_midnight = naive_date.and_hms_opt(0, 0, 0).unwrap();
                let local_midnight_in_microsec = tz
                    .from_local_datetime(&local_midnight)
                    // return earliest possible time (edge case with spring / fall DST changes)
                    .earliest()
                    .map(|dt| dt.timestamp_micros())
                    // in case there is an issue with DST and returns None , we fall back to UTC
                    .unwrap_or((date as i64) * 86_400 * 1_000_000);
                builder.append_value(local_midnight_in_microsec);
            }
            None => {
                builder.append_null();
            }
        }
    }
    Ok(Arc::new(
        builder.finish().with_timezone_opt(target_tz.clone()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    #[test]
    fn test_cast_date_to_timestamp() {
        use crate::EvalMode;
        use arrow::array::Date32Array;
        use arrow::array::{Array, ArrayRef};
        use arrow::datatypes::TimestampMicrosecondType;

        // verifying epoch , DST change dates (US) and a null value (comprehensive tests on spark side)
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(0),
            Some(19723),
            Some(19793),
            None,
        ]));

        let non_dst_date = 1704067200000000i64;
        let dst_date = 1710115200000000i64;
        let seven_hours_ts = 25200000000i64;
        let eight_hours_ts = 28800000000i64;

        // validate UTC
        let target_tz: Option<Arc<str>> = Some("UTC".into());
        let result = cast_date_to_timestamp(
            &dates,
            &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
            &target_tz,
        )
        .unwrap();
        let ts = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.value(0), 0);
        assert_eq!(ts.value(1), non_dst_date);
        assert_eq!(ts.value(2), dst_date);
        assert!(ts.is_null(3));

        // validate LA timezone (follows Daylight savings)
        let result = cast_date_to_timestamp(
            &dates,
            &SparkCastOptions::new(EvalMode::Legacy, "America/Los_Angeles", false),
            &target_tz,
        )
        .unwrap();
        let ts = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.value(0), eight_hours_ts);
        assert_eq!(ts.value(1), non_dst_date + eight_hours_ts);
        // should adjust for DST
        assert_eq!(ts.value(2), dst_date + seven_hours_ts);
        assert!(ts.is_null(3));

        // Phoenix timezone (does not follow Daylight savings)
        let result = cast_date_to_timestamp(
            &dates,
            &SparkCastOptions::new(EvalMode::Legacy, "America/Phoenix", false),
            &target_tz,
        )
        .unwrap();
        let ts = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.value(0), seven_hours_ts);
        assert_eq!(ts.value(1), non_dst_date + seven_hours_ts);
        assert_eq!(ts.value(2), dst_date + seven_hours_ts);
        assert!(ts.is_null(3));
    }
}
