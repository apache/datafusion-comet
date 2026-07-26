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

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::errors::CometError;
use crate::jvm_bridge::JVMClasses;
use arrow::array::{
    Array, ArrayRef, Date32Array, Date32Builder, TimestampMicrosecondArray,
    TimestampMicrosecondBuilder, TimestampMillisecondArray, TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion_comet_common::SparkError;
use serde::Deserialize;

/// A valid UTC alias used to retain the physical INT96 origin after Arrow conversion.
pub const INT96_TIMEZONE_MARKER: &str = "+00:00";

const SPARK_VERSION_METADATA_KEY: &str = "org.apache.spark.version";
const SPARK_TIMEZONE_METADATA_KEY: &str = "org.apache.spark.timeZone";
const LAST_SWITCH_JULIAN_DAY: i32 = -141_427;
const LAST_SWITCH_JULIAN_MICROS: i64 = -2_208_988_800_000_000;

const JULIAN_GREGORIAN_SWITCH_DAYS: [i32; 14] = [
    -719_164, -682_945, -646_420, -609_895, -536_845, -500_320, -463_795, -390_745, -354_220,
    -317_695, -244_645, -208_120, -171_595, -141_427,
];
const JULIAN_GREGORIAN_DIFFS: [i32; 14] = [2, 1, 0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, 0];

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RebaseMode {
    Corrected,
    Legacy,
    Exception,
}

impl RebaseMode {
    pub fn parse(value: &str) -> DataFusionResult<Self> {
        match value.to_ascii_uppercase().as_str() {
            "CORRECTED" => Ok(Self::Corrected),
            "LEGACY" => Ok(Self::Legacy),
            "EXCEPTION" => Ok(Self::Exception),
            _ => Err(DataFusionError::Configuration(format!(
                "Unknown datetime rebase mode: {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RebaseSpec {
    pub mode: RebaseMode,
    pub timezone: String,
}

impl RebaseSpec {
    pub fn new(mode: RebaseMode, timezone: impl Into<String>) -> Self {
        Self {
            mode,
            timezone: timezone.into(),
        }
    }
}

/// Resolve Spark's per-file rebase mode. File metadata takes precedence over the read config.
pub fn resolve_rebase_spec(
    metadata: &HashMap<String, String>,
    configured_mode: RebaseMode,
    min_version: &str,
    legacy_key: &str,
    default_timezone: &str,
) -> RebaseSpec {
    let mode = match metadata.get(SPARK_VERSION_METADATA_KEY) {
        Some(version) if version.as_str() < min_version || metadata.contains_key(legacy_key) => {
            RebaseMode::Legacy
        }
        Some(_) => RebaseMode::Corrected,
        None => configured_mode,
    };
    let timezone = if mode == RebaseMode::Legacy {
        metadata
            .get(SPARK_TIMEZONE_METADATA_KEY)
            .map(String::as_str)
            .unwrap_or(default_timezone)
    } else {
        default_timezone
    };
    RebaseSpec::new(mode, timezone)
}

pub fn needs_rebase(
    physical: &DataType,
    logical: &DataType,
    datetime: &RebaseSpec,
    int96: &RebaseSpec,
) -> bool {
    if primitive_rebase_spec(physical, logical, datetime, int96)
        .is_some_and(|(spec, _)| spec.mode != RebaseMode::Corrected)
    {
        return true;
    }

    match (physical, logical) {
        (DataType::Struct(physical), DataType::Struct(logical)) => {
            // Nested fields may be reordered or selected by name/field id later.
            physical.iter().any(|physical| {
                logical.iter().any(|logical| {
                    needs_rebase(physical.data_type(), logical.data_type(), datetime, int96)
                })
            })
        }
        (DataType::List(physical), DataType::List(logical))
        | (DataType::LargeList(physical), DataType::LargeList(logical))
        | (DataType::Map(physical, _), DataType::Map(logical, _)) => {
            needs_rebase(physical.data_type(), logical.data_type(), datetime, int96)
        }
        _ => false,
    }
}

pub fn rebase_primitive_array(
    array: ArrayRef,
    logical_type: &DataType,
    datetime: &RebaseSpec,
    int96: &RebaseSpec,
) -> DataFusionResult<ArrayRef> {
    let Some((spec, format)) =
        primitive_rebase_spec(array.data_type(), logical_type, datetime, int96)
    else {
        return Ok(array);
    };
    if spec.mode == RebaseMode::Corrected {
        return Ok(array);
    }

    match array.data_type() {
        DataType::Date32 => rebase_dates(array, spec, format),
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("timestamp millisecond array");
            let record = legacy_record(spec)?;
            let mut builder = TimestampMillisecondBuilder::with_capacity(array.len());
            for value in array.iter() {
                match value {
                    Some(value) => {
                        let micros = value.checked_mul(1_000).ok_or_else(|| {
                            DataFusionError::Execution(
                                "Timestamp milliseconds overflow while rebasing".to_string(),
                            )
                        })?;
                        builder
                            .append_value(rebase_timestamp(micros, spec, record, format)? / 1_000);
                    }
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(
                builder.finish().with_timezone_opt(timezone.clone()),
            ))
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp microsecond array");
            let record = legacy_record(spec)?;
            let mut builder = TimestampMicrosecondBuilder::with_capacity(array.len());
            for value in array.iter() {
                match value {
                    Some(value) => {
                        builder.append_value(rebase_timestamp(value, spec, record, format)?);
                    }
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(
                builder.finish().with_timezone_opt(timezone.clone()),
            ))
        }
        DataType::Timestamp(unit, _) => Err(DataFusionError::Execution(format!(
            "Datetime rebasing does not support {unit:?} timestamps"
        ))),
        data_type => Err(DataFusionError::Execution(format!(
            "Datetime rebasing does not support {data_type}"
        ))),
    }
}

fn primitive_rebase_spec<'a>(
    physical: &DataType,
    logical: &DataType,
    datetime: &'a RebaseSpec,
    int96: &'a RebaseSpec,
) -> Option<(&'a RebaseSpec, &'static str)> {
    match (physical, logical) {
        (DataType::Date32, DataType::Date32) | (DataType::Date32, DataType::Timestamp(_, None)) => {
            Some((datetime, "Parquet"))
        }
        (DataType::Timestamp(_, physical_timezone), DataType::Timestamp(_, Some(_))) => {
            if physical_timezone.as_deref() == Some(INT96_TIMEZONE_MARKER) {
                Some((int96, "Parquet INT96"))
            } else {
                Some((datetime, "Parquet"))
            }
        }
        // Spark does not rebase TimestampNTZ values.
        _ => None,
    }
}

fn rebase_dates(
    array: ArrayRef,
    spec: &RebaseSpec,
    format: &'static str,
) -> DataFusionResult<ArrayRef> {
    let array = array
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("date32 array");
    let mut builder = Date32Builder::with_capacity(array.len());
    for value in array.iter() {
        match value {
            Some(value) => builder.append_value(match spec.mode {
                RebaseMode::Corrected => value,
                RebaseMode::Legacy => rebase_julian_to_gregorian_days(value)?,
                RebaseMode::Exception if value < LAST_SWITCH_JULIAN_DAY => {
                    return Err(rebase_exception(format));
                }
                RebaseMode::Exception => value,
            }),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn rebase_julian_to_gregorian_days(days: i32) -> DataFusionResult<i32> {
    if days >= JULIAN_GREGORIAN_SWITCH_DAYS[0] {
        let index = JULIAN_GREGORIAN_SWITCH_DAYS.partition_point(|switch| *switch <= days) - 1;
        return days
            .checked_add(JULIAN_GREGORIAN_DIFFS[index])
            .ok_or_else(|| DataFusionError::Execution("Date overflow while rebasing".to_string()));
    }

    // Spark falls back to its hybrid java.util.Calendar before Common Era.
    let julian_day = days as i64 + 2_440_588;
    let c = julian_day + 32_082;
    let d = (4 * c + 3).div_euclid(1_461);
    let e = c - (1_461 * d).div_euclid(4);
    let m = (5 * e + 2).div_euclid(153);
    let day = e - (153 * m + 2).div_euclid(5) + 1;
    let month = m + 3 - 12 * m.div_euclid(10);
    let year = d - 4_800 + m.div_euclid(10);
    let year_before_march = year - i64::from(month <= 2);
    let era = year_before_march.div_euclid(400);
    let year_of_era = year_before_march - era * 400;
    let month_from_march = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_from_march + 2).div_euclid(5) + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    i32::try_from(era * 146_097 + day_of_era - 719_468)
        .map_err(|_| DataFusionError::Execution("Date overflow while rebasing".to_string()))
}

#[derive(Deserialize)]
struct JsonRebaseRecord {
    tz: String,
    switches: Vec<i64>,
    diffs: Vec<i64>,
}

struct RebaseRecord {
    switches: Vec<i64>,
    diffs: Vec<i64>,
}

static JULIAN_GREGORIAN_REBASE: OnceLock<Result<HashMap<String, RebaseRecord>, String>> =
    OnceLock::new();

fn rebase_records() -> DataFusionResult<&'static HashMap<String, RebaseRecord>> {
    JULIAN_GREGORIAN_REBASE
        .get_or_init(|| {
            let records: Vec<JsonRebaseRecord> =
                serde_json::from_str(include_str!("julian-gregorian-rebase-micros.json"))
                    .map_err(|error| format!("Invalid Spark datetime rebase table: {error}"))?;
            records
                .into_iter()
                .map(|record| {
                    if record.switches.is_empty()
                        || record.switches.len() != record.diffs.len()
                        || !record.switches.windows(2).all(|pair| pair[0] < pair[1])
                    {
                        return Err(format!(
                            "Invalid Spark datetime rebase record for {}",
                            record.tz
                        ));
                    }
                    let scale = |values: Vec<i64>| {
                        values
                            .into_iter()
                            .map(|value| {
                                value.checked_mul(1_000_000).ok_or_else(|| {
                                    "Spark datetime rebase table overflow".to_string()
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()
                    };
                    Ok((
                        record.tz,
                        RebaseRecord {
                            switches: scale(record.switches)?,
                            diffs: scale(record.diffs)?,
                        },
                    ))
                })
                .collect()
        })
        .as_ref()
        .map_err(|error| DataFusionError::Internal(error.clone()))
}

fn legacy_record(spec: &RebaseSpec) -> DataFusionResult<Option<&'static RebaseRecord>> {
    if spec.mode != RebaseMode::Legacy {
        return Ok(None);
    }
    Ok(rebase_records()?.get(&spec.timezone))
}

fn rebase_timestamp(
    micros: i64,
    spec: &RebaseSpec,
    record: Option<&RebaseRecord>,
    format: &'static str,
) -> DataFusionResult<i64> {
    match spec.mode {
        RebaseMode::Corrected => Ok(micros),
        RebaseMode::Exception if micros < LAST_SWITCH_JULIAN_MICROS => {
            Err(rebase_exception(format))
        }
        RebaseMode::Exception => Ok(micros),
        RebaseMode::Legacy if micros >= LAST_SWITCH_JULIAN_MICROS => Ok(micros),
        RebaseMode::Legacy => {
            if let Some(record) = record {
                let index = record.switches.partition_point(|switch| *switch <= micros);
                if index > 0 {
                    return micros.checked_add(record.diffs[index - 1]).ok_or_else(|| {
                        DataFusionError::Execution("Timestamp overflow while rebasing".to_string())
                    });
                }
            }
            rebase_timestamp_with_spark(&spec.timezone, micros)
        }
    }
}

fn rebase_timestamp_with_spark(timezone: &str, micros: i64) -> DataFusionResult<i64> {
    // ponytail: per-value JNI is limited to table misses; add file preflight if this is measurable.
    JVMClasses::with_env(|env| -> Result<i64, CometError> {
        let timezone = env.new_string(timezone)?;
        unsafe {
            jni_static_call!(env,
                rebase_date_time.rebase_julian_to_gregorian_micros(&timezone, micros) -> i64
            )
        }
    })
    .map_err(DataFusionError::from)
}

fn rebase_exception(format: &'static str) -> DataFusionError {
    DataFusionError::External(Box::new(SparkError::ParquetDatetimeRebase {
        format: format.to_string(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(mode: RebaseMode) -> RebaseSpec {
        RebaseSpec::new(mode, "UTC")
    }

    #[test]
    fn metadata_precedes_config() {
        let mut metadata = HashMap::new();
        assert_eq!(
            resolve_rebase_spec(
                &metadata,
                RebaseMode::Exception,
                "3.0.0",
                "org.apache.spark.legacyDateTime",
                "Asia/Taipei",
            ),
            RebaseSpec::new(RebaseMode::Exception, "Asia/Taipei")
        );

        metadata.insert(SPARK_VERSION_METADATA_KEY.to_string(), "2.4.8".to_string());
        metadata.insert(
            SPARK_TIMEZONE_METADATA_KEY.to_string(),
            "America/Los_Angeles".to_string(),
        );
        assert_eq!(
            resolve_rebase_spec(
                &metadata,
                RebaseMode::Corrected,
                "3.0.0",
                "org.apache.spark.legacyDateTime",
                "UTC",
            ),
            RebaseSpec::new(RebaseMode::Legacy, "America/Los_Angeles")
        );

        metadata.insert(SPARK_VERSION_METADATA_KEY.to_string(), "3.5.0".to_string());
        assert_eq!(
            resolve_rebase_spec(
                &metadata,
                RebaseMode::Exception,
                "3.0.0",
                "org.apache.spark.legacyDateTime",
                "UTC",
            )
            .mode,
            RebaseMode::Corrected
        );
        metadata.insert("org.apache.spark.legacyDateTime".to_string(), String::new());
        assert_eq!(
            resolve_rebase_spec(
                &metadata,
                RebaseMode::Corrected,
                "3.0.0",
                "org.apache.spark.legacyDateTime",
                "UTC",
            )
            .mode,
            RebaseMode::Legacy
        );
    }

    #[test]
    fn rebases_year_1000_date() {
        let array = Arc::new(Date32Array::from(vec![Some(-354_280), None])) as ArrayRef;
        let result = rebase_primitive_array(
            array,
            &DataType::Date32,
            &spec(RebaseMode::Legacy),
            &spec(RebaseMode::Corrected),
        )
        .unwrap();
        assert_eq!(
            result.as_any().downcast_ref::<Date32Array>().unwrap(),
            &Date32Array::from(vec![Some(-354_285), None])
        );
    }

    #[test]
    fn rebases_year_1000_utc_timestamp() {
        let array = Arc::new(
            TimestampMicrosecondArray::from(vec![Some(-30_609_792_000_000_000), None])
                .with_timezone("UTC"),
        ) as ArrayRef;
        let result = rebase_primitive_array(
            array,
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &spec(RebaseMode::Legacy),
            &spec(RebaseMode::Corrected),
        )
        .unwrap();
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            -30_610_224_000_000_000
        );
    }

    #[test]
    fn exception_cutoffs_match_spark() {
        let date = Arc::new(Date32Array::from(vec![LAST_SWITCH_JULIAN_DAY])) as ArrayRef;
        rebase_primitive_array(
            date,
            &DataType::Date32,
            &spec(RebaseMode::Exception),
            &spec(RebaseMode::Corrected),
        )
        .unwrap();
        let date = Arc::new(Date32Array::from(vec![LAST_SWITCH_JULIAN_DAY - 1])) as ArrayRef;
        assert!(matches!(
            rebase_primitive_array(
                date,
                &DataType::Date32,
                &spec(RebaseMode::Exception),
                &spec(RebaseMode::Corrected),
            ),
            Err(DataFusionError::External(_))
        ));

        let logical = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let timestamp = Arc::new(
            TimestampMicrosecondArray::from(vec![LAST_SWITCH_JULIAN_MICROS]).with_timezone("UTC"),
        ) as ArrayRef;
        rebase_primitive_array(
            timestamp,
            &logical,
            &spec(RebaseMode::Exception),
            &spec(RebaseMode::Corrected),
        )
        .unwrap();
        let timestamp = Arc::new(
            TimestampMicrosecondArray::from(vec![LAST_SWITCH_JULIAN_MICROS - 1])
                .with_timezone("UTC"),
        ) as ArrayRef;
        assert!(matches!(
            rebase_primitive_array(
                timestamp,
                &logical,
                &spec(RebaseMode::Exception),
                &spec(RebaseMode::Corrected),
            ),
            Err(DataFusionError::External(_))
        ));
    }

    #[test]
    fn int96_marker_selects_int96_spec() {
        let physical =
            DataType::Timestamp(TimeUnit::Microsecond, Some(INT96_TIMEZONE_MARKER.into()));
        let logical = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert!(needs_rebase(
            &physical,
            &logical,
            &spec(RebaseMode::Corrected),
            &spec(RebaseMode::Legacy),
        ));
        assert!(!needs_rebase(
            &physical,
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            &spec(RebaseMode::Legacy),
            &spec(RebaseMode::Legacy),
        ));
        assert!(needs_rebase(
            &DataType::Date32,
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            &spec(RebaseMode::Legacy),
            &spec(RebaseMode::Corrected),
        ));
    }
}
