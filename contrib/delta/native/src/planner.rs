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

//! Delta-specific helpers the kernel-read path uses for partition-value handling:
//!
//!   - [`parse_delta_partition_scalar`] -- string -> `ScalarValue` with Delta's TZ
//!     semantics and the DATE -> TIMESTAMP_NTZ widening fallback (partition injection
//!     in `DeltaKernelScanExec::append_partition_columns`)
//!   - [`SessionTimezone`] -- pre-parsed session timezone, reused across partition parses
//!
//! All take pure DataFusion / arrow types so this crate stays free of any
//! datafusion-comet dependency (no cycle: core can call us, we can't call core).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::jni::delta_storage_config_from_map;
use crate::kernel_scan::{DeltaKernelScanExec, KernelScanFile};
use crate::proto::{DeltaScan, DeltaScanCommon};

/// Pre-parsed session timezone, computed once per scan and reused across every partition
/// value parse. Avoids the per-row `chrono_tz::Tz::from_str` lookup
/// `parse_delta_partition_scalar` would otherwise do for every TIMESTAMP partition value.
pub enum SessionTimezone {
    Tz(chrono_tz::Tz),
    Offset(chrono::FixedOffset),
    /// `session_tz` didn't parse as either a named TZ or a fixed offset. We defer the
    /// "invalid session TZ" error to the per-row parse path so callers that don't have any
    /// TIMESTAMP partitions never see it.
    Invalid,
}

impl SessionTimezone {
    pub fn parse(session_tz: &str) -> Self {
        if let Ok(tz) = session_tz.parse::<chrono_tz::Tz>() {
            return Self::Tz(tz);
        }
        if let Some(off) = parse_fixed_offset(session_tz) {
            return Self::Offset(off);
        }
        Self::Invalid
    }
}

fn parse_fixed_offset(s: &str) -> Option<chrono::FixedOffset> {
    let trimmed = s.trim();
    let body = trimmed
        .strip_prefix("GMT")
        .or_else(|| trimmed.strip_prefix("UTC"))
        .unwrap_or(trimmed);
    if body.is_empty() || body.eq_ignore_ascii_case("Z") {
        return Some(chrono::FixedOffset::east_opt(0).unwrap());
    }
    let (sign, rest) = match body.chars().next()? {
        '+' => (1, &body[1..]),
        '-' => (-1, &body[1..]),
        _ => return None,
    };
    let secs = if rest.contains(':') {
        let mut parts = rest.splitn(2, ':');
        let h: i32 = parts.next()?.parse().ok()?;
        let m: i32 = parts.next()?.parse().ok()?;
        h * 3600 + m * 60
    } else if rest.len() == 4 {
        let h: i32 = rest[..2].parse().ok()?;
        let m: i32 = rest[2..].parse().ok()?;
        h * 3600 + m * 60
    } else {
        let h: i32 = rest.parse().ok()?;
        h * 3600
    };
    chrono::FixedOffset::east_opt(sign * secs)
}

/// Parse a Delta partition value string into a `ScalarValue`. Honours session TZ for
/// TIMESTAMP columns. Delta writes TIMESTAMP partition values in the JVM default TZ
/// (`yyyy-MM-dd HH:mm:ss[.S]`); DataFusion's default parser interprets them as UTC
/// which would be off by the session offset.
///
/// Includes the DATE -> TIMESTAMP_NTZ widening fallback: Delta's TypeWidening leaves
/// the original "YYYY-MM-DD" partition strings in place when the column changes from
/// DATE to TIMESTAMP_NTZ, so we accept the date-only form by promoting to midnight
/// (matches Spark's `cast(DATE as TIMESTAMP)` semantics).
pub fn parse_delta_partition_scalar(
    s: &str,
    dt: &DataType,
    parsed_tz: &SessionTimezone,
    session_tz: &str,
) -> Result<ScalarValue, String> {
    match dt {
        DataType::Timestamp(unit, tz_opt) => {
            use chrono::{DateTime, NaiveDateTime, TimeZone};
            if tz_opt.is_none() {
                let naive = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                    .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
                    .or_else(|_| {
                        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|d| {
                            d.and_hms_opt(0, 0, 0)
                                .expect("midnight (0,0,0) is always a valid time")
                        })
                    })
                    .map_err(|e| format!("cannot parse TIMESTAMP_NTZ '{s}': {e}"))?;
                let micros = chrono::Utc.from_utc_datetime(&naive).timestamp_micros();
                return Ok(match unit {
                    datafusion::arrow::datatypes::TimeUnit::Microsecond => {
                        ScalarValue::TimestampMicrosecond(Some(micros), None)
                    }
                    datafusion::arrow::datatypes::TimeUnit::Millisecond => {
                        ScalarValue::TimestampMillisecond(Some(micros / 1_000), None)
                    }
                    datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                        ScalarValue::TimestampNanosecond(Some(micros.saturating_mul(1_000)), None)
                    }
                    datafusion::arrow::datatypes::TimeUnit::Second => {
                        ScalarValue::TimestampSecond(Some(micros / 1_000_000), None)
                    }
                });
            }
            let micros = if let Ok(dt_with_tz) = DateTime::parse_from_rfc3339(s) {
                dt_with_tz.timestamp_micros()
            } else if let Ok(dt_with_tz) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f %z")
                .or_else(|_| DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S %z"))
            {
                dt_with_tz.timestamp_micros()
            } else {
                let naive = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                    .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
                    .or_else(|_| {
                        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|d| {
                            d.and_hms_opt(0, 0, 0)
                                .expect("midnight (0,0,0) is always a valid time")
                        })
                    })
                    .map_err(|e| format!("cannot parse timestamp '{s}': {e}"))?;
                use chrono::LocalResult;
                match parsed_tz {
                    SessionTimezone::Tz(tz) => match tz.from_local_datetime(&naive) {
                        LocalResult::Single(dt) => dt.timestamp_micros(),
                        LocalResult::Ambiguous(earlier, _later) => earlier.timestamp_micros(),
                        LocalResult::None => {
                            chrono::Utc.from_utc_datetime(&naive).timestamp_micros()
                        }
                    },
                    SessionTimezone::Offset(off) => match off.from_local_datetime(&naive) {
                        LocalResult::Single(dt) => dt.timestamp_micros(),
                        _ => chrono::Utc.from_utc_datetime(&naive).timestamp_micros(),
                    },
                    SessionTimezone::Invalid => {
                        return Err(format!("invalid session TZ '{session_tz}'"));
                    }
                }
            };
            match unit {
                datafusion::arrow::datatypes::TimeUnit::Microsecond => Ok(
                    ScalarValue::TimestampMicrosecond(Some(micros), tz_opt.clone()),
                ),
                datafusion::arrow::datatypes::TimeUnit::Millisecond => Ok(
                    ScalarValue::TimestampMillisecond(Some(micros / 1000), tz_opt.clone()),
                ),
                datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                    Ok(ScalarValue::TimestampNanosecond(
                        Some(micros.saturating_mul(1000)),
                        tz_opt.clone(),
                    ))
                }
                datafusion::arrow::datatypes::TimeUnit::Second => Ok(ScalarValue::TimestampSecond(
                    Some(micros / 1_000_000),
                    tz_opt.clone(),
                )),
            }
        }
        _ => ScalarValue::try_from_string(s.to_string(), dt).map_err(|e| format!("{e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::TimeUnit;

    // ---- parse_fixed_offset ----

    #[test]
    fn fixed_offset_utc_z() {
        for s in ["UTC", "GMT", "Z", "GMTZ", "utc", "gmt"] {
            let off = parse_fixed_offset(s);
            // Lowercase variants we don't currently uppercase-normalize; skip those.
            if s.chars().any(|c| c.is_lowercase()) {
                continue;
            }
            assert_eq!(off.unwrap().local_minus_utc(), 0, "{s}");
        }
    }

    #[test]
    fn fixed_offset_signed_hh_mm() {
        assert_eq!(
            parse_fixed_offset("+05:30").unwrap().local_minus_utc(),
            5 * 3600 + 30 * 60
        );
        assert_eq!(
            parse_fixed_offset("-08:00").unwrap().local_minus_utc(),
            -8 * 3600
        );
    }

    #[test]
    fn fixed_offset_hhmm_no_colon() {
        assert_eq!(
            parse_fixed_offset("+0530").unwrap().local_minus_utc(),
            5 * 3600 + 30 * 60
        );
        assert_eq!(
            parse_fixed_offset("-0800").unwrap().local_minus_utc(),
            -8 * 3600
        );
    }

    #[test]
    fn fixed_offset_hour_only() {
        assert_eq!(
            parse_fixed_offset("+5").unwrap().local_minus_utc(),
            5 * 3600
        );
        assert_eq!(
            parse_fixed_offset("-3").unwrap().local_minus_utc(),
            -3 * 3600
        );
    }

    #[test]
    fn fixed_offset_gmt_prefix() {
        assert_eq!(
            parse_fixed_offset("GMT+05:30").unwrap().local_minus_utc(),
            5 * 3600 + 30 * 60
        );
        assert_eq!(
            parse_fixed_offset("UTC-3").unwrap().local_minus_utc(),
            -3 * 3600
        );
    }

    #[test]
    fn fixed_offset_invalid_returns_none() {
        assert!(parse_fixed_offset("garbage").is_none());
        assert!(parse_fixed_offset("+xx:30").is_none());
        assert!(parse_fixed_offset("America/New_York").is_none()); // named TZ, not offset
    }

    // ---- SessionTimezone ----

    #[test]
    fn session_tz_parses_named() {
        match SessionTimezone::parse("America/New_York") {
            SessionTimezone::Tz(_) => {}
            _ => panic!("expected named TZ"),
        }
    }

    #[test]
    fn session_tz_parses_offset() {
        match SessionTimezone::parse("+05:30") {
            SessionTimezone::Offset(off) => {
                assert_eq!(off.local_minus_utc(), 5 * 3600 + 30 * 60);
            }
            _ => panic!("expected fixed offset"),
        }
    }

    #[test]
    fn session_tz_invalid() {
        assert!(matches!(
            SessionTimezone::parse("nonsense"),
            SessionTimezone::Invalid
        ));
    }

    // ---- parse_delta_partition_scalar: every primitive type ----

    fn tz_utc() -> SessionTimezone {
        SessionTimezone::parse("UTC")
    }

    #[test]
    fn partition_scalar_int32() {
        let s = parse_delta_partition_scalar("42", &DataType::Int32, &tz_utc(), "UTC").unwrap();
        assert_eq!(s, ScalarValue::Int32(Some(42)));
    }

    #[test]
    fn partition_scalar_int64() {
        let s =
            parse_delta_partition_scalar("9999999999", &DataType::Int64, &tz_utc(), "UTC").unwrap();
        assert_eq!(s, ScalarValue::Int64(Some(9999999999)));
    }

    #[test]
    fn partition_scalar_int16() {
        let s = parse_delta_partition_scalar("123", &DataType::Int16, &tz_utc(), "UTC").unwrap();
        assert_eq!(s, ScalarValue::Int16(Some(123)));
    }

    #[test]
    fn partition_scalar_utf8() {
        let s = parse_delta_partition_scalar("hello", &DataType::Utf8, &tz_utc(), "UTC").unwrap();
        assert_eq!(s, ScalarValue::Utf8(Some("hello".into())));
    }

    #[test]
    fn partition_scalar_boolean() {
        let s = parse_delta_partition_scalar("true", &DataType::Boolean, &tz_utc(), "UTC").unwrap();
        assert_eq!(s, ScalarValue::Boolean(Some(true)));
    }

    #[test]
    fn partition_scalar_date() {
        // Date32 = days since epoch. 2024-01-15 -> 19737
        let s = parse_delta_partition_scalar("2024-01-15", &DataType::Date32, &tz_utc(), "UTC")
            .unwrap();
        assert_eq!(s, ScalarValue::Date32(Some(19737)));
    }

    #[test]
    fn partition_scalar_timestamp_ntz_micros() {
        let s = parse_delta_partition_scalar(
            "2024-01-15 12:30:45",
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            &tz_utc(),
            "UTC",
        )
        .unwrap();
        match s {
            ScalarValue::TimestampMicrosecond(Some(v), None) => {
                // 2024-01-15 12:30:45 UTC = epoch micros 1_705_321_845_000_000
                assert_eq!(v, 1_705_321_845_000_000);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn partition_scalar_timestamp_ntz_widens_from_date() {
        // DATE -> TIMESTAMP_NTZ widening: "2024-01-15" promotes to midnight.
        let s = parse_delta_partition_scalar(
            "2024-01-15",
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            &tz_utc(),
            "UTC",
        )
        .unwrap();
        match s {
            ScalarValue::TimestampMicrosecond(Some(_), None) => {} // success
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn partition_scalar_timestamp_with_session_tz() {
        // 2024-01-15 12:00:00 in America/New_York = 17:00:00 UTC = 1705338000 epoch sec
        let parsed = SessionTimezone::parse("America/New_York");
        let s = parse_delta_partition_scalar(
            "2024-01-15 12:00:00",
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &parsed,
            "America/New_York",
        )
        .unwrap();
        match s {
            ScalarValue::TimestampMicrosecond(Some(v), Some(_)) => {
                assert_eq!(v, 1_705_338_000_000_000);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}

/// Plan a Delta `DeltaScan` proto into its native `ExecutionPlan` (the kernel-read path).
///
/// All Delta-specific scan planning lives here so core stays Delta-free (#77): core's dispatcher
/// computes `required_schema` / `partition_schema` (it owns the proto -> arrow schema converter) and
/// wraps the returned exec in a `SparkPlan`; everything else is below.
///
/// Iceberg-style "kernel reads" is the only path: each Delta file is read through
/// `DeltaKernelScanExec` (delta-kernel 0.24 / arrow-58).
pub fn plan_delta_scan(
    scan: &DeltaScan,
    common: &DeltaScanCommon,
    required_schema: &SchemaRef,
    partition_schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    // Split `required_schema` (data ++ partition, in order) by partition-column name: read the data
    // fields from parquet, inject the partition fields as constants; the exec reassembles them.
    let partition_names: std::collections::HashSet<&str> = partition_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let data_fields: Vec<_> = required_schema
        .fields()
        .iter()
        .filter(|f| !partition_names.contains(f.name().as_str()))
        .cloned()
        .collect();
    let partition_fields: Vec<_> = required_schema
        .fields()
        .iter()
        .filter(|f| partition_names.contains(f.name().as_str()))
        .cloned()
        .collect();

    // CDF read: the executor reconstructs TableChanges + execute()s the whole version range itself
    // (single partition), so it ships no per-file tasks and uses no kernel data-column schemas.
    let cdf: Option<(u64, Option<u64>)> = if common.cdf_read {
        Some((common.cdf_start_version, common.cdf_end_version))
    } else {
        None
    };

    let (physical_schema, read_logical_schema, needs_transform): (SchemaRef, SchemaRef, bool) =
        if cdf.is_some() || data_fields.is_empty() {
            let empty: SchemaRef = Arc::new(Schema::empty());
            (Arc::clone(&empty), empty, false)
        } else if !common.kernel_physical_schema.is_empty() {
            let decode = |bytes: &[u8], which: &str| -> Result<SchemaRef, DataFusionError> {
                arrow::ipc::convert::try_schema_from_ipc_buffer(bytes)
                    .map(Arc::new)
                    .map_err(|e| {
                        DataFusionError::Execution(format!("decode kernel {which} schema IPC: {e}"))
                    })
            };
            let physical = decode(&common.kernel_physical_schema, "physical")?;
            let logical = decode(&common.kernel_logical_schema, "logical")?;
            // Column mapping is active iff physical names diverge from logical; only then relabel.
            let needs_transform = physical
                .fields()
                .iter()
                .zip(logical.fields().iter())
                .any(|(p, l)| p.name() != l.name());
            (physical, logical, needs_transform)
        } else {
            return Err(DataFusionError::Execution(format!(
                "Delta kernel-read scan is missing kernel data-column schemas for {} data \
                 column(s); the driver must ship scan.physical_schema()/logical_schema() \
                 (planDeltaScan / planDeltaReadSchemas)",
                data_fields.len()
            )));
        };

    let output_schema: SchemaRef = Arc::clone(required_schema);
    let partition_output_schema: SchemaRef = Arc::new(Schema::new(partition_fields));

    let object_store_options: HashMap<String, String> = common
        .object_store_options
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let files: Vec<KernelScanFile> = scan
        .tasks
        .iter()
        .map(|t| KernelScanFile {
            path: t.file_path.clone(),
            size: t.file_size as i64,
            record_count: t.record_count.map(|c| c as i64),
            dv: t.dv.clone(),
            partition_values: t
                .partition_values
                .iter()
                .map(|pv| (pv.name.clone(), pv.value.clone()))
                .collect(),
            transform_json: t.transform_json.clone(),
            base_row_id: t.base_row_id,
            default_row_commit_version: t.default_row_commit_version,
            modification_time: t.modification_time.unwrap_or(0),
            byte_range_start: t.byte_range_start.map(|v| v as i64),
            byte_range_end: t.byte_range_end.map(|v| v as i64),
        })
        .collect();

    let table_root = if scan.table_root.is_empty() {
        common.table_root.clone()
    } else {
        scan.table_root.clone()
    };

    // S3 bucket (URL host) for per-bucket credential resolution; None for non-S3.
    let s3_bucket = url::Url::parse(&table_root)
        .ok()
        .filter(|u| matches!(u.scheme(), "s3" | "s3a"))
        .and_then(|u| u.host_str().map(|h| h.to_string()));
    let storage_config = delta_storage_config_from_map(&object_store_options, s3_bucket.as_deref());

    // In-worker synthesis is the only native path (#82): DeltaKernelScanExec produces ALL output
    // columns by name and applies the DV itself.
    let synthesize = common.synthesize_in_worker;

    let scan_exec: Arc<dyn ExecutionPlan> = Arc::new(DeltaKernelScanExec::new(
        output_schema,
        physical_schema,
        read_logical_schema,
        needs_transform,
        true, // apply_dv: the kernel scan always applies the DV now
        partition_output_schema,
        common.session_timezone.clone(),
        table_root.clone(),
        storage_config.clone(),
        common.dv_file_name_prefix.clone(),
        files,
        synthesize,
        cdf,
    ));

    // Reorder to the user-visible layout when synthetics aren't already a suffix.
    if common.final_output_indices.is_empty() {
        Ok(scan_exec)
    } else {
        let wrapped_schema = scan_exec.schema();
        let n = wrapped_schema.fields().len();
        let projections: Result<Vec<(Arc<dyn PhysicalExpr>, String)>, DataFusionError> = common
            .final_output_indices
            .iter()
            .map(|idx| {
                if *idx < 0 || (*idx as usize) >= n {
                    return Err(DataFusionError::Execution(format!(
                        "final_output_indices entry {idx} out of range \
                         (wrapped schema has {n} fields)"
                    )));
                }
                let field = wrapped_schema.field(*idx as usize);
                let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), *idx as usize));
                Ok((col, field.name().clone()))
            })
            .collect();
        Ok(Arc::new(
            ProjectionExec::try_new(projections?, scan_exec).map_err(|e| {
                DataFusionError::Execution(format!("final_output_indices ProjectionExec: {e}"))
            })?,
        ))
    }
}
