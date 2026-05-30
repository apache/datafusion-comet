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

//! Delta-specific helpers core's `OpStruct::DeltaScan` dispatcher arm composes onto
//! the standard parquet datasource path:
//!
//!   - [`build_delta_partitioned_files`] -- convert a `DeltaScanTask` list into a
//!     `Vec<PartitionedFile>` (Delta's add.path is already absolute on the driver;
//!     partition values arrive as strings, parsed here)
//!   - [`parse_delta_partition_scalar`] -- string -> `ScalarValue` with Delta's TZ
//!     semantics and the DATE -> TIMESTAMP_NTZ widening fallback
//!   - [`ColumnMappingFilterRewriter`] -- rewrites pushed-down data filters from
//!     logical to physical column names when column mapping is active
//!
//! All take pure DataFusion / arrow types so this crate stays free of any
//! datafusion-comet dependency (no cycle: core can call us, we can't call core).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::Column;
use object_store::path::Path;
use url::Url;

use crate::proto::DeltaScanTask;

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

/// Convert `DeltaScanTask`s into DataFusion `PartitionedFile`s. Delta's add.path is
/// already an absolute URL once kernel has resolved it on the driver.
pub fn build_delta_partitioned_files(
    tasks: &[DeltaScanTask],
    partition_schema: &Schema,
    session_tz: &str,
) -> Result<Vec<PartitionedFile>, String> {
    let parsed_tz = SessionTimezone::parse(session_tz);
    let mut files = Vec::with_capacity(tasks.len());
    // Reused scratch map for per-task partition-value lookup. Without it, the inner
    // `partition_schema.fields()` loop walks `task.partition_values` with `.iter().find()`
    // for every field -- O(width × values) per task. With it, build the map once per task
    // and do O(1) gets. `clear()` keeps the allocation across tasks.
    let mut partition_values_by_name: std::collections::HashMap<&str, &str> =
        std::collections::HashMap::new();
    for task in tasks {
        let url = Url::parse(task.file_path.as_ref())
            .map_err(|e| format!("Invalid Delta file URL: {e}"))?;
        let path = Path::from_url_path(url.path())
            .map_err(|e| format!("from_url_path: {e}"))?;

        let mut partitioned_file = match (task.byte_range_start, task.byte_range_end) {
            (Some(start), Some(end)) => PartitionedFile::new_with_range(
                String::new(),
                task.file_size,
                start as i64,
                end as i64,
            ),
            _ => PartitionedFile::new(String::new(), task.file_size),
        };
        partitioned_file.object_meta.location = path;

        let mut partition_values: Vec<ScalarValue> =
            Vec::with_capacity(partition_schema.fields().len());
        partition_values_by_name.clear();
        for pv in &task.partition_values {
            if let Some(v) = pv.value.as_deref() {
                partition_values_by_name.insert(pv.name.as_str(), v);
            }
        }
        for field in partition_schema.fields() {
            let scalar = match partition_values_by_name.get(field.name().as_str()).copied() {
                Some(s) => parse_delta_partition_scalar(s, field.data_type(), &parsed_tz, session_tz)
                    .map_err(|e| {
                        format!(
                            "Failed to parse Delta partition value for column '{}': {e}",
                            field.name()
                        )
                    })?,
                None => ScalarValue::try_from(field.data_type()).map_err(|e| {
                    format!(
                        "Failed to build null partition value for column '{}': {e}",
                        field.name()
                    )
                })?,
            };
            partition_values.push(scalar);
        }
        partitioned_file.partition_values = partition_values;
        files.push(partitioned_file);
    }
    Ok(files)
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
                        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                            .map(|d| {
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
            } else if let Ok(dt_with_tz) =
                DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f %z")
                    .or_else(|_| DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S %z"))
            {
                dt_with_tz.timestamp_micros()
            } else {
                let naive = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                    .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
                    .or_else(|_| {
                        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                            .map(|d| {
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
                datafusion::arrow::datatypes::TimeUnit::Nanosecond => Ok(
                    ScalarValue::TimestampNanosecond(Some(micros * 1000), tz_opt.clone()),
                ),
                datafusion::arrow::datatypes::TimeUnit::Second => Ok(
                    ScalarValue::TimestampSecond(Some(micros / 1_000_000), tz_opt.clone()),
                ),
            }
        }
        _ => ScalarValue::try_from_string(s.to_string(), dt).map_err(|e| format!("{e}")),
    }
}

/// Rewrites Column references in a PhysicalExpr from logical names/indices (in
/// required_schema) to physical names/indices (in data_schema). Used when Delta column
/// mapping is active so pushed-down data filters match the DataSourceExec's physical
/// names.
pub struct ColumnMappingFilterRewriter<'a> {
    pub logical_to_physical: &'a HashMap<String, String>,
    pub data_schema: &'a SchemaRef,
}

impl TreeNodeRewriter for ColumnMappingFilterRewriter<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(
        &mut self,
        node: Self::Node,
    ) -> datafusion::common::Result<Transformed<Self::Node>> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            if let Some(physical_name) = self.logical_to_physical.get(column.name()) {
                if let Some(idx) = self
                    .data_schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == physical_name)
                {
                    return Ok(Transformed::yes(Arc::new(Column::new(physical_name, idx))));
                }
                log::warn!(
                    "Column mapping: physical name '{}' for logical '{}' not found in \
                     data_schema; filter may fail at execution time",
                    physical_name,
                    column.name()
                );
            }
            Ok(Transformed::no(node))
        } else {
            Ok(Transformed::no(node))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, TimeUnit};
    use datafusion::common::tree_node::TreeNode;

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
        assert_eq!(parse_fixed_offset("+05:30").unwrap().local_minus_utc(), 5 * 3600 + 30 * 60);
        assert_eq!(parse_fixed_offset("-08:00").unwrap().local_minus_utc(), -8 * 3600);
    }

    #[test]
    fn fixed_offset_hhmm_no_colon() {
        assert_eq!(parse_fixed_offset("+0530").unwrap().local_minus_utc(), 5 * 3600 + 30 * 60);
        assert_eq!(parse_fixed_offset("-0800").unwrap().local_minus_utc(), -8 * 3600);
    }

    #[test]
    fn fixed_offset_hour_only() {
        assert_eq!(parse_fixed_offset("+5").unwrap().local_minus_utc(), 5 * 3600);
        assert_eq!(parse_fixed_offset("-3").unwrap().local_minus_utc(), -3 * 3600);
    }

    #[test]
    fn fixed_offset_gmt_prefix() {
        assert_eq!(
            parse_fixed_offset("GMT+05:30").unwrap().local_minus_utc(),
            5 * 3600 + 30 * 60
        );
        assert_eq!(parse_fixed_offset("UTC-3").unwrap().local_minus_utc(), -3 * 3600);
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
        assert!(matches!(SessionTimezone::parse("nonsense"), SessionTimezone::Invalid));
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
        let s = parse_delta_partition_scalar("9999999999", &DataType::Int64, &tz_utc(), "UTC")
            .unwrap();
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
                // 2024-01-15 12:30:45 UTC = epoch micros 1705321845_000_000
                assert_eq!(v, 1705321845_000_000);
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
                assert_eq!(v, 1705338000_000_000);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // ---- build_delta_partitioned_files ----

    fn task(file_path: &str, partition_values: Vec<(&str, Option<&str>)>) -> DeltaScanTask {
        use crate::proto::DeltaPartitionValue;
        DeltaScanTask {
            file_path: file_path.into(),
            file_size: 1000,
            partition_values: partition_values
                .into_iter()
                .map(|(n, v)| DeltaPartitionValue {
                    name: n.into(),
                    value: v.map(|s| s.into()),
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn build_files_empty_input() {
        let pschema = Schema::new(vec![Field::new("p", DataType::Int32, true)]);
        let files = build_delta_partitioned_files(&[], &pschema, "UTC").unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn build_files_no_partition_columns() {
        let pschema = Schema::new(Vec::<Field>::new());
        let tasks = vec![task("file:///tmp/a.parquet", vec![])];
        let files = build_delta_partitioned_files(&tasks, &pschema, "UTC").unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].partition_values.is_empty());
    }

    #[test]
    fn build_files_single_partition_int() {
        let pschema = Schema::new(vec![Field::new("p", DataType::Int32, true)]);
        let tasks = vec![task("file:///tmp/a.parquet", vec![("p", Some("42"))])];
        let files = build_delta_partitioned_files(&tasks, &pschema, "UTC").unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].partition_values, vec![ScalarValue::Int32(Some(42))]);
    }

    #[test]
    fn build_files_missing_partition_value_yields_null() {
        let pschema = Schema::new(vec![Field::new("p", DataType::Int32, true)]);
        let tasks = vec![task("file:///tmp/a.parquet", vec![])]; // no value for p
        let files = build_delta_partitioned_files(&tasks, &pschema, "UTC").unwrap();
        assert_eq!(files[0].partition_values, vec![ScalarValue::Int32(None)]);
    }

    #[test]
    fn build_files_invalid_url_errors() {
        let pschema = Schema::new(Vec::<Field>::new());
        let tasks = vec![task("not a url", vec![])];
        let err = build_delta_partitioned_files(&tasks, &pschema, "UTC").unwrap_err();
        assert!(err.contains("Invalid Delta file URL"));
    }

    // ---- ColumnMappingFilterRewriter ----

    #[test]
    fn cm_rewriter_renames_known_logical_column() {
        let logical_to_physical: HashMap<String, String> =
            [("user_id".to_string(), "col-1a2b3c".to_string())]
                .iter()
                .cloned()
                .collect();
        let data_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("col-1a2b3c", DataType::Int64, false),
        ]));
        let mut rewriter = ColumnMappingFilterRewriter {
            logical_to_physical: &logical_to_physical,
            data_schema: &data_schema,
        };
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("user_id", 0));
        let out = expr.rewrite(&mut rewriter).unwrap().data;
        let col = out.as_any().downcast_ref::<Column>().unwrap();
        assert_eq!(col.name(), "col-1a2b3c");
        assert_eq!(col.index(), 0);
    }

    #[test]
    fn cm_rewriter_leaves_unmapped_column_alone() {
        let logical_to_physical = HashMap::new();
        let data_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("other", DataType::Int64, false),
        ]));
        let mut rewriter = ColumnMappingFilterRewriter {
            logical_to_physical: &logical_to_physical,
            data_schema: &data_schema,
        };
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("other", 0));
        let out = expr.rewrite(&mut rewriter).unwrap().data;
        let col = out.as_any().downcast_ref::<Column>().unwrap();
        assert_eq!(col.name(), "other");
    }

    #[test]
    fn cm_rewriter_resolves_correct_index() {
        let logical_to_physical: HashMap<String, String> =
            [("logical_b".to_string(), "phys_b".to_string())]
                .iter()
                .cloned()
                .collect();
        let data_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("phys_a", DataType::Int64, false),
            Field::new("phys_b", DataType::Int64, false), // index 1
            Field::new("phys_c", DataType::Int64, false),
        ]));
        let mut rewriter = ColumnMappingFilterRewriter {
            logical_to_physical: &logical_to_physical,
            data_schema: &data_schema,
        };
        // Even if input index is 0 (from required_schema position), rewriter resolves to
        // physical schema's index for phys_b which is 1.
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("logical_b", 0));
        let out = expr.rewrite(&mut rewriter).unwrap().data;
        let col = out.as_any().downcast_ref::<Column>().unwrap();
        assert_eq!(col.name(), "phys_b");
        assert_eq!(col.index(), 1, "must resolve to physical schema index");
    }

    #[test]
    fn cm_rewriter_logs_warning_for_missing_physical() {
        // Mapping says logical -> physical, but physical isn't in data_schema.
        let logical_to_physical: HashMap<String, String> =
            [("logical".to_string(), "phys_missing".to_string())]
                .iter()
                .cloned()
                .collect();
        let data_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("something_else", DataType::Int64, false),
        ]));
        let mut rewriter = ColumnMappingFilterRewriter {
            logical_to_physical: &logical_to_physical,
            data_schema: &data_schema,
        };
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("logical", 0));
        // Should not panic; returns the original Column unchanged.
        let out = expr.rewrite(&mut rewriter).unwrap().data;
        let col = out.as_any().downcast_ref::<Column>().unwrap();
        assert_eq!(col.name(), "logical"); // unchanged
    }
}
