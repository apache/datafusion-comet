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

//! Rejection of Parquet reads that would silently return unrebased dates/timestamps.
//!
//! Spark writes dates/timestamps in the legacy hybrid (Julian + Gregorian) calendar when
//! `spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY`, and every Spark before 3.0 did so
//! unconditionally. Spark rebases those values back to the Proleptic Gregorian calendar on read.
//! Comet's native scan does not implement rebasing (#5010), so it would return values shifted by
//! up to ten days. `spark.comet.exceptionOnDatetimeRebase` (on by default) fails such a read
//! instead.
//!
//! # Why the decision is two-stage
//!
//! Footer metadata alone is too coarse in both directions.
//!
//! Too strict: Spark stamps `org.apache.spark.legacyDateTime` on a whole file whenever the write
//! mode was LEGACY, whether or not any value is old enough to rebase -- and dates from 1582-10-15
//! onward rebase to themselves. Rejecting on the marker alone would fail a large number of reads
//! that return perfectly correct results.
//!
//! Too lax: Spark 2.4.5 and earlier wrote no `org.apache.spark.version` key at all, so the
//! canonical legacy files carry no marker whatsoever. Spark handles those through
//! `spark.sql.parquet.datetimeRebaseModeInRead`, whose default of EXCEPTION makes Spark raise when
//! it decodes an actually-ancient value.
//!
//! So the footer is used only to ask whether a column's values are *provably* already Proleptic
//! Gregorian. Anything it does not clear falls through to Parquet row-group statistics, and is
//! rejected only when the file demonstrably holds a value below Spark's rebase threshold, or when
//! statistics cannot prove otherwise. The result lines up with Spark's own defaults: the reads
//! Comet refuses are, with one narrow exception noted on [`ReadModes`], the reads Spark either
//! rebases or refuses too.

use arrow::datatypes::{DataType, Schema};
use datafusion_comet_common::SparkError;
use parquet::basic::{
    ConvertedType, LogicalType, TimeUnit as ParquetTimeUnit, Type as PhysicalType,
};
use parquet::errors::ParquetError;
use parquet::file::metadata::{KeyValue, ParquetMetaData};
use parquet::file::statistics::Statistics;
use parquet::schema::types::ColumnDescriptor;

/// Spark's Parquet footer key-value metadata keys, from `org.apache.spark.sql.package`.
const SPARK_VERSION_METADATA_KEY: &str = "org.apache.spark.version";
const SPARK_LEGACY_DATETIME_METADATA_KEY: &str = "org.apache.spark.legacyDateTime";
const SPARK_LEGACY_INT96_METADATA_KEY: &str = "org.apache.spark.legacyINT96";

/// The Spark version that switched each encoding to the Proleptic Gregorian calendar. Compared as
/// plain strings, exactly as Spark's `DataSourceUtils.getRebaseSpec` does -- matching Spark matters
/// more here than being right about version ordering, and the two only diverge for a hypothetical
/// major version of 10 or above.
const DATETIME_GREGORIAN_SINCE: &str = "3.0.0";
const INT96_GREGORIAN_SINCE: &str = "3.1.0";

/// Days since the epoch at and above which julian-to-gregorian date rebasing is the identity:
/// 1582-10-15, the first Gregorian day. Mirrors Spark's `RebaseDateTime.lastSwitchJulianDay`.
const LAST_SWITCH_JULIAN_DAY: i32 = -141_427;

/// Micros since the epoch at and above which julian-to-gregorian timestamp rebasing is the
/// identity for every time zone: 1900-01-01T00:00:00Z. Mirrors Spark's
/// `RebaseDateTime.lastSwitchJulianTs`, which Spark derives as the maximum switch point across its
/// per-timezone rebase tables.
///
/// Both constants are asserted against the running Spark's own values by
/// `ParquetDatetimeRebaseSuite`, so a change on Spark's side fails a test rather than silently
/// shifting the threshold. They are duplicated here rather than sent from the JVM because reading
/// `lastSwitchJulianTs` forces `RebaseDateTime`'s static initializer, which parses ~590 KB of
/// bundled JSON and retains several MB -- a cost every driver would otherwise pay on its first
/// native scan, whether or not the guard is armed.
const LAST_SWITCH_JULIAN_MICROS: i64 = -2_208_988_800_000_000;

/// Whether the user has told Spark to assume Proleptic Gregorian for files whose provenance the
/// footer does not record, via `spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED` and its INT96
/// counterpart. Both default to EXCEPTION, under which Spark raises on an ancient value, so the
/// default leaves Comet and Spark agreeing.
///
/// The one place Comet still diverges from Spark: with a mode of LEGACY, Spark rebases a
/// version-less file's values and returns them correctly, whereas Comet has no rebasing to do it
/// with and refuses the read.
#[derive(Debug, Clone, Copy)]
pub struct ReadModes {
    /// `spark.sql.parquet.datetimeRebaseModeInRead == CORRECTED`.
    pub datetime_corrected: bool,
    /// `spark.sql.parquet.int96RebaseModeInRead == CORRECTED`.
    pub int96_corrected: bool,
}

/// The armed legacy-calendar guard for one scan. Built once per scan; consulted once per file open.
#[derive(Debug, Clone)]
pub struct LegacyCalendarGuard {
    /// Top-level requested field names that can decode a date or timestamp, used to ignore
    /// calendar-sensitive columns the scan never reads.
    requested_roots: Vec<String>,
    case_sensitive: bool,
    read_modes: ReadModes,
}

impl LegacyCalendarGuard {
    /// `None` when the guard cannot ever fire for this scan, either because the config is off or
    /// because nothing calendar-sensitive is being read. Callers keep that as `None` so a disarmed
    /// guard costs a single `Option` check per file rather than any footer inspection.
    pub fn for_scan(
        enabled: bool,
        required_schema: &Schema,
        case_sensitive: bool,
        read_modes: ReadModes,
    ) -> Option<Self> {
        if !enabled {
            return None;
        }
        let requested_roots: Vec<String> = required_schema
            .fields()
            .iter()
            .filter(|field| data_type_has_date_or_timestamp(field.data_type()))
            .map(|field| field.name().clone())
            .collect();
        if requested_roots.is_empty() {
            return None;
        }
        Some(Self {
            requested_roots,
            case_sensitive,
            read_modes,
        })
    }

    /// `Err` if this file must not be read: some calendar-sensitive column the scan reads is not
    /// provably Proleptic Gregorian, and either holds a value that would need rebasing or cannot be
    /// shown not to.
    pub fn check(&self, metadata: &ParquetMetaData) -> Result<(), ParquetError> {
        if self.reads_unrebasable_values(metadata) {
            return Err(legacy_calendar_error());
        }
        Ok(())
    }

    fn reads_unrebasable_values(&self, metadata: &ParquetMetaData) -> bool {
        let file_metadata = metadata.file_metadata();
        // Footer facts are per file, so resolve them once rather than per column.
        let provenance = Provenance::from_footer(file_metadata.key_value_metadata());
        let descr = file_metadata.schema_descr();

        for (leaf_index, column) in descr.columns().iter().enumerate() {
            // `calendar_kind` is the cheaper of the two filters and prunes far more columns on a
            // wide schema, so it goes first.
            let Some(kind) = calendar_kind(column) else {
                continue;
            };
            if !self.reads(column) || provenance.is_gregorian(kind, &self.read_modes) {
                continue;
            }
            let Some(threshold) = kind.threshold() else {
                // INT96 carries no usable statistics -- the Parquet spec gives its 12 bytes no
                // meaningful ordering, so writers either omit min/max or write values that must
                // not be compared. There is nothing to prove safety with.
                return true;
            };
            if row_groups_may_hold_values_below(metadata, leaf_index, threshold) {
                return true;
            }
        }
        false
    }

    /// Whether this leaf column sits under a top-level field the scan actually reads. Comparing
    /// the root of the column path (rather than the full path) keeps nested date/timestamp fields
    /// covered without having to reconstruct Parquet's list/map path encodings.
    fn reads(&self, column: &ColumnDescriptor) -> bool {
        let Some(root) = column.path().parts().first() else {
            return false;
        };
        self.requested_roots.iter().any(|name| {
            if self.case_sensitive {
                name == root
            } else {
                // Matches how the schema adapter resolves the same names.
                name.eq_ignore_ascii_case(root)
            }
        })
    }
}

/// What a file's footer says about the calendar its values were written in, resolved once per file.
struct Provenance<'a> {
    /// The Spark version that wrote the file, absent for non-Spark writers and for Spark 2.4.5 and
    /// earlier, which did not stamp the key.
    writer_version: Option<&'a str>,
    has_legacy_datetime_marker: bool,
    has_legacy_int96_marker: bool,
}

impl<'a> Provenance<'a> {
    fn from_footer(key_value_metadata: Option<&'a Vec<KeyValue>>) -> Self {
        let has_key = |key: &str| {
            key_value_metadata.is_some_and(|kv| kv.iter().any(|entry| entry.key == key))
        };
        Self {
            writer_version: key_value_metadata.and_then(|kv| {
                kv.iter()
                    .find(|entry| entry.key == SPARK_VERSION_METADATA_KEY)
                    .and_then(|entry| entry.value.as_deref())
            }),
            has_legacy_datetime_marker: has_key(SPARK_LEGACY_DATETIME_METADATA_KEY),
            has_legacy_int96_marker: has_key(SPARK_LEGACY_INT96_METADATA_KEY),
        }
    }

    /// Whether the footer proves this column's values are already Proleptic Gregorian, so no
    /// rebasing would apply and Comet can read them as-is.
    ///
    /// Mirrors the CORRECTED arm of Spark's `DataSourceUtils.getRebaseSpec`: a writer at or after
    /// the switch version that did not explicitly opt back into LEGACY. A file with no recorded
    /// writer version proves nothing, and falls back to the read-mode config exactly as Spark does.
    fn is_gregorian(&self, kind: CalendarKind, read_modes: &ReadModes) -> bool {
        let (gregorian_since, has_legacy_marker, versionless_is_corrected) = match kind {
            CalendarKind::Int96 => (
                INT96_GREGORIAN_SINCE,
                self.has_legacy_int96_marker,
                read_modes.int96_corrected,
            ),
            CalendarKind::Date | CalendarKind::Timestamp(_) => (
                DATETIME_GREGORIAN_SINCE,
                self.has_legacy_datetime_marker,
                read_modes.datetime_corrected,
            ),
        };
        match self.writer_version {
            None => versionless_is_corrected,
            Some(version) => version >= gregorian_since && !has_legacy_marker,
        }
    }
}

/// Which of Spark's two rebase regimes a calendar-sensitive leaf column falls under. Spark tracks
/// INT96 separately from the other encodings: it switched calendars a release later and has its own
/// footer marker and read-mode config.
#[derive(Debug, Clone, Copy)]
enum CalendarKind {
    /// A DATE column, in days.
    Date,
    /// TIMESTAMP_MILLIS or TIMESTAMP_MICROS, in an INT64 column whose statistics are ordinary
    /// signed integers.
    Timestamp(ParquetTimeUnit),
    /// An INT96 timestamp.
    Int96,
}

impl CalendarKind {
    /// The rebase threshold in this column's own physical units, or `None` when statistics cannot
    /// decide the question.
    fn threshold(self) -> Option<i64> {
        match self {
            CalendarKind::Date => Some(LAST_SWITCH_JULIAN_DAY as i64),
            CalendarKind::Timestamp(unit) => Some(timestamp_threshold(unit)),
            CalendarKind::Int96 => None,
        }
    }
}

/// Classify a leaf column.
///
/// Both `LogicalType` and the deprecated `ConvertedType` are consulted: legacy-calendar files are
/// by definition old, and files written before Parquet's logical-type rework carry only the
/// converted type.
fn calendar_kind(column: &ColumnDescriptor) -> Option<CalendarKind> {
    if column.physical_type() == PhysicalType::INT96 {
        return Some(CalendarKind::Int96);
    }
    match column.logical_type_ref() {
        Some(LogicalType::Date) => Some(CalendarKind::Date),
        Some(LogicalType::Timestamp { unit, .. }) => Some(CalendarKind::Timestamp(*unit)),
        Some(_) => None,
        // Pre-logical-type files.
        None => match column.converted_type() {
            ConvertedType::DATE => Some(CalendarKind::Date),
            ConvertedType::TIMESTAMP_MILLIS => {
                Some(CalendarKind::Timestamp(ParquetTimeUnit::MILLIS))
            }
            ConvertedType::TIMESTAMP_MICROS => {
                Some(CalendarKind::Timestamp(ParquetTimeUnit::MICROS))
            }
            _ => None,
        },
    }
}

/// Scale the micros threshold into `unit`.
///
/// Scaling down to millis rounds toward negative infinity, which is the safe direction: it can
/// only make the threshold earlier, so a value in the truncated sub-millisecond window is treated
/// as affected rather than cleared.
fn timestamp_threshold(unit: ParquetTimeUnit) -> i64 {
    match unit {
        ParquetTimeUnit::MILLIS => LAST_SWITCH_JULIAN_MICROS.div_euclid(1_000),
        ParquetTimeUnit::MICROS => LAST_SWITCH_JULIAN_MICROS,
        ParquetTimeUnit::NANOS => LAST_SWITCH_JULIAN_MICROS.saturating_mul(1_000),
    }
}

/// Whether any row group's minimum for this column is below `threshold`, treating "no usable
/// statistics" as below it. An all-null row group is cleared: it has no value to rebase.
fn row_groups_may_hold_values_below(
    metadata: &ParquetMetaData,
    leaf_index: usize,
    threshold: i64,
) -> bool {
    metadata.row_groups().iter().any(|row_group| {
        // An empty row group decodes nothing.
        if row_group.num_rows() == 0 {
            return false;
        }
        let Some(statistics) = row_group.column(leaf_index).statistics() else {
            return true;
        };
        match statistics_min(statistics) {
            Some(min) => min < threshold,
            // No minimum. Either the column is entirely null in this row group, in which case
            // there is nothing to rebase, or the writer omitted the bound and we cannot tell.
            None => statistics.null_count_opt() != Some(row_group.num_rows() as u64),
        }
    })
}

/// The minimum of a date/timestamp column's statistics, widened to `i64`. Only the two physical
/// types those logical types can use are handled; anything else is treated as no minimum.
fn statistics_min(statistics: &Statistics) -> Option<i64> {
    match statistics {
        Statistics::Int32(value) => value.min_opt().map(|min| *min as i64),
        Statistics::Int64(value) => value.min_opt().copied(),
        _ => None,
    }
}

fn data_type_has_date_or_timestamp(data_type: &DataType) -> bool {
    match data_type {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _)
        | DataType::RunEndEncoded(_, field) => data_type_has_date_or_timestamp(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| data_type_has_date_or_timestamp(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| data_type_has_date_or_timestamp(field.data_type())),
        DataType::Dictionary(_, value_type) => data_type_has_date_or_timestamp(value_type),
        _ => false,
    }
}

/// The failure raised for a file that would need rebasing.
///
/// Boxed inside `ParquetError::External` so the JNI error layer can recover it by downcast rather
/// than by matching on message text: `get_metadata` can only return a `ParquetError`, and a bare
/// `ParquetError::General` would be classified as a corrupt-file read (`FAILED_READ_FILE`), which
/// this is not. `file_path` is left empty for the JVM side to fill from the per-task file list,
/// which knows the file's real URI -- the object-store location available here has had its scheme
/// and leading slash normalised away.
pub fn legacy_calendar_error() -> ParquetError {
    ParquetError::External(Box::new(SparkError::LegacyDatetimeRebase {
        file_path: String::new(),
        message: LEGACY_CALENDAR_MESSAGE.to_string(),
    }))
}

/// The user-facing explanation, defined once here and carried to the JVM in the error payload so
/// the shim that builds the exception does not restate it.
const LEGACY_CALENDAR_MESSAGE: &str =
    "Comet cannot read this Parquet file: it holds dates or timestamps written in the legacy \
     hybrid (Julian + Gregorian) calendar, which Comet's native scan does not rebase to the \
     Proleptic Gregorian calendar. Reading it would return values shifted by up to ten days. \
     Set spark.comet.exceptionOnDatetimeRebase=false to read these values as-is without \
     rebasing, or disable Comet for this query so that Spark rebases them.";

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, TimeUnit};
    use std::sync::Arc;

    /// Spark's defaults: both read modes EXCEPTION, so neither clears a version-less file.
    const DEFAULT_MODES: ReadModes = ReadModes {
        datetime_corrected: false,
        int96_corrected: false,
    };

    const DATE: CalendarKind = CalendarKind::Date;
    const INT96: CalendarKind = CalendarKind::Int96;

    fn kv(pairs: &[(&str, Option<&str>)]) -> Vec<KeyValue> {
        pairs
            .iter()
            .map(|(key, value)| KeyValue {
                key: key.to_string(),
                value: value.map(|v| v.to_string()),
            })
            .collect()
    }

    /// Whether the footer clears `kind` for a file with this metadata, under Spark's default read
    /// modes.
    fn cleared(kind: CalendarKind, pairs: &[(&str, Option<&str>)]) -> bool {
        let kv = kv(pairs);
        Provenance::from_footer(Some(&kv)).is_gregorian(kind, &DEFAULT_MODES)
    }

    #[test]
    fn a_modern_corrected_writer_is_cleared() {
        // The overwhelmingly common case: Spark 3.0+ with the default CORRECTED write mode stamps
        // its version and no legacy marker, which positively proves the values are Gregorian.
        for version in ["3.0.0", "3.5.9", "4.1.3"] {
            assert!(
                cleared(DATE, &[(SPARK_VERSION_METADATA_KEY, Some(version))]),
                "date should be cleared for writer {version}"
            );
        }
        for version in ["3.1.0", "3.5.9", "4.1.3"] {
            assert!(
                cleared(INT96, &[(SPARK_VERSION_METADATA_KEY, Some(version))]),
                "INT96 should be cleared for writer {version}"
            );
        }
    }

    #[test]
    fn a_pre_switch_writer_is_not_cleared() {
        assert!(!cleared(
            DATE,
            &[(SPARK_VERSION_METADATA_KEY, Some("2.4.6"))]
        ));
        // INT96 switched a release later than the other encodings, so Spark 3.0 wrote hybrid INT96
        // while its DATE values were already Gregorian.
        assert!(!cleared(
            INT96,
            &[(SPARK_VERSION_METADATA_KEY, Some("3.0.3"))]
        ));
        assert!(cleared(
            DATE,
            &[(SPARK_VERSION_METADATA_KEY, Some("3.0.3"))]
        ));
    }

    #[test]
    fn an_explicit_legacy_marker_is_not_cleared() {
        // Written by a modern Spark with rebaseModeInWrite=LEGACY. The value Spark stamps is the
        // empty string; Spark tests for key presence, not for a value.
        for value in [Some(""), None] {
            assert!(!cleared(
                DATE,
                &[
                    (SPARK_VERSION_METADATA_KEY, Some("4.1.3")),
                    (SPARK_LEGACY_DATETIME_METADATA_KEY, value),
                ]
            ));
            assert!(!cleared(
                INT96,
                &[
                    (SPARK_VERSION_METADATA_KEY, Some("4.1.3")),
                    (SPARK_LEGACY_INT96_METADATA_KEY, value),
                ]
            ));
        }
    }

    #[test]
    fn the_two_legacy_markers_are_tracked_separately() {
        // A modern writer that opted INT96 back into LEGACY says nothing about its DATE columns.
        let footer = [
            (SPARK_VERSION_METADATA_KEY, Some("4.1.3")),
            (SPARK_LEGACY_INT96_METADATA_KEY, Some("")),
        ];
        assert!(cleared(DATE, &footer));
        assert!(!cleared(INT96, &footer));
    }

    #[test]
    fn a_file_with_no_writer_version_is_not_cleared_by_default() {
        // Spark 2.4.5 and earlier stamped no version, and neither do non-Spark writers. Spark
        // resolves these through datetimeRebaseModeInRead, whose EXCEPTION default raises on an
        // ancient value -- so leaving them uncleared here is what agrees with Spark. Whether the
        // read actually fails is then decided by row-group statistics.
        assert!(!cleared(DATE, &[]));
        assert!(!cleared(INT96, &[]));
        assert!(!cleared(DATE, &[("parquet-mr version", Some("1.13.1"))]));
        assert!(!Provenance::from_footer(None).is_gregorian(DATE, &DEFAULT_MODES));
    }

    #[test]
    fn a_version_key_with_no_value_is_treated_as_no_version() {
        assert!(!cleared(DATE, &[(SPARK_VERSION_METADATA_KEY, None)]));
    }

    #[test]
    fn corrected_read_mode_clears_a_version_less_file() {
        // The user has asserted the values are already Gregorian, which is exactly what
        // datetimeRebaseModeInRead=CORRECTED means to Spark. Comet honors it per encoding.
        let modes = ReadModes {
            datetime_corrected: true,
            int96_corrected: false,
        };
        let none = Provenance::from_footer(None);
        assert!(none.is_gregorian(DATE, &modes));
        assert!(!none.is_gregorian(INT96, &modes));

        let modes = ReadModes {
            datetime_corrected: false,
            int96_corrected: true,
        };
        assert!(!none.is_gregorian(DATE, &modes));
        assert!(none.is_gregorian(INT96, &modes));
    }

    #[test]
    fn corrected_read_mode_does_not_override_an_explicit_legacy_marker() {
        // The footer is authoritative when it records provenance; the read mode only fills the gap
        // when it does not. This mirrors Spark, which ignores the config for stamped files.
        let modes = ReadModes {
            datetime_corrected: true,
            int96_corrected: true,
        };
        let kv = kv(&[
            (SPARK_VERSION_METADATA_KEY, Some("4.1.3")),
            (SPARK_LEGACY_DATETIME_METADATA_KEY, Some("")),
        ]);
        assert!(!Provenance::from_footer(Some(&kv)).is_gregorian(DATE, &modes));
    }

    #[test]
    fn int96_statistics_can_never_clear_a_column() {
        assert_eq!(CalendarKind::Int96.threshold(), None);
    }

    #[test]
    fn date_threshold_is_the_switch_day() {
        // 1582-10-15, the first Gregorian day.
        assert_eq!(CalendarKind::Date.threshold(), Some(-141_427));
    }

    #[test]
    fn millis_threshold_rounds_toward_the_unsafe_direction() {
        // Truncating a negative micros threshold must not move it later in time, which would
        // clear values that actually need rebasing.
        let scaled = timestamp_threshold(ParquetTimeUnit::MILLIS);
        assert!(scaled * 1_000 <= LAST_SWITCH_JULIAN_MICROS);
    }

    #[test]
    fn micros_and_nanos_thresholds_scale_exactly() {
        assert_eq!(
            timestamp_threshold(ParquetTimeUnit::MICROS),
            LAST_SWITCH_JULIAN_MICROS
        );
        assert_eq!(
            timestamp_threshold(ParquetTimeUnit::NANOS),
            LAST_SWITCH_JULIAN_MICROS * 1_000
        );
    }

    #[test]
    fn statistics_min_reads_the_two_physical_types_dates_and_timestamps_use() {
        let int32 = Statistics::int32(Some(-141_428), Some(0), None, Some(0), false);
        assert_eq!(statistics_min(&int32), Some(-141_428));
        let int64 = Statistics::int64(Some(-2_208_988_800_000_001), Some(0), None, Some(0), false);
        assert_eq!(statistics_min(&int64), Some(-2_208_988_800_000_001));
        let float = Statistics::float(Some(1.0), Some(2.0), None, Some(0), false);
        assert_eq!(statistics_min(&float), None);
    }

    fn date_field() -> Field {
        Field::new("d", DataType::Date32, true)
    }

    fn string_field() -> Field {
        Field::new("s", DataType::Utf8, true)
    }

    fn guard_for(schema: &Schema, enabled: bool) -> Option<LegacyCalendarGuard> {
        LegacyCalendarGuard::for_scan(enabled, schema, true, DEFAULT_MODES)
    }

    #[test]
    fn guard_is_disarmed_when_the_config_is_off() {
        let schema = Schema::new(vec![date_field()]);
        assert!(guard_for(&schema, false).is_none());
    }

    #[test]
    fn guard_is_disarmed_when_no_calendar_sensitive_column_is_read() {
        let schema = Schema::new(vec![
            string_field(),
            Field::new("i", DataType::Int64, true),
            Field::new(
                "nested",
                DataType::Struct(
                    vec![
                        Field::new("b", DataType::Binary, true),
                        Field::new(
                            "l",
                            DataType::List(Arc::new(Field::new("e", DataType::Float64, true))),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]);
        assert!(guard_for(&schema, true).is_none());
    }

    #[test]
    fn guard_arms_on_top_level_and_nested_date_or_timestamp_columns() {
        let ts = Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        );
        for field in [
            date_field(),
            ts.clone(),
            Field::new("l", DataType::List(Arc::new(date_field())), true),
            Field::new(
                "st",
                DataType::Struct(vec![string_field(), date_field()].into()),
                true,
            ),
            Field::new(
                "m",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(vec![string_field(), ts].into()),
                        false,
                    )),
                    false,
                ),
                true,
            ),
            Field::new(
                "dict",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Date32)),
                true,
            ),
        ] {
            let name = field.name().clone();
            let schema = Schema::new(vec![field]);
            let guard = guard_for(&schema, true);
            assert!(guard.is_some(), "expected {name} to arm the guard");
            assert_eq!(guard.unwrap().requested_roots, vec![name]);
        }
    }

    #[test]
    fn guard_only_tracks_the_calendar_sensitive_roots() {
        let schema = Schema::new(vec![
            string_field(),
            date_field(),
            Field::new("i", DataType::Int64, true),
        ]);
        let guard = guard_for(&schema, true).unwrap();
        assert_eq!(guard.requested_roots, vec!["d".to_string()]);
    }

    /// A DATE leaf named `name`, as a Parquet schema descriptor would report it.
    fn date_leaf(name: &str) -> ColumnDescriptor {
        use parquet::basic::Repetition;
        use parquet::schema::types::{ColumnPath, Type};
        let primitive = Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Date))
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();
        ColumnDescriptor::new(
            Arc::new(primitive),
            1,
            0,
            ColumnPath::new(vec![name.to_string()]),
        )
    }

    #[test]
    fn case_sensitive_scans_require_an_exact_root_match() {
        let schema = Schema::new(vec![Field::new("MyDate", DataType::Date32, true)]);
        let guard = LegacyCalendarGuard::for_scan(true, &schema, true, DEFAULT_MODES).unwrap();
        assert!(guard.reads(&date_leaf("MyDate")));
        assert!(!guard.reads(&date_leaf("mydate")));
    }

    #[test]
    fn case_insensitive_scans_match_a_root_in_any_case() {
        let schema = Schema::new(vec![Field::new("MyDate", DataType::Date32, true)]);
        let guard = LegacyCalendarGuard::for_scan(true, &schema, false, DEFAULT_MODES).unwrap();
        assert!(guard.reads(&date_leaf("MyDate")));
        assert!(guard.reads(&date_leaf("mydate")));
        assert!(guard.reads(&date_leaf("MYDATE")));
        assert!(!guard.reads(&date_leaf("other")));
    }
}
