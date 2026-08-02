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
//! Footer metadata alone is too coarse in both directions, so the footer narrows the question and
//! row-group statistics answer it.
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
//! So the footer answers a three-way question per column -- see [`Calendar`] -- and how much
//! Parquet row-group statistics then have to prove depends on the answer:
//!
//! - [`Calendar::Gregorian`]: no rebasing would apply. Read as-is, no statistics consulted.
//! - [`Calendar::Legacy`]: the file says these values *are* hybrid-calendar, so anything
//!   statistics cannot rule out is assumed affected. A column with no statistics is refused, and
//!   so is any INT96 column, whose 12 bytes the Parquet spec gives no meaningful ordering.
//! - [`Calendar::Unknown`]: nothing says they are. Refuse only on positive proof -- a row-group
//!   minimum below Spark's threshold. Missing statistics and INT96 prove nothing and are read.
//!
//! That last asymmetry is what keeps the default-on guard usable. Every non-Spark writer -- Hive,
//! Impala, Trino, plain parquet-mr -- leaves the version key unset, and Hive in particular writes
//! its timestamps as INT96. Treating unknown provenance as conservatively as a legacy marker would
//! refuse every Hive-written timestamp column ever, whatever its values, where Spark reads them
//! without complaint. Spark under its own EXCEPTION default raises only for a value it actually
//! decodes below the switch point, so refusing only on proof is what agrees with Spark.
//!
//! The residual gap, which [`ReadModes`] also notes: an unknown-provenance file whose affected
//! values statistics cannot expose -- an INT96 column, or one written with statistics off -- is
//! read unrebased, where Spark would have raised. Closing that needs a per-value check in the
//! decoder, or the rebasing itself (#5010).

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
/// counterpart. Both default to EXCEPTION, under which such a file is read only as far as
/// statistics can show it holds nothing affected.
///
/// Where Comet still diverges from Spark for a version-less file: Spark decides per decoded value,
/// so it raises (under EXCEPTION) or rebases (under LEGACY) exactly the values that need it, while
/// Comet decides per column from statistics. So Comet reads an affected value unrebased when
/// statistics cannot expose it -- an INT96 column, or one written with statistics off -- and
/// refuses a whole column under LEGACY, which Spark would have rebased and returned correctly,
/// because Comet has no rebasing to do it with.
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
            if !self.reads(column) {
                continue;
            }
            // How to treat a value statistics cannot decide on. See the module docs: a file that
            // declares itself legacy gets the benefit of the doubt, one that declares nothing does
            // not, because assuming the worst there would refuse every Hive-written timestamp.
            let undecidable_is_affected = match provenance.calendar(kind, &self.read_modes) {
                Calendar::Gregorian => continue,
                Calendar::Legacy => true,
                Calendar::Unknown => false,
            };
            let Some(threshold) = kind.threshold() else {
                // INT96 carries no usable statistics -- the Parquet spec gives its 12 bytes no
                // meaningful ordering, so writers either omit min/max or write values that must
                // not be compared. There is nothing to prove or disprove safety with.
                if undecidable_is_affected {
                    return true;
                }
                continue;
            };
            if row_groups_hold_values_below(
                metadata,
                leaf_index,
                threshold,
                undecidable_is_affected,
            ) {
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

    /// What the footer says about the calendar this column's values were written in.
    ///
    /// Mirrors Spark's `DataSourceUtils.getRebaseSpec`, which resolves a legacy marker first, then
    /// the writer version, then -- when the file records no version -- the read-mode config. The
    /// only difference is that Comet keeps "the config did not say CORRECTED" as its own
    /// [`Calendar::Unknown`] answer rather than collapsing it into legacy, because what it can
    /// prove from statistics differs. See the module docs.
    fn calendar(&self, kind: CalendarKind, read_modes: &ReadModes) -> Calendar {
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
        // An explicit marker is authoritative, and outranks both the writer version (a modern Spark
        // writing with rebaseModeInWrite=LEGACY stamps both) and the read mode.
        if has_legacy_marker {
            return Calendar::Legacy;
        }
        match self.writer_version {
            Some(version) if version >= gregorian_since => Calendar::Gregorian,
            Some(_) => Calendar::Legacy,
            None if versionless_is_corrected => Calendar::Gregorian,
            None => Calendar::Unknown,
        }
    }
}

/// What a file's footer establishes about the calendar a column's values were written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Calendar {
    /// Provably already Proleptic Gregorian: a writer at or after the switch version that did not
    /// opt back into LEGACY, or a version-less file the read mode declares CORRECTED. No rebasing
    /// would apply, so Comet reads the values as-is.
    Gregorian,
    /// Provably the legacy hybrid calendar: an explicit legacy marker, or a Spark older than the
    /// switch version. Spark would rebase these, so Comet refuses anything statistics cannot clear.
    Legacy,
    /// The footer records no writer version -- Spark 2.4.5 and earlier, and every non-Spark writer
    /// -- and no read mode declared it corrected. The values may or may not be hybrid-calendar, so
    /// Comet refuses only what statistics positively expose.
    Unknown,
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

/// Classify a leaf column, or `None` if its values are not calendar-sensitive.
///
/// Both `LogicalType` and the deprecated `ConvertedType` are consulted: legacy-calendar files are
/// by definition old, and files written before Parquet's logical-type rework carry only the
/// converted type.
///
/// A `TIMESTAMP` that is not adjusted to UTC is Spark's TIMESTAMP_NTZ, which Spark never rebases in
/// either direction -- "TIMESTAMP_NTZ is a new data type and has no legacy files that need to do
/// rebase", as its `ParquetVectorUpdaterFactory` puts it. Spark still stamps
/// `org.apache.spark.legacyDateTime` on any file written under a LEGACY rebase mode whether or not
/// the schema has a column the mode could apply to, so without this an NTZ column in such a file
/// would be refused over values Spark reads back exactly as written.
///
/// The classification is deliberately taken from the Parquet type rather than the requested Arrow
/// type: `spark.comet.allowTimestampLtzAsNtz` can request an NTZ Arrow type over a UTC-adjusted
/// Parquet column, whose values do need rebasing, and the file is what knows which it holds.
fn calendar_kind(column: &ColumnDescriptor) -> Option<CalendarKind> {
    if column.physical_type() == PhysicalType::INT96 {
        return Some(CalendarKind::Int96);
    }
    match column.logical_type_ref() {
        Some(LogicalType::Date) => Some(CalendarKind::Date),
        Some(LogicalType::Timestamp {
            unit,
            is_adjusted_to_u_t_c,
        }) => is_adjusted_to_u_t_c.then(|| CalendarKind::Timestamp(*unit)),
        Some(_) => None,
        // Pre-logical-type files. Both of these converted types are UTC-normalised by definition,
        // so there is no NTZ case to exclude here.
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

/// Whether any row group's minimum for this column is below `threshold`.
///
/// `undecidable_is_affected` is the answer for a row group whose statistics cannot settle the
/// question, either because the writer wrote none or because it omitted the minimum: `true` assumes
/// the worst, `false` requires positive proof. An all-null row group is cleared either way -- it has
/// no value to rebase -- and so is an empty one.
fn row_groups_hold_values_below(
    metadata: &ParquetMetaData,
    leaf_index: usize,
    threshold: i64,
    undecidable_is_affected: bool,
) -> bool {
    metadata.row_groups().iter().any(|row_group| {
        // An empty row group decodes nothing.
        if row_group.num_rows() == 0 {
            return false;
        }
        let Some(statistics) = row_group.column(leaf_index).statistics() else {
            return undecidable_is_affected;
        };
        match statistics_min(statistics) {
            Some(min) => min < threshold,
            // No minimum. Either the column is entirely null in this row group, in which case
            // there is nothing to rebase, or the writer omitted the bound and we cannot tell.
            None if statistics.null_count_opt() == Some(row_group.num_rows() as u64) => false,
            None => undecidable_is_affected,
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

    /// The calendar the footer establishes for `kind`, under Spark's default read modes.
    fn calendar_of(kind: CalendarKind, pairs: &[(&str, Option<&str>)]) -> Calendar {
        let kv = kv(pairs);
        Provenance::from_footer(Some(&kv)).calendar(kind, &DEFAULT_MODES)
    }

    /// Whether the footer proves `kind` needs no rebasing, under Spark's default read modes.
    fn cleared(kind: CalendarKind, pairs: &[(&str, Option<&str>)]) -> bool {
        calendar_of(kind, pairs) == Calendar::Gregorian
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
    fn a_pre_switch_writer_is_provably_legacy() {
        assert_eq!(
            calendar_of(DATE, &[(SPARK_VERSION_METADATA_KEY, Some("2.4.6"))]),
            Calendar::Legacy
        );
        // INT96 switched a release later than the other encodings, so Spark 3.0 wrote hybrid INT96
        // while its DATE values were already Gregorian.
        assert_eq!(
            calendar_of(INT96, &[(SPARK_VERSION_METADATA_KEY, Some("3.0.3"))]),
            Calendar::Legacy
        );
        assert!(cleared(
            DATE,
            &[(SPARK_VERSION_METADATA_KEY, Some("3.0.3"))]
        ));
    }

    #[test]
    fn an_explicit_legacy_marker_is_provably_legacy() {
        // Written by a modern Spark with rebaseModeInWrite=LEGACY. The value Spark stamps is the
        // empty string; Spark tests for key presence, not for a value.
        for value in [Some(""), None] {
            assert_eq!(
                calendar_of(
                    DATE,
                    &[
                        (SPARK_VERSION_METADATA_KEY, Some("4.1.3")),
                        (SPARK_LEGACY_DATETIME_METADATA_KEY, value),
                    ]
                ),
                Calendar::Legacy
            );
            assert_eq!(
                calendar_of(
                    INT96,
                    &[
                        (SPARK_VERSION_METADATA_KEY, Some("4.1.3")),
                        (SPARK_LEGACY_INT96_METADATA_KEY, value),
                    ]
                ),
                Calendar::Legacy
            );
        }
        // The marker outranks a missing writer version too, rather than falling through to the
        // read mode as an unmarked version-less file does.
        assert_eq!(
            calendar_of(DATE, &[(SPARK_LEGACY_DATETIME_METADATA_KEY, Some(""))]),
            Calendar::Legacy
        );
    }

    #[test]
    fn the_two_legacy_markers_are_tracked_separately() {
        // A modern writer that opted INT96 back into LEGACY says nothing about its DATE columns.
        let footer = [
            (SPARK_VERSION_METADATA_KEY, Some("4.1.3")),
            (SPARK_LEGACY_INT96_METADATA_KEY, Some("")),
        ];
        assert!(cleared(DATE, &footer));
        assert_eq!(calendar_of(INT96, &footer), Calendar::Legacy);
    }

    #[test]
    fn a_file_with_no_writer_version_is_unknown_by_default() {
        // Spark 2.4.5 and earlier stamped no version, and neither do Hive, Impala or plain
        // parquet-mr. Nothing here proves either calendar, so the read is refused only where
        // statistics positively expose an affected value -- never merely because a bound is
        // missing, which would make every Hive timestamp column unreadable.
        for footer in [
            &[][..],
            &[("parquet-mr version", Some("1.13.1"))][..],
            // A version key with no value is treated as no version at all.
            &[(SPARK_VERSION_METADATA_KEY, None)][..],
        ] {
            assert_eq!(calendar_of(DATE, footer), Calendar::Unknown);
            assert_eq!(calendar_of(INT96, footer), Calendar::Unknown);
        }
        assert_eq!(
            Provenance::from_footer(None).calendar(DATE, &DEFAULT_MODES),
            Calendar::Unknown
        );
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
        assert_eq!(none.calendar(DATE, &modes), Calendar::Gregorian);
        assert_eq!(none.calendar(INT96, &modes), Calendar::Unknown);

        let modes = ReadModes {
            datetime_corrected: false,
            int96_corrected: true,
        };
        assert_eq!(none.calendar(DATE, &modes), Calendar::Unknown);
        assert_eq!(none.calendar(INT96, &modes), Calendar::Gregorian);
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
        assert_eq!(
            Provenance::from_footer(Some(&kv)).calendar(DATE, &modes),
            Calendar::Legacy
        );
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

    /// Footer of a file no Spark wrote: parquet-mr stamps its own version and nothing else. Hive,
    /// Impala and Trino all look like this.
    const NON_SPARK_FOOTER: &[(&str, Option<&str>)] = &[("parquet-mr version", Some("1.10.1"))];

    /// A DATE primitive, as the schema descriptor of a real file would hold it.
    fn date_primitive() -> parquet::schema::types::Type {
        use parquet::basic::Repetition;
        use parquet::schema::types::Type;
        Type::primitive_type_builder("d", PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Date))
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap()
    }

    /// An INT96 timestamp primitive: what Hive writes for a TIMESTAMP column.
    fn int96_primitive() -> parquet::schema::types::Type {
        use parquet::basic::Repetition;
        use parquet::schema::types::Type;
        Type::primitive_type_builder("t", PhysicalType::INT96)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap()
    }

    /// An INT64 TIMESTAMP primitive. `is_adjusted_to_u_t_c` false is Spark's TIMESTAMP_NTZ.
    fn timestamp_primitive(is_adjusted_to_u_t_c: bool) -> parquet::schema::types::Type {
        use parquet::basic::Repetition;
        use parquet::schema::types::Type;
        Type::primitive_type_builder("t", PhysicalType::INT64)
            .with_logical_type(Some(LogicalType::Timestamp {
                unit: ParquetTimeUnit::MICROS,
                is_adjusted_to_u_t_c,
            }))
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap()
    }

    /// Metadata for a single-column file with one row group -- everything the guard consults.
    fn one_column_metadata(
        leaf: parquet::schema::types::Type,
        num_rows: i64,
        statistics: Option<Statistics>,
        footer: &[(&str, Option<&str>)],
    ) -> ParquetMetaData {
        use parquet::file::metadata::{
            ColumnChunkMetaData, FileMetaData, ParquetMetaDataBuilder, RowGroupMetaData,
        };
        use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor, Type};

        let root = Type::group_type_builder("spark_schema")
            .with_fields(vec![Arc::new(leaf)])
            .build()
            .unwrap();
        let descr: SchemaDescPtr = Arc::new(SchemaDescriptor::new(Arc::new(root)));
        let mut column = ColumnChunkMetaData::builder(descr.column(0));
        if let Some(statistics) = statistics {
            column = column.set_statistics(statistics);
        }
        let row_group = RowGroupMetaData::builder(Arc::clone(&descr))
            .set_num_rows(num_rows)
            .set_column_metadata(vec![column.build().unwrap()])
            .build()
            .unwrap();
        let file_metadata = FileMetaData::new(
            1,
            num_rows,
            None,
            Some(kv(footer)),
            Arc::clone(&descr),
            None,
        );
        ParquetMetaDataBuilder::new(file_metadata)
            .set_row_groups(vec![row_group])
            .build()
    }

    /// A guard for a scan that reads the single named column, at Spark's default read modes.
    fn guard_reading(name: &str, data_type: DataType) -> LegacyCalendarGuard {
        let schema = Schema::new(vec![Field::new(name, data_type, true)]);
        LegacyCalendarGuard::for_scan(true, &schema, true, DEFAULT_MODES).unwrap()
    }

    fn timestamp_guard() -> LegacyCalendarGuard {
        guard_reading("t", DataType::Timestamp(TimeUnit::Microsecond, None))
    }

    fn date_guard() -> LegacyCalendarGuard {
        guard_reading("d", DataType::Date32)
    }

    /// Statistics whose minimum is `min`, with no nulls.
    fn date_statistics(min: i32) -> Statistics {
        Statistics::int32(Some(min), Some(0), None, Some(0), false)
    }

    #[test]
    fn a_hive_written_int96_column_is_read() {
        // The regression this guard must not cause: Hive writes TIMESTAMP as INT96 and stamps no
        // Spark version, and INT96 statistics can never clear a column. Refusing on that
        // combination would make every Hive-written timestamp column in existence unreadable,
        // whatever its values, where Spark reads them without complaint.
        let metadata = one_column_metadata(int96_primitive(), 10, None, NON_SPARK_FOOTER);
        assert!(timestamp_guard().check(&metadata).is_ok());
    }

    #[test]
    fn an_int96_column_a_footer_marks_legacy_is_refused() {
        // Here the file itself says the values are hybrid-calendar, and nothing can narrow that to
        // the affected rows, so the read is refused.
        for footer in [
            &[
                (SPARK_VERSION_METADATA_KEY, Some("4.1.3")),
                (SPARK_LEGACY_INT96_METADATA_KEY, Some("")),
            ][..],
            // Spark 3.0 predates the INT96 switch.
            &[(SPARK_VERSION_METADATA_KEY, Some("3.0.3"))][..],
        ] {
            let metadata = one_column_metadata(int96_primitive(), 10, None, footer);
            assert!(timestamp_guard().check(&metadata).is_err());
        }
    }

    #[test]
    fn a_modern_spark_int96_column_is_read() {
        let metadata = one_column_metadata(
            int96_primitive(),
            10,
            None,
            &[(SPARK_VERSION_METADATA_KEY, Some("3.5.9"))],
        );
        assert!(timestamp_guard().check(&metadata).is_ok());
    }

    #[test]
    fn an_unknown_provenance_column_is_refused_only_on_proof() {
        // Statistics expose an ancient value: Spark under its EXCEPTION default raises here too.
        let ancient = one_column_metadata(
            date_primitive(),
            10,
            Some(date_statistics(LAST_SWITCH_JULIAN_DAY - 1)),
            NON_SPARK_FOOTER,
        );
        assert!(date_guard().check(&ancient).is_err());

        // Statistics prove the values are all at or after the switch day.
        let modern = one_column_metadata(
            date_primitive(),
            10,
            Some(date_statistics(LAST_SWITCH_JULIAN_DAY)),
            NON_SPARK_FOOTER,
        );
        assert!(date_guard().check(&modern).is_ok());

        // No statistics at all proves nothing either way, so the read goes ahead. Writers with
        // statistics disabled are common, and refusing them all is not worth the narrow class of
        // genuinely ancient values it would catch.
        let unknown = one_column_metadata(date_primitive(), 10, None, NON_SPARK_FOOTER);
        assert!(date_guard().check(&unknown).is_ok());
    }

    #[test]
    fn a_legacy_marked_column_is_refused_unless_statistics_clear_it() {
        let legacy_footer = &[
            (SPARK_VERSION_METADATA_KEY, Some("4.1.3")),
            (SPARK_LEGACY_DATETIME_METADATA_KEY, Some("")),
        ];

        // The case that makes a default-on guard tolerable: Spark stamps the marker on the whole
        // file whenever the write mode was LEGACY, and dates from 1582-10-15 on rebase to
        // themselves, so a legacy-marked file of modern dates still reads.
        let modern = one_column_metadata(
            date_primitive(),
            10,
            Some(date_statistics(LAST_SWITCH_JULIAN_DAY)),
            legacy_footer,
        );
        assert!(date_guard().check(&modern).is_ok());

        let ancient = one_column_metadata(
            date_primitive(),
            10,
            Some(date_statistics(LAST_SWITCH_JULIAN_DAY - 1)),
            legacy_footer,
        );
        assert!(date_guard().check(&ancient).is_err());

        // Unlike the unknown-provenance case, a missing bound here is assumed to be affected.
        let no_statistics = one_column_metadata(date_primitive(), 10, None, legacy_footer);
        assert!(date_guard().check(&no_statistics).is_err());
    }

    #[test]
    fn an_all_null_legacy_marked_row_group_is_read() {
        // No minimum, but the null count accounts for every row, so there is no value to rebase.
        let metadata = one_column_metadata(
            date_primitive(),
            10,
            Some(Statistics::int32(None, None, None, Some(10), false)),
            &[
                (SPARK_VERSION_METADATA_KEY, Some("4.1.3")),
                (SPARK_LEGACY_DATETIME_METADATA_KEY, Some("")),
            ],
        );
        assert!(date_guard().check(&metadata).is_ok());
    }

    #[test]
    fn a_calendar_sensitive_column_the_scan_does_not_read_is_ignored() {
        // A legacy-marked INT96 column, which would otherwise be refused outright, in a file the
        // scan only reads a string column out of.
        let metadata = one_column_metadata(
            int96_primitive(),
            10,
            None,
            &[
                (SPARK_VERSION_METADATA_KEY, Some("4.1.3")),
                (SPARK_LEGACY_INT96_METADATA_KEY, Some("")),
            ],
        );
        // The guard would not even arm for a string-only scan, so arm it on an unrelated date
        // column to prove the per-column check, not just `for_scan`, does the filtering.
        assert!(date_guard().check(&metadata).is_ok());
    }

    #[test]
    fn a_timestamp_ntz_column_is_never_calendar_sensitive() {
        // Spark stamps the legacy marker from the write-mode conf alone, without regard to whether
        // the schema holds a column the mode could apply to, and it never rebases NTZ in either
        // direction. So a legacy-marked file of ancient NTZ values reads back exactly as written,
        // and refusing it would fail a read Spark answers correctly.
        let ancient = Statistics::int64(
            Some(LAST_SWITCH_JULIAN_MICROS - 1),
            Some(0),
            None,
            Some(0),
            false,
        );
        let legacy_footer = &[
            (SPARK_VERSION_METADATA_KEY, Some("4.1.3")),
            (SPARK_LEGACY_DATETIME_METADATA_KEY, Some("")),
        ];

        let ntz = one_column_metadata(
            timestamp_primitive(false),
            10,
            Some(ancient.clone()),
            legacy_footer,
        );
        assert!(timestamp_guard().check(&ntz).is_ok());
        assert!(calendar_kind(&ntz.file_metadata().schema_descr().column(0)).is_none());

        // The UTC-adjusted counterpart, which Spark does rebase, is still refused. The two differ
        // only in `isAdjustedToUTC`, so this is what proves the exclusion is not too broad.
        let ltz = one_column_metadata(timestamp_primitive(true), 10, Some(ancient), legacy_footer);
        assert!(timestamp_guard().check(&ltz).is_err());
    }
}
