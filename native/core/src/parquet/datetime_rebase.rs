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

//! Per-file datetime calendar-rebase handling for the parquet scan.
//!
//! Spark 2.4 and earlier wrote dates and timestamps in the hybrid Julian + Gregorian calendar;
//! Spark 3.0+ uses the proleptic Gregorian calendar and records the calendar policy of every
//! file it writes in the parquet footer's key-value metadata (`org.apache.spark.version`,
//! `org.apache.spark.legacyDateTime`, `org.apache.spark.legacyINT96`,
//! `org.apache.spark.timeZone`). Spark's reader resolves the rebase policy from EACH FILE's
//! writer metadata (`DataSourceUtils.datetimeRebaseSpec` / `int96RebaseSpec`) -- the session's
//! `spark.sql.parquet.datetimeRebaseModeInRead` conf only applies to files whose metadata does
//! not decide the policy on its own -- so a reader that ignores the metadata silently returns
//! values shifted by up to ten days for dates before 1582-10-15 (e.g. `1500-01-01` reads as
//! `1500-01-10`).
//!
//! This module mirrors that per-file resolution: [`resolve_file_rebase_policies`] computes the
//! date / INT64-timestamp / INT96-timestamp policies from a file's arrow schema metadata (the
//! parquet key-value pairs survive the parquet -> arrow schema conversion), and
//! [`wrap_datetime_rebase`] wraps the per-file rewritten expressions' column references in a
//! [`SparkDatetimeRebaseExpr`] that rebases values exactly where that is possible without the
//! JVM's historical timezone tables (dates always; timestamps for a fixed UTC writer zone) and
//! refuses -- rather than silently corrupting -- ancient values it cannot rebase. Nested
//! columns are rebuilt leaf by leaf (struct / list / map / fixed-size list / dictionary), each
//! leaf under its own policy, with nulls and offsets preserved. Modern values are always the
//! identity under every policy: from 1582-10-15 onward for dates, and from
//! [`LAST_SWITCH_JULIAN_TS_SECONDS`] (1900-01-01T00:00:00Z, Spark's
//! `RebaseDateTime.lastSwitchJulianTs`) onward for timestamps.
//!
//! Spark applies `datetimeRebaseSpec` to INT64 `TIMESTAMP_MICROS` / `TIMESTAMP_MILLIS` columns
//! and `int96RebaseSpec` to INT96 columns. The two physical types are indistinguishable in the
//! arrow schema DataFusion hands the expression adapter (both surface as `Timestamp(us, "UTC")`
//! after INT96 coercion), so Comet's parquet reader factory stamps the file's INT96 leaf
//! ordinals -- taken from the parquet footer's own `SchemaDescriptor` -- into the key-value
//! metadata under [`INT96_LEAVES_METADATA_KEY`] before the arrow schema is derived (see
//! [`stamp_int96_leaves`] and `eager_page_index_reader_factory.rs`), and the adapter attributes
//! every timestamp leaf to its spec from that stamp. Without a stamp, the two specs are merged:
//! agreement decides, disagreement degrades to [`RebasePolicy::CheckAncient`].
//!
//! The wrapper sits BENEATH the schema adapter's nested narrowing (the struct -> struct convert
//! that keeps only the requested children), which is what keeps those ordinals physical -- but
//! it means the wrapper sees every physical child, requested or not. Spark only ever decodes
//! the requested nested schema, so [`FileRebasePolicies::restrict_to_requested`] marks the
//! physical leaves the narrowing drops as the identity: an unrequested ancient `s.ts` never
//! blocks `select s.d`, exactly as in Spark.
//!
//! Currently only enabled by the Delta scan arms via
//! `SparkParquetOptions::rebase_from_file_metadata`, which also carries the session read modes
//! ([`SessionRebaseModes`], forwarded from the JVM) that decide the policy for files without
//! Spark writer metadata; the plain NativeScan keeps its documented no-rebase behavior (see
//! the compatibility guide and issue #5010).

use std::collections::HashMap;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, FixedSizeListArray, GenericListArray, MapArray,
    OffsetSizeTrait, PrimitiveArray, RecordBatch, StructArray,
};
use arrow::datatypes::{
    ArrowTimestampType, DataType, Date32Type, FieldRef, Schema, SchemaRef, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow::error::ArrowError;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use parquet::basic::Type as ParquetPhysicalType;
use parquet::file::metadata::{FileMetaData, KeyValue, ParquetMetaData};
use parquet::schema::types::SchemaDescriptor;

use super::name_fold::fold_names;
use super::schema_adapter::parse_field_id;

/// Footer key naming the Spark release that wrote the file; absent for non-Spark writers.
const SPARK_VERSION_METADATA_KEY: &str = "org.apache.spark.version";
/// Present (empty value) when the file's dates and INT64 timestamps were written with
/// `spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY`.
const SPARK_LEGACY_DATETIME_KEY: &str = "org.apache.spark.legacyDateTime";
/// Present (empty value) when the file's INT96 timestamps were written with
/// `spark.sql.parquet.int96RebaseModeInWrite=LEGACY`.
const SPARK_LEGACY_INT96_KEY: &str = "org.apache.spark.legacyINT96";
/// The writer session's time zone, stamped alongside either legacy flag.
const SPARK_TIMEZONE_KEY: &str = "org.apache.spark.timeZone";

/// Key-value metadata entry Comet's parquet reader factory adds to a file's footer metadata
/// (in memory only, never written back) so the expression adapter can tell INT96 timestamp
/// columns from INT64 ones after both have been coerced to the same arrow type. Value:
/// `"<leaf count>:<comma-separated INT96 leaf ordinals>"`, where leaves are the file's
/// primitive columns in `SchemaDescriptor::columns()` order -- the same depth-first order
/// parquet-rs assigns arrow leaves, so an arrow-side depth-first walk lines up with it. The
/// leaf count lets the reader detect a stamp that does not describe the schema it is paired
/// with (see [`Int96Attribution::from_schema`]).
pub(crate) const INT96_LEAVES_METADATA_KEY: &str = "comet.int96_leaf_columns";

/// Day of the Gregorian cutover (1582-10-15) as days since the epoch; rebasing is the identity
/// from this day onward. Same value as Spark's `RebaseDateTime.lastSwitchJulianDay`.
const LAST_SWITCH_JULIAN_DAY: i32 = -141427;

/// Spark's `RebaseDateTime.lastSwitchJulianTs` (and `lastSwitchGregorianTs`) in seconds since
/// the epoch: 1900-01-01T00:00:00Z. Spark derives it as the latest switch instant across every
/// zone in its `julian-gregorian-rebase-micros.json` table (`getLastSwitchTs`, which also
/// asserts the calendars' difference is zero for every zone from then on): most zones ran on
/// local mean time before 1900, so the last instant at which rebasing changes a value in ANY
/// zone is 1900-01-01T00:00:00Z, not the 1582 cutover. `createTimestampRebaseFuncInRead`
/// under `EXCEPTION` throws exactly for `micros < lastSwitchJulianTs` (after converting
/// `TIMESTAMP_MILLIS` to micros), and `rebaseJulianToGregorianMicros` is the identity from it
/// onward in every zone. The value is in seconds so it scales exactly to any timestamp unit.
pub(crate) const LAST_SWITCH_JULIAN_TS_SECONDS: i64 = -2_208_988_800;

/// The per-century differences between the Julian and proleptic Gregorian calendars, and the
/// Julian-calendar switch days at which each difference starts to apply. Copied verbatim from
/// Spark's `RebaseDateTime.julianGregDiffs` / `julianGregDiffSwitchDay` (which Spark generated
/// from `localRebaseJulianToGregorianDays`); `rebase_julian_to_gregorian_days` must stay
/// value-for-value equal to Spark's `rebaseJulianToGregorianDays`.
const JULIAN_GREG_DIFFS: [i32; 14] = [2, 1, 0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, 0];
const JULIAN_GREG_DIFF_SWITCH_DAY: [i32; 14] = [
    -719164, -682945, -646420, -609895, -536845, -500320, -463795, -390745, -354220, -317695,
    -244645, -208120, -171595, -141427,
];

/// Proleptic-Gregorian days since 1970-01-01 for a nominal civil date, via Howard Hinnant's
/// `days_from_civil`. `d` may exceed the month's length; the excess rolls into the following
/// month exactly like `LocalDate.of(y, m, 1).plusDays(d - 1)` in Spark's
/// `localRebaseJulianToGregorianDays` (how the non-existent proleptic date `1000-02-29`,
/// valid in the Julian calendar, lands on `1000-03-01`).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400; // [0, 399]
    let mp = (m + 9) % 12; // [0, 11], March = 0
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Julian-calendar civil date `(year, month, day)` for a day count since 1970-01-01 that labels
/// days in the Julian calendar (astronomical year numbering: 1 BCE is year 0). Standard
/// Julian-day-number conversion (E.G. Richards' algorithm), exact for any day.
fn julian_day_to_civil(days: i64) -> (i64, i64, i64) {
    // Integer (noon) Julian Day Number of this civil day: 1970-01-01 is JDN 2440588.
    let jdn = days + 2_440_588;
    let f = jdn + 1401;
    let e = 4 * f + 3;
    let g = e.rem_euclid(1461) / 4;
    let h = 5 * g + 2;
    let day = h.rem_euclid(153) / 5 + 1;
    let month = (h / 153 + 2).rem_euclid(12) + 1;
    let year = e.div_euclid(1461) - 4716 + (14 - month) / 12;
    (year, month, day)
}

/// Exact port of Spark's `RebaseDateTime.rebaseJulianToGregorianDays`: reinterprets a day count
/// written in the hybrid Julian + Gregorian calendar as the proleptic Gregorian day count of the
/// same nominal civil date. Identity for days from 1582-10-15 onward. Days before the tables'
/// range (before Julian `0001-01-01`) take the calendar-arithmetic path, mirroring Spark's
/// `localRebaseJulianToGregorianDays` fallback.
pub(crate) fn rebase_julian_to_gregorian_days(days: i32) -> i32 {
    if days < JULIAN_GREG_DIFF_SWITCH_DAY[0] {
        let (y, m, d) = julian_day_to_civil(days as i64);
        (days_from_civil(y, m, 1) + (d - 1)) as i32
    } else {
        // Spark's rebaseDays: linear search from the most recent switch day.
        let mut i = JULIAN_GREG_DIFF_SWITCH_DAY.len();
        loop {
            i -= 1;
            if i == 0 || days >= JULIAN_GREG_DIFF_SWITCH_DAY[i] {
                break;
            }
        }
        days + JULIAN_GREG_DIFFS[i]
    }
}

/// Timezone strings from `org.apache.spark.timeZone` that denote a fixed zero-offset zone in
/// both `java.util.TimeZone` and `java.time`. Only for these is timestamp rebasing the pure
/// nominal-date shift [`SparkDatetimeRebaseExpr::rebase_timestamp_utc`] computes; any other (or
/// absent) zone needs the JVM's historical timezone tables and stays on the
/// refuse-ancient-values path.
const UTC_EQUIVALENT_TIMEZONES: [&str; 6] = ["UTC", "Etc/UTC", "GMT", "Etc/GMT", "Z", "+00:00"];

/// How the writer's session time zone (if recorded) affects timestamp rebasing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum WriterTimeZone {
    /// A fixed zero-offset zone: rebasing reduces to the exact nominal-date shift.
    Utc,
    /// Any other zone, or none recorded (pre-3.0 files): ancient values cannot be rebased
    /// without the JVM's historical timezone data.
    OtherOrUnknown,
}

/// One session-level datetime rebase read mode (a `LegacyBehaviorPolicy` value of
/// `spark.sql.parquet.datetimeRebaseModeInRead` / `int96RebaseModeInRead`), consulted by
/// [`resolve_file_rebase_policies`] ONLY for files whose footer metadata does not decide the
/// policy on its own -- exactly the `getOrElse` fallback in Spark's
/// `DataSourceUtils.getRebaseSpec`. Files that carry `org.apache.spark.version` ignore these
/// modes entirely, on every Spark version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub(crate) enum RebaseReadMode {
    /// Refuse ancient values (Spark raises `SparkUpgradeException`); maps to
    /// [`RebasePolicy::CheckAncient`]. The default mirrors the conservative posture used
    /// before the conf was plumbed through (and Spark 3.x's own conf default).
    #[default]
    Exception,
    /// Read values as proleptic Gregorian without rebasing.
    Corrected,
    /// Rebase from the hybrid Julian + Gregorian calendar.
    Legacy,
}

impl RebaseReadMode {
    /// Parses a `LegacyBehaviorPolicy` conf value. `SQLConf` validates and upper-cases the
    /// session conf, but a per-relation `datetimeRebaseMode` option arrives verbatim, so the
    /// match is case-insensitive. Anything unrecognized -- including the empty string a proto
    /// producer that predates the field sends -- falls back to [`RebaseReadMode::Exception`],
    /// which refuses ancient values rather than silently corrupting them.
    pub(crate) fn from_conf_value(value: &str) -> Self {
        match value.to_ascii_uppercase().as_str() {
            "CORRECTED" => RebaseReadMode::Corrected,
            "LEGACY" => RebaseReadMode::Legacy,
            _ => RebaseReadMode::Exception,
        }
    }
}

/// The session's effective datetime rebase read modes, one per spec class (INT64
/// dates/timestamps vs INT96 timestamps), forwarded from the JVM at planning time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub(crate) struct SessionRebaseModes {
    /// `spark.sql.parquet.datetimeRebaseModeInRead` (or the relation's `datetimeRebaseMode`).
    pub datetime: RebaseReadMode,
    /// `spark.sql.parquet.int96RebaseModeInRead` (or the relation's `int96RebaseMode`).
    pub int96: RebaseReadMode,
}

/// Calendar policy of one file's date or timestamp columns, resolved from writer metadata the
/// same way Spark's `DataSourceUtils.getRebaseSpec` resolves it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum RebasePolicy {
    /// Written in the proleptic Gregorian calendar; values pass through untouched.
    Corrected,
    /// Written in the hybrid Julian + Gregorian calendar; values must be rebased.
    Legacy(WriterTimeZone),
    /// Policy could not be pinned down (contradictory flags, or a non-Spark writer under the
    /// `EXCEPTION` read mode): modern values -- identical under either calendar -- pass,
    /// ancient values raise. Mirrors Spark's `EXCEPTION` behavior (`SparkUpgradeException`).
    CheckAncient,
}

/// Which of a file's leaf columns are physically INT96, from the stamp the parquet reader
/// factory adds under [`INT96_LEAVES_METADATA_KEY`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Int96Attribution {
    /// No stamp, or a stamp whose leaf count does not match the schema it arrived with: the
    /// INT64 and INT96 timestamp specs cannot be told apart per column and are merged.
    Unknown,
    /// Sorted leaf ordinals (depth-first over the file schema's primitive columns) that are
    /// INT96; every other timestamp leaf is INT64.
    Known(Vec<usize>),
}

impl Int96Attribution {
    /// Parses the stamp out of `schema`'s metadata and validates its leaf count against the
    /// schema's own depth-first leaf count, so a stamp that does not describe this schema (a
    /// crafted footer key, or a cached-metadata mismatch) degrades to [`Self::Unknown`].
    fn from_schema(schema: &Schema) -> Self {
        let Some(stamp) = schema.metadata().get(INT96_LEAVES_METADATA_KEY) else {
            return Int96Attribution::Unknown;
        };
        let Some((count, ordinals)) = stamp.split_once(':') else {
            return Int96Attribution::Unknown;
        };
        let schema_leaves: usize = schema
            .fields()
            .iter()
            .map(|f| leaf_count(f.data_type()))
            .sum();
        if count.parse::<usize>().ok() != Some(schema_leaves) {
            return Int96Attribution::Unknown;
        }
        let parsed: Option<Vec<usize>> = if ordinals.is_empty() {
            Some(Vec::new())
        } else {
            ordinals
                .split(',')
                .map(|o| o.parse::<usize>().ok().filter(|o| *o < schema_leaves))
                .collect()
        };
        match parsed {
            Some(mut leaves) => {
                leaves.sort_unstable();
                Int96Attribution::Known(leaves)
            }
            None => Int96Attribution::Unknown,
        }
    }

    /// `Some(true)` / `Some(false)` when the leaf is known to be INT96 / INT64, `None` when
    /// the attribution is unknown.
    fn is_int96(&self, leaf: usize) -> Option<bool> {
        match self {
            Int96Attribution::Unknown => None,
            Int96Attribution::Known(leaves) => Some(leaves.binary_search(&leaf).is_ok()),
        }
    }
}

/// The [`INT96_LEAVES_METADATA_KEY`] value describing `schema`: its leaf count and the
/// ordinals of its INT96 primitive columns.
pub(crate) fn int96_leaf_stamp(schema: &SchemaDescriptor) -> String {
    let ordinals: Vec<String> = schema
        .columns()
        .iter()
        .enumerate()
        .filter(|(_, column)| column.physical_type() == ParquetPhysicalType::INT96)
        .map(|(ordinal, _)| ordinal.to_string())
        .collect();
    format!("{}:{}", schema.num_columns(), ordinals.join(","))
}

/// Returns a copy of `metadata` whose key-value metadata carries the [`int96_leaf_stamp`] of
/// its own schema, or `None` when it already does (the common case after the first open of a
/// file, since the caller caches the stamped copy). Any pre-existing entry under the key --
/// a file cannot legitimately carry one -- is replaced, never trusted. Only the file-level
/// key-value list changes; row groups and page indexes are carried over as-is. The parquet
/// API cannot carry a file decryptor, nor `FileMetaData`'s crate-private encryption fields
/// (encryption algorithm, footer signing key metadata), across this rebuild, so callers must
/// not stamp opens that supply decryption properties -- and the only consumer, the Delta
/// scan, declines every encrypted-parquet configuration before planning, so a parquet
/// modular encryption file never reaches this path with or without those properties.
pub(crate) fn stamp_int96_leaves(metadata: &ParquetMetaData) -> Option<ParquetMetaData> {
    let file_metadata = metadata.file_metadata();
    let stamp = int96_leaf_stamp(file_metadata.schema_descr());
    let existing = file_metadata
        .key_value_metadata()
        .and_then(|kvs| kvs.iter().find(|kv| kv.key == INT96_LEAVES_METADATA_KEY))
        .and_then(|kv| kv.value.as_deref());
    if existing == Some(stamp.as_str()) {
        return None;
    }
    let mut key_values: Vec<KeyValue> = file_metadata
        .key_value_metadata()
        .map(|kvs| {
            kvs.iter()
                .filter(|kv| kv.key != INT96_LEAVES_METADATA_KEY)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    key_values.push(KeyValue::new(INT96_LEAVES_METADATA_KEY.to_string(), stamp));
    let stamped_file_metadata = FileMetaData::new(
        file_metadata.version(),
        file_metadata.num_rows(),
        file_metadata.created_by().map(str::to_string),
        Some(key_values),
        file_metadata.schema_descr_ptr(),
        file_metadata.column_orders().cloned(),
    );
    Some(
        ParquetMetaData::new(stamped_file_metadata, metadata.row_groups().to_vec())
            .into_builder()
            .set_column_index(metadata.column_index().cloned())
            .set_offset_index(metadata.offset_index().cloned())
            .build(),
    )
}

/// Per-file rebase policies for the three affected column classes, plus the INT96
/// attribution that selects between the two timestamp specs per leaf.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FileRebasePolicies {
    /// `DATE` columns, governed by `org.apache.spark.legacyDateTime` alone.
    pub date: RebasePolicy,
    /// INT64 `TIMESTAMP_MICROS` / `TIMESTAMP_MILLIS` columns: the datetime spec (same
    /// resolution as `date`), as Spark's `ParquetVectorUpdaterFactory` selects for INT64.
    pub int64_timestamp: RebasePolicy,
    /// INT96 columns: the INT96 spec (`org.apache.spark.legacyINT96`, min version 3.1.0).
    pub int96_timestamp: RebasePolicy,
    /// Which timestamp leaves are INT96. See [`Int96Attribution`].
    pub int96_leaves: Int96Attribution,
    /// Sorted depth-first leaf ordinals -- over the physical file schema, the same ordinals
    /// `int96_leaves` uses -- that the query does not read: nested children the schema
    /// adapter's struct narrowing drops before any value leaves the scan. Spark never decodes
    /// them either, so their policy is the identity whatever the file's calendar. Empty until
    /// [`Self::restrict_to_requested`] runs (every leaf requested).
    pub unrequested_leaves: Vec<usize>,
}

impl FileRebasePolicies {
    /// True when some policy is not the plain proleptic-Gregorian pass-through, i.e. when the
    /// per-column wrap in [`wrap_datetime_rebase`] can install anything at all.
    pub(crate) fn any_rebase_needed(&self) -> bool {
        self.date != RebasePolicy::Corrected
            || self.int64_timestamp != RebasePolicy::Corrected
            || self.int96_timestamp != RebasePolicy::Corrected
    }

    fn is_requested(&self, leaf: usize) -> bool {
        self.unrequested_leaves.binary_search(&leaf).is_err()
    }

    /// The policy of the `Date32` leaf at depth-first ordinal `leaf`: the file's date policy,
    /// or the identity when the query does not read that leaf.
    fn date_policy(&self, leaf: usize) -> RebasePolicy {
        if self.is_requested(leaf) {
            self.date
        } else {
            RebasePolicy::Corrected
        }
    }

    /// The policy of the timezone-carrying timestamp leaf at depth-first ordinal `leaf`: the
    /// identity when the query does not read it; otherwise its physical type's spec when the
    /// attribution is known, or else the two specs merged -- agreement decides, disagreement
    /// degrades to [`RebasePolicy::CheckAncient`], which still passes every modern value and
    /// refuses only ancient ones.
    fn timestamp_policy(&self, leaf: usize) -> RebasePolicy {
        if !self.is_requested(leaf) {
            return RebasePolicy::Corrected;
        }
        match self.int96_leaves.is_int96(leaf) {
            Some(true) => self.int96_timestamp,
            Some(false) => self.int64_timestamp,
            None if self.int64_timestamp == self.int96_timestamp => self.int64_timestamp,
            None => RebasePolicy::CheckAncient,
        }
    }

    /// These policies with every physical leaf the query does not read marked the identity.
    /// `requested` pairs each top-level field of `physical_schema` (by position) with the type
    /// of the logical field the schema adapter narrows it to -- `None` for a column without a
    /// logical counterpart, whose leaves are left as they are (no expression reads it anyway).
    /// Nested children pair the way the adapter's struct convert selects them (see
    /// [`push_unrequested_leaves`]); the INT96 attribution is untouched, since the ordinals
    /// stay physical. `requested` is parallel to the schema's fields; should a caller pass a
    /// shorter slice, the trailing columns simply keep every leaf (the safe direction).
    pub(crate) fn restrict_to_requested(
        mut self,
        physical_schema: &Schema,
        requested: &[Option<&DataType>],
        case_sensitive: bool,
        use_field_id: bool,
    ) -> Self {
        debug_assert_eq!(requested.len(), physical_schema.fields().len());
        let matching = FieldMatching {
            case_sensitive,
            use_field_id,
        };
        let mut next_leaf = 0;
        let mut unrequested = Vec::new();
        for (field, requested) in physical_schema.fields().iter().zip(requested) {
            match requested {
                Some(logical) => push_unrequested_leaves(
                    field.data_type(),
                    logical,
                    &mut next_leaf,
                    matching,
                    &mut unrequested,
                ),
                None => next_leaf += leaf_count(field.data_type()),
            }
        }
        // Emitted in depth-first order, so already sorted for `is_requested`'s binary search.
        self.unrequested_leaves = unrequested;
        self
    }
}

/// The field-matching rules of the schema adapter's nested narrowing
/// (`parquet_convert_struct_to_struct`): names fold per `case_sensitive`, and Parquet field ids
/// select fields when `use_field_id` is set.
#[derive(Debug, Clone, Copy)]
struct FieldMatching {
    case_sensitive: bool,
    use_field_id: bool,
}

/// Appends to `out` the depth-first leaf ordinals of `physical` (counting from `next_leaf`,
/// which advances past every leaf of `physical`) that reading it as `requested` drops.
///
/// Recurses through exactly the pairings `parquet_convert_array` narrows, and no others: a
/// struct child is dropped only when NO requested child selects it by either rule the struct
/// convert uses -- folded name, or Parquet field id when ids are in play -- and an ambiguous
/// child (several requested children select it) is kept; `List` pairs with `List` by element
/// type, and `Map` with a `Map` of the same key ordering by its entries, positionally. Any
/// other pairing -- a leaf, a `LargeList` / `FixedSizeList` / dictionary, a map whose ordering
/// differs, or a shape mismatch -- is handed to arrow's cast or passed through whole by the
/// convert, so it keeps every leaf. Keeping a superset of what the narrowing reads is always
/// safe (a spurious check at worst); dropping a leaf the narrowing reads would skip its
/// rebase, so every doubt resolves to "requested".
fn push_unrequested_leaves(
    physical: &DataType,
    requested: &DataType,
    next_leaf: &mut usize,
    matching: FieldMatching,
    out: &mut Vec<usize>,
) {
    match (physical, requested) {
        (DataType::Struct(physical_fields), DataType::Struct(requested_fields)) => {
            let names: Vec<&str> = physical_fields
                .iter()
                .chain(requested_fields.iter())
                .map(|f| f.name().as_str())
                .collect();
            let folded = fold_names(&names, matching.case_sensitive);
            let (physical_folded, requested_folded) = folded.split_at(physical_fields.len());
            for (i, child) in physical_fields.iter().enumerate() {
                let child_id = if matching.use_field_id {
                    parse_field_id(child)
                } else {
                    None
                };
                let mut selectors = requested_fields.iter().enumerate().filter(|(j, r)| {
                    requested_folded[*j] == physical_folded[i]
                        || (child_id.is_some() && parse_field_id(r) == child_id)
                });
                match (selectors.next(), selectors.next()) {
                    (None, _) => {
                        let n = leaf_count(child.data_type());
                        out.extend(*next_leaf..*next_leaf + n);
                        *next_leaf += n;
                    }
                    (Some((_, requested_child)), None) => push_unrequested_leaves(
                        child.data_type(),
                        requested_child.data_type(),
                        next_leaf,
                        matching,
                        out,
                    ),
                    (Some(_), Some(_)) => *next_leaf += leaf_count(child.data_type()),
                }
            }
        }
        (DataType::List(physical_item), DataType::List(requested_item)) => push_unrequested_leaves(
            physical_item.data_type(),
            requested_item.data_type(),
            next_leaf,
            matching,
            out,
        ),
        (
            DataType::Map(physical_entries, physical_sorted),
            DataType::Map(requested_entries, requested_sorted),
        ) if physical_sorted == requested_sorted => {
            match (physical_entries.data_type(), requested_entries.data_type()) {
                (DataType::Struct(physical_kv), DataType::Struct(requested_kv))
                    if physical_kv.len() == requested_kv.len() =>
                {
                    for (p, r) in physical_kv.iter().zip(requested_kv.iter()) {
                        push_unrequested_leaves(
                            p.data_type(),
                            r.data_type(),
                            next_leaf,
                            matching,
                            out,
                        );
                    }
                }
                _ => *next_leaf += leaf_count(physical),
            }
        }
        _ => *next_leaf += leaf_count(physical),
    }
}

/// The writer time zone recorded in `metadata`, classified for timestamp rebasing. Mirrors the
/// `Option(lookupFileMeta(SPARK_TIMEZONE_METADATA_KEY))` lookup Spark's `getRebaseSpec` performs
/// for every LEGACY resolution, conf-fallback included; Spark substitutes the JVM default zone
/// when the key is absent (`RebaseSpec.timeZone`), which is unavailable natively, so an absent or
/// non-UTC zone classifies as [`WriterTimeZone::OtherOrUnknown`] (dates still rebase fully --
/// the day rebase is zone-free -- while ancient timestamps refuse rather than guess).
fn writer_time_zone(metadata: &HashMap<String, String>) -> WriterTimeZone {
    match metadata.get(SPARK_TIMEZONE_KEY) {
        Some(tz) if UTC_EQUIVALENT_TIMEZONES.contains(&tz.as_str()) => WriterTimeZone::Utc,
        _ => WriterTimeZone::OtherOrUnknown,
    }
}

/// One spec resolution, mirroring Spark's `DataSourceUtils.getRebaseSpec` exactly: a Spark
/// version below `min_version` (lexicographic comparison, same as the Scala `String.<`) or a
/// present legacy flag means LEGACY; a Spark version at/after `min_version` without the flag
/// means CORRECTED; no Spark version at all falls back to `conf_mode`, the session read conf
/// forwarded from the JVM (`getRebaseSpec`'s `modeByConfig` fallback, its ONLY use of the
/// conf): CORRECTED passes values through, LEGACY rebases (with the writer zone from the
/// file's `org.apache.spark.timeZone` key, same lookup as the metadata-driven LEGACY path),
/// and EXCEPTION refuses ancient values as [`RebasePolicy::CheckAncient`].
fn resolve_spec(
    metadata: &HashMap<String, String>,
    min_version: &str,
    legacy_key: &str,
    conf_mode: RebaseReadMode,
) -> RebasePolicy {
    match metadata.get(SPARK_VERSION_METADATA_KEY) {
        None => match conf_mode {
            RebaseReadMode::Corrected => RebasePolicy::Corrected,
            RebaseReadMode::Legacy => RebasePolicy::Legacy(writer_time_zone(metadata)),
            RebaseReadMode::Exception => RebasePolicy::CheckAncient,
        },
        Some(version) => {
            if version.as_str() < min_version || metadata.contains_key(legacy_key) {
                RebasePolicy::Legacy(writer_time_zone(metadata))
            } else {
                RebasePolicy::Corrected
            }
        }
    }
}

/// Resolves the per-file rebase policies from a file's arrow schema: the parquet footer's
/// key-value pairs in its metadata decide the specs (the datetime spec uses min version
/// `3.0.0` and the INT96 spec `3.1.0`, matching `DataSourceUtils.datetimeRebaseSpec` /
/// `int96RebaseSpec`; `session_modes` supplies the per-spec conf fallback for files without
/// Spark writer metadata), and the reader factory's INT96 stamp -- validated against the
/// schema's leaf structure -- attributes each timestamp leaf to its spec.
pub(crate) fn resolve_file_rebase_policies(
    physical_file_schema: &Schema,
    session_modes: SessionRebaseModes,
) -> FileRebasePolicies {
    let metadata = physical_file_schema.metadata();
    let datetime_spec = resolve_spec(
        metadata,
        "3.0.0",
        SPARK_LEGACY_DATETIME_KEY,
        session_modes.datetime,
    );
    let int96_spec = resolve_spec(
        metadata,
        "3.1.0",
        SPARK_LEGACY_INT96_KEY,
        session_modes.int96,
    );
    FileRebasePolicies {
        date: datetime_spec,
        int64_timestamp: datetime_spec,
        int96_timestamp: int96_spec,
        int96_leaves: Int96Attribution::from_schema(physical_file_schema),
        unrequested_leaves: Vec::new(),
    }
}

/// Number of primitive leaves `dt` contains in a depth-first walk -- the same count and order
/// parquet-rs uses when it maps the file's `SchemaDescriptor` columns onto the arrow schema, so
/// arrow-side leaf ordinals line up with [`int96_leaf_stamp`]'s.
fn leaf_count(dt: &DataType) -> usize {
    match dt {
        DataType::Struct(fields) => fields.iter().map(|f| leaf_count(f.data_type())).sum(),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::Map(f, _) => leaf_count(f.data_type()),
        DataType::Dictionary(_, value) => leaf_count(value),
        DataType::RunEndEncoded(_, value) => leaf_count(value.data_type()),
        DataType::Union(fields, _) => fields.iter().map(|(_, f)| leaf_count(f.data_type())).sum(),
        _ => 1,
    }
}

/// Appends the policy of every leaf of `dt`, in depth-first order, to `out`, consuming leaf
/// ordinals from `next_leaf` (exactly [`leaf_count`] of them). Only `Date32` and
/// timezone-carrying timestamps have a policy to apply, and only when the query reads the
/// leaf; timezone-free timestamps are `TIMESTAMP_NTZ`, which Spark never rebases, and every
/// other leaf is the identity ([`RebasePolicy::Corrected`]).
fn leaf_policies(
    dt: &DataType,
    next_leaf: &mut usize,
    policies: &FileRebasePolicies,
    out: &mut Vec<RebasePolicy>,
) {
    match dt {
        DataType::Date32 => {
            out.push(policies.date_policy(*next_leaf));
            *next_leaf += 1;
        }
        DataType::Timestamp(_, Some(_)) => {
            out.push(policies.timestamp_policy(*next_leaf));
            *next_leaf += 1;
        }
        DataType::Struct(fields) => {
            for f in fields {
                leaf_policies(f.data_type(), next_leaf, policies, out);
            }
        }
        // Mirrors `leaf_count` variant for variant, so a rebase-affected leaf inside a nested
        // type `rebase_array` cannot rebuild (views, run-end, union -- never produced from a
        // parquet schema) still gets its real policy and makes `rebase_array` refuse loudly
        // instead of being stamped the identity.
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::Map(f, _) => leaf_policies(f.data_type(), next_leaf, policies, out),
        DataType::Dictionary(_, value) => leaf_policies(value, next_leaf, policies, out),
        DataType::RunEndEncoded(_, value) => {
            leaf_policies(value.data_type(), next_leaf, policies, out)
        }
        DataType::Union(fields, _) => {
            for (_, f) in fields.iter() {
                leaf_policies(f.data_type(), next_leaf, policies, out);
            }
        }
        _ => {
            *next_leaf += 1;
            out.push(RebasePolicy::Corrected);
        }
    }
}

/// Wraps every column reference in `expr` whose physical file type contains a rebase-affected
/// leaf under a policy that needs handling with a [`SparkDatetimeRebaseExpr`] carrying that
/// column's per-leaf policies, so both the per-file projection and the pushed-down predicate
/// evaluate rebased values. Columns whose leaves are all the identity -- unaffected types,
/// affected types under [`RebasePolicy::Corrected`], or leaves the query does not read (see
/// [`FileRebasePolicies::restrict_to_requested`]) -- pass through unwrapped. (The pruning
/// predicates derived from the wrapped predicate treat the wrapper as an opaque expression and
/// skip pruning on those columns -- conservative, since file-level statistics are in the
/// file's own calendar.)
pub(crate) fn wrap_datetime_rebase(
    expr: Arc<dyn PhysicalExpr>,
    physical_schema: &SchemaRef,
    policies: &FileRebasePolicies,
) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
    expr.transform(|e| {
        let Some(col) = e.downcast_ref::<Column>() else {
            return Ok(Transformed::no(e));
        };
        // Missing columns were already replaced with literals; any surviving reference is
        // physical-schema-indexed. Out-of-range means a non-file column (defensive): skip.
        let Some(field) = physical_schema.fields().get(col.index()) else {
            return Ok(Transformed::no(e));
        };
        // This column's first leaf ordinal: the leaves of every preceding top-level field.
        let mut next_leaf: usize = physical_schema.fields()[..col.index()]
            .iter()
            .map(|f| leaf_count(f.data_type()))
            .sum();
        let mut column_leaf_policies = Vec::with_capacity(leaf_count(field.data_type()));
        leaf_policies(
            field.data_type(),
            &mut next_leaf,
            policies,
            &mut column_leaf_policies,
        );
        if column_leaf_policies
            .iter()
            .all(|p| *p == RebasePolicy::Corrected)
        {
            return Ok(Transformed::no(e));
        }
        Ok(Transformed::yes(Arc::new(SparkDatetimeRebaseExpr {
            child: e,
            field: Arc::clone(field),
            leaf_policies: column_leaf_policies,
        }) as Arc<dyn PhysicalExpr>))
    })
    .map(|t| t.data)
}

/// Applies a file's calendar-rebase policies to one column: rebases exactly where possible,
/// raises on ancient values it cannot rebase, and passes modern values (the identity under
/// every policy) through untouched. Nested columns are rebuilt leaf by leaf with nulls and
/// offsets preserved. See the module doc for the policy table.
#[derive(Debug, Eq)]
struct SparkDatetimeRebaseExpr {
    child: Arc<dyn PhysicalExpr>,
    /// The physical file field this expression reads (type preserved by the rebase).
    field: FieldRef,
    /// One policy per primitive leaf of `field`'s type, in depth-first order (a single entry
    /// for a flat column). At least one is not [`RebasePolicy::Corrected`].
    leaf_policies: Vec<RebasePolicy>,
}

impl SparkDatetimeRebaseExpr {
    /// The refusal error, as an [`ArrowError`] so `try_unary` closures can raise it directly;
    /// it converts into a `DataFusionError` at the `?` in `evaluate`.
    fn rebase_error(&self, detail: &str) -> ArrowError {
        ArrowError::ComputeError(format!(
            "Native scan cannot rebase ancient values in column '{}': the file was written \
             with the legacy (hybrid Julian/Gregorian) calendar, or does not declare which \
             calendar it used, and {detail}. Reading it natively would return silently \
             shifted values; disable the native Delta scan \
             (spark.comet.scan.delta.enabled=false) to let Spark read this table",
            self.field.name(),
        ))
    }

    fn internal_error(&self, detail: impl Display) -> DataFusionError {
        DataFusionError::Internal(format!(
            "SparkDatetimeRebaseExpr on column '{}': {detail}",
            self.field.name()
        ))
    }

    /// Rebases a timestamp column written at a fixed zero-offset zone: shift the nominal day
    /// with the exact date table, keep the time of day. Matches Spark's
    /// `rebaseJulianToGregorianMicros` for UTC, where the hybrid calendar's day boundaries sit
    /// exactly on multiples of a day and no timezone transition can apply (UTC's last switch
    /// instant in Spark's rebase table is the 1582-10-15 cutover itself).
    fn rebase_timestamp_utc(&self, v: i64, units_per_day: i64) -> Result<i64, ArrowError> {
        // Compare in days, not units: the cutover day times a nanosecond day does not fit i64.
        let day = v.div_euclid(units_per_day);
        if day >= LAST_SWITCH_JULIAN_DAY as i64 {
            return Ok(v);
        }
        let time_of_day = v - day * units_per_day;
        let day = i32::try_from(day).map_err(|_| {
            self.rebase_error("the value is outside the rebaseable timestamp range")
        })?;
        let rebased = rebase_julian_to_gregorian_days(day) as i64;
        rebased
            .checked_mul(units_per_day)
            .and_then(|d| d.checked_add(time_of_day))
            .ok_or_else(|| self.rebase_error("the rebased value overflows the timestamp range"))
    }

    /// The refuse-ancient-values policy for timestamps: values from
    /// [`LAST_SWITCH_JULIAN_TS_SECONDS`] onward are identical under both calendars in every
    /// zone (Spark's `createTimestampRebaseFuncInRead` under `EXCEPTION` accepts exactly
    /// these, and `rebaseJulianToGregorianMicros` is the identity on them for any zone);
    /// older values raise.
    fn check_ancient_timestamp(
        &self,
        v: i64,
        units_per_second: i64,
        detail: &str,
    ) -> Result<i64, ArrowError> {
        if v >= LAST_SWITCH_JULIAN_TS_SECONDS * units_per_second {
            Ok(v)
        } else {
            Err(self.rebase_error(detail))
        }
    }

    fn rebase_timestamp_array<T: ArrowTimestampType>(
        &self,
        array: &PrimitiveArray<T>,
        policy: RebasePolicy,
        units_per_second: i64,
    ) -> DataFusionResult<ArrayRef> {
        let tz = array.timezone().map(Arc::<str>::from);
        let rebased: PrimitiveArray<T> = match policy {
            RebasePolicy::Corrected => return Ok(Arc::new(array.clone())),
            RebasePolicy::Legacy(WriterTimeZone::Utc) => arrow::compute::try_unary(array, |v| {
                self.rebase_timestamp_utc(v, units_per_second * 86_400)
            })?,
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown) => {
                arrow::compute::try_unary(array, |v| {
                    self.check_ancient_timestamp(
                        v,
                        units_per_second,
                        "rebasing timestamps outside a fixed UTC writer zone needs the JVM's \
                         historical timezone tables, which are unavailable natively",
                    )
                })?
            }
            RebasePolicy::CheckAncient => arrow::compute::try_unary(array, |v| {
                self.check_ancient_timestamp(
                    v,
                    units_per_second,
                    "the timestamp's calendar cannot be determined from the file's metadata",
                )
            })?,
        };
        Ok(Arc::new(rebased.with_timezone_opt(tz)))
    }

    fn rebase_date_array(
        &self,
        dates: &Date32Array,
        policy: RebasePolicy,
    ) -> DataFusionResult<ArrayRef> {
        let rebased: Date32Array = match policy {
            RebasePolicy::Corrected => return Ok(Arc::new(dates.clone())),
            // The day rebase is a pure calendar reinterpretation, independent of any timezone,
            // so every legacy writer zone rebases dates exactly.
            RebasePolicy::Legacy(_) => arrow::compute::unary::<Date32Type, _, Date32Type>(
                dates,
                rebase_julian_to_gregorian_days,
            ),
            RebasePolicy::CheckAncient => {
                arrow::compute::try_unary(dates, |v| -> Result<i32, ArrowError> {
                    if v >= LAST_SWITCH_JULIAN_DAY {
                        Ok(v)
                    } else {
                        Err(self.rebase_error(
                            "the date's calendar cannot be determined from the file's metadata",
                        ))
                    }
                })?
            }
        };
        Ok(Arc::new(rebased))
    }

    fn rebase_list<O: OffsetSizeTrait>(
        &self,
        list: &GenericListArray<O>,
        field: &FieldRef,
        cursor: &mut usize,
    ) -> DataFusionResult<ArrayRef> {
        let values = self.rebase_array(list.values(), cursor)?;
        Ok(Arc::new(GenericListArray::<O>::try_new(
            Arc::clone(field),
            list.offsets().clone(),
            values,
            list.nulls().cloned(),
        )?))
    }

    /// Applies the leaf policies starting at `cursor` (advanced past every leaf of `array`'s
    /// type) to `array`, rebuilding nested arrays around their transformed leaves. Subtrees
    /// whose leaves are all the identity are returned as-is without a rebuild.
    fn rebase_array(&self, array: &ArrayRef, cursor: &mut usize) -> DataFusionResult<ArrayRef> {
        let dt = array.data_type();
        let n = leaf_count(dt);
        let span = self
            .leaf_policies
            .get(*cursor..*cursor + n)
            .ok_or_else(|| {
                self.internal_error(format!(
                    "array of type {dt} does not match the planned leaf layout (leaf {cursor} \
                     of {})",
                    self.leaf_policies.len()
                ))
            })?;
        if span.iter().all(|p| *p == RebasePolicy::Corrected) {
            *cursor += n;
            return Ok(Arc::clone(array));
        }
        match dt {
            DataType::Date32 => {
                let policy = span[0];
                *cursor += 1;
                self.rebase_date_array(array.as_primitive::<Date32Type>(), policy)
            }
            DataType::Timestamp(unit, _) => {
                let policy = span[0];
                *cursor += 1;
                match unit {
                    TimeUnit::Second => self.rebase_timestamp_array(
                        array.as_primitive::<TimestampSecondType>(),
                        policy,
                        1,
                    ),
                    TimeUnit::Millisecond => self.rebase_timestamp_array(
                        array.as_primitive::<TimestampMillisecondType>(),
                        policy,
                        1_000,
                    ),
                    TimeUnit::Microsecond => self.rebase_timestamp_array(
                        array.as_primitive::<TimestampMicrosecondType>(),
                        policy,
                        1_000_000,
                    ),
                    TimeUnit::Nanosecond => self.rebase_timestamp_array(
                        array.as_primitive::<TimestampNanosecondType>(),
                        policy,
                        1_000_000_000,
                    ),
                }
            }
            DataType::Struct(fields) => {
                let structs = array.as_struct();
                let columns = structs
                    .columns()
                    .iter()
                    .map(|c| self.rebase_array(c, cursor))
                    .collect::<DataFusionResult<Vec<_>>>()?;
                Ok(Arc::new(StructArray::try_new(
                    fields.clone(),
                    columns,
                    structs.nulls().cloned(),
                )?))
            }
            DataType::List(field) => self.rebase_list(array.as_list::<i32>(), field, cursor),
            DataType::LargeList(field) => self.rebase_list(array.as_list::<i64>(), field, cursor),
            DataType::FixedSizeList(field, size) => {
                let list = array.as_fixed_size_list();
                let values = self.rebase_array(list.values(), cursor)?;
                Ok(Arc::new(FixedSizeListArray::try_new(
                    Arc::clone(field),
                    *size,
                    values,
                    list.nulls().cloned(),
                )?))
            }
            DataType::Map(field, ordered) => {
                let map = array.as_map();
                let entries: ArrayRef = Arc::new(map.entries().clone());
                let entries = self.rebase_array(&entries, cursor)?;
                Ok(Arc::new(MapArray::try_new(
                    Arc::clone(field),
                    map.offsets().clone(),
                    entries.as_struct().clone(),
                    map.nulls().cloned(),
                    *ordered,
                )?))
            }
            DataType::Dictionary(_, _) => {
                let dictionary = array.as_any_dictionary();
                let values = self.rebase_array(dictionary.values(), cursor)?;
                Ok(dictionary.with_values(values))
            }
            other => Err(self.internal_error(format!(
                "cannot rebase values inside unsupported type {other}"
            ))),
        }
    }
}

impl PartialEq for SparkDatetimeRebaseExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.field.eq(&other.field)
            && self.leaf_policies == other.leaf_policies
    }
}

impl Hash for SparkDatetimeRebaseExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.field.hash(state);
        self.leaf_policies.hash(state);
    }
}

impl Display for SparkDatetimeRebaseExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SPARK_DATETIME_REBASE({})", self.field.name())
    }
}

impl PhysicalExpr for SparkDatetimeRebaseExpr {
    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(self.field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let array = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        let mut cursor = 0;
        let rebased = self.rebase_array(&array, &mut cursor)?;
        if cursor != self.leaf_policies.len() {
            return Err(self.internal_error(format!(
                "array of type {} consumed {cursor} of {} planned leaves",
                array.data_type(),
                self.leaf_policies.len()
            )));
        }
        Ok(ColumnarValue::Array(rebased))
    }

    fn return_field(&self, _input_schema: &Schema) -> DataFusionResult<FieldRef> {
        Ok(Arc::clone(&self.field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(SparkDatetimeRebaseExpr {
            child: children.pop().expect("child"),
            field: Arc::clone(&self.field),
            leaf_policies: self.leaf_policies.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, ListArray, TimestampMicrosecondArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Field;
    use parquet::schema::parser::parse_message_type;

    /// Julian-calendar civil date -> hybrid day count (the number a legacy writer stores for
    /// that nominal date), the inverse of `julian_day_to_civil`. Fliegel-Van Flandern style
    /// Julian-calendar JDN formula, exact with euclidean division.
    fn julian_civil_to_day(y: i64, m: i64, d: i64) -> i32 {
        let a = (14 - m).div_euclid(12);
        let y2 = y + 4800 - a;
        let m2 = m + 12 * a - 3;
        let jdn = d + (153 * m2 + 2).div_euclid(5) + 365 * y2 + y2.div_euclid(4) - 32083;
        (jdn - 2_440_588) as i32
    }

    #[test]
    fn day_rebase_matches_spark_table_anchors() {
        // Julian 0001-01-01 is hybrid day -719164 and proleptic Gregorian 0001-01-01 is day
        // -719162 -- the first entry (+2) of Spark's julianGregDiffs table.
        assert_eq!(julian_civil_to_day(1, 1, 1), -719164);
        assert_eq!(rebase_julian_to_gregorian_days(-719164), -719162);
        // Spark's doc example: Julian 1582-01-01 (-141704) rebases to proleptic -141714.
        assert_eq!(julian_civil_to_day(1582, 1, 1), -141704);
        assert_eq!(rebase_julian_to_gregorian_days(-141704), -141714);
        // The last Julian day (1582-10-04) shifts by the full -10; the first Gregorian day
        // (1582-10-15, day -141427) and everything after is the identity.
        assert_eq!(julian_civil_to_day(1582, 10, 4), -141428);
        assert_eq!(rebase_julian_to_gregorian_days(-141428), -141438);
        assert_eq!(rebase_julian_to_gregorian_days(-141427), -141427);
        assert_eq!(rebase_julian_to_gregorian_days(0), 0);
        assert_eq!(rebase_julian_to_gregorian_days(19876), 19876);
    }

    #[test]
    fn day_rebase_handles_the_maintainer_repro_date() {
        // A legacy writer stores proleptic 1500-01-01 as the hybrid day labeled Julian
        // 1500-01-01 (numerically the proleptic day of 1500-01-10); reading without rebasing
        // shows 1500-01-10. Rebasing must restore proleptic 1500-01-01.
        let stored = julian_civil_to_day(1500, 1, 1);
        assert_eq!(stored, days_from_civil(1500, 1, 10) as i32);
        assert_eq!(
            rebase_julian_to_gregorian_days(stored),
            days_from_civil(1500, 1, 1) as i32
        );
    }

    #[test]
    fn day_rebase_rolls_julian_only_leap_days_forward() {
        // 1500 is a Julian leap year but not a Gregorian one: Julian 1500-02-29 lands on
        // proleptic 1500-03-01, mirroring Spark's LocalDate.of(y, m, 1).plusDays trick.
        let stored = julian_civil_to_day(1500, 2, 29);
        assert_eq!(
            rebase_julian_to_gregorian_days(stored),
            days_from_civil(1500, 3, 1) as i32
        );
    }

    #[test]
    fn day_rebase_falls_back_to_calendar_arithmetic_before_common_era() {
        // One day before the table's range: Julian 0000-12-31 -> proleptic 0000-12-31, which
        // is days_from_civil(1,1,1) - 1.
        let day = julian_civil_to_day(1, 1, 1) - 1;
        assert!(day < JULIAN_GREG_DIFF_SWITCH_DAY[0]);
        assert_eq!(
            rebase_julian_to_gregorian_days(day),
            days_from_civil(1, 1, 1) as i32 - 1
        );
    }

    #[test]
    fn day_rebase_is_continuous_across_every_table_switch() {
        // At each switch day the table's diff takes over from the previous interval; both must
        // agree with the calendar-arithmetic ground truth. The hybrid calendar labels days in
        // Julian only BEFORE the 1582-10-15 cutover; from the cutover onward it is Gregorian
        // and rebasing is the identity.
        for &switch in &JULIAN_GREG_DIFF_SWITCH_DAY {
            for day in [switch - 1, switch, switch + 1] {
                let expected = if day >= LAST_SWITCH_JULIAN_DAY {
                    day
                } else {
                    let (y, m, d) = julian_day_to_civil(day as i64);
                    (days_from_civil(y, m, 1) + (d - 1)) as i32
                };
                assert_eq!(
                    rebase_julian_to_gregorian_days(day),
                    expected,
                    "mismatch at hybrid day {day}"
                );
            }
        }
    }

    fn spark_metadata(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// A one-column (`Date32`) schema carrying `entries` as its metadata, for spec-resolution
    /// tests that only care about the footer key-value pairs.
    fn schema_with(entries: &[(&str, &str)]) -> Schema {
        Schema::new_with_metadata(
            vec![Field::new("d", DataType::Date32, true)],
            spark_metadata(entries),
        )
    }

    /// The [`SessionRebaseModes`] used by tests that exercise metadata-driven resolution: the
    /// default (EXCEPTION, EXCEPTION), matching an unplumbed conf.
    fn default_modes() -> SessionRebaseModes {
        SessionRebaseModes::default()
    }

    fn modes(datetime: RebaseReadMode, int96: RebaseReadMode) -> SessionRebaseModes {
        SessionRebaseModes { datetime, int96 }
    }

    fn flat_policies(
        date: RebasePolicy,
        int64_timestamp: RebasePolicy,
        int96_timestamp: RebasePolicy,
    ) -> FileRebasePolicies {
        FileRebasePolicies {
            date,
            int64_timestamp,
            int96_timestamp,
            int96_leaves: Int96Attribution::Unknown,
            unrequested_leaves: Vec::new(),
        }
    }

    #[test]
    fn policies_for_modern_spark_file_without_flags_are_corrected() {
        let schema = schema_with(&[(SPARK_VERSION_METADATA_KEY, "3.5.9")]);
        let policies = resolve_file_rebase_policies(&schema, default_modes());
        assert_eq!(policies.date, RebasePolicy::Corrected);
        assert_eq!(policies.int64_timestamp, RebasePolicy::Corrected);
        assert_eq!(policies.int96_timestamp, RebasePolicy::Corrected);
        assert!(!policies.any_rebase_needed());
    }

    #[test]
    fn policies_for_both_legacy_flags_with_utc_zone_are_legacy_utc() {
        let schema = schema_with(&[
            (SPARK_VERSION_METADATA_KEY, "3.5.9"),
            (SPARK_LEGACY_DATETIME_KEY, ""),
            (SPARK_LEGACY_INT96_KEY, ""),
            (SPARK_TIMEZONE_KEY, "UTC"),
        ]);
        let policies = resolve_file_rebase_policies(&schema, default_modes());
        assert_eq!(policies.date, RebasePolicy::Legacy(WriterTimeZone::Utc));
        assert_eq!(
            policies.int64_timestamp,
            RebasePolicy::Legacy(WriterTimeZone::Utc)
        );
        assert_eq!(
            policies.int96_timestamp,
            RebasePolicy::Legacy(WriterTimeZone::Utc)
        );
    }

    #[test]
    fn policies_for_non_utc_writer_zone_mark_the_zone_unusable() {
        let schema = schema_with(&[
            (SPARK_VERSION_METADATA_KEY, "3.5.9"),
            (SPARK_LEGACY_DATETIME_KEY, ""),
            (SPARK_LEGACY_INT96_KEY, ""),
            (SPARK_TIMEZONE_KEY, "America/Los_Angeles"),
        ]);
        let policies = resolve_file_rebase_policies(&schema, default_modes());
        assert_eq!(
            policies.date,
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown)
        );
        assert_eq!(
            policies.int64_timestamp,
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown)
        );
        assert_eq!(
            policies.int96_timestamp,
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown)
        );
    }

    #[test]
    fn mixed_flags_without_attribution_degrade_timestamps_to_check_ancient() {
        // legacyDateTime present, legacyINT96 absent on a 3.x file, and no INT96 stamp: dates
        // are definitely legacy, but a timestamp leaf cannot be attributed to INT64 (legacy)
        // vs INT96 (corrected), so the merged policy is CheckAncient.
        let schema = schema_with(&[
            (SPARK_VERSION_METADATA_KEY, "3.5.9"),
            (SPARK_LEGACY_DATETIME_KEY, ""),
            (SPARK_TIMEZONE_KEY, "UTC"),
        ]);
        let policies = resolve_file_rebase_policies(&schema, default_modes());
        assert_eq!(policies.date, RebasePolicy::Legacy(WriterTimeZone::Utc));
        assert_eq!(
            policies.int64_timestamp,
            RebasePolicy::Legacy(WriterTimeZone::Utc)
        );
        assert_eq!(policies.int96_timestamp, RebasePolicy::Corrected);
        assert_eq!(policies.int96_leaves, Int96Attribution::Unknown);
        assert_eq!(policies.timestamp_policy(0), RebasePolicy::CheckAncient);
    }

    #[test]
    fn mixed_flags_with_attribution_follow_each_leafs_physical_type() {
        // Same file, but the reader factory stamped which leaves are INT96: leaf 1 is INT96
        // (corrected), leaf 2 is INT64 (legacy UTC). Leaf 0 is the date.
        let ts_dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("d", DataType::Date32, true),
                Field::new("ts96", ts_dt.clone(), true),
                Field::new("ts", ts_dt, true),
            ],
            spark_metadata(&[
                (SPARK_VERSION_METADATA_KEY, "3.5.9"),
                (SPARK_LEGACY_DATETIME_KEY, ""),
                (SPARK_TIMEZONE_KEY, "UTC"),
                (INT96_LEAVES_METADATA_KEY, "3:1"),
            ]),
        );
        let policies = resolve_file_rebase_policies(&schema, default_modes());
        assert_eq!(policies.int96_leaves, Int96Attribution::Known(vec![1]));
        assert_eq!(policies.timestamp_policy(1), RebasePolicy::Corrected);
        assert_eq!(
            policies.timestamp_policy(2),
            RebasePolicy::Legacy(WriterTimeZone::Utc)
        );
    }

    #[test]
    fn int96_attribution_rejects_stamps_that_do_not_describe_the_schema() {
        // The stamp's leaf count must equal the schema's depth-first leaf count (2 here:
        // s.d and s.ts); anything else -- or unparsable ordinals -- is Unknown.
        let ts_dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let nested = |stamp: &str| {
            Schema::new_with_metadata(
                vec![Field::new(
                    "s",
                    DataType::Struct(
                        vec![
                            Field::new("d", DataType::Date32, true),
                            Field::new("ts", ts_dt.clone(), true),
                        ]
                        .into(),
                    ),
                    true,
                )],
                spark_metadata(&[(INT96_LEAVES_METADATA_KEY, stamp)]),
            )
        };
        assert_eq!(
            Int96Attribution::from_schema(&nested("2:1")),
            Int96Attribution::Known(vec![1])
        );
        assert_eq!(
            Int96Attribution::from_schema(&nested("2:")),
            Int96Attribution::Known(vec![])
        );
        for bad in ["3:1", "2:5", "2:x", "garbage", ""] {
            assert_eq!(
                Int96Attribution::from_schema(&nested(bad)),
                Int96Attribution::Unknown,
                "stamp {bad:?}"
            );
        }
        assert_eq!(
            Int96Attribution::from_schema(&schema_with(&[])),
            Int96Attribution::Unknown
        );
    }

    #[test]
    fn int96_leaf_stamp_lists_int96_leaf_ordinals_in_depth_first_order() {
        let message = "message m {
            required int32 id;
            optional int96 ts96;
            optional group s {
                optional int64 ts (TIMESTAMP(MICROS,true));
                optional int96 inner96;
            }
            optional group l (LIST) {
                repeated group list {
                    optional int96 element;
                }
            }
        }";
        let schema = SchemaDescriptor::new(Arc::new(parse_message_type(message).unwrap()));
        assert_eq!(int96_leaf_stamp(&schema), "5:1,3,4");

        let flat = SchemaDescriptor::new(Arc::new(
            parse_message_type("message m { required int32 id; }").unwrap(),
        ));
        assert_eq!(int96_leaf_stamp(&flat), "1:");
    }

    #[test]
    fn stamp_int96_leaves_adds_the_key_once_and_replaces_a_forged_one() {
        use parquet::file::properties::WriterProperties;
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use parquet::file::writer::SerializedFileWriter;

        let write = |kvs: Option<Vec<KeyValue>>| -> ParquetMetaData {
            let schema = Arc::new(
                parse_message_type("message m { required int32 id; optional int96 ts96; }")
                    .unwrap(),
            );
            let mut buffer = Vec::new();
            let props = WriterProperties::builder()
                .set_key_value_metadata(kvs)
                .build();
            // No row groups: only the footer matters here.
            SerializedFileWriter::new(&mut buffer, schema, Arc::new(props))
                .unwrap()
                .close()
                .unwrap();
            SerializedFileReader::new(bytes::Bytes::from(buffer))
                .unwrap()
                .metadata()
                .clone()
        };
        let stamp_of = |md: &ParquetMetaData| -> Option<String> {
            md.file_metadata()
                .key_value_metadata()
                .and_then(|kvs| kvs.iter().find(|kv| kv.key == INT96_LEAVES_METADATA_KEY))
                .and_then(|kv| kv.value.clone())
        };

        let plain = write(Some(vec![KeyValue::new(
            SPARK_VERSION_METADATA_KEY.to_string(),
            "3.5.9".to_string(),
        )]));
        let stamped = stamp_int96_leaves(&plain).expect("first stamp rebuilds");
        assert_eq!(stamp_of(&stamped).as_deref(), Some("2:1"));
        // The original entries survive next to the stamp; nothing else changed.
        assert_eq!(
            stamped.file_metadata().key_value_metadata().unwrap().len(),
            2
        );
        assert_eq!(stamped.num_row_groups(), plain.num_row_groups());
        assert_eq!(
            stamped.file_metadata().num_rows(),
            plain.file_metadata().num_rows()
        );
        // Already stamped: no rebuild.
        assert!(stamp_int96_leaves(&stamped).is_none());

        // A file that carries the key itself (it cannot legitimately) is never trusted.
        let forged = write(Some(vec![KeyValue::new(
            INT96_LEAVES_METADATA_KEY.to_string(),
            "2:".to_string(),
        )]));
        let restamped = stamp_int96_leaves(&forged).expect("forged stamp is replaced");
        assert_eq!(stamp_of(&restamped).as_deref(), Some("2:1"));
        assert_eq!(
            restamped
                .file_metadata()
                .key_value_metadata()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn policies_for_pre_spark3_files_are_legacy_with_unknown_zone() {
        // Spark 2.4 wrote the hybrid calendar unconditionally and stamped neither the legacy
        // flags nor the writer zone; both specs resolve LEGACY via the version comparison.
        let schema = schema_with(&[(SPARK_VERSION_METADATA_KEY, "2.4.8")]);
        let policies = resolve_file_rebase_policies(&schema, default_modes());
        assert_eq!(
            policies.date,
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown)
        );
        assert_eq!(
            policies.int64_timestamp,
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown)
        );
        assert_eq!(
            policies.int96_timestamp,
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown)
        );
    }

    #[test]
    fn policies_for_int96_min_version_gap_follow_each_spec() {
        // A 3.0.x file: datetime spec resolves by flag (absent -> CORRECTED) but the INT96
        // spec's min version is 3.1.0, so 3.0.x is LEGACY for INT96. Without attribution the
        // disagreement merges to CheckAncient.
        let schema = schema_with(&[(SPARK_VERSION_METADATA_KEY, "3.0.3")]);
        let policies = resolve_file_rebase_policies(&schema, default_modes());
        assert_eq!(policies.date, RebasePolicy::Corrected);
        assert_eq!(policies.int64_timestamp, RebasePolicy::Corrected);
        assert_eq!(
            policies.int96_timestamp,
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown)
        );
        assert_eq!(policies.timestamp_policy(0), RebasePolicy::CheckAncient);
    }

    #[test]
    fn policies_for_non_spark_files_are_check_ancient_by_default() {
        // The default session modes are (EXCEPTION, EXCEPTION): a producer that predates the
        // mode fields (empty strings) keeps the conservative refuse-ancient posture.
        let policies = resolve_file_rebase_policies(&schema_with(&[]), default_modes());
        assert_eq!(policies.date, RebasePolicy::CheckAncient);
        assert_eq!(policies.int64_timestamp, RebasePolicy::CheckAncient);
        assert_eq!(policies.int96_timestamp, RebasePolicy::CheckAncient);
    }

    #[test]
    fn rebase_read_mode_parses_conf_values_and_defaults_to_exception() {
        assert_eq!(
            RebaseReadMode::from_conf_value("CORRECTED"),
            RebaseReadMode::Corrected
        );
        assert_eq!(
            RebaseReadMode::from_conf_value("LEGACY"),
            RebaseReadMode::Legacy
        );
        assert_eq!(
            RebaseReadMode::from_conf_value("EXCEPTION"),
            RebaseReadMode::Exception
        );
        // Per-relation options arrive verbatim (SQLConf only upper-cases the session conf).
        assert_eq!(
            RebaseReadMode::from_conf_value("corrected"),
            RebaseReadMode::Corrected
        );
        // The proto default (producer predates the field) and anything unrecognized refuse
        // ancient values rather than silently corrupting them.
        assert_eq!(
            RebaseReadMode::from_conf_value(""),
            RebaseReadMode::Exception
        );
        assert_eq!(
            RebaseReadMode::from_conf_value("BOGUS"),
            RebaseReadMode::Exception
        );
    }

    #[test]
    fn non_spark_files_follow_corrected_read_modes() {
        // Spark 4.0 defaults both read modes to CORRECTED: a non-Spark file's ancient values
        // must read as-is (getRebaseSpec's modeByConfig fallback), not refuse.
        let policies = resolve_file_rebase_policies(
            &schema_with(&[]),
            modes(RebaseReadMode::Corrected, RebaseReadMode::Corrected),
        );
        assert_eq!(policies.date, RebasePolicy::Corrected);
        assert_eq!(policies.int64_timestamp, RebasePolicy::Corrected);
        assert_eq!(policies.int96_timestamp, RebasePolicy::Corrected);
        assert!(!policies.any_rebase_needed());
    }

    #[test]
    fn non_spark_files_follow_legacy_read_modes() {
        // LEGACY conf fallback: Spark rebases with the file's recorded writer zone, or the JVM
        // default zone when unrecorded -- unavailable natively, so the zone classifies as
        // OtherOrUnknown (dates rebase fully, ancient timestamps refuse).
        let policies = resolve_file_rebase_policies(
            &schema_with(&[]),
            modes(RebaseReadMode::Legacy, RebaseReadMode::Legacy),
        );
        assert_eq!(
            policies.date,
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown)
        );
        assert_eq!(
            policies.int64_timestamp,
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown)
        );
        assert_eq!(
            policies.int96_timestamp,
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown)
        );

        // A recorded UTC-equivalent writer zone upgrades the timestamp path to the exact
        // rebase, same as the metadata-driven LEGACY branch (getRebaseSpec looks the timezone
        // key up for every LEGACY resolution, conf-fallback included).
        let policies = resolve_file_rebase_policies(
            &schema_with(&[(SPARK_TIMEZONE_KEY, "UTC")]),
            modes(RebaseReadMode::Legacy, RebaseReadMode::Legacy),
        );
        assert_eq!(policies.date, RebasePolicy::Legacy(WriterTimeZone::Utc));
        assert_eq!(
            policies.int64_timestamp,
            RebasePolicy::Legacy(WriterTimeZone::Utc)
        );
        assert_eq!(
            policies.int96_timestamp,
            RebasePolicy::Legacy(WriterTimeZone::Utc)
        );
    }

    #[test]
    fn non_spark_files_with_mixed_read_modes_resolve_each_spec_independently() {
        // datetime CORRECTED + int96 EXCEPTION on a metadata-free file: dates and INT64
        // timestamps follow the datetime spec alone (the maintainer's corrected 1500-01-01
        // INT64 timestamp must read verbatim), INT96 leaves follow the INT96 spec.
        let ts_dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("ts", ts_dt.clone(), true),
                Field::new("ts96", ts_dt, true),
            ],
            spark_metadata(&[(INT96_LEAVES_METADATA_KEY, "2:1")]),
        );
        let policies = resolve_file_rebase_policies(
            &schema,
            modes(RebaseReadMode::Corrected, RebaseReadMode::Exception),
        );
        assert_eq!(policies.date, RebasePolicy::Corrected);
        assert_eq!(policies.timestamp_policy(0), RebasePolicy::Corrected);
        assert_eq!(policies.timestamp_policy(1), RebasePolicy::CheckAncient);

        // Without the stamp the disagreeing specs merge to CheckAncient for every leaf.
        let policies = resolve_file_rebase_policies(
            &schema_with(&[]),
            modes(RebaseReadMode::Corrected, RebaseReadMode::Legacy),
        );
        assert_eq!(policies.date, RebasePolicy::Corrected);
        assert_eq!(policies.timestamp_policy(0), RebasePolicy::CheckAncient);
    }

    #[test]
    fn spark_files_ignore_the_session_read_modes() {
        // getRebaseSpec consults modeByConfig ONLY when org.apache.spark.version is absent: a
        // legacy 2.4 file stays LEGACY under CORRECTED read modes, and a modern flag-free file
        // stays CORRECTED under LEGACY read modes.
        let legacy = schema_with(&[(SPARK_VERSION_METADATA_KEY, "2.4.8")]);
        let policies = resolve_file_rebase_policies(
            &legacy,
            modes(RebaseReadMode::Corrected, RebaseReadMode::Corrected),
        );
        assert_eq!(
            policies.date,
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown)
        );
        assert_eq!(
            policies.int64_timestamp,
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown)
        );
        assert_eq!(
            policies.int96_timestamp,
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown)
        );

        let modern = schema_with(&[(SPARK_VERSION_METADATA_KEY, "3.5.9")]);
        let policies = resolve_file_rebase_policies(
            &modern,
            modes(RebaseReadMode::Legacy, RebaseReadMode::Legacy),
        );
        assert_eq!(policies.date, RebasePolicy::Corrected);
        assert_eq!(policies.int64_timestamp, RebasePolicy::Corrected);
        assert_eq!(policies.int96_timestamp, RebasePolicy::Corrected);
    }

    /// A wrapper applying `policy` to every leaf of `field` (the same policy for dates and
    /// timestamps alike).
    fn rebase_expr(field: Field, policy: RebasePolicy) -> SparkDatetimeRebaseExpr {
        let policies = flat_policies(policy, policy, policy);
        let mut next_leaf = 0;
        let mut leaf_pols = Vec::new();
        leaf_policies(field.data_type(), &mut next_leaf, &policies, &mut leaf_pols);
        SparkDatetimeRebaseExpr {
            child: Arc::new(Column::new(field.name(), 0)),
            field: Arc::new(field),
            leaf_policies: leaf_pols,
        }
    }

    fn eval_on(
        expr: &SparkDatetimeRebaseExpr,
        array: ArrayRef,
        field: Field,
    ) -> DataFusionResult<ArrayRef> {
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        expr.evaluate(&batch)?.into_array(batch.num_rows())
    }

    #[test]
    fn legacy_dates_rebase_and_preserve_nulls() {
        let field = Field::new("d", DataType::Date32, true);
        let expr = rebase_expr(field.clone(), RebasePolicy::Legacy(WriterTimeZone::Utc));
        let stored = julian_civil_to_day(1500, 1, 1);
        let array: ArrayRef = Arc::new(Date32Array::from(vec![Some(stored), None, Some(19876)]));
        let rebased = eval_on(&expr, array, field).unwrap();
        let rebased = rebased.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(rebased.value(0), days_from_civil(1500, 1, 1) as i32);
        assert!(rebased.is_null(1));
        assert_eq!(rebased.value(2), 19876);
    }

    #[test]
    fn check_ancient_dates_error_only_when_ancient_values_appear() {
        let field = Field::new("d", DataType::Date32, true);
        let expr = rebase_expr(field.clone(), RebasePolicy::CheckAncient);
        let modern: ArrayRef = Arc::new(Date32Array::from(vec![Some(0), Some(19876), None]));
        assert!(eval_on(&expr, modern, field.clone()).is_ok());

        let ancient: ArrayRef = Arc::new(Date32Array::from(vec![Some(-141428)]));
        let err = eval_on(&expr, ancient, field).unwrap_err().to_string();
        assert!(err.contains("rebase"), "unexpected error: {err}");
        assert!(err.contains("'d'"), "unexpected error: {err}");
    }

    #[test]
    fn legacy_utc_timestamps_rebase_by_nominal_day_shift() {
        const MICROS_PER_DAY: i64 = 86_400_000_000;
        let dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let field = Field::new("ts", dt.clone(), true);
        let expr = rebase_expr(field.clone(), RebasePolicy::Legacy(WriterTimeZone::Utc));
        // Julian 1500-01-01T12:34:56.789Z as a legacy writer stores it.
        let time_of_day = (12i64 * 3600 + 34 * 60 + 56) * 1_000_000 + 789_000;
        let stored = julian_civil_to_day(1500, 1, 1) as i64 * MICROS_PER_DAY + time_of_day;
        let modern = 1_700_000_000_000_000i64;
        let array: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![Some(stored), None, Some(modern)])
                .with_timezone("UTC"),
        );
        let rebased = eval_on(&expr, array, field).unwrap();
        assert_eq!(rebased.data_type(), &dt);
        let rebased = rebased
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            rebased.value(0),
            days_from_civil(1500, 1, 1) * MICROS_PER_DAY + time_of_day
        );
        assert!(rebased.is_null(1));
        assert_eq!(rebased.value(2), modern);
    }

    #[test]
    fn legacy_utc_timestamps_rebase_in_every_unit_without_overflow() {
        // The cutover day times a nanosecond day exceeds i64, so the identity check must
        // compare in days. Julian 1500-01-01T00:00:01 in each unit that can hold it rebases
        // to proleptic 1500-01-01T00:00:01; the epoch, a modern value and -- for nanoseconds,
        // whose i64 range only reaches back to 1677 -- i64::MIN are the identity.
        let stored_day = julian_civil_to_day(1500, 1, 1) as i64;
        let expected_day = days_from_civil(1500, 1, 1);
        for (unit, per_second, holds_ancient) in [
            (TimeUnit::Second, 1i64, true),
            (TimeUnit::Millisecond, 1_000, true),
            (TimeUnit::Microsecond, 1_000_000, true),
            (TimeUnit::Nanosecond, 1_000_000_000, false),
        ] {
            let per_day = per_second * 86_400;
            let expr = rebase_expr(ts_field(unit), RebasePolicy::Legacy(WriterTimeZone::Utc));
            let modern = 1_700_000_000 * per_second;
            let (ancient_in, ancient_out) = if holds_ancient {
                (
                    stored_day * per_day + per_second,
                    expected_day * per_day + per_second,
                )
            } else {
                (i64::MIN, i64::MIN)
            };
            let input = ts_array(unit, vec![Some(ancient_in), Some(0), Some(modern), None]);
            let out =
                eval_on(&expr, input, ts_field(unit)).unwrap_or_else(|e| panic!("{unit:?}: {e}"));
            let expected = ts_array(unit, vec![Some(ancient_out), Some(0), Some(modern), None]);
            assert_eq!(&out, &expected, "{unit:?}");
        }
    }

    #[test]
    fn legacy_non_utc_timestamps_pass_modern_and_refuse_ancient_values() {
        let dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let field = Field::new("ts", dt, true);
        let expr = rebase_expr(
            field.clone(),
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown),
        );
        let modern: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![Some(0), Some(1_700_000_000_000_000)])
                .with_timezone("UTC"),
        );
        assert!(eval_on(&expr, modern, field.clone()).is_ok());

        let ancient: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![Some(
                LAST_SWITCH_JULIAN_TS_SECONDS * 1_000_000 - 1,
            )])
            .with_timezone("UTC"),
        );
        let err = eval_on(&expr, ancient, field).unwrap_err().to_string();
        assert!(err.contains("rebase"), "unexpected error: {err}");
        assert!(err.contains("timezone tables"), "unexpected error: {err}");
    }

    fn ts_field(unit: TimeUnit) -> Field {
        Field::new("ts", DataType::Timestamp(unit, Some("UTC".into())), true)
    }

    fn ts_array(unit: TimeUnit, values: Vec<Option<i64>>) -> ArrayRef {
        let tz: Option<Arc<str>> = Some("UTC".into());
        match unit {
            TimeUnit::Second => {
                Arc::new(PrimitiveArray::<TimestampSecondType>::from(values).with_timezone_opt(tz))
            }
            TimeUnit::Millisecond => Arc::new(
                PrimitiveArray::<TimestampMillisecondType>::from(values).with_timezone_opt(tz),
            ),
            TimeUnit::Microsecond => Arc::new(
                PrimitiveArray::<TimestampMicrosecondType>::from(values).with_timezone_opt(tz),
            ),
            TimeUnit::Nanosecond => Arc::new(
                PrimitiveArray::<TimestampNanosecondType>::from(values).with_timezone_opt(tz),
            ),
        }
    }

    #[test]
    fn check_ancient_timestamps_reject_only_values_before_1900_in_every_unit() {
        // Spark's EXCEPTION read mode (`createTimestampRebaseFuncInRead`) throws only for
        // micros < RebaseDateTime.lastSwitchJulianTs (1900-01-01T00:00:00Z, the last instant at
        // which rebasing changes a value in ANY zone), after converting MILLIS to micros. A
        // timestamp one microsecond before the epoch is well after that and must read.
        assert_eq!(
            LAST_SWITCH_JULIAN_TS_SECONDS,
            days_from_civil(1900, 1, 1) * 86_400
        );
        for (unit, per_second) in [
            (TimeUnit::Second, 1i64),
            (TimeUnit::Millisecond, 1_000),
            (TimeUnit::Microsecond, 1_000_000),
            (TimeUnit::Nanosecond, 1_000_000_000),
        ] {
            let cutoff = LAST_SWITCH_JULIAN_TS_SECONDS * per_second;
            for policy in [
                RebasePolicy::CheckAncient,
                RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown),
            ] {
                let expr = rebase_expr(ts_field(unit), policy);
                let passing = ts_array(unit, vec![Some(-1), Some(cutoff), Some(0), None]);
                let out = eval_on(&expr, Arc::clone(&passing), ts_field(unit))
                    .unwrap_or_else(|e| panic!("{unit:?} under {policy:?}: {e}"));
                assert_eq!(&out, &passing, "{unit:?} under {policy:?}");

                let failing = ts_array(unit, vec![Some(cutoff - 1)]);
                let err = eval_on(&expr, failing, ts_field(unit))
                    .unwrap_err()
                    .to_string();
                assert!(err.contains("rebase"), "{unit:?} under {policy:?}: {err}");
            }
        }
    }

    #[test]
    fn wrap_targets_only_affected_columns() {
        let policies = flat_policies(
            RebasePolicy::Legacy(WriterTimeZone::Utc),
            RebasePolicy::Legacy(WriterTimeZone::Utc),
            RebasePolicy::Legacy(WriterTimeZone::Utc),
        );
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("d", DataType::Date32, true),
            Field::new(
                "ntz",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let unaffected = wrap_datetime_rebase(
            Arc::new(Column::new("i", 0)) as Arc<dyn PhysicalExpr>,
            &schema,
            &policies,
        )
        .unwrap();
        assert!(unaffected.downcast_ref::<Column>().is_some());

        let ntz = wrap_datetime_rebase(
            Arc::new(Column::new("ntz", 2)) as Arc<dyn PhysicalExpr>,
            &schema,
            &policies,
        )
        .unwrap();
        assert!(ntz.downcast_ref::<Column>().is_some());

        let wrapped = wrap_datetime_rebase(
            Arc::new(Column::new("d", 1)) as Arc<dyn PhysicalExpr>,
            &schema,
            &policies,
        )
        .unwrap();
        let wrapped = wrapped.downcast_ref::<SparkDatetimeRebaseExpr>().unwrap();
        assert_eq!(
            wrapped.leaf_policies,
            vec![RebasePolicy::Legacy(WriterTimeZone::Utc)]
        );
    }

    #[test]
    fn wrap_attributes_timestamp_leaves_by_ordinal_across_preceding_columns() {
        // Leaf ordinals count every leaf of the preceding top-level fields: `s` holds leaves
        // 0..3 (i, ts96, ts) and the top-level `ts96` is leaf 3. The stamp marks 1 and 3.
        let ts_dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let schema: SchemaRef = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new(
                    "s",
                    DataType::Struct(
                        vec![
                            Field::new("i", DataType::Int64, true),
                            Field::new("ts96", ts_dt.clone(), true),
                            Field::new("ts", ts_dt.clone(), true),
                        ]
                        .into(),
                    ),
                    true,
                ),
                Field::new("ts96", ts_dt, true),
            ],
            spark_metadata(&[(INT96_LEAVES_METADATA_KEY, "4:1,3")]),
        ));
        let policies = resolve_file_rebase_policies(
            &schema,
            modes(RebaseReadMode::Corrected, RebaseReadMode::Exception),
        );
        let s = wrap_datetime_rebase(
            Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>,
            &schema,
            &policies,
        )
        .unwrap();
        let s = s.downcast_ref::<SparkDatetimeRebaseExpr>().unwrap();
        assert_eq!(
            s.leaf_policies,
            vec![
                RebasePolicy::Corrected,
                RebasePolicy::CheckAncient,
                RebasePolicy::Corrected
            ]
        );
        let top = wrap_datetime_rebase(
            Arc::new(Column::new("ts96", 1)) as Arc<dyn PhysicalExpr>,
            &schema,
            &policies,
        )
        .unwrap();
        let top = top.downcast_ref::<SparkDatetimeRebaseExpr>().unwrap();
        assert_eq!(top.leaf_policies, vec![RebasePolicy::CheckAncient]);

        // Swap the modes: the INT64 leaf inside `s` is now the only one that needs handling.
        let policies = resolve_file_rebase_policies(
            &schema,
            modes(RebaseReadMode::Exception, RebaseReadMode::Corrected),
        );
        let top = wrap_datetime_rebase(
            Arc::new(Column::new("ts96", 1)) as Arc<dyn PhysicalExpr>,
            &schema,
            &policies,
        )
        .unwrap();
        assert!(top.downcast_ref::<Column>().is_some());
        let s = wrap_datetime_rebase(
            Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>,
            &schema,
            &policies,
        )
        .unwrap();
        let s = s.downcast_ref::<SparkDatetimeRebaseExpr>().unwrap();
        assert_eq!(
            s.leaf_policies,
            vec![
                RebasePolicy::Corrected,
                RebasePolicy::Corrected,
                RebasePolicy::CheckAncient
            ]
        );
    }

    #[test]
    fn wrap_passes_nested_columns_whose_affected_leaves_are_all_corrected() {
        // Date policy Corrected, timestamp policies Legacy, column STRUCT<d: DATE>: the
        // struct's only rebase-relevant leaf is a date, and the date policy needs no
        // handling, so the column must pass through unwrapped instead of being wrapped just
        // because SOME policy (timestamps -- absent here) needs handling.
        let policies = flat_policies(
            RebasePolicy::Corrected,
            RebasePolicy::Legacy(WriterTimeZone::Utc),
            RebasePolicy::Legacy(WriterTimeZone::Utc),
        );
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(vec![Field::new("d", DataType::Date32, true)].into()),
            true,
        )]));
        let out = wrap_datetime_rebase(
            Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>,
            &schema,
            &policies,
        )
        .unwrap();
        assert!(out.downcast_ref::<Column>().is_some());

        // Mirror image: STRUCT<ts: TIMESTAMP> under Corrected timestamp policies with a
        // non-Corrected date policy passes too.
        let policies = flat_policies(
            RebasePolicy::CheckAncient,
            RebasePolicy::Corrected,
            RebasePolicy::Corrected,
        );
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                )]
                .into(),
            ),
            true,
        )]));
        let out = wrap_datetime_rebase(
            Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>,
            &schema,
            &policies,
        )
        .unwrap();
        assert!(out.downcast_ref::<Column>().is_some());
    }

    #[test]
    fn wrap_installs_the_wrapper_on_nested_columns_with_an_affected_leaf() {
        // A nested column whose leaves DO include an affected type under a policy that needs
        // handling gets the wrapper (not a refusal), across struct, list, and map nesting.
        let date_legacy = flat_policies(
            RebasePolicy::Legacy(WriterTimeZone::Utc),
            RebasePolicy::Corrected,
            RebasePolicy::Corrected,
        );
        let ts_check = flat_policies(
            RebasePolicy::Corrected,
            RebasePolicy::CheckAncient,
            RebasePolicy::CheckAncient,
        );
        let ts_field = Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        );
        let cases: Vec<(DataType, &FileRebasePolicies, Vec<RebasePolicy>)> = vec![
            (
                DataType::Struct(vec![Field::new("d", DataType::Date32, true)].into()),
                &date_legacy,
                vec![RebasePolicy::Legacy(WriterTimeZone::Utc)],
            ),
            (
                DataType::List(Arc::new(Field::new("item", DataType::Date32, true))),
                &date_legacy,
                vec![RebasePolicy::Legacy(WriterTimeZone::Utc)],
            ),
            (
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Int64, false),
                                Field::new("value", DataType::Date32, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                &date_legacy,
                vec![
                    RebasePolicy::Corrected,
                    RebasePolicy::Legacy(WriterTimeZone::Utc),
                ],
            ),
            (
                DataType::Struct(vec![ts_field.clone()].into()),
                &ts_check,
                vec![RebasePolicy::CheckAncient],
            ),
        ];
        for (dt, policies, expected) in cases {
            let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("n", dt.clone(), true)]));
            let wrapped = wrap_datetime_rebase(
                Arc::new(Column::new("n", 0)) as Arc<dyn PhysicalExpr>,
                &schema,
                policies,
            )
            .unwrap();
            let wrapped = wrapped
                .downcast_ref::<SparkDatetimeRebaseExpr>()
                .unwrap_or_else(|| panic!("expected {dt} to be wrapped"));
            assert_eq!(wrapped.leaf_policies, expected, "{dt}");
        }
    }

    #[test]
    fn wrap_passes_nested_columns_with_no_affected_leaves_at_all() {
        // TIMESTAMP_NTZ (no timezone) and plain types are never rebased, so a nested column
        // built only from them passes even when every policy needs handling.
        let policies = flat_policies(
            RebasePolicy::Legacy(WriterTimeZone::Utc),
            RebasePolicy::Legacy(WriterTimeZone::Utc),
            RebasePolicy::Legacy(WriterTimeZone::Utc),
        );
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(
                vec![
                    Field::new("i", DataType::Int64, true),
                    Field::new(
                        "ntz",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ),
                ]
                .into(),
            ),
            true,
        )]));
        let out = wrap_datetime_rebase(
            Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>,
            &schema,
            &policies,
        )
        .unwrap();
        assert!(out.downcast_ref::<Column>().is_some());
    }

    #[test]
    fn nested_struct_list_map_with_modern_and_null_leaves_pass_under_every_policy() {
        let date_field = Arc::new(Field::new("d", DataType::Date32, true));
        let ts_field = Arc::new(ts_field(TimeUnit::Microsecond));
        let struct_dt =
            DataType::Struct(vec![Arc::clone(&date_field), Arc::clone(&ts_field)].into());
        let struct_arr = StructArray::try_new(
            vec![Arc::clone(&date_field), Arc::clone(&ts_field)].into(),
            vec![
                Arc::new(Date32Array::from(vec![Some(0), None, Some(19876)])),
                ts_array(TimeUnit::Microsecond, vec![Some(-1), None, Some(0)]),
            ],
            Some(vec![true, true, false].into()),
        )
        .unwrap();
        let list_item = Arc::new(Field::new("item", DataType::Date32, true));
        let list_dt = DataType::List(Arc::clone(&list_item));
        let list_arr = ListArray::try_new(
            Arc::clone(&list_item),
            OffsetBuffer::new(vec![0, 2, 2, 3].into()),
            Arc::new(Date32Array::from(vec![Some(0), None, Some(19876)])),
            Some(vec![true, false, true].into()),
        )
        .unwrap();
        let key_field = Arc::new(Field::new("key", DataType::Int64, false));
        let value_field = Arc::new(Field::new("value", DataType::Date32, true));
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![Arc::clone(&key_field), Arc::clone(&value_field)].into()),
            false,
        ));
        let map_dt = DataType::Map(Arc::clone(&entries_field), false);
        let entries = StructArray::try_new(
            vec![key_field, value_field].into(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Date32Array::from(vec![Some(19876), None])),
            ],
            None,
        )
        .unwrap();
        let map_arr = MapArray::try_new(
            entries_field,
            OffsetBuffer::new(vec![0, 1, 2, 2].into()),
            entries,
            Some(vec![true, true, false].into()),
            false,
        )
        .unwrap();

        let cases: Vec<(DataType, ArrayRef)> = vec![
            (struct_dt, Arc::new(struct_arr)),
            (list_dt, Arc::new(list_arr)),
            (map_dt, Arc::new(map_arr)),
        ];
        for policy in [
            RebasePolicy::Legacy(WriterTimeZone::Utc),
            RebasePolicy::Legacy(WriterTimeZone::OtherOrUnknown),
            RebasePolicy::CheckAncient,
        ] {
            for (dt, array) in &cases {
                let field = Field::new("n", dt.clone(), true);
                let expr = rebase_expr(field.clone(), policy);
                let out = eval_on(&expr, Arc::clone(array), field)
                    .unwrap_or_else(|e| panic!("{dt} under {policy:?}: {e}"));
                assert_eq!(&out, array, "{dt} under {policy:?} must be the identity");
            }
        }
    }

    #[test]
    fn nested_ancient_date_leaf_rebases_under_legacy_and_errors_under_check_ancient() {
        // list<struct<d: date>>: one ancient leaf among modern and null ones.
        let stored = julian_civil_to_day(1500, 1, 1);
        let date_field = Arc::new(Field::new("d", DataType::Date32, true));
        let struct_field = Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Arc::clone(&date_field)].into()),
            true,
        ));
        let dt = DataType::List(Arc::clone(&struct_field));
        let structs = StructArray::try_new(
            vec![date_field].into(),
            vec![Arc::new(Date32Array::from(vec![
                Some(stored),
                None,
                Some(19876),
            ]))],
            Some(vec![true, false, true].into()),
        )
        .unwrap();
        let array: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::clone(&struct_field),
                OffsetBuffer::new(vec![0, 1, 3].into()),
                Arc::new(structs),
                None,
            )
            .unwrap(),
        );
        let field = Field::new("n", dt, true);

        let legacy = rebase_expr(field.clone(), RebasePolicy::Legacy(WriterTimeZone::Utc));
        let out = eval_on(&legacy, Arc::clone(&array), field.clone()).unwrap();
        assert_eq!(out.data_type(), field.data_type());
        let out_list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let in_list = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out_list.offsets(), in_list.offsets());
        assert_eq!(out_list.nulls(), in_list.nulls());
        let out_structs = out_list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(out_structs.nulls(), in_list.values().nulls());
        let dates = out_structs
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(dates.value(0), days_from_civil(1500, 1, 1) as i32);
        assert!(dates.is_null(1));
        assert_eq!(dates.value(2), 19876);

        let check = rebase_expr(field.clone(), RebasePolicy::CheckAncient);
        let err = eval_on(&check, array, field).unwrap_err().to_string();
        assert!(err.contains("rebase"), "unexpected error: {err}");
        assert!(err.contains("'n'"), "unexpected error: {err}");
    }

    #[test]
    fn nested_leaves_each_follow_their_own_policy() {
        // struct<ts96: timestamp, ts: timestamp> where the stamp marks the first leaf INT96:
        // under datetime CORRECTED + int96 EXCEPTION, an ancient INT64 value passes verbatim
        // while an ancient INT96 value in the same struct is refused.
        let ts_dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let ts96_field = Arc::new(Field::new("ts96", ts_dt.clone(), true));
        let ts_field = Arc::new(Field::new("ts", ts_dt, true));
        let struct_dt =
            DataType::Struct(vec![Arc::clone(&ts96_field), Arc::clone(&ts_field)].into());
        let schema: SchemaRef = Arc::new(Schema::new_with_metadata(
            vec![Field::new("s", struct_dt.clone(), true)],
            spark_metadata(&[(INT96_LEAVES_METADATA_KEY, "2:0")]),
        ));
        let policies = resolve_file_rebase_policies(
            &schema,
            modes(RebaseReadMode::Corrected, RebaseReadMode::Exception),
        );
        let wrapped = wrap_datetime_rebase(
            Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>,
            &schema,
            &policies,
        )
        .unwrap();
        let expr = wrapped.downcast_ref::<SparkDatetimeRebaseExpr>().unwrap();

        let ancient = LAST_SWITCH_JULIAN_TS_SECONDS * 1_000_000 - 1;
        let build = |ts96: i64, ts: i64| -> ArrayRef {
            Arc::new(
                StructArray::try_new(
                    vec![Arc::clone(&ts96_field), Arc::clone(&ts_field)].into(),
                    vec![
                        ts_array(TimeUnit::Microsecond, vec![Some(ts96)]),
                        ts_array(TimeUnit::Microsecond, vec![Some(ts)]),
                    ],
                    None,
                )
                .unwrap(),
            )
        };
        let field = Field::new("s", struct_dt, true);
        let passing = build(0, ancient);
        let out = eval_on(expr, Arc::clone(&passing), field.clone()).unwrap();
        assert_eq!(&out, &passing);
        let err = eval_on(expr, build(ancient, 0), field)
            .unwrap_err()
            .to_string();
        assert!(err.contains("rebase"), "unexpected error: {err}");
    }

    /// `STRUCT<d: Date32, ts: Timestamp>` as (field, type, array builder), the maintainer's
    /// physical `s` column: a modern date next to a timestamp that may be ancient.
    fn date_ts_struct(ts: i64) -> (FieldRef, DataType, ArrayRef) {
        let d_field = Arc::new(Field::new("d", DataType::Date32, true));
        let ts_field = Arc::new(ts_field(TimeUnit::Microsecond));
        let fields: arrow::datatypes::Fields =
            vec![Arc::clone(&d_field), Arc::clone(&ts_field)].into();
        let dt = DataType::Struct(fields.clone());
        let array: ArrayRef = Arc::new(
            StructArray::try_new(
                fields,
                vec![
                    Arc::new(Date32Array::from(vec![Some(19875)])),
                    ts_array(TimeUnit::Microsecond, vec![Some(ts)]),
                ],
                None,
            )
            .unwrap(),
        );
        (Arc::new(Field::new("s", dt.clone(), true)), dt, array)
    }

    fn struct_of(fields: Vec<Field>) -> DataType {
        DataType::Struct(fields.into())
    }

    #[test]
    fn unrequested_struct_leaves_are_never_checked() {
        // The maintainer's P2 probe: a metadata-free file with s.d = 2024-06-01 and
        // s.ts = 1500-01-01 under EXCEPTION read modes. Spark's requested schema for
        // `select s.d` is STRUCT<d>, so Spark never decodes s.ts and reads fine; the wrapper,
        // sitting beneath the schema adapter's struct narrowing, must not check the leaf the
        // narrowing is about to drop.
        let ancient = LAST_SWITCH_JULIAN_TS_SECONDS * 1_000_000 - 1;
        let (field, dt, array) = date_ts_struct(ancient);
        let schema = Schema::new(vec![Field::new("s", dt.clone(), true)]);
        let policies = resolve_file_rebase_policies(&schema, default_modes());
        assert_eq!(policies.date, RebasePolicy::CheckAncient);

        let requested = struct_of(vec![Field::new("d", DataType::Date32, true)]);
        let narrowed =
            policies
                .clone()
                .restrict_to_requested(&schema, &[Some(&requested)], true, false);
        assert_eq!(narrowed.unrequested_leaves, vec![1]);
        let wrapped = wrap_datetime_rebase(
            Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>,
            &Arc::new(schema.clone()),
            &narrowed,
        )
        .unwrap();
        let expr = wrapped.downcast_ref::<SparkDatetimeRebaseExpr>().unwrap();
        assert_eq!(
            expr.leaf_policies,
            vec![RebasePolicy::CheckAncient, RebasePolicy::Corrected]
        );
        let out = eval_on(expr, Arc::clone(&array), field.as_ref().clone()).unwrap();
        assert_eq!(&out, &array, "the requested modern date passes untouched");

        // Requesting both leaves (or the whole struct) still refuses the ancient timestamp.
        let full = policies
            .clone()
            .restrict_to_requested(&schema, &[Some(&dt)], true, false);
        assert!(full.unrequested_leaves.is_empty());
        for policies in [&policies, &full] {
            let wrapped = wrap_datetime_rebase(
                Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>,
                &Arc::new(schema.clone()),
                policies,
            )
            .unwrap();
            let expr = wrapped.downcast_ref::<SparkDatetimeRebaseExpr>().unwrap();
            let err = eval_on(expr, Arc::clone(&array), field.as_ref().clone())
                .unwrap_err()
                .to_string();
            assert!(err.contains("rebase"), "unexpected error: {err}");
        }

        // A column with no requested affected leaf at all is not wrapped.
        let only_ts_unrequested = policies.clone().restrict_to_requested(
            &schema,
            &[Some(&struct_of(vec![Field::new(
                "d",
                DataType::Int32,
                true,
            )]))],
            true,
            false,
        );
        // (a leaf whose requested type mismatches is still requested -- the cast reads it)
        assert!(only_ts_unrequested.unrequested_leaves == vec![1]);
        let none_requested = policies.clone().restrict_to_requested(
            &schema,
            &[Some(&struct_of(vec![Field::new(
                "x",
                DataType::Int32,
                true,
            )]))],
            true,
            false,
        );
        assert_eq!(none_requested.unrequested_leaves, vec![0, 1]);
        let passthrough = wrap_datetime_rebase(
            Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>,
            &Arc::new(schema),
            &none_requested,
        )
        .unwrap();
        assert!(passthrough.downcast_ref::<Column>().is_some());
    }

    #[test]
    fn unrequested_leaves_inside_lists_are_never_checked() {
        // LIST<STRUCT<d, ts>> read as LIST<STRUCT<d>>: the list pairs positionally with the
        // requested list and the struct beneath it narrows by name.
        let ancient = LAST_SWITCH_JULIAN_TS_SECONDS * 1_000_000 - 1;
        let (_, struct_dt, structs) = date_ts_struct(ancient);
        let item = Arc::new(Field::new("item", struct_dt, true));
        let list_dt = DataType::List(Arc::clone(&item));
        let array: ArrayRef = Arc::new(
            ListArray::try_new(item, OffsetBuffer::new(vec![0, 1].into()), structs, None).unwrap(),
        );
        let field = Field::new("l", list_dt.clone(), true);
        let schema = Schema::new(vec![field.clone()]);
        let policies = resolve_file_rebase_policies(&schema, default_modes());

        let requested = DataType::List(Arc::new(Field::new(
            "element",
            struct_of(vec![Field::new("d", DataType::Date32, true)]),
            true,
        )));
        let narrowed =
            policies
                .clone()
                .restrict_to_requested(&schema, &[Some(&requested)], true, false);
        assert_eq!(narrowed.unrequested_leaves, vec![1]);
        let wrapped = wrap_datetime_rebase(
            Arc::new(Column::new("l", 0)) as Arc<dyn PhysicalExpr>,
            &Arc::new(schema.clone()),
            &narrowed,
        )
        .unwrap();
        let expr = wrapped.downcast_ref::<SparkDatetimeRebaseExpr>().unwrap();
        let out = eval_on(expr, Arc::clone(&array), field.clone()).unwrap();
        assert_eq!(&out, &array);

        let wrapped = wrap_datetime_rebase(
            Arc::new(Column::new("l", 0)) as Arc<dyn PhysicalExpr>,
            &Arc::new(schema),
            &policies,
        )
        .unwrap();
        let expr = wrapped.downcast_ref::<SparkDatetimeRebaseExpr>().unwrap();
        let err = eval_on(expr, array, field).unwrap_err().to_string();
        assert!(err.contains("rebase"), "unexpected error: {err}");
    }

    #[test]
    fn requested_leaf_narrowing_keeps_int96_ordinals_physical() {
        // struct<ts96: INT96, ts: INT64> with the stamp marking physical leaf 0 as INT96, read
        // as struct<ts> only. The attribution must stay keyed on PHYSICAL ordinals: `ts` is
        // physical leaf 1 (INT64) even though it is the requested struct's first leaf, so under
        // datetime EXCEPTION + int96 CORRECTED it is CheckAncient, and under the swapped modes
        // it is Corrected (and the unrequested INT96 leaf is never checked either way).
        let ts_dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let physical = struct_of(vec![
            Field::new("ts96", ts_dt.clone(), true),
            Field::new("ts", ts_dt.clone(), true),
        ]);
        let schema = Schema::new_with_metadata(
            vec![Field::new("s", physical, true)],
            spark_metadata(&[(INT96_LEAVES_METADATA_KEY, "2:0")]),
        );
        let requested = struct_of(vec![Field::new("ts", ts_dt, true)]);
        let leaf_policies_under = |datetime, int96| {
            let policies = resolve_file_rebase_policies(&schema, modes(datetime, int96))
                .restrict_to_requested(&schema, &[Some(&requested)], true, false);
            assert_eq!(policies.unrequested_leaves, vec![0]);
            wrap_datetime_rebase(
                Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>,
                &Arc::new(schema.clone()),
                &policies,
            )
            .unwrap()
            .downcast_ref::<SparkDatetimeRebaseExpr>()
            .map(|e| e.leaf_policies.clone())
        };
        assert_eq!(
            leaf_policies_under(RebaseReadMode::Exception, RebaseReadMode::Corrected),
            Some(vec![RebasePolicy::Corrected, RebasePolicy::CheckAncient])
        );
        assert_eq!(
            leaf_policies_under(RebaseReadMode::Corrected, RebaseReadMode::Exception),
            None
        );
    }

    #[test]
    fn requested_leaf_narrowing_matches_children_like_the_struct_convert() {
        // The mask pairs struct children the way `parquet_convert_struct_to_struct` selects
        // them -- by folded name in case-insensitive mode, by Parquet field id when ids are in
        // play -- and keeps a child whenever EITHER rule matches, so it can only ever drop
        // leaves the narrowing drops too. Shape mismatches and unpaired columns keep every leaf.
        use arrow::datatypes::Field as F;
        let id = |field: F, id: &str| {
            field.with_metadata(HashMap::from([(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )]))
        };
        let physical = struct_of(vec![
            id(F::new("A", DataType::Date32, true), "1"),
            id(F::new("b", DataType::Date32, true), "2"),
            F::new("c", DataType::Date32, true),
            F::new("m", DataType::Date32, true),
        ]);
        let schema = Schema::new(vec![
            Field::new("s", physical, true),
            Field::new("d", DataType::Date32, true),
        ]);
        let policies = resolve_file_rebase_policies(&schema, default_modes());
        let restrict = |requested: &DataType, case_sensitive: bool, use_field_id: bool| {
            policies
                .clone()
                .restrict_to_requested(
                    &schema,
                    &[Some(requested), None],
                    case_sensitive,
                    use_field_id,
                )
                .unrequested_leaves
        };

        // Case-insensitive name match keeps `A` for a requested `a`; case-sensitive drops it.
        let by_name = struct_of(vec![F::new("a", DataType::Date32, true)]);
        assert_eq!(restrict(&by_name, false, false), vec![1, 2, 3]);
        assert_eq!(restrict(&by_name, true, false), vec![0, 1, 2, 3]);

        // Field id 2 selects `b` even though the requested name (`zzz`) matches nothing; the
        // unpaired top-level `d` (leaf 4) is never dropped.
        let by_id = struct_of(vec![id(F::new("zzz", DataType::Date32, true), "2")]);
        assert_eq!(restrict(&by_id, false, true), vec![0, 2, 3]);
        // Without field-id matching the id is ignored and nothing pairs.
        assert_eq!(restrict(&by_id, false, false), vec![0, 1, 2, 3]);
        // Id AND name both count: requested `c` (id 1) keeps physical `A` (id 1) and `c`.
        let both = struct_of(vec![id(F::new("c", DataType::Date32, true), "1")]);
        assert_eq!(restrict(&both, false, true), vec![1, 3]);

        // A requested type of another shape keeps every leaf (the cast reads them all).
        assert_eq!(
            restrict(&DataType::Date32, false, false),
            Vec::<usize>::new()
        );

        // Only the pairings `parquet_convert_array` narrows recurse. A LargeList, a
        // FixedSizeList or a dictionary around the struct is handed to arrow's cast (which
        // cannot narrow a struct) or passed through whole, so every leaf must stay requested
        // even though a plain List around the same struct narrows.
        let (_, ts_struct, _) = date_ts_struct(0);
        let narrowed_item = struct_of(vec![F::new("d", DataType::Date32, true)]);
        let list_schema = |dt: DataType| Schema::new(vec![Field::new("l", dt, true)]);
        let list_restrict = |physical: DataType, requested: DataType| {
            let schema = list_schema(physical);
            resolve_file_rebase_policies(&schema, default_modes())
                .restrict_to_requested(&schema, &[Some(&requested)], true, false)
                .unrequested_leaves
        };
        let item = |dt: &DataType| Arc::new(F::new("item", dt.clone(), true));
        assert_eq!(
            list_restrict(
                DataType::List(item(&ts_struct)),
                DataType::List(item(&narrowed_item))
            ),
            vec![1]
        );
        assert_eq!(
            list_restrict(
                DataType::LargeList(item(&ts_struct)),
                DataType::LargeList(item(&narrowed_item))
            ),
            Vec::<usize>::new()
        );
        assert_eq!(
            list_restrict(
                DataType::FixedSizeList(item(&ts_struct), 1),
                DataType::FixedSizeList(item(&narrowed_item), 1)
            ),
            Vec::<usize>::new()
        );
        assert_eq!(
            list_restrict(
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(ts_struct.clone())),
                narrowed_item.clone()
            ),
            Vec::<usize>::new()
        );

        // Map: entries pair positionally (key with key, value with value), and a struct value
        // narrows by name beneath it -- but only for the same key ordering, the gate
        // `parquet_convert_array` puts on its map convert; otherwise every leaf stays.
        let entries = |value: DataType, sorted: bool| {
            DataType::Map(
                Arc::new(F::new(
                    "entries",
                    struct_of(vec![
                        F::new("key", DataType::Int64, false),
                        F::new("value", value, true),
                    ]),
                    false,
                )),
                sorted,
            )
        };
        let map_schema = Schema::new(vec![Field::new(
            "m",
            entries(ts_struct.clone(), false),
            true,
        )]);
        let map_policies = resolve_file_rebase_policies(&map_schema, default_modes());
        let requested_value = struct_of(vec![F::new(
            "ts",
            ts_field(TimeUnit::Microsecond).data_type().clone(),
            true,
        )]);
        let map_restrict = |requested: &DataType| {
            map_policies
                .clone()
                .restrict_to_requested(&map_schema, &[Some(requested)], true, false)
                .unrequested_leaves
        };
        assert_eq!(
            map_restrict(&entries(requested_value.clone(), false)),
            vec![1]
        );
        assert_eq!(
            map_restrict(&entries(requested_value, true)),
            Vec::<usize>::new()
        );
    }
}
