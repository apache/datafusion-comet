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

//! Detection of Parquet files whose dates/timestamps were written in the legacy hybrid
//! (Julian + Gregorian) calendar, for `spark.comet.exceptionOnDatetimeRebase`.
//!
//! Spark rebases such values back to the Proleptic Gregorian calendar on read. Comet's native
//! scan does not implement rebasing, so it silently returns shifted values for dates before
//! 1582-10-15 and timestamps before 1900-01-01T00:00:00Z (#5010). This module supplies the
//! signal that lets Comet fail such a scan instead, when the user asks for that.

use arrow::datatypes::{DataType, Fields, Schema};
use parquet::basic::Type as PhysicalType;
use parquet::errors::ParquetError;
use parquet::file::metadata::{KeyValue, ParquetMetaData};
use parquet::schema::types::SchemaDescriptor;

/// Spark's Parquet footer key-value metadata keys, from `org.apache.spark.sql.package`.
const SPARK_VERSION_METADATA_KEY: &str = "org.apache.spark.version";
const SPARK_LEGACY_DATETIME_METADATA_KEY: &str = "org.apache.spark.legacyDateTime";
const SPARK_LEGACY_INT96_METADATA_KEY: &str = "org.apache.spark.legacyINT96";

/// True if this file's dates/timestamps were written in the legacy hybrid calendar and would
/// need rebasing on read.
///
/// Mirrors the LEGACY arm of Spark's `DataSourceUtils.getRebaseSpec`, which is driven entirely by
/// footer metadata: a file needs rebasing if it was written before the version that switched to
/// the Proleptic Gregorian calendar (3.0.0 for DATE/TIMESTAMP_MILLIS/TIMESTAMP_MICROS, 3.1.0 for
/// INT96), or if a later writer explicitly opted back in via
/// `spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY` / `int96RebaseModeInWrite=LEGACY`, which
/// stamps `legacyDateTime` / `legacyINT96` into the footer.
///
/// Files with no `org.apache.spark.version` key were not written by Spark and carry no rebase
/// signal at all. Spark falls back to `spark.sql.parquet.datetimeRebaseModeInRead` for them
/// (default EXCEPTION, which then raises only on an actually-ancient value it has decoded).
/// Comet has no decoded values here — only the footer — so it treats them as not needing
/// rebasing rather than failing every non-Spark Parquet file.
pub(crate) fn written_in_legacy_calendar(metadata: &ParquetMetaData) -> bool {
    let file_metadata = metadata.file_metadata();
    legacy_calendar_from_footer(file_metadata.key_value_metadata(), || {
        has_int96_column(file_metadata.schema_descr())
    })
}

/// The footer-only decision, split out from [`written_in_legacy_calendar`] so it can be tested
/// without a real file. `has_int96_column` is lazy because the INT96 threshold is only consulted
/// for writer versions in [3.0.0, 3.1.0).
fn legacy_calendar_from_footer(
    key_value_metadata: Option<&Vec<KeyValue>>,
    has_int96_column: impl FnOnce() -> bool,
) -> bool {
    let Some(kv) = key_value_metadata else {
        return false;
    };
    let has_key = |key: &str| kv.iter().any(|entry| entry.key == key);
    let version = kv
        .iter()
        .find(|entry| entry.key == SPARK_VERSION_METADATA_KEY)
        .and_then(|entry| entry.value.as_deref());
    let Some(version) = version else {
        return false;
    };

    // Spark compares `SPARK_VERSION_SHORT` to the threshold as a plain string, so we do too --
    // matching Spark matters more here than being right about version ordering, and the two only
    // diverge for a hypothetical major version of 10 or above.
    if version < "3.0.0" || has_key(SPARK_LEGACY_DATETIME_METADATA_KEY) {
        return true;
    }
    // Spark 3.0 rebased INT96 unconditionally; the CORRECTED/LEGACY choice (and the
    // `legacyINT96` footer key that records it) only arrived in 3.1.0.
    if has_key(SPARK_LEGACY_INT96_METADATA_KEY) {
        return true;
    }
    version < "3.1.0" && has_int96_column()
}

fn has_int96_column(schema_descr: &SchemaDescriptor) -> bool {
    schema_descr
        .columns()
        .iter()
        .any(|col| col.physical_type() == PhysicalType::INT96)
}

/// True if reading `schema` can decode a date or timestamp, and therefore can be affected by
/// calendar rebasing. Nothing else in a Parquet file is calendar-sensitive, so a scan that
/// projects only (say) string columns out of a legacy-calendar file is not at risk and must not
/// be failed.
pub(crate) fn reads_date_or_timestamp(schema: &Schema) -> bool {
    fields_have_date_or_timestamp(schema.fields())
}

fn fields_have_date_or_timestamp(fields: &Fields) -> bool {
    fields
        .iter()
        .any(|field| data_type_has_date_or_timestamp(field.data_type()))
}

fn data_type_has_date_or_timestamp(data_type: &DataType) -> bool {
    match data_type {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => data_type_has_date_or_timestamp(field.data_type()),
        DataType::Struct(fields) => fields_have_date_or_timestamp(fields),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| data_type_has_date_or_timestamp(field.data_type())),
        DataType::Dictionary(_, value_type) => data_type_has_date_or_timestamp(value_type),
        DataType::RunEndEncoded(_, values_field) => {
            data_type_has_date_or_timestamp(values_field.data_type())
        }
        _ => false,
    }
}

/// The failure raised for a legacy-calendar file when
/// `spark.comet.exceptionOnDatetimeRebase` is enabled.
///
/// This travels out of the Parquet reader as a `ParquetError`, so the JVM side surfaces it as a
/// `FAILED_READ_FILE` `SparkException` with this text as the cause -- the same envelope Spark's
/// `FileScanRDD` puts a read-time failure in. That envelope names the offending file, so this
/// message does not repeat it.
pub(crate) fn legacy_calendar_error() -> ParquetError {
    ParquetError::General(
        "this file was written using the legacy hybrid (Julian + Gregorian) calendar. Comet's \
         native scan does not rebase dates/timestamps to the Proleptic Gregorian calendar, and so \
         would return incorrect values for dates before 1582-10-15 and timestamps before \
         1900-01-01T00:00:00Z. This scan failed instead because \
         spark.comet.exceptionOnDatetimeRebase is enabled. Set it to false to read these values \
         as-is without rebasing, or disable Comet for this query so that Spark rebases them."
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, TimeUnit};
    use std::sync::Arc;

    fn kv(pairs: &[(&str, Option<&str>)]) -> Vec<KeyValue> {
        pairs
            .iter()
            .map(|(key, value)| KeyValue {
                key: key.to_string(),
                value: value.map(|v| v.to_string()),
            })
            .collect()
    }

    /// `panic` guards that the INT96 probe stays lazy for versions that don't need it.
    fn legacy(pairs: &[(&str, Option<&str>)]) -> bool {
        legacy_calendar_from_footer(Some(&kv(pairs)), || panic!("INT96 probe not expected"))
    }

    fn legacy_with_int96(pairs: &[(&str, Option<&str>)], has_int96: bool) -> bool {
        legacy_calendar_from_footer(Some(&kv(pairs)), || has_int96)
    }

    #[test]
    fn no_footer_metadata_is_not_legacy() {
        assert!(!legacy_calendar_from_footer(None, || panic!(
            "INT96 probe not expected"
        )));
        assert!(!legacy(&[]));
    }

    #[test]
    fn non_spark_writer_is_not_legacy() {
        // No `org.apache.spark.version`: nothing in the footer says the values were rebased on
        // write, and failing every non-Spark file would make the config unusable.
        assert!(!legacy_with_int96(
            &[("parquet-mr version", Some("1.13.1"))],
            true
        ));
    }

    #[test]
    fn pre_spark_3_writer_is_legacy() {
        assert!(legacy(&[(SPARK_VERSION_METADATA_KEY, Some("2.4.8"))]));
        assert!(legacy(&[(SPARK_VERSION_METADATA_KEY, Some("1.6.3"))]));
    }

    #[test]
    fn spark_3_plus_writer_is_not_legacy_by_default() {
        for version in ["3.0.0", "3.1.0", "3.5.9", "4.1.3"] {
            assert!(
                !legacy_with_int96(&[(SPARK_VERSION_METADATA_KEY, Some(version))], false),
                "version {version} should not be legacy"
            );
        }
    }

    #[test]
    fn legacy_datetime_key_marks_a_modern_writer_legacy() {
        // Written by Spark 4.x with datetimeRebaseModeInWrite=LEGACY. The value Spark stamps is
        // the empty string; Spark tests for key presence, not for a value.
        assert!(legacy(&[
            (SPARK_VERSION_METADATA_KEY, Some("4.1.3")),
            (SPARK_LEGACY_DATETIME_METADATA_KEY, Some("")),
        ]));
        assert!(legacy(&[
            (SPARK_VERSION_METADATA_KEY, Some("4.1.3")),
            (SPARK_LEGACY_DATETIME_METADATA_KEY, None),
        ]));
    }

    #[test]
    fn legacy_int96_key_marks_a_modern_writer_legacy() {
        assert!(legacy_with_int96(
            &[
                (SPARK_VERSION_METADATA_KEY, Some("3.5.9")),
                (SPARK_LEGACY_INT96_METADATA_KEY, Some("")),
            ],
            false
        ));
    }

    #[test]
    fn spark_30_int96_is_legacy_only_when_the_file_has_int96_columns() {
        // Spark 3.0 predates int96RebaseModeInWrite, so it stamps no `legacyINT96` key and its
        // INT96 values are hybrid-calendar. Its DATE/TIMESTAMP_MICROS values are not.
        let footer = [(SPARK_VERSION_METADATA_KEY, Some("3.0.3"))];
        assert!(legacy_with_int96(&footer, true));
        assert!(!legacy_with_int96(&footer, false));
    }

    #[test]
    fn version_key_without_a_value_is_not_legacy() {
        assert!(!legacy_with_int96(
            &[(SPARK_VERSION_METADATA_KEY, None)],
            true
        ));
    }

    #[test]
    fn reads_date_or_timestamp_finds_top_level_and_nested_columns() {
        let date = || Arc::new(Field::new("d", DataType::Date32, true));
        let ts = || {
            Arc::new(Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ))
        };
        let string = || Arc::new(Field::new("s", DataType::Utf8, true));

        assert!(reads_date_or_timestamp(&Schema::new(vec![date()])));
        assert!(reads_date_or_timestamp(&Schema::new(vec![ts()])));
        assert!(reads_date_or_timestamp(&Schema::new(vec![Field::new(
            "l",
            DataType::List(date()),
            true
        )])));
        assert!(reads_date_or_timestamp(&Schema::new(vec![Field::new(
            "s",
            DataType::Struct(vec![string(), date()].into()),
            true
        )])));
        // A map whose *value* is a timestamp: the entries struct is the map's single field.
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![string(), ts()].into()),
            false,
        ));
        assert!(reads_date_or_timestamp(&Schema::new(vec![Field::new(
            "m",
            DataType::Map(entries, false),
            true
        )])));
        assert!(reads_date_or_timestamp(&Schema::new(vec![Field::new(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Date32)),
            true
        )])));
    }

    #[test]
    fn reads_date_or_timestamp_ignores_calendar_insensitive_schemas() {
        let schema = Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
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
        assert!(!reads_date_or_timestamp(&schema));
    }
}
