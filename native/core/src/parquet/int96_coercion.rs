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

//! INT96 coercion for the leaves DataFusion's `Int96Coercer` does not find.
//!
//! The scan sets `coerce_int96`, so DataFusion's opener rewrites every INT96 column's Arrow type
//! from `Timestamp(Nanosecond)` to `Timestamp(Microsecond, UTC)` before arrow-rs decodes it. A
//! nanosecond count only spans 1677-09-21 to 2262-04-11, so a leaf the rewrite misses decodes to
//! a wrapped value outside that range, with no error.
//!
//! `Int96Coercer` finds a leaf by rebuilding its Parquet column path from the Arrow schema, and it
//! assumes every list is the standard `list` / `element` layout. A list in another layout has a
//! different path, so its leaf is never coerced: the `bag` / `array` form Spark writes under
//! `spark.sql.parquet.writeLegacyFormat=true`, the `bag` / `array_element` form Hive writes, and
//! the two-level `repeated int96` form. Tracked as apache/datafusion#26393 upstream and
//! apache/datafusion-comet#7029 here.
//!
//! The fix pairs each Arrow leaf with its Parquet column by ordinal, which holds for any layout,
//! and hands the opener the coerced types as the footer's `ARROW:schema` hint. arrow-rs applies the
//! hint when it derives the file's schema, so the opener sees the leaves already coerced and its
//! own coercion skips them. The hint is the schema the opener would have derived anyway, with only
//! those leaves' types changed.
//!
//! Cost: a file with no INT96 leaf under a non-standard repeated group (Spark's default layout, and
//! every file with no INT96 in a list) costs one walk of the Parquet schema tree per INT96 leaf and
//! rebuilds nothing. A file that needs the hint pays a second schema derivation and a copy of its
//! row groups and page indexes on every open, which is linear in its columns times row groups and
//! far below the Thrift decode that the footer cache saves (about 1 ms for 100 columns by 10 row
//! groups, about 70 ms for 3000 by 50, in a debug build).

use crate::parquet::footer_rewrite::with_key_values;
use arrow::datatypes::{DataType, FieldRef, Schema, TimeUnit};
use datafusion::datasource::physical_plan::parquet::Int96Coercer;
use parquet::arrow::{encode_arrow_schema, parquet_to_arrow_schema, ARROW_SCHEMA_META_KEY};
use parquet::basic::{ConvertedType, LogicalType, Repetition, Type as PhysicalType};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{KeyValue, ParquetMetaData};
use parquet::schema::types::{ColumnDescPtr, SchemaDescriptor, Type};
use std::collections::HashMap;
use std::sync::Arc;

/// The `coerce_int96` resolution of every native scan, in DataFusion's option syntax. It must
/// name the `TimeUnit::Microsecond` this module coerces to.
pub(crate) const COERCE_INT96_UNIT: &str = "us";
/// The `coerce_int96_tz` of every native scan. INT96 encodes a UTC-adjusted instant.
pub(crate) const COERCE_INT96_TZ: &str = "UTC";

/// Returns a copy of `metadata` whose `ARROW:schema` hint coerces every INT96 leaf that DataFusion's
/// `Int96Coercer` would leave as `Timestamp(Nanosecond)`, or `None` when it finds them all. Only the
/// file-level key-value list changes: row groups and page indexes are carried over as-is.
///
/// A file whose INT96 leaves all sit under no repeated group, or only under the standard `list`
/// and `key_value` groups, returns `None` without building a schema (see
/// [`has_unrecognized_repeated_int96`]). The result already holds the coerced types, so passing
/// it back returns `None`.
///
/// The parquet API cannot carry a file decryptor across this rebuild, so callers must not pass the
/// metadata of an open that supplied decryption properties.
pub(crate) fn coerce_missed_int96_leaves(
    metadata: &ParquetMetaData,
) -> ParquetResult<Option<ParquetMetaData>> {
    let file_metadata = metadata.file_metadata();
    let descriptor = file_metadata.schema_descr();
    if !has_unrecognized_repeated_int96(descriptor) {
        return Ok(None);
    }
    let columns = descriptor.columns();

    // The schema the opener starts from: the footer's own key-value metadata, hint included.
    let schema = parquet_to_arrow_schema(descriptor, file_metadata.key_value_metadata())?;
    let unit = TimeUnit::Microsecond;
    let timezone: Arc<str> = Arc::from(COERCE_INT96_TZ);
    // What the opener's coercion does on its own; this pass takes over only what it leaves.
    let schema = Int96Coercer::new(descriptor, &schema, &unit)
        .with_timezone(Some(Arc::clone(&timezone)))
        .coerce()
        .unwrap_or(schema);

    let mut leaf = 0;
    let mut missed = false;
    let fields = schema
        .fields()
        .iter()
        .map(|field| coerce_field(field, columns, &mut leaf, &unit, &timezone, &mut missed))
        .collect::<ParquetResult<Vec<_>>>()?;
    if leaf != columns.len() {
        return Err(ParquetError::General(
            "Parquet schema contains more leaves than the Arrow schema".to_string(),
        ));
    }
    if !missed {
        return Ok(None);
    }

    let mut key_values = file_metadata
        .key_value_metadata()
        .into_iter()
        .flatten()
        .filter(|key_value| key_value.key != ARROW_SCHEMA_META_KEY)
        .cloned()
        .collect::<Vec<_>>();
    // arrow-rs reads the footer's other entries into the schema metadata by itself, so the hint
    // carries only what the footer does not (the metadata of a hint that was already there).
    let hint_metadata: HashMap<String, String> = schema
        .metadata()
        .iter()
        .filter(|(key, value)| {
            !key_values
                .iter()
                .any(|entry| &entry.key == *key && entry.value.as_deref() == Some(value.as_str()))
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let hint = Schema::new_with_metadata(fields, hint_metadata);
    key_values.push(KeyValue {
        key: ARROW_SCHEMA_META_KEY.to_string(),
        value: Some(encode_arrow_schema(&hint)),
    });
    Ok(Some(with_key_values(metadata, key_values)))
}

/// Whether some INT96 column sits under a repeated group that is not one of the two the coercer's
/// path matching handles: the `list` group of a `LIST` (the standard layout), or the `key_value`
/// group of a `MAP`. A repeated primitive counts, since it has no group above it.
///
/// This is a conservative gate for the work above. A file it admits that the coercer already
/// handles costs a wasted pass and gets no hint, and a file it rejects reads exactly as before, so
/// it must never reject a layout the coercer misses.
fn has_unrecognized_repeated_int96(descriptor: &SchemaDescriptor) -> bool {
    descriptor.columns().iter().any(|column| {
        column.physical_type() == PhysicalType::INT96
            && column.max_rep_level() > 0
            && !repeated_nodes_are_standard(descriptor.root_schema(), column.path().parts())
    })
}

/// Whether every repeated node on `path` below `root` is the standard repeated group of its
/// parent. A node the path names but the schema lacks counts as nonstandard.
fn repeated_nodes_are_standard(root: &Type, path: &[String]) -> bool {
    let mut parent = root;
    for part in path {
        let Some(node) = parent
            .get_fields()
            .iter()
            .find(|field| field.name() == part.as_str())
        else {
            return false;
        };
        let info = node.get_basic_info();
        if info.has_repetition() && info.repetition() == Repetition::REPEATED {
            let standard = node.is_group()
                && match node.name() {
                    "list" => is_list(parent),
                    "key_value" => is_map(parent),
                    _ => false,
                };
            if !standard {
                return false;
            }
        }
        parent = node;
    }
    true
}

fn is_list(node: &Type) -> bool {
    let info = node.get_basic_info();
    matches!(info.logical_type_ref(), Some(LogicalType::List))
        || info.converted_type() == ConvertedType::LIST
}

fn is_map(node: &Type) -> bool {
    let info = node.get_basic_info();
    matches!(info.logical_type_ref(), Some(LogicalType::Map))
        || matches!(
            info.converted_type(),
            ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE
        )
}

/// `field` with each INT96 leaf under it that is still `Timestamp(Nanosecond)` coerced to
/// `Timestamp(unit, timezone)`, setting `missed` when it coerces one. `leaf` is the ordinal of the
/// next Parquet column and advances over every leaf under `field`, in the depth-first order both
/// schemas list them in.
fn coerce_field(
    field: &FieldRef,
    columns: &[ColumnDescPtr],
    leaf: &mut usize,
    unit: &TimeUnit,
    timezone: &Arc<str>,
    missed: &mut bool,
) -> ParquetResult<FieldRef> {
    let mut child = |field: &FieldRef| coerce_field(field, columns, leaf, unit, timezone, missed);
    let data_type = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(&mut child)
                .collect::<ParquetResult<Vec<_>>>()?
                .into(),
        ),
        DataType::List(item) => DataType::List(child(item)?),
        DataType::LargeList(item) => DataType::LargeList(child(item)?),
        DataType::FixedSizeList(item, size) => DataType::FixedSizeList(child(item)?, *size),
        DataType::ListView(item) => DataType::ListView(child(item)?),
        DataType::LargeListView(item) => DataType::LargeListView(child(item)?),
        DataType::Map(entries, sorted) => DataType::Map(child(entries)?, *sorted),
        data_type => {
            let column = columns.get(*leaf).ok_or_else(|| {
                ParquetError::General(
                    "Arrow schema contains more leaves than the Parquet schema".to_string(),
                )
            })?;
            *leaf += 1;
            return Ok(
                if column.physical_type() == PhysicalType::INT96
                    && *data_type == DataType::Timestamp(TimeUnit::Nanosecond, None)
                {
                    *missed = true;
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(DataType::Timestamp(*unit, Some(Arc::clone(timezone)))),
                    )
                } else {
                    Arc::clone(field)
                },
            );
        }
    };
    Ok(Arc::new(field.as_ref().clone().with_data_type(data_type)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::TimestampMicrosecondType;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
    };
    use parquet::data_type::{Int96, Int96Type};
    use parquet::file::metadata::ParquetMetaDataReader;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;

    const SECONDS: [i64; 5] = [
        253_402_300_799, // 9999-12-31 23:59:59
        -11_676_096_000, // 1600-01-01 00:00:00
        9_246_182_400,   // 2263-01-01 00:00:00
        -30_610_224_000, // 1000-01-01 00:00:00, before the 1582 calendar cutover
        1_577_836_800,   // 2020-01-01 00:00:00
    ];

    const STANDARD: &str =
        "optional group at (LIST) { repeated group list { optional int96 element; } }";
    const SPARK_LEGACY: &str =
        "optional group at (LIST) { repeated group bag { optional int96 array; } }";
    const HIVE: &str =
        "optional group at (LIST) { repeated group bag { optional int96 array_element; } }";
    const TWO_LEVEL: &str = "optional group at (LIST) { repeated int96 array; }";
    const BARE_REPEATED: &str = "repeated int96 at;";

    fn int96(unix_seconds: i64) -> Int96 {
        let nanos_of_day = unix_seconds.rem_euclid(86_400) * 1_000_000_000;
        let julian_day = unix_seconds.div_euclid(86_400) + 2_440_588;
        let mut value = Int96::new();
        value.set_data(
            nanos_of_day as u32,
            (nanos_of_day >> 32) as u32,
            julian_day as u32,
        );
        value
    }

    /// Writes a file of `message` whose leaf columns hold `SECONDS`, one single-element list (or
    /// struct holding one) per row. `max_defs` is each leaf's maximum definition level, in order.
    fn write_file(message: &str, max_defs: &[i16], key_values: Vec<KeyValue>) -> Bytes {
        let schema = Arc::new(parse_message_type(message).unwrap());
        let properties = WriterProperties::builder()
            .set_key_value_metadata(Some(key_values))
            .build();
        let mut buffer = Vec::new();
        let mut writer =
            SerializedFileWriter::new(&mut buffer, schema, Arc::new(properties)).unwrap();
        let mut row_group = writer.next_row_group().unwrap();
        let values: Vec<Int96> = SECONDS.iter().map(|s| int96(*s)).collect();
        for max_def in max_defs {
            let mut column = row_group.next_column().unwrap().unwrap();
            column
                .typed::<Int96Type>()
                .write_batch(
                    &values,
                    Some(&vec![*max_def; values.len()]),
                    Some(&vec![0; values.len()]),
                )
                .unwrap();
            column.close().unwrap();
        }
        assert!(row_group.next_column().unwrap().is_none());
        row_group.close().unwrap();
        writer.close().unwrap();
        Bytes::from(buffer)
    }

    fn metadata_of(file: &Bytes) -> ParquetMetaData {
        ParquetMetaDataReader::new().parse_and_finish(file).unwrap()
    }

    /// Every INT96 leaf of the file as arrow-rs decodes it from `metadata` alone, in
    /// microseconds, one vector per leaf in column order. Asserts each leaf is decoded as
    /// `Timestamp(Microsecond, UTC)`, which is only possible when the footer's hint says so.
    fn decode_leaves(file: &Bytes, metadata: ParquetMetaData) -> Vec<Vec<i64>> {
        fn collect(array: &dyn Array, out: &mut Vec<Vec<i64>>) {
            match array.data_type() {
                DataType::Struct(_) => {
                    for column in array.as_struct().columns() {
                        collect(column.as_ref(), out);
                    }
                }
                DataType::List(_) => collect(array.as_list::<i32>().values().as_ref(), out),
                DataType::Timestamp(unit, timezone) => {
                    assert_eq!(*unit, TimeUnit::Microsecond);
                    assert_eq!(timezone.as_deref(), Some("UTC"));
                    out.push(
                        array
                            .as_primitive::<TimestampMicrosecondType>()
                            .values()
                            .to_vec(),
                    );
                }
                other => panic!("unexpected type {other}"),
            }
        }

        let reader_metadata =
            ArrowReaderMetadata::try_new(Arc::new(metadata), ArrowReaderOptions::new()).unwrap();
        let reader =
            ParquetRecordBatchReaderBuilder::new_with_metadata(file.clone(), reader_metadata)
                .build()
                .unwrap();
        let mut leaves: Vec<Vec<i64>> = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let mut batch_leaves = Vec::new();
            for column in batch.columns() {
                collect(column.as_ref(), &mut batch_leaves);
            }
            if leaves.is_empty() {
                leaves = batch_leaves;
            } else {
                for (all, new) in leaves.iter_mut().zip(batch_leaves) {
                    all.extend(new);
                }
            }
        }
        leaves
    }

    fn expected_micros() -> Vec<i64> {
        SECONDS.iter().map(|s| s * 1_000_000).collect()
    }

    fn descriptor_of(message: &str) -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(parse_message_type(message).unwrap()))
    }

    #[test]
    fn coerces_the_leaf_of_every_legacy_list_layout() {
        for (layout, max_def) in [
            (SPARK_LEGACY, 3),
            (HIVE, 3),
            (TWO_LEVEL, 2),
            (BARE_REPEATED, 1),
        ] {
            let file = write_file(&format!("message m {{ {layout} }}"), &[max_def], vec![]);
            let metadata = metadata_of(&file);
            // Fails once DataFusion's own coercion handles these layouts (apache/datafusion#26393),
            // which is the signal to remove this module.
            let coerced = coerce_missed_int96_leaves(&metadata)
                .unwrap()
                .unwrap_or_else(|| panic!("{layout}: DataFusion already coerces this layout"));
            assert_eq!(
                decode_leaves(&file, coerced),
                vec![expected_micros()],
                "{layout}"
            );
        }
    }

    #[test]
    fn leaves_the_standard_list_layout_to_datafusion() {
        let file = write_file(&format!("message m {{ {STANDARD} }}"), &[3], vec![]);
        assert!(coerce_missed_int96_leaves(&metadata_of(&file))
            .unwrap()
            .is_none());
    }

    #[test]
    fn ignores_a_file_without_a_repeated_int96_column() {
        let file = write_file("message m { optional int96 ts; }", &[1], vec![]);
        assert!(coerce_missed_int96_leaves(&metadata_of(&file))
            .unwrap()
            .is_none());
    }

    #[test]
    fn coerces_only_the_missed_leaves_of_a_mixed_schema() {
        // A standard list, a legacy list, a top-level INT96 and a legacy list inside a struct.
        let message = format!(
            "message m {{ {STANDARD} {SPARK_LEGACY} optional int96 plain; \
             optional group s {{ optional group inner (LIST) {{ \
             repeated group bag {{ optional int96 array; }} }} }} }}"
        );
        let file = write_file(&message, &[3, 3, 1, 4], vec![]);
        let coerced = coerce_missed_int96_leaves(&metadata_of(&file))
            .unwrap()
            .expect("the legacy lists are missed");
        // `plain` is a column, not a list, and holds the same values.
        assert_eq!(
            decode_leaves(&file, coerced),
            vec![expected_micros(); 4],
            "all four leaves read as microseconds"
        );
    }

    #[test]
    fn a_second_pass_finds_nothing_left_to_coerce() {
        let file = write_file(&format!("message m {{ {SPARK_LEGACY} }}"), &[3], vec![]);
        let coerced = coerce_missed_int96_leaves(&metadata_of(&file))
            .unwrap()
            .unwrap();
        assert!(coerce_missed_int96_leaves(&coerced).unwrap().is_none());
    }

    #[test]
    fn keeps_the_other_footer_entries_and_the_row_groups() {
        let message = format!("message m {{ {SPARK_LEGACY} }}");
        // A footer that already carries a (valid) hint, as a writer such as arrow-rs leaves.
        let plain = write_file(&message, &[3], vec![]);
        let mut existing_hint =
            parquet_to_arrow_schema(metadata_of(&plain).file_metadata().schema_descr(), None)
                .unwrap();
        existing_hint
            .metadata
            .insert("from.hint".to_string(), "kept".to_string());
        let key_values = vec![
            KeyValue::new("org.apache.spark.version".to_string(), "3.5.9".to_string()),
            KeyValue::new(
                ARROW_SCHEMA_META_KEY.to_string(),
                encode_arrow_schema(&existing_hint),
            ),
        ];
        let file = write_file(&message, &[3], key_values);
        let metadata = metadata_of(&file);
        let coerced = coerce_missed_int96_leaves(&metadata).unwrap().unwrap();

        let entries = coerced.file_metadata().key_value_metadata().unwrap();
        // The hint is replaced, not duplicated, and the rest of the footer is untouched.
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry.key == ARROW_SCHEMA_META_KEY)
                .count(),
            1
        );
        assert!(entries
            .iter()
            .any(|entry| entry.key == "org.apache.spark.version"
                && entry.value.as_deref() == Some("3.5.9")));
        assert_eq!(coerced.num_row_groups(), metadata.num_row_groups());
        assert_eq!(
            coerced.file_metadata().schema_descr(),
            metadata.file_metadata().schema_descr()
        );
        // The schema the opener derives keeps both the footer's entries and the old hint's.
        let derived = parquet_to_arrow_schema(
            coerced.file_metadata().schema_descr(),
            coerced.file_metadata().key_value_metadata(),
        )
        .unwrap();
        assert_eq!(
            derived.metadata().get("org.apache.spark.version"),
            Some(&"3.5.9".to_string())
        );
        assert_eq!(
            derived.metadata().get("from.hint"),
            Some(&"kept".to_string())
        );
        assert_eq!(decode_leaves(&file, coerced), vec![expected_micros()]);
    }

    #[test]
    fn the_hint_does_not_repeat_the_footers_own_entries() {
        let key_values = vec![KeyValue::new(
            "org.apache.spark.sql.parquet.row.metadata".to_string(),
            "x".repeat(10_000),
        )];
        let file = write_file(&format!("message m {{ {SPARK_LEGACY} }}"), &[3], key_values);
        let coerced = coerce_missed_int96_leaves(&metadata_of(&file))
            .unwrap()
            .unwrap();
        let hint = coerced
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .find(|entry| entry.key == ARROW_SCHEMA_META_KEY)
            .and_then(|entry| entry.value.as_ref())
            .unwrap();
        assert!(
            hint.len() < 2_000,
            "the hint repeats the footer: {} bytes",
            hint.len()
        );
    }

    #[test]
    fn only_a_nonstandard_repeated_group_above_an_int96_leaf_is_admitted() {
        for (layout, admitted) in [
            (STANDARD, false),
            (SPARK_LEGACY, true),
            (HIVE, true),
            (TWO_LEVEL, true),
            (BARE_REPEATED, true),
            // The standard group names without the annotation of their parent are not a LIST.
            (
                "repeated group list { optional int96 element; }",
                true,
            ),
            (
                "optional group at { repeated group list { optional int96 element; } }",
                true,
            ),
            // A standard list holding a struct, a standard list in a list, and a map.
            (
                "optional group at (LIST) { repeated group list { optional group element { optional int96 ts; } } }",
                false,
            ),
            (
                "optional group at (LIST) { repeated group list { optional group element (LIST) { \
                 repeated group list { optional int96 element; } } } }",
                false,
            ),
            (
                "optional group at (MAP) { repeated group key_value { required binary key (STRING); \
                 optional int96 value; } }",
                false,
            ),
            // A map-shaped group with another name, and a legacy list nested in a standard one.
            (
                "optional group at (MAP) { repeated group map { required binary key (STRING); \
                 optional int96 value; } }",
                true,
            ),
            (
                "optional group at (LIST) { repeated group list { optional group element (LIST) { \
                 repeated group bag { optional int96 array; } } } }",
                true,
            ),
            // No INT96 under a repeated group at all.
            ("optional int96 plain;", false),
            ("optional group at (LIST) { repeated group bag { optional int32 array; } }", false),
        ] {
            let descriptor = descriptor_of(&format!("message m {{ {layout} }}"));
            assert_eq!(has_unrecognized_repeated_int96(&descriptor), admitted, "{layout}");
        }
    }
}
