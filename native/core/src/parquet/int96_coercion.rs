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

use arrow::datatypes::{DataType, FieldRef, Schema, TimeUnit};
use datafusion::datasource::physical_plan::parquet::Int96Coercer;
use parquet::arrow::{encode_arrow_schema, parquet_to_arrow_schema, ARROW_SCHEMA_META_KEY};
use parquet::basic::Type as PhysicalType;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{FileMetaData, KeyValue, ParquetMetaData, ParquetMetaDataBuilder};
use parquet::schema::types::ColumnDescPtr;
use std::sync::Arc;

/// The `coerce_int96` resolution of every native scan, in DataFusion's option syntax.
pub(crate) const COERCE_INT96_UNIT: &str = "us";
/// The `coerce_int96_tz` of every native scan. INT96 encodes a UTC-adjusted instant.
pub(crate) const COERCE_INT96_TZ: &str = "UTC";

/// Returns a copy of `metadata` whose `ARROW:schema` hint coerces every INT96 leaf that DataFusion's
/// `Int96Coercer` would leave as `Timestamp(Nanosecond)`, or `None` when it finds them all. Only the
/// file-level key-value list changes: row groups and page indexes are carried over as-is.
///
/// A file with no repeated INT96 column returns `None` without building a schema, since only a
/// column inside a list can have a path the coercer gets wrong. The result already holds the
/// coerced types, so passing it back returns `None`.
///
/// The parquet API cannot carry a file decryptor across this rebuild, so callers must not pass the
/// metadata of an open that supplied decryption properties.
pub(crate) fn coerce_missed_int96_leaves(
    metadata: &ParquetMetaData,
) -> ParquetResult<Option<ParquetMetaData>> {
    let file_metadata = metadata.file_metadata();
    let descriptor = file_metadata.schema_descr();
    let columns = descriptor.columns();
    if !columns
        .iter()
        .any(|column| column.physical_type() == PhysicalType::INT96 && column.max_rep_level() > 0)
    {
        return Ok(None);
    }

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

    let hint = Schema::new_with_metadata(fields, schema.metadata().clone());
    let mut key_values = file_metadata
        .key_value_metadata()
        .into_iter()
        .flatten()
        .filter(|key_value| key_value.key != ARROW_SCHEMA_META_KEY)
        .cloned()
        .collect::<Vec<_>>();
    key_values.push(KeyValue {
        key: ARROW_SCHEMA_META_KEY.to_string(),
        value: Some(encode_arrow_schema(&hint)),
    });
    let file_metadata = FileMetaData::new(
        file_metadata.version(),
        file_metadata.num_rows(),
        file_metadata.created_by().map(str::to_owned),
        Some(key_values),
        file_metadata.schema_descr_ptr(),
        file_metadata.column_orders().cloned(),
    );
    Ok(Some(
        ParquetMetaDataBuilder::new(file_metadata)
            .set_row_groups(metadata.row_groups().to_vec())
            .set_column_index(metadata.column_index().cloned())
            .set_offset_index(metadata.offset_index().cloned())
            .build(),
    ))
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

    const SECONDS: [i64; 4] = [
        253_402_300_799, // 9999-12-31 23:59:59
        -11_676_096_000, // 1600-01-01 00:00:00
        9_246_182_400,   // 2263-01-01 00:00:00
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

    /// The reader the opener builds for `metadata`: the schema arrow-rs derives from the footer,
    /// then DataFusion's own coercion on top, as `ParquetOpener` applies it.
    fn opener_reader_metadata(metadata: ParquetMetaData) -> ArrowReaderMetadata {
        let metadata = Arc::new(metadata);
        let mut options = ArrowReaderOptions::new();
        let mut reader_metadata =
            ArrowReaderMetadata::try_new(Arc::clone(&metadata), options.clone()).unwrap();
        let timezone: Arc<str> = Arc::from(COERCE_INT96_TZ);
        if let Some(coerced) = Int96Coercer::new(
            reader_metadata.parquet_schema(),
            reader_metadata.schema(),
            &TimeUnit::Microsecond,
        )
        .with_timezone(Some(timezone))
        .coerce()
        {
            options = options.with_schema(Arc::new(coerced));
            reader_metadata = ArrowReaderMetadata::try_new(metadata, options).unwrap();
        }
        reader_metadata
    }

    /// Every INT96 leaf of the file as the scan decodes it, in microseconds, one vector per
    /// leaf in column order. Asserts the leaf is read as `Timestamp(Microsecond, UTC)`.
    fn scan_leaves(file: &Bytes, metadata: ParquetMetaData) -> Vec<Vec<i64>> {
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

        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
            file.clone(),
            opener_reader_metadata(metadata),
        )
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
                scan_leaves(&file, coerced),
                vec![expected_micros()],
                "{layout}"
            );
        }
    }

    #[test]
    fn leaves_the_standard_list_layout_to_datafusion() {
        let file = write_file(&format!("message m {{ {STANDARD} }}"), &[3], vec![]);
        let metadata = metadata_of(&file);
        assert!(coerce_missed_int96_leaves(&metadata).unwrap().is_none());
        assert_eq!(scan_leaves(&file, metadata), vec![expected_micros()]);
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
        // `plain` is a column, not a list, and holds the same four values.
        assert_eq!(
            scan_leaves(&file, coerced),
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
        let existing_hint =
            parquet_to_arrow_schema(metadata_of(&plain).file_metadata().schema_descr(), None)
                .unwrap();
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
            coerced.file_metadata().num_rows(),
            metadata.file_metadata().num_rows()
        );
        assert_eq!(
            coerced.file_metadata().schema_descr(),
            metadata.file_metadata().schema_descr()
        );
        assert_eq!(scan_leaves(&file, coerced), vec![expected_micros()]);
    }
}
