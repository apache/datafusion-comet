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

//! Rebuilding a file's [`ParquetMetaData`] with a different footer key-value list.
//!
//! The scan rewrites the footer it hands DataFusion's opener in three places, each by changing the
//! key-value pairs that parquet-rs copies into the Arrow schema it derives: the INT96 leaf stamp
//! (`datetime_rebase::stamp_int96_leaves`), the Variant `ARROW:schema` rewrite
//! (`eager_page_index_reader_factory::with_spark_arrow_schema`) and the INT96 coercion hint
//! (`int96_coercion`). They share this rebuild.

use parquet::file::metadata::{FileMetaData, KeyValue, ParquetMetaData, ParquetMetaDataBuilder};

/// Returns a copy of `metadata` whose footer key-value metadata is `key_values`. Row groups and
/// page indexes are carried over as-is, and the Parquet schema is shared.
///
/// The parquet API cannot carry a file decryptor, nor `FileMetaData`'s crate-private encryption
/// fields (encryption algorithm, footer signing key metadata), across this rebuild. Callers must
/// not rebuild the metadata of an open that supplied decryption properties.
pub(crate) fn with_key_values(
    metadata: &ParquetMetaData,
    key_values: Vec<KeyValue>,
) -> ParquetMetaData {
    let file_metadata = metadata.file_metadata();
    let file_metadata = FileMetaData::new(
        file_metadata.version(),
        file_metadata.num_rows(),
        file_metadata.created_by().map(str::to_owned),
        Some(key_values),
        file_metadata.schema_descr_ptr(),
        file_metadata.column_orders().cloned(),
    );
    ParquetMetaDataBuilder::new(file_metadata)
        .set_row_groups(metadata.row_groups().to_vec())
        .set_column_index(metadata.column_index().cloned())
        .set_offset_index(metadata.offset_index().cloned())
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use parquet::data_type::Int32Type;
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;

    #[test]
    fn replaces_only_the_key_values() {
        let schema = Arc::new(parse_message_type("message m { optional int32 a; }").unwrap());
        let properties = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "old".to_string(),
                "value".to_string(),
            )]))
            .build();
        let mut buffer = Vec::new();
        let mut writer =
            SerializedFileWriter::new(&mut buffer, schema, Arc::new(properties)).unwrap();
        for _ in 0..2 {
            let mut row_group = writer.next_row_group().unwrap();
            let mut column = row_group.next_column().unwrap().unwrap();
            column
                .typed::<Int32Type>()
                .write_batch(&[1, 2, 3], Some(&[1, 1, 1]), None)
                .unwrap();
            column.close().unwrap();
            row_group.close().unwrap();
        }
        writer.close().unwrap();
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Optional)
            .parse_and_finish(&Bytes::from(buffer))
            .unwrap();
        assert!(metadata.column_index().is_some() && metadata.offset_index().is_some());

        let rebuilt = with_key_values(
            &metadata,
            vec![KeyValue::new("new".to_string(), "entry".to_string())],
        );

        let entries = rebuilt.file_metadata().key_value_metadata().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            (entries[0].key.as_str(), entries[0].value.as_deref()),
            ("new", Some("entry"))
        );
        assert_eq!(rebuilt.num_row_groups(), 2);
        assert_eq!(rebuilt.row_groups(), metadata.row_groups());
        assert_eq!(rebuilt.column_index(), metadata.column_index());
        assert_eq!(rebuilt.offset_index(), metadata.offset_index());
        let (before, after) = (metadata.file_metadata(), rebuilt.file_metadata());
        assert_eq!(after.schema_descr(), before.schema_descr());
        assert_eq!(after.num_rows(), before.num_rows());
        assert_eq!(after.version(), before.version());
        assert_eq!(after.created_by(), before.created_by());
    }
}
