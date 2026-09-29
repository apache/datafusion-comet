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

//! Carries the Parquet field ids of struct, list and map fields past DataFusion's INT96
//! coercion.
//!
//! When a file holds an INT96 leaf, DataFusion 55.1's `Int96Coercer` rebuilds every struct,
//! list and map field of the file's Arrow schema without its metadata, so `PARQUET:field_id` is
//! gone from those fields in the schema `SparkPhysicalExprAdapterFactory::create` receives, and
//! from the batches the decoder produces. Spark's `clipParquetGroupFields` matches the id on the
//! raw Parquet group, so a container whose id sits on the container alone read as null
//! (#6131).
//!
//! The coercion keeps the schema-level metadata, which parquet-rs fills from the footer's
//! key-value list. So `EagerPageIndexReader::get_metadata` records every field's name and id
//! under [`FIELD_IDS_KEY`] in the footer it returns, and the schema adapter puts the ids back
//! with [`restore_field_ids`]. Delete this module once DataFusion carries
//! apache/datafusion#24790; `int96_coercion_drops_container_ids` below fails at that point.

use crate::parquet::eager_page_index_reader_factory::contains_field_ids;
use crate::parquet::parquet_support::field_id;
use arrow::datatypes::{DataType, FieldRef, Fields, Schema, SchemaRef};
use parquet::arrow::{parquet_to_arrow_schema, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Type as PhysicalType;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{FileMetaData, KeyValue};
use parquet::schema::types::SchemaDescriptor;
use std::sync::Arc;

/// Footer key, and so Arrow schema metadata key, of the stamp. Its value is a JSON array with
/// one `[name, id or null]` pair per Arrow field, in pre-order.
pub(crate) const FIELD_IDS_KEY: &str = "comet.parquet.field_ids";

/// The field types whose id the INT96 coercion drops. It rebuilds these three and clones every
/// other field, including the other list types, unchanged.
fn loses_id(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _)
    )
}

/// The child fields of `data_type`, in the order the walks below visit them. Every list type
/// and Map is descended, so a walk over a schema converted with an `ARROW:schema` hint visits
/// the same fields as one over the hint-free conversion. Dictionary is a leaf.
fn children(data_type: &DataType) -> &[FieldRef] {
    match data_type {
        DataType::Struct(fields) => fields,
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::FixedSizeList(child, _)
        | DataType::ListView(child)
        | DataType::LargeListView(child)
        | DataType::Map(child, _) => std::slice::from_ref(child),
        _ => &[],
    }
}

fn collect_ids<'a>(field: &'a FieldRef, nodes: &mut Vec<(&'a str, Option<i32>)>) {
    nodes.push((field.name(), field_id(field)));
    for child in children(field.data_type()) {
        collect_ids(child, nodes);
    }
}

/// The stamp for a file, or `None` when the coercion loses no id: the file has no INT96 leaf,
/// or none of its struct, list and map fields carries an id. The ids come from the hint-free
/// Arrow conversion, which takes them from the Parquet schema, as Spark does.
fn field_ids_stamp(schema: &SchemaDescriptor) -> ParquetResult<Option<String>> {
    let has_int96 = schema
        .columns()
        .iter()
        .any(|column| column.physical_type() == PhysicalType::INT96);
    // Without an id below the message root no Arrow field carries one; skip the conversion.
    // Leaves count too: a repeated primitive without a LIST annotation becomes a list that
    // carries the primitive's id.
    if !has_int96
        || !schema
            .root_schema()
            .get_fields()
            .iter()
            .any(|field| contains_field_ids(field))
    {
        return Ok(None);
    }
    let arrow_schema = parquet_to_arrow_schema(schema, None)?;
    if !arrow_schema.fields().iter().any(has_container_id) {
        return Ok(None);
    }
    let mut nodes = Vec::new();
    for field in arrow_schema.fields() {
        collect_ids(field, &mut nodes);
    }
    serde_json::to_string(&nodes)
        .map(Some)
        .map_err(|e| ParquetError::External(Box::new(e)))
}

fn has_container_id(field: &FieldRef) -> bool {
    (loses_id(field.data_type()) && field_id(field).is_some())
        || children(field.data_type()).iter().any(has_container_id)
}

/// How [`with_field_ids_stamp`] changed the footer key-value list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StampChange {
    /// The list is as it was.
    Unchanged,
    /// The file needs the stamp, which was added.
    Added,
    /// The file needs no stamp but its footer carried a [`FIELD_IDS_KEY`] entry, which was
    /// removed.
    Removed,
}

/// The footer key-value list with this file's stamp in place, or `key_values` unchanged when
/// the file needs none, and what changed. `key_values` is the list an earlier footer rewrite
/// produced, if any; `None` in and out means the footer's own list stands.
///
/// A [`FIELD_IDS_KEY`] entry in the footer's own list is removed, so that list, which parquet-rs
/// turns into the Arrow schema metadata, holds only a stamp computed here. parquet-rs also
/// merges the custom metadata of an `ARROW:schema` hint into the schema metadata, and a key
/// there survives when the file needs no stamp (a footer entry takes precedence over it). It
/// can do no more than the hint itself, which can already put `PARQUET:field_id` on any field:
/// [`restore_field_ids`] only adds ids to fields that lack one, after checking the field count,
/// every name and every id the schema carries.
pub(crate) fn with_field_ids_stamp(
    file: &FileMetaData,
    key_values: Option<Vec<KeyValue>>,
) -> ParquetResult<(Option<Vec<KeyValue>>, StampChange)> {
    let stamp = field_ids_stamp(file.schema_descr())?;
    let current = key_values
        .as_deref()
        .or(file.key_value_metadata().map(Vec::as_slice))
        .unwrap_or_default();
    let change = match (&stamp, current.iter().any(|kv| kv.key == FIELD_IDS_KEY)) {
        (Some(_), _) => StampChange::Added,
        (None, true) => StampChange::Removed,
        (None, false) => return Ok((key_values, StampChange::Unchanged)),
    };
    let mut key_values =
        key_values.unwrap_or_else(|| file.key_value_metadata().cloned().unwrap_or_default());
    key_values.retain(|kv| kv.key != FIELD_IDS_KEY);
    key_values.extend(stamp.map(|value| KeyValue::new(FIELD_IDS_KEY.to_string(), value)));
    Ok((Some(key_values), change))
}

/// Put back the ids the INT96 coercion dropped from `schema`'s struct, list and map fields,
/// using the stamp in its metadata. Only a missing `PARQUET:field_id` is added, and only where
/// the coercion rebuilds: on a struct, list or map field whose ancestors are all structs, lists
/// or maps. The walk checks the stamp against the schema, field count, every name, and every id
/// the schema still carries, and on any mismatch, or without a stamp, returns `schema` itself.
///
/// The stamp is trusted as computed from the file's Parquet schema because
/// `EagerPageIndexReaderFactory` removes any footer entry of that name before adding its own
/// (see [`with_field_ids_stamp`]). Only `init_datasource_exec` turns field-id matching on today,
/// and it always reads through that factory.
pub(crate) fn restore_field_ids(schema: &SchemaRef) -> SchemaRef {
    let Some(stamp) = schema.metadata().get(FIELD_IDS_KEY) else {
        return Arc::clone(schema);
    };
    let nodes = match serde_json::from_str::<Vec<(String, Option<i32>)>>(stamp) {
        Ok(nodes) => nodes,
        Err(e) => {
            log::debug!("Parquet field id stamp is not valid ({e}); ids not restored");
            return Arc::clone(schema);
        }
    };
    let mut next = 0;
    let fields = schema
        .fields()
        .iter()
        .map(|field| restore_field(field, &nodes, &mut next, true))
        .collect::<Option<Vec<_>>>();
    match fields {
        Some(fields) if next == nodes.len() => {
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
        }
        _ => {
            log::debug!(
                "Parquet field id stamp with {} entries does not match the file schema of {} \
                 root fields (last entry checked: {next}, {:?}); ids not restored",
                nodes.len(),
                schema.fields().len(),
                next.checked_sub(1).and_then(|i| nodes.get(i)),
            );
            Arc::clone(schema)
        }
    }
}

/// Restore `field` and its subtree against the stamp entries from `next` on, or `None` on a
/// mismatch. `restorable` is false below a field the coercion leaves alone.
fn restore_field(
    field: &FieldRef,
    nodes: &[(String, Option<i32>)],
    next: &mut usize,
    restorable: bool,
) -> Option<FieldRef> {
    let (name, stamped_id) = nodes.get(*next)?;
    *next += 1;
    let current_id = field_id(field);
    if name != field.name() || current_id.is_some_and(|id| Some(id) != *stamped_id) {
        return None;
    }
    let restorable = restorable && loses_id(field.data_type());
    let mut restore = |child: &FieldRef| restore_field(child, nodes, next, restorable);
    let data_type = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(&mut restore)
                .collect::<Option<Fields>>()?,
        ),
        DataType::List(child) => DataType::List(restore(child)?),
        DataType::LargeList(child) => DataType::LargeList(restore(child)?),
        DataType::FixedSizeList(child, size) => DataType::FixedSizeList(restore(child)?, *size),
        DataType::ListView(child) => DataType::ListView(restore(child)?),
        DataType::LargeListView(child) => DataType::LargeListView(restore(child)?),
        DataType::Map(child, sorted) => DataType::Map(restore(child)?, *sorted),
        other => other.clone(),
    };
    let mut restored = field.as_ref().clone().with_data_type(data_type);
    if let (true, None, Some(id)) = (restorable, current_id, stamped_id) {
        let mut metadata = field.metadata().clone();
        metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
        restored = restored.with_metadata(metadata);
    }
    Some(Arc::new(restored))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, TimeUnit};
    use datafusion::datasource::physical_plan::parquet::Int96Coercer;
    use parquet::schema::parser::parse_message_type;
    use std::collections::HashMap;

    /// A struct, a list and a map with ids on the containers and some leaves, next to INT96
    /// leaves. The map's `key_value` group never carries an Arrow id.
    const NESTED: &str = "message schema {
        optional group s = 1 { optional int32 a = 2; optional int96 ts; }
        optional group l (LIST) = 3 { repeated group list { optional int96 element = 4; } }
        optional group m (MAP) = 5 {
            repeated group key_value { required binary key (STRING); optional int32 value = 6; }
        }
    }";

    /// The ids of `NESTED`'s Arrow fields in pre-order: s, a, ts, l, element, m, key_value,
    /// key, value.
    const NESTED_IDS: [Option<i32>; 9] = [
        Some(1),
        Some(2),
        None,
        Some(3),
        Some(4),
        Some(5),
        None,
        None,
        Some(6),
    ];

    fn descriptor(message: &str) -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(parse_message_type(message).unwrap()))
    }

    fn ids_in_pre_order(schema: &Schema) -> Vec<Option<i32>> {
        fn walk(field: &FieldRef, ids: &mut Vec<Option<i32>>) {
            ids.push(field_id(field));
            for child in children(field.data_type()) {
                walk(child, ids);
            }
        }
        let mut ids = Vec::new();
        for field in schema.fields() {
            walk(field, &mut ids);
        }
        ids
    }

    /// The Arrow schema DataFusion hands the schema adapter for `NESTED`, with `stamp` in its
    /// metadata as parquet-rs copies it there from the footer.
    fn coerced(stamp: Option<String>) -> SchemaRef {
        let descriptor = descriptor(NESTED);
        let metadata: HashMap<String, String> = stamp
            .into_iter()
            .map(|value| (FIELD_IDS_KEY.to_string(), value))
            .collect();
        let file_schema = parquet_to_arrow_schema(&descriptor, None)
            .unwrap()
            .with_metadata(metadata);
        Arc::new(
            Int96Coercer::new(&descriptor, &file_schema, &TimeUnit::Microsecond)
                .with_timezone(Some(Arc::from("UTC")))
                .coerce()
                .unwrap(),
        )
    }

    fn stamp_of(nodes: &[(&str, Option<i32>)]) -> String {
        serde_json::to_string(nodes).unwrap()
    }

    #[test]
    fn stamp_lists_every_field_in_pre_order() {
        let stamp = field_ids_stamp(&descriptor(NESTED)).unwrap().unwrap();
        assert_eq!(
            stamp,
            r#"[["s",1],["a",2],["ts",null],["l",3],["element",4],["m",5],["key_value",null],["key",null],["value",6]]"#
        );
    }

    /// A repeated primitive without a LIST annotation becomes a list that carries the
    /// primitive's id, with an item named after it, although no group carries an id.
    #[test]
    fn stamp_covers_a_legacy_repeated_primitive() {
        let message = "message schema { repeated int32 x = 5; optional int96 ts; }";
        assert_eq!(
            field_ids_stamp(&descriptor(message)).unwrap().as_deref(),
            Some(r#"[["x",5],["x",null],["ts",null]]"#)
        );
    }

    #[test]
    fn stamp_is_needed_only_for_container_ids_next_to_int96() {
        let leaf_ids_only = "message schema {
            optional group s { optional int32 a = 2; optional int96 ts = 3; }
        }";
        let without_int96 = "message schema {
            optional group s = 1 { optional int32 a; optional int64 ts; }
        }";
        let list_group_id_only = "message schema {
            optional group l (LIST) { repeated group list = 5 { optional int96 element; } }
        }";
        for message in [leaf_ids_only, without_int96, list_group_id_only] {
            assert_eq!(field_ids_stamp(&descriptor(message)).unwrap(), None);
        }
    }

    /// Pins DataFusion's behavior this module works around. When a DataFusion upgrade carries
    /// apache/datafusion#24790, this fails: delete this module and its call sites in
    /// `eager_page_index_reader_factory.rs`, `schema_adapter.rs` and `cast_column.rs`.
    #[test]
    fn int96_coercion_drops_container_ids() {
        assert_eq!(
            ids_in_pre_order(&coerced(None)),
            vec![
                None,
                Some(2),
                None,
                None,
                Some(4),
                None,
                None,
                None,
                Some(6)
            ]
        );
    }

    #[test]
    fn restore_puts_back_the_container_ids() {
        let stamp = field_ids_stamp(&descriptor(NESTED)).unwrap();
        let restored = restore_field_ids(&coerced(stamp));
        assert_eq!(ids_in_pre_order(&restored), NESTED_IDS.to_vec());
        // Nothing but the ids changes.
        let shape = |schema: &Schema| DataType::Struct(schema.fields().clone());
        assert!(shape(&restored).equals_datatype(&shape(&coerced(None))));
    }

    #[test]
    fn restore_returns_the_schema_on_a_mismatch() {
        let names = [
            "s",
            "a",
            "ts",
            "l",
            "element",
            "m",
            "key_value",
            "key",
            "value",
        ];
        let nodes: Vec<(&str, Option<i32>)> = names.into_iter().zip(NESTED_IDS).collect();
        let mut renamed = nodes.clone();
        renamed[3].0 = "other";
        let mut leaf_disagrees = nodes.clone();
        leaf_disagrees[1].1 = Some(9);
        let mut leaf_id_missing = nodes.clone();
        leaf_id_missing[1].1 = None;
        let stamps = [
            stamp_of(&nodes[..8]),
            stamp_of(&[nodes.as_slice(), &[("extra", None)]].concat()),
            stamp_of(&renamed),
            stamp_of(&leaf_disagrees),
            stamp_of(&leaf_id_missing),
            "not json".to_string(),
        ];
        for stamp in stamps {
            let schema = coerced(Some(stamp.clone()));
            assert!(
                Arc::ptr_eq(&restore_field_ids(&schema), &schema),
                "stamp {stamp}"
            );
        }
        let schema = coerced(None);
        assert!(Arc::ptr_eq(&restore_field_ids(&schema), &schema));
    }

    /// The coercion leaves a large list and everything below it alone, so a field there that
    /// lacks an id lacks it for another reason and stays as it is.
    #[test]
    fn restore_skips_fields_the_coercion_leaves_alone() {
        let item = Field::new("item", DataType::Struct(Fields::empty()), true);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("l", DataType::LargeList(Arc::new(item)), true)],
            HashMap::from([(
                FIELD_IDS_KEY.to_string(),
                stamp_of(&[("l", Some(1)), ("item", Some(2))]),
            )]),
        ));
        let restored = restore_field_ids(&schema);
        assert_eq!(ids_in_pre_order(&restored), vec![None, None]);
    }

    #[test]
    fn stamp_replaces_one_the_file_carries() {
        let forged = KeyValue::new(FIELD_IDS_KEY.to_string(), "[]".to_string());
        let other = KeyValue::new("other".to_string(), "kept".to_string());
        let file = |message: &str| {
            FileMetaData::new(
                1,
                0,
                None,
                Some(vec![forged.clone(), other.clone()]),
                Arc::new(descriptor(message)),
                None,
            )
        };

        let (needs_stamp, change) = with_field_ids_stamp(&file(NESTED), None).unwrap();
        let needs_stamp = needs_stamp.unwrap();
        assert_eq!(change, StampChange::Added);
        assert_eq!(needs_stamp.len(), 2);
        assert_eq!(needs_stamp[0], other);
        assert_eq!(needs_stamp[1].key, FIELD_IDS_KEY);
        assert_eq!(
            needs_stamp[1].value,
            field_ids_stamp(&descriptor(NESTED)).unwrap()
        );

        let no_stamp = "message schema { optional int32 a = 1; }";
        assert_eq!(
            with_field_ids_stamp(&file(no_stamp), None).unwrap(),
            (Some(vec![other.clone()]), StampChange::Removed)
        );
        let plain = FileMetaData::new(1, 0, None, None, Arc::new(descriptor(no_stamp)), None);
        assert_eq!(
            with_field_ids_stamp(&plain, None).unwrap(),
            (None, StampChange::Unchanged)
        );
        // A list an earlier rewrite produced passes through, with the stamp added when needed.
        let earlier = vec![other.clone()];
        assert_eq!(
            with_field_ids_stamp(&plain, Some(earlier.clone())).unwrap(),
            (Some(earlier), StampChange::Unchanged)
        );
    }
}
