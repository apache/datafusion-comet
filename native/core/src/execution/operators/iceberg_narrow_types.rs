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

//! Keeps iceberg-rust from dropping rows of a data file that stores a filtered column with a
//! narrower type than the table (<https://github.com/apache/datafusion-comet/issues/7138>).
//!
//! A file can store `int` values in a `long` column, `tinyint` or `smallint` values in an `int`
//! column, or `float` values in a `double` column: files written before a type promotion, or added
//! by `add_files`, `migrate`, `snapshot` or the Java API. At Comet's iceberg-rust pin, a range, `IN`
//! or `!=` residual on such a column drops matching rows: the page index skips pages it cannot
//! compare (apache/iceberg-rust#3507), and the row filter casts the literal down to the file's type
//! (apache/iceberg-rust#3540). The JVM cannot tell which files are narrower, so the scan reads the
//! footer of a file whose residual needs it and does not push the residual for a narrower file.
//! The filter above the scan still applies the full predicate.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema as ArrowSchema};
use iceberg::arrow::ArrowFileReader;
use iceberg::encryption::StandardKeyMetadata;
use iceberg::expr::{BoundPredicate, BoundReference, PredicateOperator};
use iceberg::io::{FileIO, FileMetadata};
use iceberg::scan::FileScanTask;
use iceberg::spec::{NameMapping, PrimitiveType, Schema, Type};
use iceberg::{Error, ErrorKind};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::file::metadata::{
    PageIndexPolicy, ParquetMetaDataOptions, ParquetMetaDataReader, ParquetStatisticsPolicy,
};
use tokio::sync::OnceCell;

use crate::parquet::parquet_exec::METADATA_SIZE_HINT;
use crate::parquet::parquet_support::field_id;

/// The Arrow schemas of the data files whose tasks have a residual, each read at most once even
/// when several splits of a file are checked at the same time.
pub(crate) struct FileSchemas(HashMap<String, OnceCell<Arc<ArrowSchema>>>);

impl FileSchemas {
    pub(crate) fn new(tasks: &[FileScanTask]) -> Self {
        Self(
            tasks
                .iter()
                .filter(|task| task.predicate().is_some())
                .map(|task| (task.data_file_path().to_string(), OnceCell::new()))
                .collect(),
        )
    }

    async fn get(&self, task: &FileScanTask, file_io: &FileIO) -> Result<Arc<ArrowSchema>, Error> {
        match self.0.get(task.data_file_path()) {
            Some(cell) => cell
                .get_or_try_init(|| load_file_schema(task, file_io))
                .await
                .cloned(),
            None => load_file_schema(task, file_io).await,
        }
    }
}

/// Whether `task`'s data file stores a column that its residual compares unsafely with a
/// narrower type than the table, in which case the residual must not be pushed. Reads the footer
/// only when the residual has such a comparison.
pub(crate) async fn stores_narrow_filter_column(
    task: &FileScanTask,
    file_io: &FileIO,
    schemas: &FileSchemas,
) -> Result<bool, Error> {
    let Some(predicate) = task.predicate() else {
        return Ok(false);
    };
    let at_risk = at_risk_field_ids(predicate);
    if at_risk.is_empty() {
        return Ok(false);
    }
    let file_schema = schemas.get(task, file_io).await?;
    let name_mapping = task.name_mapping().map(Arc::as_ref);
    Ok(!narrow_field_ids(&file_schema, task.schema(), name_mapping, &at_risk).is_empty())
}

/// The Arrow type a file must store a `ty` column in for an exact comparison, for the types a
/// file can store narrower. Comet pushes no `decimal` residual.
fn exact_arrow_type(ty: &Type) -> Option<DataType> {
    match ty {
        Type::Primitive(PrimitiveType::Int) => Some(DataType::Int32),
        Type::Primitive(PrimitiveType::Long) => Some(DataType::Int64),
        Type::Primitive(PrimitiveType::Double) => Some(DataType::Float64),
        _ => None,
    }
}

/// Operators iceberg-rust evaluates correctly on a narrower file. A literal that does not fit the
/// file's type equals no stored value, and rounding a `double` literal to `float` only adds rows
/// that the filter above the scan removes.
fn safe_on_narrow_file(op: PredicateOperator) -> bool {
    matches!(
        op,
        PredicateOperator::IsNull | PredicateOperator::NotNull | PredicateOperator::Eq
    )
}

/// Ids of the `int`, `long` and `double` columns that `predicate` compares with an operator that
/// [`safe_on_narrow_file`] rejects, or with any operator under a `NOT`.
fn at_risk_field_ids(predicate: &BoundPredicate) -> HashSet<i32> {
    fn visit(predicate: &BoundPredicate, negated: bool, out: &mut HashSet<i32>) {
        let mut add = |reference: &BoundReference, op: PredicateOperator| {
            let field = reference.field();
            if (negated || !safe_on_narrow_file(op))
                && exact_arrow_type(&field.field_type).is_some()
            {
                out.insert(field.id);
            }
        };
        match predicate {
            BoundPredicate::AlwaysTrue | BoundPredicate::AlwaysFalse => {}
            BoundPredicate::And(expr) | BoundPredicate::Or(expr) => expr
                .inputs()
                .into_iter()
                .for_each(|input| visit(input, negated, out)),
            BoundPredicate::Not(expr) => visit(expr.inputs()[0], true, out),
            BoundPredicate::Unary(expr) => add(expr.term(), expr.op()),
            BoundPredicate::Binary(expr) => add(expr.term(), expr.op()),
            BoundPredicate::Set(expr) => add(expr.term(), expr.op()),
        }
    }
    let mut out = HashSet::new();
    visit(predicate, false, &mut out);
    out
}

/// Reads the Arrow schema from the data file's Parquet footer, skipping page indexes and
/// statistics.
async fn load_file_schema(
    task: &FileScanTask,
    file_io: &FileIO,
) -> Result<Arc<ArrowSchema>, Error> {
    let path = task.data_file_path();
    let file_size = task.file_size_in_bytes();
    let mut reader = ArrowFileReader::new(
        FileMetadata { size: file_size },
        file_io.new_input(path)?.reader().await?,
    );
    let options = ParquetMetaDataOptions::new()
        .with_column_stats_policy(ParquetStatisticsPolicy::SkipAll)
        .with_size_stats_policy(ParquetStatisticsPolicy::SkipAll)
        .with_encoding_stats_policy(ParquetStatisticsPolicy::SkipAll);
    let mut metadata_reader = ParquetMetaDataReader::new()
        .with_prefetch_hint(Some(METADATA_SIZE_HINT))
        .with_page_index_policy(PageIndexPolicy::Skip)
        .with_metadata_options(Some(options));
    if let Some(key_metadata) = task.key_metadata() {
        metadata_reader =
            metadata_reader.with_decryption_properties(Some(decryption_properties(key_metadata)?));
    }
    let footer_error = |e: parquet::errors::ParquetError| {
        Error::new(
            ErrorKind::Unexpected,
            format!("Failed to read the Parquet footer of '{path}' to check its column types"),
        )
        .with_source(e)
    };
    let metadata = metadata_reader
        .load_and_finish(&mut reader, file_size)
        .await
        .map_err(footer_error)?;
    let arrow_metadata =
        ArrowReaderMetadata::try_new(Arc::new(metadata), ArrowReaderOptions::new())
            .map_err(footer_error)?;
    Ok(Arc::clone(arrow_metadata.schema()))
}

/// Decryption properties built from a data file's `StandardKeyMetadata`, as iceberg-rust's reader
/// builds them.
fn decryption_properties(key_metadata: &[u8]) -> Result<Arc<FileDecryptionProperties>, Error> {
    let key_metadata = StandardKeyMetadata::decode(key_metadata)?;
    let mut builder =
        FileDecryptionProperties::builder(key_metadata.encryption_key().as_bytes().to_vec());
    if let Some(aad) = key_metadata.aad_prefix() {
        builder = builder.with_aad_prefix(aad.to_vec());
    }
    builder.build().map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to build Parquet file decryption properties",
        )
        .with_source(e)
    })
}

/// The ids among `candidates` whose column the file may store with another type than
/// [`exact_arrow_type`].
///
/// Columns are matched as iceberg-rust matches them: by embedded field id, and when the first
/// column has none, also by top-level name through the name mapping. Without either, and for a
/// nested field, every candidate counts. A column the file lacks is read as NULL, so it does not.
fn narrow_field_ids(
    file_schema: &ArrowSchema,
    table_schema: &Schema,
    name_mapping: Option<&NameMapping>,
    candidates: &HashSet<i32>,
) -> HashSet<i32> {
    let first_has_id = file_schema
        .fields()
        .first()
        .is_some_and(|field| field_id(field).is_some());
    let name_mapping = match (first_has_id, name_mapping) {
        (true, _) => None,
        (false, Some(name_mapping)) => Some(name_mapping),
        (false, None) => return candidates.clone(),
    };

    let mut narrow = HashSet::new();
    let mut expected = HashMap::with_capacity(candidates.len());
    for &id in candidates {
        let field = table_schema.as_struct().field_by_id(id);
        match field.and_then(|field| exact_arrow_type(&field.field_type)) {
            Some(exact) => {
                expected.insert(id, exact);
            }
            None => {
                narrow.insert(id);
            }
        }
    }
    // The first mapped field with a name wins, as in iceberg-rust.
    let mut id_by_name: HashMap<&str, Option<i32>> = HashMap::new();
    for mapped in name_mapping.map_or(&[][..], |name_mapping| name_mapping.fields()) {
        for name in mapped.names() {
            id_by_name.entry(name.as_str()).or_insert(mapped.field_id());
        }
    }
    for field in file_schema.fields() {
        let mapped_id = id_by_name.get(field.name().as_str()).copied().flatten();
        for id in [field_id(field), mapped_id].into_iter().flatten() {
            if expected
                .get(&id)
                .is_some_and(|exact| field.data_type() != exact)
            {
                narrow.insert(id);
            }
        }
    }
    narrow
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, Int8Array, RecordBatch,
        UInt32Array,
    };
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use iceberg::expr::{Bind, BoundPredicate, Predicate, Reference};
    use iceberg::io::{FileIO, FileIOBuilder};
    use iceberg::scan::FileScanTask;
    use iceberg::spec::{
        DataFileFormat, Datum, MappedField, NameMapping, NestedField, PrimitiveType, Schema, Type,
    };
    use iceberg_storage_opendal::OpenDalStorageFactory;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

    use super::{at_risk_field_ids, narrow_field_ids, stores_narrow_filter_column, FileSchemas};

    /// `id BIGINT` (1), `t INT` (2), `d DOUBLE` (3), `s STRING` (4).
    fn table_schema() -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "t", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "d", Type::Primitive(PrimitiveType::Double)).into(),
                    NestedField::optional(4, "s", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        )
    }

    fn bind(predicate: Predicate) -> BoundPredicate {
        predicate.bind(table_schema(), true).unwrap()
    }

    fn ids(ids: &[i32]) -> HashSet<i32> {
        ids.iter().copied().collect()
    }

    fn id_gt(v: i64) -> Predicate {
        Reference::new("id").greater_than(Datum::long(v))
    }

    fn t_gt(v: i32) -> Predicate {
        Reference::new("t").greater_than(Datum::int(v))
    }

    fn field(name: &str, data_type: DataType, id: Option<i32>) -> Field {
        let field = Field::new(name, data_type, true);
        match id {
            Some(id) => field.with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])),
            None => field,
        }
    }

    fn mapping(entries: &[(i32, &str)]) -> NameMapping {
        NameMapping::new(
            entries
                .iter()
                .map(|(id, name)| MappedField::new(Some(*id), vec![name.to_string()], vec![]))
                .collect(),
        )
    }

    fn narrow(fields: Vec<Field>, name_mapping: Option<&NameMapping>) -> HashSet<i32> {
        narrow_field_ids(
            &ArrowSchema::new(fields),
            &table_schema(),
            name_mapping,
            &ids(&[1, 2, 3]),
        )
    }

    #[test]
    fn at_risk_counts_ranges_in_and_inequality_on_int_long_double() {
        let at_risk = |p: Predicate| at_risk_field_ids(&bind(p));
        assert_eq!(at_risk(id_gt(0)), ids(&[1]));
        assert_eq!(
            at_risk(Reference::new("t").is_in([Datum::int(1), Datum::int(200)])),
            ids(&[2])
        );
        assert_eq!(
            at_risk(Reference::new("d").not_equal_to(Datum::double(1.5))),
            ids(&[3])
        );
        assert_eq!(
            at_risk(
                Reference::new("id")
                    .less_than_or_equal_to(Datum::long(3))
                    .and(t_gt(1))
            ),
            ids(&[1, 2])
        );
        // Null checks and equality are safe, and a string column has no narrower form.
        assert_eq!(
            at_risk(
                Reference::new("id")
                    .equal_to(Datum::long(3_000_000_000_i64))
                    .or(Reference::new("t").is_null())
                    .and(Reference::new("d").is_not_null())
            ),
            ids(&[])
        );
        assert_eq!(
            at_risk(Reference::new("s").greater_than(Datum::string("a"))),
            ids(&[])
        );
        // Under a NOT, equality becomes inequality.
        assert_eq!(
            at_risk(!Reference::new("id").equal_to(Datum::long(1))),
            ids(&[1])
        );
    }

    #[test]
    fn narrow_by_embedded_field_ids() {
        assert_eq!(
            narrow(
                vec![
                    field("id", DataType::Int32, Some(1)),
                    field("t", DataType::Int32, Some(2)),
                    field("d", DataType::Float32, Some(3)),
                ],
                None
            ),
            ids(&[1, 3])
        );
        assert_eq!(
            narrow(
                vec![
                    field("id", DataType::UInt32, Some(1)),
                    field("t", DataType::Int8, Some(2)),
                    field("d", DataType::Float64, Some(3)),
                ],
                None
            ),
            ids(&[1, 2])
        );
        // Matched by id, not name; `t` is not in the file.
        assert_eq!(
            narrow(
                vec![
                    field("renamed", DataType::Int64, Some(1)),
                    field("d", DataType::Float64, Some(3)),
                ],
                None
            ),
            ids(&[])
        );
        // A file whose first column has an id ignores the name mapping.
        assert_eq!(
            narrow(
                vec![
                    field("id", DataType::Int64, Some(1)),
                    field("t", DataType::Int16, None),
                ],
                Some(&mapping(&[(2, "t")]))
            ),
            ids(&[])
        );
    }

    #[test]
    fn narrow_through_the_name_mapping() {
        let name_mapping = mapping(&[(1, "id"), (2, "t"), (3, "d")]);
        assert_eq!(
            narrow(
                vec![
                    field("d", DataType::Float32, None),
                    field("id", DataType::Int64, None),
                    field("t", DataType::Int16, None),
                ],
                Some(&name_mapping)
            ),
            ids(&[2, 3])
        );
        assert_eq!(
            narrow(
                vec![
                    field("t", DataType::Int32, None),
                    field("unmapped", DataType::Int8, None),
                ],
                Some(&name_mapping)
            ),
            ids(&[])
        );
        // `a` gets id 1 from the mapping and `b` keeps its own id 1; `a` is narrower.
        assert_eq!(
            narrow(
                vec![
                    field("a", DataType::Int32, None),
                    field("b", DataType::Int64, Some(1)),
                ],
                Some(&mapping(&[(1, "a")]))
            ),
            ids(&[1])
        );
    }

    #[test]
    fn narrow_is_every_candidate_without_ids_or_name_mapping_and_for_nested_fields() {
        assert_eq!(
            narrow(
                vec![
                    field("id", DataType::Int64, None),
                    field("t", DataType::Int32, None),
                    field("d", DataType::Float64, None),
                ],
                None
            ),
            ids(&[1, 2, 3])
        );
        assert_eq!(
            narrow_field_ids(
                &ArrowSchema::new(vec![field("id", DataType::Int64, Some(1))]),
                &table_schema(),
                None,
                &ids(&[1, 99]),
            ),
            ids(&[99])
        );
    }

    fn fs_file_io() -> FileIO {
        FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Fs)).build()
    }

    fn write_parquet(path: &Path, fields: Vec<Field>, columns: Vec<ArrayRef>) -> u64 {
        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        std::fs::metadata(path).unwrap().len()
    }

    fn task(path: &Path, size: u64, predicate: Option<Predicate>) -> FileScanTask {
        FileScanTask::builder()
            .with_file_size_in_bytes(size)
            .with_start(0)
            .with_length(0)
            .with_data_file_path(path.to_str().unwrap().to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(table_schema())
            .with_project_field_ids(vec![1, 2, 3])
            .with_predicate(predicate.map(|p| bind(p.rewrite_not())))
            .with_case_sensitive(true)
            .build()
            .unwrap()
    }

    async fn check(task: &FileScanTask) -> bool {
        let schemas = FileSchemas::new(std::slice::from_ref(task));
        stores_narrow_filter_column(task, &fs_file_io(), &schemas)
            .await
            .unwrap()
    }

    // A file written before `id` became BIGINT and `d` became DOUBLE, and one written after.
    #[tokio::test]
    async fn check_flags_only_a_file_that_stores_a_filtered_column_narrower() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("old.parquet");
        let old_size = write_parquet(
            &old,
            vec![
                field("id", DataType::Int32, Some(1)),
                field("t", DataType::Int32, Some(2)),
                field("d", DataType::Float32, Some(3)),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Float32Array::from(vec![0.1, 1.5])),
            ],
        );
        let new = dir.path().join("new.parquet");
        let new_size = write_parquet(
            &new,
            vec![
                field("id", DataType::Int64, Some(1)),
                field("t", DataType::Int32, Some(2)),
                field("d", DataType::Float64, Some(3)),
            ],
            vec![
                Arc::new(Int64Array::from(vec![4, 5])),
                Arc::new(Int32Array::from(vec![4, 5])),
                Arc::new(Float64Array::from(vec![4.0, 5.0])),
            ],
        );

        assert!(check(&task(&old, old_size, Some(id_gt(0).and(t_gt(1))))).await);
        assert!(!check(&task(&new, new_size, Some(id_gt(0).and(t_gt(1))))).await);
        assert!(
            check(&task(
                &old,
                old_size,
                Some(Reference::new("d").less_than(Datum::double(2.0)))
            ))
            .await
        );
        assert!(
            check(&task(
                &old,
                old_size,
                Some(!Reference::new("id").is_in([Datum::long(1), Datum::long(2)]))
            ))
            .await
        );
        // `t` has the table's type in the old file too.
        assert!(!check(&task(&old, old_size, Some(t_gt(1)))).await);
    }

    // These paths do not exist, so a footer read would fail.
    #[tokio::test]
    async fn check_reads_no_footer_for_a_safe_residual() {
        let missing = Path::new("/nonexistent/comet-7138/missing.parquet");
        assert!(!check(&task(missing, 100, None)).await);
        assert!(
            !check(&task(
                missing,
                100,
                Some(
                    Reference::new("id")
                        .equal_to(Datum::long(3_000_000_000_i64))
                        .and(Reference::new("s").greater_than(Datum::string("a")))
                )
            ))
            .await
        );
    }

    #[tokio::test]
    async fn file_schemas_read_each_footer_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tinyint.parquet");
        let size = write_parquet(
            &path,
            vec![
                field("t", DataType::Int8, Some(2)),
                field("id", DataType::UInt32, Some(1)),
            ],
            vec![
                Arc::new(Int8Array::from(vec![1, 100])),
                Arc::new(UInt32Array::from(vec![1_u32, 3_000_000_000])),
            ],
        );
        let first = task(&path, size, Some(t_gt(1)));
        let second = task(&path, size, Some(id_gt(0)));
        let schemas = FileSchemas::new(&[first.clone(), second.clone()]);
        let file_io = fs_file_io();
        assert!(stores_narrow_filter_column(&first, &file_io, &schemas)
            .await
            .unwrap());
        std::fs::remove_file(&path).unwrap();
        assert!(stores_narrow_filter_column(&second, &file_io, &schemas)
            .await
            .unwrap());
    }
}
