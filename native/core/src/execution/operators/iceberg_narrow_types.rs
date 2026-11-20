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

//! Keeps iceberg-rust from dropping the rows of a data file that stores a narrower type than its
//! column (<https://github.com/apache/datafusion-comet/issues/7138>).
//!
//! A data file can store `int` values in a `long` column, `tinyint` or `smallint` values in an
//! `int` column, or `float` values in a `double` column. Files written before a type promotion
//! do, and so can files that `add_files`, `migrate`, `snapshot` or the Java API append, since none
//! of them checks a file's types against the table schema. At Comet's iceberg-rust pin, a residual
//! on such a column drops matching rows in two places:
//!
//! - The page index cannot compare the file's `INT32` or `FLOAT` page bounds with a `long` or
//!   `double` literal, and range and `IN` predicates then skip the page
//!   (fixed upstream by apache/iceberg-rust#3507).
//! - The row filter casts the literal down to the file's Arrow type, so a literal outside that
//!   type's range becomes NULL and a `double` literal rounds to `float`. `IN` combines its
//!   literals with a NULL-propagating `or`, so one such literal drops every row
//!   (apache/iceberg-rust#3540).
//!
//! `IS NULL`, `IS NOT NULL` and `=` are not affected. A literal that does not fit the file's type
//! equals no value the file stores, the page index keeps a page whose bounds it cannot compare
//! for `=`, and rounding a `double` literal to `float` can only match more rows, which the filter
//! above the scan removes.
//!
//! The JVM cannot tell which files are narrower: Spark plans the scan without column statistics,
//! and neither the schema history nor the name mapping records every such file. So before a task
//! reaches the reader, [`check_narrow_types`] reads the file's Parquet footer when the residual
//! compares an `int`, `long` or `double` column with another operator, and replaces each such
//! comparison on a column the file stores with another type by TRUE. The planner binds the
//! residual after `rewrite_not`, so it holds no `NOT`, and replacing a leaf by TRUE only weakens
//! it. Iceberg keeps every residual predicate in the filter above the scan, which restores the
//! exact result. Files that store the column's own type keep their row-group, page and row
//! filtering.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use iceberg::arrow::ArrowFileReader;
use iceberg::encryption::StandardKeyMetadata;
use iceberg::expr::{
    BinaryExpression, Bind, BoundPredicate, BoundReference, Predicate, PredicateOperator,
    Reference, SetExpression, UnaryExpression,
};
use iceberg::io::{FileIO, FileMetadata};
use iceberg::scan::FileScanTask;
use iceberg::spec::{NameMapping, PrimitiveType, Schema, Type};
use iceberg::{Error, ErrorKind};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use tokio::sync::OnceCell;

/// Footer prefetch, as `IcebergScanExec` configures the reader.
const FOOTER_PREFETCH_HINT: usize = 512 * 1024;

/// What [`check_narrow_types`] found for a task.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum NarrowTypeCheck {
    /// The residual compares no `int`, `long` or `double` column with an operator that a narrower
    /// file mishandles, so the footer was not read.
    NotNeeded,
    /// The file stores each such column with the table's type.
    Unchanged,
    /// The file stores at least one such column with another type. Holds the weakened residual,
    /// `None` when nothing is left to push.
    Weakened(Option<BoundPredicate>),
}

/// The Arrow schemas of the data files that a scan's tasks read. Each footer is read at most
/// once, even when several splits of one file are checked at the same time.
pub(crate) struct FileSchemas(HashMap<String, OnceCell<Arc<ArrowSchema>>>);

impl FileSchemas {
    pub(crate) fn new(tasks: &[FileScanTask]) -> Self {
        Self(
            tasks
                .iter()
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

/// Checks the column types of `task`'s data file against its residual. See the module
/// documentation.
///
/// The footer is read as Parquet, the only format the planner builds tasks for. A task of another
/// format fails here rather than skipping the check, since such a file can store narrower types
/// too.
pub(crate) async fn check_narrow_types(
    task: &FileScanTask,
    file_io: &FileIO,
    schemas: &FileSchemas,
) -> Result<NarrowTypeCheck, Error> {
    let Some(predicate) = task.predicate() else {
        return Ok(NarrowTypeCheck::NotNeeded);
    };
    // The planner binds the residual after rewrite_not, so this is expected to change nothing. It
    // makes the NOT-free shape that weaken relies on a local guarantee.
    let predicate = predicate.clone().rewrite_not();
    let at_risk = at_risk_field_ids(&predicate);
    if at_risk.is_empty() {
        return Ok(NarrowTypeCheck::NotNeeded);
    }
    let file_schema = schemas.get(task, file_io).await?;
    let narrow = narrow_field_ids(
        &file_schema,
        task.schema(),
        task.name_mapping().map(Arc::as_ref),
        &at_risk,
    );
    if narrow.is_empty() {
        return Ok(NarrowTypeCheck::Unchanged);
    }
    weaken(
        &predicate,
        &narrow,
        task.schema_ref(),
        task.case_sensitive(),
    )
    .map(NarrowTypeCheck::Weakened)
}

/// The Arrow type a file must store a column of `ty` in for iceberg-rust to compare it with a
/// literal exactly, for the column types that a file can store narrower. `None` for every other
/// type: Comet pushes no `decimal` residual, and the other types have no narrower Parquet form.
fn exact_arrow_type(ty: &Type) -> Option<DataType> {
    match ty {
        Type::Primitive(PrimitiveType::Int) => Some(DataType::Int32),
        Type::Primitive(PrimitiveType::Long) => Some(DataType::Int64),
        Type::Primitive(PrimitiveType::Double) => Some(DataType::Float64),
        _ => None,
    }
}

/// Whether iceberg-rust evaluates `op` correctly on a file that stores the column narrower. An
/// allowlist, so that an operator Comet starts to push later is guarded until shown safe.
fn safe_on_narrow_file(op: PredicateOperator) -> bool {
    matches!(
        op,
        PredicateOperator::IsNull | PredicateOperator::NotNull | PredicateOperator::Eq
    )
}

/// Field ids of the `int`, `long` and `double` columns that `predicate` compares with an operator
/// that a file storing the column narrower mishandles. Under a `NOT`, which `weaken` drops whole,
/// every operator counts.
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

/// Reads the data file's Parquet footer, without its page indexes, and returns the Arrow schema
/// that iceberg-rust derives from it before applying a name mapping.
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
    let mut metadata_reader = ParquetMetaDataReader::new()
        .with_prefetch_hint(Some(FOOTER_PREFETCH_HINT))
        .with_page_index_policy(PageIndexPolicy::Skip);
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

/// Decryption properties for a file encrypted with Parquet Modular Encryption, built from its
/// `StandardKeyMetadata` as iceberg-rust's reader builds them.
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

/// The ids among `candidates` whose column the file may store with another Arrow type than
/// [`exact_arrow_type`].
///
/// iceberg-rust matches a predicate's columns by the field ids embedded in the file when every
/// leaf has one, and otherwise through the Arrow schema it reads with. When the file's first
/// column has no id, that schema takes its ids from the table's name mapping, by top-level name,
/// and a column the mapping does not cover keeps any id it has. Without a name mapping it matches
/// by position. Each column that may carry a candidate's id is compared here, and a candidate
/// counts as narrower if any of them has another type. Position is not mirrored, so without ids
/// and a name mapping every candidate counts, and so does a nested field, since only top-level
/// columns are compared (Comet pushes no residual on one). A candidate the file does not store is
/// read as NULL or its default, with no cast, so it does not count.
fn narrow_field_ids(
    file_schema: &ArrowSchema,
    table_schema: &Schema,
    name_mapping: Option<&NameMapping>,
    candidates: &HashSet<i32>,
) -> HashSet<i32> {
    let embedded_id = |field: &Field| {
        field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|id| id.parse::<i32>().ok())
    };
    let first_has_id = file_schema
        .fields()
        .first()
        .is_some_and(|field| field.metadata().contains_key(PARQUET_FIELD_ID_META_KEY));
    let name_mapping = match (first_has_id, name_mapping) {
        (true, _) => None,
        (false, Some(name_mapping)) => Some(name_mapping),
        (false, None) => return candidates.clone(),
    };

    let mut file_types: HashMap<i32, Vec<&DataType>> = HashMap::new();
    for field in file_schema.fields() {
        if let Some(id) = embedded_id(field) {
            file_types.entry(id).or_default().push(field.data_type());
        }
    }
    if let Some(name_mapping) = name_mapping {
        // The first mapped field with a name wins, as in iceberg-rust's
        // apply_name_mapping_to_arrow_schema.
        let mut id_by_name: HashMap<&str, Option<i32>> = HashMap::new();
        for mapped in name_mapping.fields() {
            for name in mapped.names() {
                id_by_name.entry(name.as_str()).or_insert(mapped.field_id());
            }
        }
        for field in file_schema.fields() {
            if let Some(Some(id)) = id_by_name.get(field.name().as_str()) {
                file_types.entry(*id).or_default().push(field.data_type());
            }
        }
    }

    candidates
        .iter()
        .copied()
        .filter(|id| {
            let Some(exact) = table_schema
                .as_struct()
                .field_by_id(*id)
                .and_then(|field| exact_arrow_type(&field.field_type))
            else {
                return true;
            };
            file_types
                .get(id)
                .is_some_and(|types| types.iter().any(|ty| **ty != exact))
        })
        .collect()
}

/// `predicate`, which must hold no `NOT`, with every comparison on a `narrow` column that
/// [`safe_on_narrow_file`] rejects replaced by TRUE, and bound again to `schema`. `None` when
/// nothing is left to push, so that the reader neither loads page indexes nor builds a row filter.
///
/// iceberg-rust keeps the constructors of a bound `AND` and `OR` private, so the result is built
/// as an unbound predicate, whose `and` and `or` also drop the TRUE operands.
fn weaken(
    predicate: &BoundPredicate,
    narrow: &HashSet<i32>,
    schema: Arc<Schema>,
    case_sensitive: bool,
) -> Result<Option<BoundPredicate>, Error> {
    fn unbind(predicate: &BoundPredicate, narrow: &HashSet<i32>) -> Predicate {
        let keep = |reference: &BoundReference, op: PredicateOperator| {
            safe_on_narrow_file(op) || !narrow.contains(&reference.field().id)
        };
        // A bound reference prints the full column path it was bound from.
        let reference = |reference: &BoundReference| Reference::new(reference.to_string());
        match predicate {
            BoundPredicate::AlwaysTrue => Predicate::AlwaysTrue,
            BoundPredicate::AlwaysFalse => Predicate::AlwaysFalse,
            BoundPredicate::And(expr) => {
                let [left, right] = expr.inputs();
                unbind(left, narrow).and(unbind(right, narrow))
            }
            BoundPredicate::Or(expr) => {
                let [left, right] = expr.inputs();
                unbind(left, narrow).or(unbind(right, narrow))
            }
            // Weakening the operand of a NOT would strengthen the predicate. check_narrow_types
            // removes every NOT first, so dropping one whole only keeps this function sound on
            // its own.
            BoundPredicate::Not(_) => Predicate::AlwaysTrue,
            BoundPredicate::Unary(expr) if keep(expr.term(), expr.op()) => {
                Predicate::Unary(UnaryExpression::new(expr.op(), reference(expr.term())))
            }
            BoundPredicate::Binary(expr) if keep(expr.term(), expr.op()) => Predicate::Binary(
                BinaryExpression::new(expr.op(), reference(expr.term()), expr.literal().clone()),
            ),
            BoundPredicate::Set(expr) if keep(expr.term(), expr.op()) => Predicate::Set(
                SetExpression::new(expr.op(), reference(expr.term()), expr.literals().clone()),
            ),
            BoundPredicate::Unary(_) | BoundPredicate::Binary(_) | BoundPredicate::Set(_) => {
                Predicate::AlwaysTrue
            }
        }
    }

    // Binding can simplify further, e.g. `IS NOT NULL` on a required column to TRUE. A predicate
    // that fails to bind again is not pushed, as the planner does when it binds the residual.
    match unbind(predicate, narrow) {
        Predicate::AlwaysTrue => Ok(None),
        weakened => match weakened.bind(schema, case_sensitive) {
            Ok(BoundPredicate::AlwaysTrue) => Ok(None),
            Ok(bound) => Ok(Some(bound)),
            Err(e) => {
                log::warn!("Skipping Iceberg predicate pushdown; binding the weakened residual failed: {e}");
                Ok(None)
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, Int8Array};
    use arrow::array::{RecordBatch, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use iceberg::expr::{Bind, BoundPredicate, Predicate, Reference};
    use iceberg::io::{FileIO, FileIOBuilder};
    use iceberg::scan::FileScanTask;
    use iceberg::spec::{
        DataFileFormat, Datum, MappedField, NameMapping, NestedField, PrimitiveType, Schema, Type,
    };
    use iceberg_storage_opendal::OpenDalStorageFactory;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

    use super::{
        at_risk_field_ids, check_narrow_types, narrow_field_ids, weaken, FileSchemas,
        NarrowTypeCheck,
    };

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

    fn s_eq(v: &str) -> Predicate {
        Reference::new("s").equal_to(Datum::string(v))
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
        // Null checks and equality are safe, and a string column cannot be stored narrower.
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
        // Under a NOT, equality turns into inequality.
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
        // The table's own types, under other names, and a column the file does not store.
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
        // A file with field ids is not resolved through the name mapping.
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
        // The first column has no id, so iceberg-rust maps `a` to field 1 by name, while `b`
        // keeps its embedded id 1. One of them is narrower.
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
    fn narrow_without_field_ids_or_name_mapping_is_every_candidate() {
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
    }

    #[test]
    fn weaken_replaces_unsafe_leaves_on_narrow_columns() {
        let weakened = |p: Predicate, narrow: &[i32]| {
            weaken(&bind(p), &ids(narrow), table_schema(), true).unwrap()
        };
        assert_eq!(
            weakened(id_gt(0).and(s_eq("x")), &[1]),
            Some(bind(s_eq("x")))
        );
        assert_eq!(weakened(id_gt(0).or(s_eq("x")), &[1]), None);
        assert_eq!(
            weakened(
                Reference::new("id")
                    .equal_to(Datum::long(1))
                    .and(Reference::new("id").less_than(Datum::long(5)))
                    .and(Reference::new("id").is_not_null()),
                &[1]
            ),
            Some(bind(
                Reference::new("id")
                    .equal_to(Datum::long(1))
                    .and(Reference::new("id").is_not_null())
            ))
        );
        assert_eq!(
            weakened(
                Reference::new("d")
                    .is_in([Datum::double(1.5), Datum::double(4.0)])
                    .and(t_gt(1)),
                &[3]
            ),
            Some(bind(t_gt(1)))
        );
        // Only the narrow columns are weakened.
        assert_eq!(weakened(id_gt(0).and(t_gt(1)), &[2]), Some(bind(id_gt(0))));
        // A NOT is dropped whole.
        assert_eq!(
            weakened((!s_eq("x")).and(t_gt(1)), &[]),
            Some(bind(t_gt(1)))
        );
    }

    // What is left can bind to TRUE: `IS NOT NULL` on a required column. The task then carries no
    // predicate, so the reader neither loads page indexes nor builds a row filter.
    #[test]
    fn weaken_returns_none_when_the_rest_binds_to_true() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(5, "r", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );
        let predicate = Reference::new("r")
            .is_not_null()
            .and(id_gt(0))
            .bind(Arc::clone(&schema), true)
            .unwrap();
        assert_eq!(weaken(&predicate, &ids(&[1]), schema, true).unwrap(), None);
    }

    // A field the table has nested in a struct cannot be matched against the file's top-level
    // columns, so it counts as narrower.
    #[test]
    fn narrow_counts_a_field_that_is_not_top_level() {
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

    async fn check(task: &FileScanTask) -> NarrowTypeCheck {
        check_narrow_types(
            task,
            &fs_file_io(),
            &FileSchemas::new(std::slice::from_ref(task)),
        )
        .await
        .unwrap()
    }

    // A file written before `id` became BIGINT and `d` became DOUBLE, next to one that stores the
    // table's types.
    #[tokio::test]
    async fn check_reads_the_footer_and_weakens_only_for_a_narrower_file() {
        let dir = tempfile::tempdir().unwrap();
        let narrow_path = dir.path().join("narrow.parquet");
        let narrow_size = write_parquet(
            &narrow_path,
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
        let wide_path = dir.path().join("wide.parquet");
        let wide_size = write_parquet(
            &wide_path,
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

        let predicate = || Some(id_gt(0).and(t_gt(1)));
        assert_eq!(
            check(&task(&narrow_path, narrow_size, predicate())).await,
            NarrowTypeCheck::Weakened(Some(bind(t_gt(1))))
        );
        assert_eq!(
            check(&task(&wide_path, wide_size, predicate())).await,
            NarrowTypeCheck::Unchanged
        );
        assert_eq!(
            check(&task(
                &narrow_path,
                narrow_size,
                Some(Reference::new("d").less_than(Datum::double(2.0)))
            ))
            .await,
            NarrowTypeCheck::Weakened(None)
        );
        // `NOT (id = 2)` reaches the reader as `id != 2`, and `NOT (id IN (...))` as NOT IN.
        assert_eq!(
            check(&task(
                &narrow_path,
                narrow_size,
                Some(!Reference::new("id").equal_to(Datum::long(2)))
            ))
            .await,
            NarrowTypeCheck::Weakened(None)
        );
        assert_eq!(
            check(&task(
                &narrow_path,
                narrow_size,
                Some(
                    (!Reference::new("id").is_in([Datum::long(1), Datum::long(3_000_000_000_i64)]))
                        .and(t_gt(1))
                )
            ))
            .await,
            NarrowTypeCheck::Weakened(Some(bind(t_gt(1))))
        );
    }

    // The footer is read only when the residual needs it: these paths do not exist.
    #[tokio::test]
    async fn check_skips_the_footer_when_the_residual_is_safe() {
        let missing = Path::new("/nonexistent/comet-7138/missing.parquet");
        assert_eq!(
            check(&task(missing, 100, None)).await,
            NarrowTypeCheck::NotNeeded
        );
        assert_eq!(
            check(&task(
                missing,
                100,
                Some(
                    Reference::new("id")
                        .equal_to(Datum::long(3_000_000_000_i64))
                        .and(Reference::new("s").greater_than(Datum::string("a")))
                )
            ))
            .await,
            NarrowTypeCheck::NotNeeded
        );
    }

    // Splits of one file share its footer: after the first check reads it, the file can go.
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
        let second = task(&path, size, Some(id_gt(0).and(s_eq("x"))));
        let schemas = FileSchemas::new(&[first.clone(), second.clone()]);
        let file_io = fs_file_io();
        assert_eq!(
            check_narrow_types(&first, &file_io, &schemas)
                .await
                .unwrap(),
            NarrowTypeCheck::Weakened(None)
        );
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            check_narrow_types(&second, &file_io, &schemas)
                .await
                .unwrap(),
            NarrowTypeCheck::Weakened(Some(bind(s_eq("x"))))
        );
    }
}
