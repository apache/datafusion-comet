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

use arrow::{
    array::{
        Array, ArrayRef, AsArray, BinaryArray, BinaryBuilder, LargeListArray, LargeListViewArray,
        ListArray, ListLikeArray, ListViewArray, StructArray,
    },
    buffer::OffsetBuffer,
    compute::{cast, cast_with_options},
    datatypes::{DataType, Field, FieldRef, TimeUnit, DECIMAL128_MAX_PRECISION},
    error::ArrowError,
};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion_comet_common::SparkError;
use datafusion_comet_spark_expr::canonicalize_nan;
use parquet::variant::{
    unshred_variant, ListBuilder, MetadataBuilder, ObjectBuilder, ParentState,
    ReadOnlyMetadataBuilder, ValueBuilder, Variant, VariantArray, VariantDecimal4, VariantDecimal8,
    VariantMetadata, WritableMetadataBuilder,
};
use std::{
    collections::{HashMap, HashSet},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};

/// Maximum object/list containers along a value path; scalars have depth zero. Typed and
/// residual containers share the budget. Traversal and container encoding use heap work stacks.
const MAX_VARIANT_NESTING_DEPTH: usize = 1000;

fn malformed<T>() -> DataFusionResult<T> {
    Err(SparkError::MalformedVariant.into())
}

/// Shallow Variant iteration checks child encodings without recursively validating descendants.
/// Check every descendant ourselves, including legacy Spark UTF-16 object ordering.
fn check_variant_depth(variant: Variant<'_, '_>, depth: usize) -> DataFusionResult<()> {
    let mut pending = vec![(variant, depth)];
    let (mut arrow_ordered, mut spark_ordered) = (true, true);
    while let Some((value, depth)) = pending.pop() {
        match value {
            Variant::Object(object) => {
                if depth >= MAX_VARIANT_NESTING_DEPTH {
                    return malformed();
                }
                let mut previous: Option<&str> = None;
                for (name, value) in object.iter() {
                    if let Some(previous) = previous {
                        if previous == name {
                            return malformed();
                        }
                        arrow_ordered &= previous < name;
                        spark_ordered &= previous.encode_utf16().cmp(name.encode_utf16()).is_lt();
                    }
                    previous = Some(name);
                    pending.push((value, depth + 1));
                }
            }
            Variant::List(list) => {
                if depth >= MAX_VARIANT_NESTING_DEPTH {
                    return malformed();
                }
                pending.extend(list.iter().map(|value| (value, depth + 1)));
            }
            _ => {}
        }
    }
    if !arrow_ordered && !spark_ordered {
        return malformed();
    }
    Ok(())
}

/// Bound the physical schema before visiting it. Each container adds a typed container and a
/// shredding-state Struct, plus the root and scalar leaf. No recursive Arrow cast sees this tree.
fn check_variant_storage_depth(data_type: &DataType) -> DataFusionResult<()> {
    let mut pending = vec![(data_type, 0)];
    while let Some((data_type, depth)) = pending.pop() {
        if depth > 2 * MAX_VARIANT_NESTING_DEPTH + 1 {
            return malformed();
        }
        match data_type {
            DataType::Struct(fields) => {
                pending.extend(fields.iter().map(|f| (f.data_type(), depth + 1)))
            }
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::ListView(f)
            | DataType::LargeListView(f)
            | DataType::FixedSizeList(f, _)
            | DataType::Map(f, _) => pending.push((f.data_type(), depth + 1)),
            DataType::Dictionary(_, value) => pending.push((value.as_ref(), depth + 1)),
            _ => {}
        }
    }
    Ok(())
}

fn scalar_storage_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::UInt8 => DataType::Int16,
        DataType::UInt16 => DataType::Int32,
        DataType::UInt32 => DataType::Int64,
        DataType::UInt64 => DataType::Decimal128(20, 0),
        DataType::Decimal256(p, s) if *p <= DECIMAL128_MAX_PRECISION => {
            DataType::Decimal128(*p, *s)
        }
        DataType::Timestamp(TimeUnit::Millisecond, zone) => {
            DataType::Timestamp(TimeUnit::Microsecond, zone.clone())
        }
        DataType::FixedSizeBinary(_) => DataType::Binary,
        value => value.clone(),
    }
}

fn check_uuid(field: &FieldRef) -> DataFusionResult<()> {
    if field.data_type() == &DataType::FixedSizeBinary(16)
        && field.extension_type_name() == Some("arrow.uuid")
    {
        return Err(DataFusionError::Execution(
            "Parquet UUID is not supported as a shredded Variant child".into(),
        ));
    }
    Ok(())
}

/// Keep rebuilt ancestors alive until their children can be released in reverse construction
/// order. Otherwise dropping a temporary normalized tree could itself recurse with schema depth.
#[derive(Default)]
struct ArrayOwners(Vec<ArrayRef>);
impl Drop for ArrayOwners {
    fn drop(&mut self) {
        while let Some(array) = self.0.pop() {
            drop(array);
        }
    }
}

/// Normalize children bottom-up. Whole-Struct casts, Array::to_data, and recursively rebuilding
/// DataTypes would put attacker-controlled schema depth back on the native stack.
fn normalize_variant_storage(
    array: &ArrayRef,
    owners: &mut ArrayOwners,
) -> DataFusionResult<ArrayRef> {
    enum Work<'a> {
        Visit(&'a ArrayRef),
        Finish(&'a ArrayRef, usize),
    }
    let mut work = vec![Work::Visit(array)];
    let mut results: Vec<(ArrayRef, bool)> = Vec::new();
    while let Some(next) = work.pop() {
        match next {
            Work::Visit(array) => match array.data_type() {
                DataType::Struct(fields) => {
                    for field in fields {
                        check_uuid(field)?;
                    }
                    let columns = array.as_struct().columns();
                    work.push(Work::Finish(array, columns.len()));
                    work.extend(columns.iter().rev().map(Work::Visit));
                }
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::ListView(field)
                | DataType::LargeListView(field)
                | DataType::FixedSizeList(field, _) => {
                    check_uuid(field)?;
                    let values = list_values(array)?;
                    work.push(Work::Finish(array, 1));
                    work.push(Work::Visit(values));
                }
                DataType::Dictionary(_, value) => {
                    // Parquet dictionary encoding applies to scalar leaves, never containers.
                    if value.is_nested() || matches!(value.as_ref(), DataType::Dictionary(..)) {
                        return Err(DataFusionError::Execution(
                            "Unsupported nested Variant dictionary storage".into(),
                        ));
                    }
                    let target = scalar_storage_type(value);
                    results.push((
                        cast_with_options(
                            array.as_ref(),
                            &target,
                            &arrow::compute::CastOptions {
                                safe: false,
                                ..Default::default()
                            },
                        )?,
                        true,
                    ));
                }
                DataType::Map(..) => {
                    return Err(DataFusionError::Execution(
                        "Unsupported Variant map storage".into(),
                    ))
                }
                data_type => {
                    let target = scalar_storage_type(data_type);
                    if &target == data_type {
                        results.push((Arc::clone(array), false));
                    } else {
                        results.push((
                            cast_with_options(
                                array.as_ref(),
                                &target,
                                &arrow::compute::CastOptions {
                                    safe: false,
                                    ..Default::default()
                                },
                            )?,
                            true,
                        ));
                    }
                }
            },
            Work::Finish(array, count) => {
                let children = results.split_off(results.len() - count);
                if children.iter().all(|(_, changed)| !changed)
                    && !matches!(array.data_type(), DataType::FixedSizeList(..))
                {
                    results.push((Arc::clone(array), false));
                    continue;
                }
                let rebuilt: ArrayRef = match array.data_type() {
                    DataType::Struct(fields) => {
                        let fields = fields
                            .iter()
                            .zip(&children)
                            .map(|(field, (value, changed))| {
                                if *changed {
                                    Arc::new(
                                        field
                                            .as_ref()
                                            .clone()
                                            .with_data_type(value.data_type().clone()),
                                    )
                                } else {
                                    Arc::clone(field)
                                }
                            })
                            .collect::<Vec<_>>();
                        Arc::new(StructArray::try_new(
                            fields.into(),
                            children.into_iter().map(|(v, _)| v).collect(),
                            array.nulls().cloned(),
                        )?)
                    }
                    DataType::List(field) => {
                        let list = array.as_list::<i32>();
                        let values = Arc::clone(&children[0].0);
                        Arc::new(ListArray::try_new(
                            Arc::new(
                                field
                                    .as_ref()
                                    .clone()
                                    .with_data_type(values.data_type().clone()),
                            ),
                            list.offsets().clone(),
                            values,
                            list.nulls().cloned(),
                        )?)
                    }
                    DataType::LargeList(field) => {
                        let list = array.as_list::<i64>();
                        let values = Arc::clone(&children[0].0);
                        Arc::new(LargeListArray::try_new(
                            Arc::new(
                                field
                                    .as_ref()
                                    .clone()
                                    .with_data_type(values.data_type().clone()),
                            ),
                            list.offsets().clone(),
                            values,
                            list.nulls().cloned(),
                        )?)
                    }
                    DataType::ListView(field) => {
                        let list = array.as_list_view::<i32>();
                        let values = Arc::clone(&children[0].0);
                        Arc::new(ListViewArray::try_new(
                            Arc::new(
                                field
                                    .as_ref()
                                    .clone()
                                    .with_data_type(values.data_type().clone()),
                            ),
                            list.offsets().clone(),
                            list.sizes().clone(),
                            values,
                            list.nulls().cloned(),
                        )?)
                    }
                    DataType::LargeListView(field) => {
                        let list = array.as_list_view::<i64>();
                        let values = Arc::clone(&children[0].0);
                        Arc::new(LargeListViewArray::try_new(
                            Arc::new(
                                field
                                    .as_ref()
                                    .clone()
                                    .with_data_type(values.data_type().clone()),
                            ),
                            list.offsets().clone(),
                            list.sizes().clone(),
                            values,
                            list.nulls().cloned(),
                        )?)
                    }
                    DataType::FixedSizeList(field, size) => {
                        let list = array.as_fixed_size_list();
                        let values = Arc::clone(&children[0].0);
                        let offsets = (0..=array.len())
                            .map(|i| {
                                i32::try_from(i * (*size as usize))
                                    .map_err(|_| ArrowError::OffsetOverflowError(i))
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        Arc::new(ListArray::try_new(
                            Arc::new(
                                field
                                    .as_ref()
                                    .clone()
                                    .with_data_type(values.data_type().clone()),
                            ),
                            OffsetBuffer::new(offsets.into()),
                            values,
                            list.nulls().cloned(),
                        )?)
                    }
                    _ => unreachable!(),
                };
                owners.0.push(Arc::clone(&rebuilt));
                results.push((rebuilt, true));
            }
        }
    }
    Ok(results.pop().unwrap().0)
}

fn list_values(array: &ArrayRef) -> DataFusionResult<&ArrayRef> {
    Ok(match array.data_type() {
        DataType::List(_) => array.as_list::<i32>().values(),
        DataType::LargeList(_) => array.as_list::<i64>().values(),
        DataType::ListView(_) => array.as_list_view::<i32>().values(),
        DataType::LargeListView(_) => array.as_list_view::<i64>().values(),
        DataType::FixedSizeList(..) => array.as_fixed_size_list().values(),
        _ => return malformed(),
    })
}

fn list_elements(
    array: &ArrayRef,
    row: usize,
) -> DataFusionResult<(&StructArray, std::ops::Range<usize>)> {
    let values = list_values(array)?
        .as_struct_opt()
        .ok_or_else(|| DataFusionError::from(SparkError::MalformedVariant))?;
    let range = match array.data_type() {
        DataType::List(_) => array.as_list::<i32>().element_range(row),
        DataType::LargeList(_) => array.as_list::<i64>().element_range(row),
        DataType::ListView(_) => array.as_list_view::<i32>().element_range(row),
        DataType::LargeListView(_) => array.as_list_view::<i64>().element_range(row),
        _ => return malformed(),
    };
    Ok((values, range))
}

fn binary_value(array: &ArrayRef, row: usize) -> Result<&[u8], ArrowError> {
    match array.data_type() {
        DataType::Binary => Ok(array.as_binary::<i32>().value(row)),
        DataType::LargeBinary => Ok(array.as_binary::<i64>().value(row)),
        DataType::BinaryView => Ok(array.as_binary_view().value(row)),
        data_type => Err(ArrowError::InvalidArgumentError(format!(
            "Expected Variant binary storage, got {data_type}"
        ))),
    }
}

fn source_metadata(bytes: &[u8]) -> DataFusionResult<VariantMetadata<'_>> {
    if let Ok(metadata) = VariantMetadata::try_new(bytes) {
        return Ok(metadata);
    }
    // Spark's unsorted empty-key dictionaries use equal offsets. Accept exactly that historical
    // writer encoding, with every dictionary entry validated and duplicate names rejected.
    let original = VariantMetadata::new(bytes);
    let names = original.iter_try().collect::<Result<Vec<_>, _>>()?;
    if !names.contains(&"") {
        return malformed();
    }
    let mut unique = HashSet::new();
    if names.iter().any(|name| !unique.insert(*name)) {
        return malformed();
    }
    let mut builder = WritableMetadataBuilder::from_iter(names);
    builder.finish();
    let mut expected = builder.into_inner();
    expected[0] &= !0x10;
    if expected != bytes {
        return malformed();
    }
    Ok(original)
}

fn state_present(state: &StructArray, row: usize) -> DataFusionResult<bool> {
    if state.is_null(row) {
        return malformed();
    }
    Ok(["typed_value", "value"]
        .iter()
        .any(|name| state.column_by_name(name).is_some_and(|v| v.is_valid(row))))
}

/// Arrow is used only for flat scalar columns. Its recursive container validation and unshredding
/// never receive the nested input tree. Cache the flat conversion for every scalar column.
#[derive(Default)]
struct ScalarColumns {
    values: HashMap<usize, ArrayRef>,
}
impl ScalarColumns {
    fn prepare(&mut self, array: &ArrayRef) -> DataFusionResult<()> {
        let key = Arc::as_ptr(array) as *const () as usize;
        if let std::collections::hash_map::Entry::Vacant(entry) = self.values.entry(key) {
            let mut residual = BinaryBuilder::new();
            for row in 0..array.len() {
                residual.append_option(array.is_null(row).then_some(&[0_u8][..]));
            }
            let metadata: ArrayRef =
                Arc::new(BinaryArray::from(vec![&[1_u8, 0, 0][..]; array.len()]));
            let residual: ArrayRef = Arc::new(residual.finish());
            let flat = StructArray::try_new(
                vec![
                    Arc::new(Field::new("metadata", DataType::Binary, false)),
                    Arc::new(Field::new("value", DataType::Binary, true)),
                    Arc::new(Field::new("typed_value", array.data_type().clone(), true)),
                ]
                .into(),
                vec![metadata, residual, Arc::clone(array)],
                None,
            )?;
            let variant = VariantArray::try_new(&flat)?;
            let value = unshred_variant(&variant)?;
            entry.insert(Arc::clone(value.value_column()));
        }
        Ok(())
    }

    fn encode(&mut self, array: &ArrayRef, row: usize) -> DataFusionResult<Vec<u8>> {
        self.prepare(array)?;
        let key = Arc::as_ptr(array) as *const () as usize;
        let value = Variant::new(&[1, 0, 0], binary_value(&self.values[&key], row)?);
        if matches!(value, Variant::Object(_) | Variant::List(_)) {
            return malformed();
        }
        scalar_bytes(spark_typed_scalar(value))
    }
}

/// Validate physical states even when all their rows are inactive. Never pass a container
/// schema to Arrow's recursive VariantArray validation.
fn validate_shredding_schema(
    root: &StructArray,
    scalars: &mut ScalarColumns,
) -> DataFusionResult<()> {
    let mut pending = vec![root];
    while let Some(state) = pending.pop() {
        let residual = state.column_by_name("value");
        let typed = state.column_by_name("typed_value");
        if residual.is_none() && typed.is_none() {
            return malformed();
        }
        if residual.is_some_and(|v| {
            !matches!(
                v.data_type(),
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            )
        }) {
            return malformed();
        }
        let Some(typed) = typed else {
            continue;
        };
        match typed.data_type() {
            DataType::Struct(_) => {
                for column in typed.as_struct().columns() {
                    pending.push(
                        column
                            .as_struct_opt()
                            .ok_or_else(|| DataFusionError::from(SparkError::MalformedVariant))?,
                    );
                }
            }
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_) => {
                pending.push(
                    list_values(typed)?
                        .as_struct_opt()
                        .ok_or_else(|| DataFusionError::from(SparkError::MalformedVariant))?,
                );
            }
            _ => scalars.prepare(typed)?,
        }
    }
    Ok(())
}

fn scalar_bytes(value: Variant<'_, '_>) -> DataFusionResult<Vec<u8>> {
    let mut builder = ValueBuilder::new();
    let metadata = VariantMetadata::new(&[1, 0, 0]);
    let mut metadata = ReadOnlyMetadataBuilder::new(&metadata);
    ValueBuilder::try_append_variant(ParentState::variant(&mut builder, &mut metadata), value)?;
    Ok(builder.into_inner())
}

#[derive(Clone)]
enum Source<'a> {
    State(&'a StructArray, usize),
    Residual(Variant<'a, 'a>, bool),
}

enum PlannedValue<'a> {
    Scalar(Vec<u8>),
    Object(Vec<(&'a str, usize)>),
    List(Vec<usize>),
}
struct Plan<'a> {
    values: Vec<PlannedValue<'a>>,
    names: Vec<&'a str>,
    root: usize,
}

/// Construct a postorder plan using heap frames. Field names are registered on entry, preserving
/// Spark's dictionary traversal order; typed fields precede residual fields in objects.
fn plan_value<'a>(
    source: Source<'a>,
    metadata: &VariantMetadata<'a>,
    scalars: &mut ScalarColumns,
    list_rows: &mut HashMap<(usize, usize), usize>,
    root_row: usize,
) -> DataFusionResult<Plan<'a>> {
    enum Work<'a> {
        Visit(Source<'a>, usize),
        Field(&'a str, Source<'a>, usize),
        FieldDone(&'a str),
        Object(usize),
        List(usize),
    }
    struct Child<'a> {
        name: Option<&'a str>,
        value: usize,
    }
    let mut work = vec![Work::Visit(source, 0)];
    let mut values = Vec::new();
    let mut results: Vec<Child<'a>> = Vec::new();
    let mut names = Vec::new();
    let mut seen_names = HashSet::new();
    while let Some(next) = work.pop() {
        match next {
            Work::Field(name, source, depth) => {
                if seen_names.insert(name) {
                    names.push(name);
                }
                work.push(Work::FieldDone(name));
                work.push(Work::Visit(source, depth));
            }
            Work::FieldDone(name) => results.last_mut().unwrap().name = Some(name),
            Work::Object(count) => {
                let children = results.split_off(results.len() - count);
                let index = values.len();
                values.push(PlannedValue::Object(
                    children
                        .into_iter()
                        .map(|v| (v.name.unwrap(), v.value))
                        .collect(),
                ));
                results.push(Child {
                    name: None,
                    value: index,
                });
            }
            Work::List(count) => {
                let children = results.split_off(results.len() - count);
                let index = values.len();
                values.push(PlannedValue::List(
                    children.into_iter().map(|v| v.value).collect(),
                ));
                results.push(Child {
                    name: None,
                    value: index,
                });
            }
            Work::Visit(Source::Residual(value, validate), depth) => {
                if validate {
                    check_variant_depth(value.clone(), depth)?;
                }
                match value {
                    Variant::Object(object) => {
                        if depth >= MAX_VARIANT_NESTING_DEPTH {
                            return malformed();
                        }
                        let children = object.iter().collect::<Vec<_>>();
                        work.push(Work::Object(children.len()));
                        work.extend(children.into_iter().rev().map(|(name, value)| {
                            Work::Field(name, Source::Residual(value, false), depth + 1)
                        }));
                    }
                    Variant::List(list) => {
                        if depth >= MAX_VARIANT_NESTING_DEPTH {
                            return malformed();
                        }
                        let children = list.iter().collect::<Vec<_>>();
                        work.push(Work::List(children.len()));
                        work.extend(
                            children.into_iter().rev().map(|value| {
                                Work::Visit(Source::Residual(value, false), depth + 1)
                            }),
                        );
                    }
                    value => {
                        let index = values.len();
                        values.push(PlannedValue::Scalar(scalar_bytes(value)?));
                        results.push(Child {
                            name: None,
                            value: index,
                        });
                    }
                }
            }
            Work::Visit(Source::State(state, row), depth) => {
                if !state_present(state, row)? {
                    return malformed();
                }
                let typed = state
                    .column_by_name("typed_value")
                    .filter(|v| v.is_valid(row));
                let residual = state.column_by_name("value").filter(|v| v.is_valid(row));
                let Some(typed) = typed else {
                    let value = Variant::new_with_metadata(
                        metadata.clone(),
                        binary_value(residual.unwrap(), row)?,
                    );
                    work.push(Work::Visit(Source::Residual(value, true), depth));
                    continue;
                };
                // Partial shredding is defined only for objects. Scalars and arrays cannot
                // carry a residual alongside a non-null typed value.
                if residual.is_some() && !matches!(typed.data_type(), DataType::Struct(_)) {
                    return malformed();
                }
                match typed.data_type() {
                    DataType::Struct(_) => {
                        if depth >= MAX_VARIANT_NESTING_DEPTH {
                            return malformed();
                        }
                        let object = typed.as_struct();
                        let mut children = Vec::new();
                        for (field, column) in object.fields().iter().zip(object.columns()) {
                            let child = column.as_struct_opt().ok_or_else(|| {
                                DataFusionError::from(SparkError::MalformedVariant)
                            })?;
                            if state_present(child, row)? {
                                children.push((field.name().as_str(), Source::State(child, row)));
                            }
                        }
                        if let Some(residual) = residual {
                            let value = Variant::new_with_metadata(
                                metadata.clone(),
                                binary_value(residual, row)?,
                            );
                            check_variant_depth(value.clone(), depth)?;
                            let Variant::Object(residual) = value else {
                                return malformed();
                            };
                            for (name, value) in residual.iter() {
                                if object.fields().iter().any(|f| f.name() == name) {
                                    return malformed();
                                }
                                children.push((name, Source::Residual(value, false)));
                            }
                        }
                        work.push(Work::Object(children.len()));
                        work.extend(
                            children
                                .into_iter()
                                .rev()
                                .map(|(name, value)| Work::Field(name, value, depth + 1)),
                        );
                    }
                    DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::ListView(_)
                    | DataType::LargeListView(_) => {
                        if depth >= MAX_VARIANT_NESTING_DEPTH {
                            return malformed();
                        }
                        let (states, range) = list_elements(typed, row)?;
                        work.push(Work::List(range.len()));
                        for row in range.rev() {
                            let key = (states as *const StructArray as usize, row);
                            if list_rows
                                .insert(key, root_row)
                                .is_some_and(|prior| prior != root_row)
                            {
                                return Err(DataFusionError::Execution(
                                    "A shared Variant list child refers to different metadata rows"
                                        .into(),
                                ));
                            }
                            work.push(Work::Visit(Source::State(states, row), depth + 1));
                        }
                    }
                    _ => {
                        let index = values.len();
                        values.push(PlannedValue::Scalar(scalars.encode(typed, row)?));
                        results.push(Child {
                            name: None,
                            value: index,
                        });
                    }
                }
            }
        }
    }
    Ok(Plan {
        values,
        names,
        root: results.pop().unwrap().value,
    })
}

#[derive(Clone, Copy)]
enum VariantObjectKeyOrder {
    #[cfg(test)]
    ArrowUtf8,
    SparkUtf16,
}

/// Encode one container at a time from already encoded children. The Arrow byte-copy APIs do
/// not traverse descendants, unlike append_variant/Variant::try_new on a container. Taking each
/// child buffer as soon as its parent consumes it also avoids retaining all ancestor encodings.
fn encode_plan(
    plan: Plan<'_>,
    metadata: &VariantMetadata<'_>,
    order: VariantObjectKeyOrder,
) -> DataFusionResult<Vec<u8>> {
    let mut encoded: Vec<Option<Vec<u8>>> = Vec::with_capacity(plan.values.len());
    let mut dictionary: Box<dyn MetadataBuilder> = match order {
        #[cfg(test)]
        VariantObjectKeyOrder::ArrowUtf8 => Box::new(ReadOnlyMetadataBuilder::new(metadata)),
        VariantObjectKeyOrder::SparkUtf16 => Box::new(SparkMetadataBuilder::new(metadata)),
    };
    for value in plan.values {
        let bytes = match value {
            PlannedValue::Scalar(bytes) => bytes,
            PlannedValue::Object(fields) => {
                let mut value = ValueBuilder::new();
                let mut builder =
                    ObjectBuilder::new(ParentState::variant(&mut value, dictionary.as_mut()), true);
                for (name, child) in fields {
                    let child = encoded[child].take().unwrap();
                    builder.try_insert_bytes(
                        name,
                        Variant::new_with_metadata(metadata.clone(), &child),
                    )?;
                }
                builder.finish();
                value.into_inner()
            }
            PlannedValue::List(children) => {
                let mut value = ValueBuilder::new();
                let mut dictionary = ReadOnlyMetadataBuilder::new(metadata);
                let mut builder =
                    ListBuilder::new(ParentState::variant(&mut value, &mut dictionary), true);
                for child in children {
                    let child = encoded[child].take().unwrap();
                    builder
                        .append_value_bytes(Variant::new_with_metadata(metadata.clone(), &child));
                }
                builder.finish();
                value.into_inner()
            }
        };
        encoded.push(Some(bytes));
    }
    Ok(encoded[plan.root].take().unwrap())
}

pub(super) fn normalize_variant_array(
    array: &ArrayRef,
    target_field: &FieldRef,
) -> DataFusionResult<ArrayRef> {
    let DataType::Struct(fields) = target_field.data_type() else {
        return malformed();
    };
    if fields.len() != 2
        || fields[0].name() != "value"
        || fields[1].name() != "metadata"
        || fields.iter().any(|f| f.data_type() != &DataType::Binary)
    {
        return malformed();
    }
    check_variant_storage_depth(array.data_type())?;
    let mut owners = ArrayOwners::default();
    let array = normalize_variant_storage(array, &mut owners)?;
    let state = array
        .as_struct_opt()
        .ok_or_else(|| DataFusionError::from(SparkError::MalformedVariant))?;
    let metadata = state
        .column_by_name("metadata")
        .ok_or_else(|| DataFusionError::from(SparkError::MalformedVariant))?;
    if !matches!(
        metadata.data_type(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    ) {
        return malformed();
    }
    let metadata = cast(metadata.as_ref(), &DataType::Binary)?;
    let metadata = metadata.as_binary::<i32>();
    let rebuild = state.column_by_name("typed_value").is_some();
    let mut scalars = ScalarColumns::default();
    validate_shredding_schema(state, &mut scalars)?;
    let mut list_rows = HashMap::new();
    let mut output_values = BinaryBuilder::new();
    let mut output_metadata = BinaryBuilder::new();
    for row in 0..state.len() {
        if state.is_null(row) {
            if rebuild {
                output_values.append_null();
                output_metadata.append_null();
            }
            continue;
        }
        if metadata.is_null(row) {
            return malformed();
        }
        let encoded = catch_unwind(AssertUnwindSafe(
            || -> DataFusionResult<Option<(Vec<u8>, Vec<u8>)>> {
                let source = source_metadata(metadata.value(row))?;
                if !rebuild {
                    let value = state
                        .column_by_name("value")
                        .filter(|v| v.is_valid(row))
                        .ok_or_else(|| DataFusionError::from(SparkError::MalformedVariant))?;
                    check_variant_depth(
                        Variant::new_with_metadata(source, binary_value(value, row)?),
                        0,
                    )?;
                    return Ok(None);
                }
                let plan = plan_value(
                    Source::State(state, row),
                    &source,
                    &mut scalars,
                    &mut list_rows,
                    row,
                )?;
                let mut dictionary = WritableMetadataBuilder::from_iter(plan.names.iter().copied());
                dictionary.finish();
                let mut dictionary = dictionary.into_inner();
                dictionary[0] &= !0x10;
                let value = encode_plan(
                    plan,
                    &VariantMetadata::new(&dictionary),
                    VariantObjectKeyOrder::SparkUtf16,
                )?;
                Ok(Some((value, dictionary)))
            },
        ))
        .map_err(|_| SparkError::MalformedVariant)??;
        if let Some((value, metadata)) = encoded {
            output_values.append_value(value);
            output_metadata.append_value(metadata);
        }
    }
    let (value, metadata): (ArrayRef, ArrayRef) = if rebuild {
        (
            Arc::new(output_values.finish()),
            Arc::new(output_metadata.finish()),
        )
    } else {
        let value = state
            .column_by_name("value")
            .ok_or_else(|| DataFusionError::from(SparkError::MalformedVariant))?;
        let mut value = cast(value.as_ref(), &DataType::Binary)?;
        if state.null_count() != 0 {
            value = arrow::compute::nullif(value.as_ref(), &arrow::compute::is_null(state)?)?;
        }
        (
            value,
            cast(
                state.column_by_name("metadata").unwrap().as_ref(),
                &DataType::Binary,
            )?,
        )
    };
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        vec![value, metadata],
        state.nulls().cloned(),
    )?))
}

#[cfg(test)]
fn variant_bytes(
    metadata: &VariantMetadata<'_>,
    variant: Variant<'_, '_>,
    order: VariantObjectKeyOrder,
) -> Result<Vec<u8>, ArrowError> {
    let plan = plan_value(
        Source::Residual(variant, true),
        metadata,
        &mut ScalarColumns::default(),
        &mut HashMap::new(),
        0,
    )
    .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?;
    encode_plan(plan, metadata, order).map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))
}
fn spark_typed_scalar<'m, 'v>(value: Variant<'m, 'v>) -> Variant<'m, 'v> {
    match value {
        Variant::Int8(_) | Variant::Int16(_) | Variant::Int32(_) | Variant::Int64(_) => {
            let value = value.as_int64().unwrap();
            if let Ok(value) = i8::try_from(value) {
                Variant::Int8(value)
            } else if let Ok(value) = i16::try_from(value) {
                Variant::Int16(value)
            } else if let Ok(value) = i32::try_from(value) {
                Variant::Int32(value)
            } else {
                Variant::Int64(value)
            }
        }
        Variant::Decimal16(decimal) => {
            if let Ok(decimal) = VariantDecimal4::try_from(decimal) {
                Variant::Decimal4(decimal)
            } else if let Ok(decimal) = VariantDecimal8::try_from(decimal) {
                Variant::Decimal8(decimal)
            } else {
                value
            }
        }
        Variant::Decimal8(decimal) => VariantDecimal4::try_from(decimal)
            .map(Variant::Decimal4)
            .unwrap_or(value),
        Variant::String(s) => Variant::from(s),
        Variant::Float(v) => Variant::Float(canonicalize_nan(v)),
        Variant::Double(v) => Variant::Double(canonicalize_nan(v)),
        _ => value,
    }
}

/// Supplies sort-only field names whose Rust ordering matches Java `String.compareTo` ordering.
/// Field IDs still come from the original metadata dictionary.
#[derive(Debug)]
struct SparkMetadataBuilder<'a, 'm> {
    metadata: &'a VariantMetadata<'m>,
    sort_keys: Vec<String>,
}

impl<'a, 'm> SparkMetadataBuilder<'a, 'm> {
    fn new(metadata: &'a VariantMetadata<'m>) -> Self {
        let sort_keys = metadata.iter().map(spark_sort_key).collect();
        Self {
            metadata,
            sort_keys,
        }
    }
}

impl MetadataBuilder for SparkMetadataBuilder<'_, '_> {
    fn try_upsert_field_name(&mut self, field_name: &str) -> Result<u32, ArrowError> {
        self.metadata
            .get_entry(field_name)
            .map(|(field_id, _)| field_id)
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "Field name '{field_name}' not found in metadata dictionary"
                ))
            })
    }

    fn field_name(&self, field_id: usize) -> &str {
        &self.sort_keys[field_id]
    }

    fn num_field_names(&self) -> usize {
        self.metadata.len()
    }

    fn truncate_field_names(&mut self, new_size: usize) {
        debug_assert_eq!(self.metadata.len(), new_size);
    }

    fn finish(&mut self) -> usize {
        self.metadata.size()
    }
}

#[cfg(test)]
fn rewrite_residual_values(
    value: &ArrayRef,
    metadata: &BinaryArray,
    target_metadata: Option<&BinaryArray>,
    metadata_rows: &[Option<usize>],
    depth: usize,
) -> DataFusionResult<(ArrayRef, bool)> {
    let binary = cast(value.as_ref(), &DataType::Binary)?;
    let binary = binary.as_binary::<i32>();
    let mut output: Option<BinaryBuilder> = None;

    for (index, metadata_row) in metadata_rows.iter().enumerate() {
        if binary.is_null(index) {
            if let Some(output) = &mut output {
                output.append_null();
            }
            continue;
        }
        let Some(metadata_row) = metadata_row else {
            if let Some(output) = &mut output {
                output.append_value(binary.value(index));
            }
            continue;
        };
        if metadata.is_null(*metadata_row) {
            return Err(DataFusionError::Execution(format!(
                "Variant metadata is null at row {metadata_row}"
            )));
        }

        catch_unwind(AssertUnwindSafe(|| {
            check_variant_depth(
                Variant::new(metadata.value(*metadata_row), binary.value(index)),
                depth,
            )
        }))
        .map_err(|_| SparkError::MalformedVariant)??;

        let rebuilt = catch_unwind(AssertUnwindSafe(
            || -> Result<Option<Vec<u8>>, ArrowError> {
                let source = metadata.value(*metadata_row);
                let target = target_metadata
                    .map(|metadata| metadata.value(*metadata_row))
                    .filter(|target| *target != source)
                    .map(VariantMetadata::try_new)
                    .transpose()?;
                let metadata = if target.is_some() {
                    // The empty-key workaround validated every original dictionary entry.
                    VariantMetadata::new(source)
                } else {
                    VariantMetadata::try_new(source)?
                };
                let variant = Variant::new_with_metadata(metadata.clone(), binary.value(index));
                let arrow_ordered =
                    is_compatible_variant(&variant, VariantObjectKeyOrder::ArrowUtf8);
                if arrow_ordered && target.is_none() {
                    return Ok(None);
                }
                if !arrow_ordered
                    && !is_compatible_variant(&variant, VariantObjectKeyOrder::SparkUtf16)
                {
                    return Err(ArrowError::InvalidArgumentError(
                        "Variant residual is neither UTF-8 nor Spark UTF-16 ordered".to_string(),
                    ));
                }
                Ok(Some(variant_bytes(
                    target.as_ref().unwrap_or(&metadata),
                    variant,
                    VariantObjectKeyOrder::ArrowUtf8,
                )?))
            },
        ))
        .map_err(|_| {
            DataFusionError::Execution(format!("Invalid Variant residual at row {metadata_row}"))
        })??;
        if rebuilt.is_some() && output.is_none() {
            output = Some(binary_prefix_builder(binary, index));
        }
        if let Some(output) = &mut output {
            output.append_value(rebuilt.as_deref().unwrap_or_else(|| binary.value(index)));
        }
    }

    if let Some(mut output) = output {
        Ok((Arc::new(output.finish()), true))
    } else {
        Ok((Arc::clone(value), false))
    }
}

/// Released Spark 4 profiles search object fields in Java UTF-16 order. Values already in that
/// order remain byte-for-byte unchanged.
/// TODO: Remove this output rewrite once every supported Spark profile includes SPARK-58949.
/// Retain input conversion for historical Spark files with UTF-16 object-key ordering.
/// https://github.com/apache/datafusion-comet/issues/5474
#[cfg(test)]
fn reorder_variant_values(
    value: &ArrayRef,
    metadata: &ArrayRef,
    parent_nulls: Option<&arrow::buffer::NullBuffer>,
) -> DataFusionResult<ArrayRef> {
    let original = value;
    let value = value.as_binary::<i32>();
    let metadata = metadata.as_binary::<i32>();
    let mut output: Option<BinaryBuilder> = None;

    for index in 0..value.len() {
        if parent_nulls.is_some_and(|nulls| nulls.is_null(index)) {
            if value.is_valid(index) && output.is_none() {
                output = Some(binary_prefix_builder(value, index));
            }
            if let Some(output) = &mut output {
                output.append_null();
            }
            continue;
        }
        if value.is_null(index) {
            return Err(SparkError::MalformedVariant.into());
        }
        if metadata.is_null(index) {
            return Err(DataFusionError::Execution(format!(
                "Variant metadata is null at row {index}"
            )));
        }

        let rebuilt = catch_unwind(AssertUnwindSafe(
            || -> Result<Option<Vec<u8>>, ArrowError> {
                let metadata = VariantMetadata::try_new(metadata.value(index))?;
                let variant = Variant::new_with_metadata(metadata.clone(), value.value(index));
                check_variant_depth(variant.clone(), 0)
                    .map_err(|error| ArrowError::InvalidArgumentError(error.to_string()))?;
                if is_compatible_variant(&variant, VariantObjectKeyOrder::SparkUtf16) {
                    return Ok(None);
                }
                Ok(Some(variant_bytes(
                    &metadata,
                    variant,
                    VariantObjectKeyOrder::SparkUtf16,
                )?))
            },
        ))
        .map_err(|_| {
            DataFusionError::Execution(format!("Invalid Variant value at row {index}"))
        })??;
        if rebuilt.is_some() && output.is_none() {
            output = Some(binary_prefix_builder(value, index));
        }
        if let Some(output) = &mut output {
            output.append_value(rebuilt.as_deref().unwrap_or_else(|| value.value(index)));
        }
    }

    match output {
        Some(mut output) => Ok(Arc::new(output.finish())),
        None => Ok(Arc::clone(original)),
    }
}

#[cfg(test)]
// Called only at the first changed row; the unchanged prefix is copied once.
fn binary_prefix_builder(value: &BinaryArray, end: usize) -> BinaryBuilder {
    let mut builder = BinaryBuilder::new();
    builder.extend(value.iter().take(end));
    builder
}

#[cfg(test)]
fn is_compatible_variant(variant: &Variant<'_, '_>, order: VariantObjectKeyOrder) -> bool {
    let mut pending = vec![variant.clone()];
    while let Some(value) = pending.pop() {
        match value {
            Variant::Object(object) => {
                let mut previous = None;
                for (name, value) in object.iter() {
                    let ordered = previous
                        .map(|previous: &str| match order {
                            VariantObjectKeyOrder::ArrowUtf8 => previous <= name,
                            VariantObjectKeyOrder::SparkUtf16 => {
                                previous.encode_utf16().cmp(name.encode_utf16())
                                    != std::cmp::Ordering::Greater
                            }
                        })
                        .unwrap_or(true);
                    if !ordered {
                        return false;
                    }
                    previous = Some(name);
                    pending.push(value);
                }
            }
            Variant::List(list) => pending.extend(list.iter()),
            _ => {}
        }
    }
    true
}

#[cfg(test)]
mod tests;

fn spark_sort_key(name: &str) -> String {
    name.encode_utf16()
        .map(|unit| char::from_u32(0x10000 + u32::from(unit)).unwrap())
        .collect()
}
