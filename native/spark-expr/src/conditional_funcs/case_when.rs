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

use crate::{Cast, EvalMode, IfExpr, NormalizeNaNAndZero, SparkCastOptions};
use arrow::array::{
    downcast_primitive, Array, ArrayRef, AsArray, BooleanArray, GenericByteArray, PrimitiveArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::nullif;
use arrow::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, BinaryType, ByteArrayType, DataType, LargeBinaryType,
    LargeUtf8Type, Schema, Utf8Type,
};
use arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::{
    exec_err, internal_datafusion_err, internal_err, DataFusionError, Result, ScalarValue,
};
use datafusion::logical_expr::type_coercion::other::get_coerce_type_for_case_expression;
use datafusion::logical_expr::{ColumnarValue, Operator};
use datafusion::physical_expr::expressions::{
    BinaryExpr, CaseExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
};
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::{Arc, OnceLock};

type WhenThen = (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>);

/// Creates a `CASE WHEN`, casting any branch value whose type differs from the others to their
/// common type.
pub fn create_case_when(
    when_then: Vec<WhenThen>,
    else_expr: Option<Arc<dyn PhysicalExpr>>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let then_types = when_then
        .iter()
        .map(|(_, then)| then.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;
    let else_type = else_expr
        .as_ref()
        .map(|e| e.data_type(input_schema))
        .transpose()?
        .unwrap_or(DataType::Null);

    let Some(coerce_type) = get_coerce_type_for_case_expression(&then_types, Some(&else_type))
    else {
        return Ok(Arc::new(CaseWhenExpr::try_new(when_then, else_expr)?));
    };
    // The branches share a Spark type, so any difference is in the Arrow representation. For a
    // timestamp that is the timezone label, and the cast only relabels it, but Comet's cast still
    // needs a timezone. Every `TimestampType` value in a native plan is labelled UTC.
    let cast_options = SparkCastOptions::new(EvalMode::Legacy, "UTC", false);
    // A branch that already has the common type is not wrapped in a cast, which would do nothing
    // but hide what the branch is from the evaluation.
    let coerce = |expr: Arc<dyn PhysicalExpr>, data_type: &DataType| -> Arc<dyn PhysicalExpr> {
        if data_type == &coerce_type {
            expr
        } else {
            Arc::new(Cast::new(
                expr,
                coerce_type.clone(),
                cast_options.clone(),
                None,
                None,
            ))
        }
    };
    let when_then = when_then
        .into_iter()
        .zip(&then_types)
        .map(|((when, then), then_type)| (when, coerce(then, then_type)))
        .collect();
    let else_expr = else_expr.map(|e| coerce(e, &else_type));
    Ok(Arc::new(CaseWhenExpr::try_new(when_then, else_expr)?))
}

/// Spark's `CASE WHEN`, which Comet also uses for `IF` and `COALESCE`.
///
/// Spark evaluates a WHEN only for the rows that no earlier WHEN matched, and a branch's value
/// only for the rows that choose that branch. DataFusion's [`CaseExpr`] does the same, by filtering
/// the batch down to those rows for every branch and merging the partial results. That is needed
/// when an expression could fail for a row Spark would not evaluate it for, such as a division by
/// a zero that the WHEN rules out.
///
/// When none of them can fail, evaluating them over the whole batch returns the same values
/// without the filters and the merge. This does that and then takes each row's value from the
/// branch the row chose. It uses [`CaseExpr`] otherwise.
#[derive(Debug)]
pub struct CaseWhenExpr {
    /// Holds the branches, and evaluates each one for just the rows that choose it
    lazy: CaseExpr,
    /// The branches with a CASE or IF in the ELSE spliced in, as they are evaluated eagerly.
    /// `IF(a, x, IF(b, y, z))` is `CASE WHEN a THEN x WHEN b THEN y ELSE z END`, and merging
    /// the branches once is cheaper than merging each level into the next.
    flat_when_then: Vec<WhenThen>,
    flat_else_expr: Option<Arc<dyn PhysicalExpr>>,
    /// The result type if every branch is evaluated over the whole batch, or `None` if they are
    /// evaluated lazily, decided on the first batch
    eager: OnceLock<Option<DataType>>,
}

impl CaseWhenExpr {
    pub fn try_new(
        when_then: Vec<WhenThen>,
        else_expr: Option<Arc<dyn PhysicalExpr>>,
    ) -> Result<Self> {
        let lazy = CaseExpr::try_new(None, when_then, else_expr)?;
        let nested = lazy.else_expr().and_then(|e| {
            e.downcast_ref::<CaseWhenExpr>()
                .or_else(|| e.downcast_ref::<IfExpr>().map(IfExpr::case_when))
        });
        let (flat_when_then, flat_else_expr) = match nested {
            Some(nested) => (
                lazy.when_then_expr()
                    .iter()
                    .chain(&nested.flat_when_then)
                    .cloned()
                    .collect(),
                nested.flat_else_expr.clone(),
            ),
            None => (lazy.when_then_expr().to_vec(), lazy.else_expr().cloned()),
        };
        Ok(Self {
            lazy,
            flat_when_then,
            flat_else_expr,
            eager: OnceLock::new(),
        })
    }

    /// The branch values, THEN and ELSE, in the order they are evaluated eagerly.
    fn values(&self) -> impl Iterator<Item = &Arc<dyn PhysicalExpr>> {
        self.flat_when_then
            .iter()
            .map(|(_, then)| then)
            .chain(&self.flat_else_expr)
    }

    /// The result type, if every expression that Spark evaluates for only some of the rows can
    /// be evaluated for all of them and the result is a type that [`merge`] handles.
    fn eager_result_type(&self, input_schema: &Schema) -> Option<DataType> {
        let data_type = self.data_type(input_schema).ok()?;
        // Spark evaluates the first WHEN for every row as well
        let later_whens = self.flat_when_then.iter().skip(1).map(|(when, _)| when);
        let eager = can_merge(&data_type)
            && self
                .values()
                .all(|v| v.data_type(input_schema).is_ok_and(|t| t == data_type))
            && self
                .values()
                .chain(later_whens)
                .all(|e| is_infallible(e, input_schema));
        eager.then_some(data_type)
    }

    fn evaluate_eagerly(&self, batch: &RecordBatch, data_type: &DataType) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        // The rows that no WHEN has matched yet
        let mut remaining = BooleanBuffer::new_set(num_rows);
        let mut remaining_count = num_rows;
        // The rows that choose each branch, and how many, for the branches that some row chooses
        let mut chosen: Vec<(BooleanBuffer, usize, &Arc<dyn PhysicalExpr>)> = vec![];
        for (when, then) in &self.flat_when_then {
            if remaining_count == 0 {
                break;
            }
            let matched = match when.evaluate(batch)? {
                ColumnarValue::Array(array) => {
                    if array.len() != num_rows {
                        return internal_err!(
                            "WHEN returned {} rows for a batch of {num_rows}",
                            array.len()
                        );
                    }
                    let array = as_boolean_array(&array)?;
                    // A NULL predicate does not match
                    match array.nulls() {
                        Some(nulls) => array.values() & nulls.inner(),
                        None => array.values().clone(),
                    }
                }
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => remaining.clone(),
                ColumnarValue::Scalar(ScalarValue::Boolean(_) | ScalarValue::Null) => continue,
                ColumnarValue::Scalar(other) => {
                    return exec_err!("WHEN returned {other:?} rather than a boolean")
                }
            };
            let rows = &remaining & &matched;
            let count = rows.count_set_bits();
            if count == 0 {
                continue;
            }
            remaining ^= &rows;
            remaining_count -= count;
            chosen.push((rows, count, then));
        }
        if remaining_count > 0 {
            if let Some(else_expr) = &self.flat_else_expr {
                chosen.push((remaining, remaining_count, else_expr));
                remaining_count = 0;
            }
        }

        // Every row chooses the same branch, so its value is the result as it is
        if let [(_, _, expr)] = chosen.as_slice() {
            if remaining_count == 0 {
                return expr.evaluate(batch);
            }
        }
        let mut branches = chosen
            .into_iter()
            .map(|(rows, row_count, expr)| {
                Branch::try_new(rows, row_count, expr.evaluate(batch)?, num_rows)
            })
            .collect::<Result<Vec<_>>>()?;
        // The rows of a NULL literal are NULL, as are the rows that choose no branch
        branches.retain(|b| !b.is_scalar || b.values.is_valid(0));
        if branches.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(data_type)?));
        }
        merge(data_type, num_rows, &branches).map(ColumnarValue::Array)
    }
}

impl Hash for CaseWhenExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.lazy.hash(state);
    }
}

impl PartialEq for CaseWhenExpr {
    fn eq(&self, other: &Self) -> bool {
        self.lazy == other.lazy
    }
}

impl Eq for CaseWhenExpr {}

impl Display for CaseWhenExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.lazy, f)
    }
}

impl PhysicalExpr for CaseWhenExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.lazy.data_type(input_schema)
    }

    /// Spark's rule: the result can be NULL when a branch's value can, or when there is no ELSE.
    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        for (_, then) in self.lazy.when_then_expr() {
            if then.nullable(input_schema)? {
                return Ok(true);
            }
        }
        match self.lazy.else_expr() {
            Some(else_expr) => else_expr.nullable(input_schema),
            None => Ok(true),
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self
            .eager
            .get_or_init(|| self.eager_result_type(&batch.schema()))
        {
            Some(data_type) => self.evaluate_eagerly(batch, data_type),
            None => self.lazy.evaluate(batch),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.lazy.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let expected = self.children().len();
        if children.len() != expected {
            return internal_err!(
                "CaseWhenExpr expects {expected} children, got {}",
                children.len()
            );
        }
        let mut children = children.into_iter();
        let when_then = (0..self.lazy.when_then_expr().len())
            .map(|_| (children.next().unwrap(), children.next().unwrap()))
            .collect();
        Ok(Arc::new(CaseWhenExpr::try_new(when_then, children.next())?))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.lazy.fmt_sql(f)
    }
}

/// Whether `expr` can be evaluated for rows that Spark would not evaluate it for.
///
/// That holds when it can neither fail nor return something different for seeing more rows: a
/// column, a literal, and comparisons, boolean logic, null checks, widening casts and wrapping
/// arithmetic over them. Anything else, including every function, is assumed to be able to fail.
fn is_infallible(expr: &Arc<dyn PhysicalExpr>, input_schema: &Schema) -> bool {
    if expr.is::<Column>() || expr.is::<Literal>() {
        return true;
    }
    let node_is_infallible = if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        binary_is_infallible(binary, input_schema)
    } else if let Some(cast) = expr.downcast_ref::<Cast>() {
        cast.is_infallible(input_schema)
    } else if let Some(normalize) = expr.downcast_ref::<NormalizeNaNAndZero>() {
        normalize.is_infallible()
    } else if let Some(not) = expr.downcast_ref::<NotExpr>() {
        not.arg()
            .data_type(input_schema)
            .is_ok_and(|t| t == DataType::Boolean)
    } else {
        // A nested CASE or IF cannot fail when none of its parts can, however it evaluates them
        expr.is::<IsNullExpr>()
            || expr.is::<IsNotNullExpr>()
            || expr.is::<CaseWhenExpr>()
            || expr.is::<IfExpr>()
    };
    node_is_infallible
        && expr
            .children()
            .into_iter()
            .all(|child| is_infallible(child, input_schema))
}

fn binary_is_infallible(binary: &BinaryExpr, input_schema: &Schema) -> bool {
    let (Ok(left), Ok(right)) = (
        binary.left().data_type(input_schema),
        binary.right().data_type(input_schema),
    ) else {
        return false;
    };
    match binary.op() {
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq
        | Operator::IsDistinctFrom
        | Operator::IsNotDistinctFrom => left == right && is_comparable(&left),
        Operator::And | Operator::Or => left == DataType::Boolean && right == DataType::Boolean,
        // Integers wrap on overflow, unless the expression was built to fail instead
        Operator::Plus | Operator::Minus | Operator::Multiply => {
            left == right && (left.is_integer() || left.is_floating()) && !fails_on_overflow(binary)
        }
        // Floating point division and remainder return infinity or NaN rather than failing
        Operator::Divide | Operator::Modulo => left == right && left.is_floating(),
        _ => false,
    }
}

/// The types of Spark's scalar values, which DataFusion compares without failing.
fn is_comparable(data_type: &DataType) -> bool {
    use DataType::*;
    data_type.is_numeric()
        || matches!(
            data_type,
            Boolean | Utf8 | LargeUtf8 | Binary | LargeBinary | Date32 | Timestamp(_, _)
        )
}

/// `BinaryExpr` does not expose whether it fails on overflow, but compares it.
fn fails_on_overflow(binary: &BinaryExpr) -> bool {
    let wrapping = BinaryExpr::new(
        Arc::clone(binary.left()),
        *binary.op(),
        Arc::clone(binary.right()),
    );
    *binary != wrapping
}

macro_rules! supported {
    ($t:ty) => {
        true
    };
}

/// Whether [`merge`] handles `data_type`.
fn can_merge(data_type: &DataType) -> bool {
    downcast_primitive! {
        data_type => (supported),
        DataType::Boolean
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary => true,
        _ => false,
    }
}

/// A branch's value, with the rows that choose the branch.
struct Branch {
    rows: BooleanBuffer,
    row_count: usize,
    /// One value per row of the batch, or a single value for a scalar
    values: ArrayRef,
    is_scalar: bool,
}

impl Branch {
    fn try_new(
        rows: BooleanBuffer,
        row_count: usize,
        value: ColumnarValue,
        num_rows: usize,
    ) -> Result<Self> {
        let (values, is_scalar) = match value {
            ColumnarValue::Array(array) if array.len() == num_rows => (array, false),
            ColumnarValue::Array(array) => {
                return internal_err!(
                    "CASE branch returned {} rows for a batch of {num_rows}",
                    array.len()
                )
            }
            ColumnarValue::Scalar(scalar) => (scalar.to_array_of_size(1)?, true),
        };
        Ok(Self {
            rows,
            row_count,
            values,
            is_scalar,
        })
    }

    fn is_valid_scalar(&self) -> bool {
        self.is_scalar && self.values.is_valid(0)
    }

    /// The error for values that are not the array type the result needs, which a downcast would
    /// otherwise panic on.
    fn type_mismatch(&self, data_type: &DataType) -> DataFusionError {
        internal_datafusion_err!(
            "CASE branch returned {} for a {data_type} result",
            self.values.data_type()
        )
    }
}

/// Takes each row's value from the branch that the row chose. No row chooses two branches, and a
/// row that chooses none is NULL.
fn merge(data_type: &DataType, num_rows: usize, branches: &[Branch]) -> Result<ArrayRef> {
    // Only the rows of this array are valid, so it is the result once the others are NULL, and
    // its buffers can be kept
    if let [branch] = branches {
        if !branch.is_scalar {
            let others = BooleanArray::new(!&branch.rows, None);
            return Ok(nullif(&branch.values, &others)?);
        }
    }
    macro_rules! primitive {
        ($t:ty) => {
            merge_primitive::<$t>(data_type, num_rows, branches)
        };
    }
    downcast_primitive! {
        data_type => (primitive),
        DataType::Boolean => merge_boolean(data_type, num_rows, branches),
        DataType::Utf8 => merge_bytes::<Utf8Type>(data_type, num_rows, branches),
        DataType::LargeUtf8 => merge_bytes::<LargeUtf8Type>(data_type, num_rows, branches),
        DataType::Binary => merge_bytes::<BinaryType>(data_type, num_rows, branches),
        DataType::LargeBinary => merge_bytes::<LargeBinaryType>(data_type, num_rows, branches),
        _ => internal_err!("CASE WHEN cannot merge {data_type}"),
    }
}

fn merge_primitive<T: ArrowPrimitiveType>(
    data_type: &DataType,
    num_rows: usize,
    branches: &[Branch],
) -> Result<ArrayRef> {
    let arrays = branches
        .iter()
        .map(|b| {
            b.values
                .as_primitive_opt::<T>()
                .ok_or_else(|| b.type_mismatch(data_type))
        })
        .collect::<Result<Vec<_>>>()?;
    // Start from the branch that most rows choose, whole, then write the others' rows over it.
    // The rows that choose no branch are NULL, so what they hold does not matter.
    let base = (0..branches.len())
        .max_by_key(|&i| branches[i].row_count)
        .unwrap();
    let mut values = if branches[base].is_scalar {
        vec![arrays[base].value(0); num_rows]
    } else {
        arrays[base].values().to_vec()
    };
    for (i, (branch, array)) in branches.iter().zip(&arrays).enumerate() {
        if i == base {
            continue;
        } else if !branch.is_scalar {
            copy_rows(&mut values, &branch.rows, array.values());
        } else {
            fill_rows(&mut values, &branch.rows, array.value(0));
        }
    }
    let nulls = merge_nulls(num_rows, branches);
    let array = PrimitiveArray::<T>::new(ScalarBuffer::from(values), nulls);
    Ok(Arc::new(array.with_data_type(data_type.clone())))
}

/// Calls `f(start, len, bits)` for each word of `rows`, where bit `i` of `bits` is row
/// `start + i`.
#[inline(always)]
fn for_each_word(rows: &BooleanBuffer, mut f: impl FnMut(usize, usize, u64)) {
    let chunks = rows.bit_chunks();
    let mut start = 0;
    for bits in chunks.iter() {
        f(start, 64, bits);
        start += 64;
    }
    if chunks.remainder_len() > 0 {
        f(start, chunks.remainder_len(), chunks.remainder_bits());
    }
}

/// The bits of a word of `len` rows that are all set.
#[inline(always)]
fn all_set(len: usize) -> u64 {
    u64::MAX >> (64 - len)
}

/// Below this many rows in a word, writing just those rows beats blending the whole word.
const SPARSE_WORD: u32 = 12;

/// Copies `source[i]` to `out[i]` for each row `i` in `rows`.
fn copy_rows<T: Copy>(out: &mut [T], rows: &BooleanBuffer, source: &[T]) {
    for_each_word(rows, |start, len, mut bits| {
        let out = &mut out[start..start + len];
        let source = &source[start..start + len];
        if bits == all_set(len) {
            out.copy_from_slice(source);
        } else if bits.count_ones() < SPARSE_WORD {
            while bits != 0 {
                let i = bits.trailing_zeros() as usize;
                out[i] = source[i];
                bits &= bits - 1;
            }
        } else {
            for (i, (out, source)) in out.iter_mut().zip(source).enumerate() {
                *out = if bits & (1 << i) != 0 { *source } else { *out };
            }
        }
    });
}

/// Sets `out[i]` to `value` for each row `i` in `rows`.
fn fill_rows<T: Copy>(out: &mut [T], rows: &BooleanBuffer, value: T) {
    for_each_word(rows, |start, len, mut bits| {
        let out = &mut out[start..start + len];
        if bits == all_set(len) {
            out.fill(value);
        } else if bits.count_ones() < SPARSE_WORD {
            while bits != 0 {
                out[bits.trailing_zeros() as usize] = value;
                bits &= bits - 1;
            }
        } else {
            for (i, out) in out.iter_mut().enumerate() {
                *out = if bits & (1 << i) != 0 { value } else { *out };
            }
        }
    });
}

fn merge_boolean(data_type: &DataType, num_rows: usize, branches: &[Branch]) -> Result<ArrayRef> {
    let mut values = BooleanBuffer::new_unset(num_rows);
    for branch in branches {
        let array = branch
            .values
            .as_boolean_opt()
            .ok_or_else(|| branch.type_mismatch(data_type))?;
        if !branch.is_scalar {
            values |= &(&branch.rows & array.values());
        } else if branch.is_valid_scalar() && array.value(0) {
            values |= &branch.rows;
        }
    }
    Ok(Arc::new(BooleanArray::new(
        values,
        merge_nulls(num_rows, branches),
    )))
}

/// How far past a value [`merge_bytes`] may copy: copying a fixed number of bytes is much cheaper
/// than copying a value's exact length, and the next value overwrites the excess.
const SLACK: usize = 16;

/// Where one branch keeps its values, for [`merge_bytes`].
struct ByteSource<'a, T: ByteArrayType> {
    offsets: &'a [T::Offset],
    data: &'a [u8],
    /// Turns a row into its position in `offsets`: every bit set for an array, none for a scalar
    position_mask: usize,
    /// A scalar's value
    scalar: Option<&'a [u8]>,
    /// A scalar's value padded to `SLACK` bytes, when it fits in them
    padded_scalar: Option<[u8; SLACK]>,
}

impl<'a, T: ByteArrayType> ByteSource<'a, T> {
    fn try_new(branch: &'a Branch, data_type: &DataType) -> Result<Self> {
        let array = branch
            .values
            .as_bytes_opt::<T>()
            .ok_or_else(|| branch.type_mismatch(data_type))?;
        let scalar: Option<&[u8]> = branch.is_scalar.then(|| array.value(0).as_ref());
        let padded_scalar = scalar.filter(|v| v.len() <= SLACK).map(|v| {
            let mut padded = [0; SLACK];
            padded[..v.len()].copy_from_slice(v);
            padded
        });
        Ok(Self {
            offsets: array.value_offsets(),
            data: array.value_data(),
            position_mask: if branch.is_scalar { 0 } else { usize::MAX },
            scalar,
            padded_scalar,
        })
    }

    fn value_len(&self, row: usize) -> usize {
        let at = row & self.position_mask;
        (self.offsets[at + 1] - self.offsets[at]).as_usize()
    }
}

fn merge_bytes<T: ByteArrayType>(
    data_type: &DataType,
    num_rows: usize,
    branches: &[Branch],
) -> Result<ArrayRef> {
    // The branch that each row takes its value from, or `NONE` when the row is NULL
    const NONE: u32 = u32::MAX;
    let mut chosen = vec![NONE; num_rows];
    for (i, branch) in branches.iter().enumerate() {
        fill_rows(&mut chosen, &branch.rows, i as u32);
    }
    let sources = branches
        .iter()
        .map(|b| ByteSource::<T>::try_new(b, data_type))
        .collect::<Result<Vec<_>>>()?;

    let mut offsets = Vec::with_capacity(num_rows + 1);
    offsets.push(T::Offset::usize_as(0));
    let mut len = 0;
    for (row, &i) in chosen.iter().enumerate() {
        // `NONE` is out of range
        if let Some(source) = sources.get(i as usize) {
            len += source.value_len(row);
        }
        offsets.push(T::Offset::usize_as(len));
    }
    if T::Offset::from_usize(len).is_none() {
        return exec_err!("CASE WHEN result is too large for {}", T::DATA_TYPE);
    }

    // Copy the values a run of rows that chose the same branch at a time. An array's values for
    // a run are contiguous, and a scalar repeats.
    let mut values = vec![0; len + SLACK];
    let mut pos = 0;
    let mut start = 0;
    while start < num_rows {
        let i = chosen[start];
        let end = chosen[start..]
            .iter()
            .position(|&c| c != i)
            .map_or(num_rows, |n| start + n);
        match sources.get(i as usize) {
            None => {}
            Some(ByteSource {
                scalar: Some(value),
                padded_scalar,
                ..
            }) => match padded_scalar {
                Some(padded) => {
                    for _ in start..end {
                        values[pos..pos + SLACK].copy_from_slice(padded);
                        pos += value.len();
                    }
                }
                None => {
                    for _ in start..end {
                        values[pos..pos + value.len()].copy_from_slice(value);
                        pos += value.len();
                    }
                }
            },
            Some(source) => {
                let from = source.offsets[start].as_usize();
                let run_len = source.offsets[end].as_usize() - from;
                if run_len <= SLACK && from + SLACK <= source.data.len() {
                    values[pos..pos + SLACK].copy_from_slice(&source.data[from..from + SLACK]);
                } else {
                    values[pos..pos + run_len].copy_from_slice(&source.data[from..from + run_len]);
                }
                pos += run_len;
            }
        }
        start = end;
    }
    values.truncate(len);

    // SAFETY: every value is a whole value copied from an array of the same type, and the offsets
    // were accumulated from the lengths of those values, so they are monotonic, end at the length
    // of `values`, and fall on the boundaries between whole values.
    let array = unsafe {
        GenericByteArray::<T>::new_unchecked(
            OffsetBuffer::new_unchecked(offsets.into()),
            values.into(),
            merge_nulls(num_rows, branches),
        )
    };
    Ok(Arc::new(array))
}

/// A row is valid when the branch that it chose has a valid value for it.
fn merge_nulls(num_rows: usize, branches: &[Branch]) -> Option<NullBuffer> {
    let chosen: usize = branches.iter().map(|b| b.row_count).sum();
    if chosen == num_rows && branches.iter().all(|b| b.values.null_count() == 0) {
        return None;
    }
    let mut valid = BooleanBuffer::new_unset(num_rows);
    for branch in branches {
        if branch.is_scalar {
            if branch.is_valid_scalar() {
                valid |= &branch.rows;
            }
        } else {
            match branch.values.nulls() {
                Some(nulls) => valid |= &(&branch.rows & nulls.inner()),
                None => valid |= &branch.rows,
            }
        }
    }
    Some(NullBuffer::new(valid)).filter(|nulls| nulls.null_count() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, Int32Array, Int64Array, LargeStringArray, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow::compute::cast;
    use arrow::datatypes::{Field, TimeUnit, TimestampMicrosecondType};
    use datafusion::physical_expr::expressions::{col, lit};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    fn value_types() -> Vec<DataType> {
        vec![
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Decimal128(12, 3),
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Boolean,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
        ]
    }

    fn random_values(data_type: &DataType, len: usize, nulls: f64, rng: &mut StdRng) -> ArrayRef {
        let ints: Int64Array = (0..len)
            .map(|_| (!rng.random_bool(nulls)).then(|| rng.random_range(-9i64..9)))
            .collect();
        // Lengths either side of `SLACK`, some multi-byte
        let text = |v: i64| {
            let pad = if v % 2 == 0 { "é" } else { "x" };
            format!("{}{v}", pad.repeat(3 * v.unsigned_abs() as usize))
        };
        match data_type {
            DataType::Boolean => cast(&ints, data_type).unwrap(),
            DataType::Date32 => cast(&cast(&ints, &DataType::Int32).unwrap(), data_type).unwrap(),
            DataType::Utf8 => Arc::new(ints.iter().map(|v| v.map(text)).collect::<StringArray>()),
            DataType::LargeUtf8 => Arc::new(
                ints.iter()
                    .map(|v| v.map(text))
                    .collect::<LargeStringArray>(),
            ),
            DataType::Binary => Arc::new(
                ints.iter()
                    .map(|v| v.map(|v| text(v).into_bytes()))
                    .collect::<BinaryArray>(),
            ),
            _ => cast(&ints, data_type).unwrap(),
        }
    }

    /// The branches and ELSE of a CASE.
    type CaseShape = (Vec<WhenThen>, Option<Arc<dyn PhysicalExpr>>);

    /// How the predicate columns are laid out.
    #[derive(Clone, Copy, Debug)]
    enum Predicates {
        Random,
        WithNulls,
        AllTrue,
        AllFalse,
        /// Each predicate is true for one contiguous run of rows
        Runs,
    }

    fn random_predicate(len: usize, layout: Predicates, rng: &mut StdRng) -> ArrayRef {
        let (start, end) = (rng.random_range(0..len), rng.random_range(0..len));
        let values: BooleanBuffer = (0..len)
            .map(|row| match layout {
                Predicates::Random | Predicates::WithNulls => rng.random_bool(0.4),
                Predicates::AllTrue => true,
                Predicates::AllFalse => false,
                Predicates::Runs => start.min(end) <= row && row < start.max(end),
            })
            .collect();
        // A comparison kernel leaves whatever it computed under a NULL, so the value bits under
        // these are random too, and a NULL must not match even where its bit is set
        let nulls = matches!(layout, Predicates::WithNulls)
            .then(|| NullBuffer::from_iter((0..len).map(|_| !rng.random_bool(0.3))));
        Arc::new(BooleanArray::new(values, nulls))
    }

    /// Predicate columns `p0..p2` and value columns `v0..v2` of `data_type`.
    fn test_batch(
        data_type: &DataType,
        len: usize,
        nulls: f64,
        layout: Predicates,
        rng: &mut StdRng,
    ) -> RecordBatch {
        let mut fields = vec![];
        let mut columns = vec![];
        for i in 0..3 {
            fields.push(Field::new(format!("p{i}"), DataType::Boolean, true));
            columns.push(random_predicate(len, layout, rng));
        }
        for i in 0..3 {
            fields.push(Field::new(format!("v{i}"), data_type.clone(), true));
            columns.push(random_values(data_type, len, nulls, rng));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    /// The first valid value in `values`, as a literal.
    fn first_valid(values: &ArrayRef) -> Option<ScalarValue> {
        (0..values.len())
            .find(|&i| values.is_valid(i))
            .map(|i| ScalarValue::try_from_array(values, i).unwrap())
    }

    /// Evaluates the CASE with both `CaseWhenExpr` and DataFusion's `CaseExpr`, checks they agree,
    /// and returns whether `CaseWhenExpr` evaluated it eagerly.
    fn check_against_case_expr(
        batch: &RecordBatch,
        when_then: Vec<WhenThen>,
        else_expr: Option<Arc<dyn PhysicalExpr>>,
    ) -> bool {
        let expected = CaseExpr::try_new(None, when_then.clone(), else_expr.clone())
            .unwrap()
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let expr = CaseWhenExpr::try_new(when_then, else_expr).unwrap();
        let actual = expr
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        assert_eq!(actual.data_type(), expected.data_type(), "{expr}");
        assert_eq!(actual.as_ref(), expected.as_ref(), "{expr}");
        expr.eager.get().unwrap().is_some()
    }

    #[test]
    fn eager_evaluation_matches_case_expr() {
        let mut rng = StdRng::seed_from_u64(42);
        let layouts = [
            Predicates::Random,
            Predicates::WithNulls,
            Predicates::AllTrue,
            Predicates::AllFalse,
            Predicates::Runs,
        ];
        for data_type in value_types() {
            for nulls in [0.0, 0.2, 0.9] {
                for layout in layouts {
                    // Longer than a word of the bitmap, and not a multiple of it
                    let batch = test_batch(&data_type, 300, nulls, layout, &mut rng);
                    for batch in [batch.clone(), batch.slice(7, 250)] {
                        let schema = batch.schema();
                        let c = |name: &str| col(name, &schema).unwrap();
                        let null = ScalarValue::try_new_null(&data_type).unwrap();
                        let literal = first_valid(batch.column(3)).unwrap_or(null.clone());
                        let shapes: Vec<CaseShape> = vec![
                            (vec![(c("p0"), c("v0"))], Some(c("v1"))),
                            (vec![(c("p0"), c("v0"))], None),
                            (
                                vec![
                                    (c("p0"), c("v0")),
                                    (c("p1"), c("v1")),
                                    (c("p2"), lit(literal.clone())),
                                ],
                                Some(c("v2")),
                            ),
                            (vec![(c("p0"), c("v0")), (c("p1"), lit(null.clone()))], None),
                            (
                                vec![(c("p0"), lit(literal.clone())), (c("p1"), lit(null))],
                                Some(lit(literal.clone())),
                            ),
                        ];
                        for (when_then, else_expr) in shapes {
                            let eager = check_against_case_expr(&batch, when_then, else_expr);
                            assert!(eager, "{data_type} should be evaluated eagerly");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn nested_else_is_flattened() {
        let mut rng = StdRng::seed_from_u64(7);
        for data_type in [DataType::Int64, DataType::Utf8, DataType::Boolean] {
            let batch = test_batch(&data_type, 300, 0.2, Predicates::WithNulls, &mut rng);
            let schema = batch.schema();
            let c = |name: &str| col(name, &schema).unwrap();
            let literal = first_valid(batch.column(3)).unwrap();
            // IF(p0, v0, IF(p1, v1, CASE WHEN p2 THEN literal ELSE v2 END))
            let inner: Arc<dyn PhysicalExpr> = Arc::new(
                CaseWhenExpr::try_new(vec![(c("p2"), lit(literal.clone()))], Some(c("v2")))
                    .unwrap(),
            );
            let middle: Arc<dyn PhysicalExpr> = Arc::new(IfExpr::new(c("p1"), c("v1"), inner));
            let expr = CaseWhenExpr::try_new(vec![(c("p0"), c("v0"))], Some(middle)).unwrap();
            assert_eq!(expr.flat_when_then.len(), 3);

            let reference = CaseExpr::try_new(
                None,
                vec![(c("p0"), c("v0"))],
                Some(Arc::new(
                    CaseExpr::try_new(
                        None,
                        vec![(c("p1"), c("v1"))],
                        Some(Arc::new(
                            CaseExpr::try_new(None, vec![(c("p2"), lit(literal))], Some(c("v2")))
                                .unwrap(),
                        )),
                    )
                    .unwrap(),
                )),
            )
            .unwrap();
            let expected = reference.evaluate(&batch).unwrap().into_array(300).unwrap();
            let actual = expr.evaluate(&batch).unwrap().into_array(300).unwrap();
            assert_eq!(actual.as_ref(), expected.as_ref(), "{data_type}");
            assert!(expr.eager.get().unwrap().is_some());
        }
    }

    fn int_batch(a: Vec<Option<i64>>, b: Vec<Option<i64>>) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(b))],
        )
        .unwrap()
    }

    fn binary(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, op, right))
    }

    #[test]
    fn branch_that_can_fail_is_evaluated_lazily() {
        // CASE WHEN b != 0 THEN a / b ELSE 0 END: integer division fails for b = 0, which the
        // WHEN excludes
        let batch = int_batch(
            vec![Some(10), Some(20), Some(30), None],
            vec![Some(2), Some(0), Some(3), Some(0)],
        );
        let schema = batch.schema();
        let (a, b) = (col("a", &schema).unwrap(), col("b", &schema).unwrap());
        let when_then = vec![(
            binary(Arc::clone(&b), Operator::NotEq, lit(0i64)),
            binary(a, Operator::Divide, b),
        )];
        let eager = check_against_case_expr(&batch, when_then, Some(lit(0i64)));
        assert!(!eager);
    }

    #[test]
    fn later_when_that_can_fail_is_evaluated_lazily() {
        // CASE WHEN b = 0 THEN -1 WHEN a / b > 5 THEN 1 ELSE 0 END: the second WHEN only sees the
        // rows where b is not 0
        let batch = int_batch(
            vec![Some(10), Some(20), Some(30), Some(1)],
            vec![Some(2), Some(0), Some(3), Some(0)],
        );
        let schema = batch.schema();
        let (a, b) = (col("a", &schema).unwrap(), col("b", &schema).unwrap());
        let when_then = vec![
            (binary(Arc::clone(&b), Operator::Eq, lit(0i64)), lit(-1i64)),
            (
                binary(binary(a, Operator::Divide, b), Operator::Gt, lit(5i64)),
                lit(1i64),
            ),
        ];
        let eager = check_against_case_expr(&batch, when_then, Some(lit(0i64)));
        assert!(!eager);
    }

    #[test]
    fn first_when_can_fail_and_still_be_eager() {
        // Spark evaluates the first WHEN for every row, so it may be anything
        let batch = int_batch(vec![Some(10), Some(-20)], vec![Some(2), Some(5)]);
        let schema = batch.schema();
        let (a, b) = (col("a", &schema).unwrap(), col("b", &schema).unwrap());
        let when_then = vec![(
            binary(
                binary(Arc::clone(&a), Operator::Divide, b),
                Operator::Gt,
                lit(0i64),
            ),
            Arc::clone(&a),
        )];
        let eager = check_against_case_expr(&batch, when_then, Some(lit(0i64)));
        assert!(eager);
    }

    #[test]
    fn infallible_expressions() {
        let schema = Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("j", DataType::Int32, true),
            Field::new("d", DataType::Float64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("dec", DataType::Decimal128(10, 2), true),
        ]);
        let c = |name: &str| col(name, &schema).unwrap();
        let cast = |e: Arc<dyn PhysicalExpr>, to: DataType| -> Arc<dyn PhysicalExpr> {
            Arc::new(Cast::new(
                e,
                to,
                SparkCastOptions::new_without_timezone(EvalMode::Ansi, false),
                None,
                None,
            ))
        };
        let infallible = |e: Arc<dyn PhysicalExpr>| is_infallible(&e, &schema);

        assert!(infallible(c("i")));
        assert!(infallible(lit("x")));
        assert!(infallible(binary(c("i"), Operator::Lt, lit(0i64))));
        assert!(infallible(binary(c("s"), Operator::Eq, lit("x"))));
        assert!(infallible(binary(c("i"), Operator::Plus, c("i"))));
        assert!(infallible(binary(c("d"), Operator::Divide, c("d"))));
        assert!(infallible(binary(c("d"), Operator::Modulo, c("d"))));
        assert!(infallible(Arc::new(NotExpr::new(binary(
            c("i"),
            Operator::Lt,
            lit(0i64)
        )))));
        assert!(infallible(Arc::new(IsNullExpr::new(c("s")))));
        assert!(infallible(cast(c("j"), DataType::Int64)));
        assert!(infallible(cast(c("i"), DataType::Float64)));
        assert!(infallible(Arc::new(IfExpr::new(
            binary(c("i"), Operator::Lt, lit(0i64)),
            c("i"),
            lit(0i64)
        ))));

        assert!(infallible(Arc::new(NormalizeNaNAndZero::new(
            DataType::Float64,
            c("d")
        ))));

        // Only a boolean can be negated, and only a float normalized
        assert!(!infallible(Arc::new(NotExpr::new(c("i")))));
        assert!(!infallible(Arc::new(NormalizeNaNAndZero::new(
            DataType::Int64,
            c("i")
        ))));
        // Integer division and remainder fail on a zero divisor
        assert!(!infallible(binary(c("i"), Operator::Divide, c("i"))));
        assert!(!infallible(binary(c("i"), Operator::Modulo, c("i"))));
        // Checked arithmetic fails on overflow
        let checked: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(c("i"), Operator::Plus, c("i")).with_fail_on_overflow(true));
        assert!(!infallible(checked));
        // Decimal arithmetic can overflow
        assert!(!infallible(binary(c("dec"), Operator::Plus, c("dec"))));
        // Narrowing and parsing casts can fail
        assert!(!infallible(cast(c("i"), DataType::Int32)));
        assert!(!infallible(cast(c("s"), DataType::Int32)));
        // So can anything built from a part that can fail
        assert!(!infallible(binary(
            binary(c("i"), Operator::Divide, c("i")),
            Operator::Lt,
            lit(0i64)
        )));
        assert!(!infallible(Arc::new(IfExpr::new(
            binary(c("i"), Operator::Lt, lit(0i64)),
            binary(c("i"), Operator::Divide, c("i")),
            lit(0i64)
        ))));
    }

    #[test]
    fn result_of_one_branch_is_returned_as_is() {
        let batch = int_batch(vec![Some(1), Some(2)], vec![Some(1), Some(2)]);
        let schema = batch.schema();
        let a = col("a", &schema).unwrap();
        let scalar = |expr: CaseWhenExpr| match expr.evaluate(&batch).unwrap() {
            ColumnarValue::Scalar(v) => v,
            other => panic!("expected a scalar, got {other:?}"),
        };
        // Every row chooses THEN, a literal
        let expr = CaseWhenExpr::try_new(
            vec![(binary(Arc::clone(&a), Operator::Gt, lit(0i64)), lit(7i64))],
            Some(Arc::clone(&a)),
        )
        .unwrap();
        assert_eq!(scalar(expr), ScalarValue::Int64(Some(7)));
        // No row chooses a branch and there is no ELSE
        let expr = CaseWhenExpr::try_new(
            vec![(binary(Arc::clone(&a), Operator::Lt, lit(0i64)), a)],
            None,
        )
        .unwrap();
        assert_eq!(scalar(expr), ScalarValue::Int64(None));
    }

    #[test]
    fn empty_batch() {
        let batch = int_batch(vec![], vec![]);
        let schema = batch.schema();
        let a = col("a", &schema).unwrap();
        let b = col("b", &schema).unwrap();
        let when_then = vec![(binary(Arc::clone(&a), Operator::Lt, lit(0i64)), a)];
        check_against_case_expr(&batch, when_then, Some(b));
    }

    #[test]
    fn nullable_follows_spark() {
        let schema = Schema::new(vec![
            Field::new("p", DataType::Boolean, false),
            Field::new("a", DataType::Int64, false),
            Field::new("n", DataType::Int64, true),
        ]);
        let c = |name: &str| col(name, &schema).unwrap();
        let nullable = |when_then: Vec<WhenThen>, else_expr: Option<Arc<dyn PhysicalExpr>>| {
            CaseWhenExpr::try_new(when_then, else_expr)
                .unwrap()
                .nullable(&schema)
                .unwrap()
        };
        assert!(!nullable(vec![(c("p"), c("a"))], Some(lit(0i64))));
        assert!(nullable(vec![(c("p"), c("a"))], None));
        assert!(nullable(vec![(c("p"), c("n"))], Some(lit(0i64))));
        assert!(nullable(vec![(c("p"), c("a"))], Some(c("n"))));
        assert!(nullable(
            vec![(c("p"), c("a"))],
            Some(lit(ScalarValue::Int64(None)))
        ));
    }

    #[test]
    fn create_case_when_casts_only_differing_branches() {
        let schema = Schema::new(vec![
            Field::new("p", DataType::Boolean, true),
            Field::new("i", DataType::Int32, true),
            Field::new("l", DataType::Int64, true),
        ]);
        let c = |name: &str| col(name, &schema).unwrap();
        let expr = create_case_when(
            vec![(c("p"), c("l")), (c("p"), c("i"))],
            Some(c("l")),
            &schema,
        )
        .unwrap();
        let children = expr.children();
        assert!(children[1].is::<Column>());
        let cast = children[3].downcast_ref::<Cast>().unwrap();
        assert_eq!(cast.data_type, DataType::Int64);
        assert!(children[4].is::<Column>());

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(BooleanArray::from(vec![Some(false), Some(true), None])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(Int64Array::from(vec![Some(10), None, Some(30)])),
            ],
        )
        .unwrap();
        let result = expr.evaluate(&batch).unwrap().into_array(3).unwrap();
        assert_eq!(
            result.as_ref(),
            &Int64Array::from(vec![Some(10), None, Some(30)]) as &dyn Array
        );
    }

    /// CASE branches that share a Spark timestamp type but carry different Arrow timezone labels
    /// are reconciled by relabelling them, which used to panic because the cast had no timezone.
    #[test]
    fn case_reconciles_timestamp_timezone_labels() {
        let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        for then_label in [Some("Etc/UTC"), None] {
            let then_type = DataType::Timestamp(TimeUnit::Microsecond, then_label.map(Into::into));
            let schema = Schema::new(vec![
                Field::new("b", DataType::Boolean, true),
                Field::new("t", then_type, true),
                Field::new("e", utc.clone(), true),
            ]);
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    Arc::new(BooleanArray::from(vec![true, false])),
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![1, 2]).with_timezone_opt(then_label),
                    ),
                    Arc::new(TimestampMicrosecondArray::from(vec![10, 20]).with_timezone("UTC")),
                ],
            )
            .unwrap();
            let c = |name: &str| col(name, &schema).unwrap();
            let case = create_case_when(vec![(c("b"), c("t"))], Some(c("e")), &schema).unwrap();
            let result = case.evaluate(&batch).unwrap().into_array(2).unwrap();
            assert_eq!(result.data_type(), &utc, "{then_label:?}");
            let values = result.as_primitive::<TimestampMicrosecondType>();
            assert_eq!(values.values().to_vec(), vec![1, 20], "{then_label:?}");
        }
    }

    #[test]
    fn many_branches() {
        let len = 1000;
        let values: Int64Array = (0..len as i64).map(Some).collect();
        let schema = Schema::new(vec![Field::new("v", DataType::Int64, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();
        let v = col("v", &schema).unwrap();
        // 300 branches, each matching the rows below its bound
        let when_then = (0..300)
            .map(|i| {
                let when = binary(Arc::clone(&v), Operator::Lt, lit(3 * i as i64));
                (when, lit(format!("b{i}")))
            })
            .collect();
        assert!(check_against_case_expr(
            &batch,
            when_then,
            Some(lit("rest"))
        ));
    }

    #[test]
    fn timestamp_keeps_its_time_zone() {
        let data_type = DataType::Timestamp(TimeUnit::Microsecond, Some("+05:00".into()));
        let values =
            TimestampMicrosecondArray::from(vec![Some(1), Some(2), None]).with_timezone("+05:00");
        let schema = Schema::new(vec![
            Field::new("p", DataType::Boolean, true),
            Field::new("t", data_type.clone(), true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                ])),
                Arc::new(values),
            ],
        )
        .unwrap();
        let c = |name: &str| col(name, &schema).unwrap();
        let null = lit(ScalarValue::try_new_null(&data_type).unwrap());
        assert!(check_against_case_expr(
            &batch,
            vec![(c("p"), c("t"))],
            Some(null)
        ));
    }
}
