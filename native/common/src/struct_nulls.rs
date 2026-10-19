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

//! Pushing a struct's null mask down into its children.
//!
//! Arrow stores a `StructArray`'s children with their own validity, independent of the parent's
//! null buffer, so at a row where the struct itself is null a child buffer can still hold a value
//! — parquet writes exactly that for `optional group c { required int32 a; }`, where the child leaf
//! has nowhere to record a null of its own. Spark's rule is that a field of a null struct is null,
//! so anything that reads the children has to combine the parent's nulls into them first.
//!
//! Getting this wrong is silent: `isnotnull(structCol.field)` reports true for a null struct
//! (<https://github.com/apache/datafusion-comet/issues/4432>), and hashing a null struct picks up
//! whatever sits in the child slot
//! (<https://github.com/apache/datafusion-comet/issues/5753>).

use arrow::array::{make_array, Array, ArrayRef, StructArray};
use arrow::buffer::NullBuffer;
use arrow::error::ArrowError;
use std::sync::Arc;

/// The parent's nulls unioned into one child, or the child unchanged when there is nothing to push.
///
/// Skips the work when the parent has no null to contribute, which covers both no null buffer at
/// all and an all-valid buffer — the latter is what slicing leaves behind. `NullBuffer` stores its
/// null count, so the test is O(1).
///
/// The union only ever adds nulls, so the child's data buffers are untouched and are not
/// revalidated. Callers on a hot path depend on that: the hash kernels reach this once per element
/// of an `array<struct<..>>`, and re-checking a string child would rescan its whole UTF-8 values
/// buffer each time.
pub fn child_with_parent_nulls(
    struct_array: &StructArray,
    ordinal: usize,
) -> Result<ArrayRef, ArrowError> {
    let child = struct_array.column(ordinal);
    match struct_array.nulls() {
        Some(parent) if parent.null_count() > 0 => {
            let combined = NullBuffer::union(Some(parent), child.nulls());
            // SAFETY: only nulls are added; every data buffer and child array is carried over
            // unchanged, which is the same argument `StructArray::flatten` makes.
            let data = child.to_data().into_builder().nulls(combined);
            Ok(make_array(unsafe { data.build_unchecked() }))
        }
        _ => Ok(Arc::clone(child)),
    }
}

/// Every child with the parent's nulls unioned in, in field order.
///
/// `StructArray::flatten` does the same union but also rebuilds `Fields`, re-marking non-nullable
/// fields as nullable. Callers that only want the arrays would discard that, so this avoids
/// building it.
pub fn children_with_parent_nulls(struct_array: &StructArray) -> Result<Vec<ArrayRef>, ArrowError> {
    match struct_array.nulls() {
        Some(parent) if parent.null_count() > 0 => (0..struct_array.num_columns())
            .map(|i| child_with_parent_nulls(struct_array, i))
            .collect(),
        _ => Ok(struct_array.columns().to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Fields};

    fn struct_with(child: ArrayRef, nulls: Option<NullBuffer>) -> StructArray {
        let fields: Fields = vec![Arc::new(Field::new("a", DataType::Int32, true))].into();
        StructArray::new(fields, vec![child], nulls)
    }

    /// The case both bugs came from: the parent is null where the child still holds a value.
    #[test]
    fn hidden_child_value_under_a_null_parent_becomes_null() {
        let child: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(999)]));
        let sa = struct_with(child, Some(NullBuffer::from(vec![true, false])));

        let out = child_with_parent_nulls(&sa, 0).unwrap();
        assert!(!out.is_null(0));
        assert!(out.is_null(1), "a field of a null struct must read as null");

        let all = children_with_parent_nulls(&sa).unwrap();
        assert_eq!(all.len(), 1);
        assert!(all[0].is_null(1));
    }

    /// No null buffer: the child is returned as is, not rebuilt.
    #[test]
    fn no_parent_nulls_returns_the_child_itself() {
        let child: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
        let sa = struct_with(Arc::clone(&child), None);

        let out = child_with_parent_nulls(&sa, 0).unwrap();
        assert!(
            Arc::ptr_eq(&out, sa.column(0)),
            "expected the same array back"
        );
        assert_eq!(out.null_count(), 0);
    }

    /// A present but all-valid buffer, which is what slicing leaves behind, is also skipped.
    #[test]
    fn all_valid_parent_buffer_is_skipped() {
        let child: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]));
        let sa = struct_with(child, Some(NullBuffer::from(vec![true, true, true])));

        let out = child_with_parent_nulls(&sa, 0).unwrap();
        assert!(
            Arc::ptr_eq(&out, sa.column(0)),
            "expected the same array back"
        );
    }

    /// A child null the parent does not have must survive the union.
    #[test]
    fn child_nulls_are_preserved() {
        let child: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let sa = struct_with(child, Some(NullBuffer::from(vec![true, true, false])));

        let out = child_with_parent_nulls(&sa, 0).unwrap();
        assert!(!out.is_null(0));
        assert!(out.is_null(1), "the child's own null must remain");
        assert!(out.is_null(2), "the parent's null must be applied");
    }

    /// The old guard compared null *counts*, which can match while the nulls sit at different rows.
    #[test]
    fn equal_null_counts_at_different_rows_still_need_the_union() {
        use arrow::array::Int32Array;
        let child: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(2)]));
        // parent null at row 1, child null at row 0: counts equal (1 == 1), positions differ.
        let sa = struct_with(child, Some(NullBuffer::from(vec![true, false])));
        assert_eq!(sa.null_count(), sa.column(0).null_count());

        let out = child_with_parent_nulls(&sa, 0).unwrap();
        assert!(out.is_null(0), "child's own null");
        assert!(out.is_null(1), "parent's null must still be applied");
        assert_eq!(out.null_count(), 2);
    }

    /// An empty struct has no children to union into.
    #[test]
    fn empty_struct_has_no_children() {
        let sa = StructArray::new_empty_fields(2, Some(NullBuffer::from(vec![true, false])));
        assert!(children_with_parent_nulls(&sa).unwrap().is_empty());
    }
}
