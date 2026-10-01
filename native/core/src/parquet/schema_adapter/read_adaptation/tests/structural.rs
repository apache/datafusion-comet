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

use super::*;
use crate::parquet::cast_column::CometCastColumnExpr;
use arrow::array::{
    Array, ArrayRef, GenericListArray, Int32Array, OffsetSizeTrait, RecordBatch, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::Fields;
use parquet::variant::VariantType;
use std::collections::HashMap;

#[test]
fn nested_subset_preserves_values_and_parent_nulls() {
    let kept = Arc::new(Field::new("kept", DataType::Int32, true));
    let removed = Arc::new(Field::new("removed", DataType::Int32, true));
    let nested_source: Fields = vec![Arc::clone(&kept), Arc::clone(&removed)].into();
    let nested_target: Fields = vec![Arc::clone(&kept)].into();
    let source_fields: Fields = vec![
        Arc::clone(&kept),
        Arc::new(Field::new(
            "nested",
            DataType::Struct(nested_source.clone()),
            true,
        )),
        removed,
    ]
    .into();
    let target_fields: Fields = vec![
        Arc::new(Field::new(
            "nested",
            DataType::Struct(nested_target.clone()),
            true,
        )),
        kept,
    ]
    .into();
    let (schema, expr) = adapt(
        Field::new("s", DataType::Struct(source_fields.clone()), true),
        Field::new("s", DataType::Struct(target_fields.clone()), true),
        options(),
    );
    assert!(expr.is::<CastExpr>(), "retain DataFusion leaf clipping");
    assert!(is_infallible_read_adaptation(&expr, &schema));
    let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None, Some(-2)]));
    let nested_nulls = Some(NullBuffer::from(vec![true, true, false]));
    let parent_nulls = Some(NullBuffer::from(vec![true, false, true]));
    let nested: ArrayRef = Arc::new(StructArray::new(
        nested_source,
        vec![Arc::clone(&values), Arc::clone(&values)],
        nested_nulls.clone(),
    ));
    let source: ArrayRef = Arc::new(StructArray::new(
        source_fields,
        vec![Arc::clone(&values), nested, Arc::clone(&values)],
        parent_nulls.clone(),
    ));
    let batch = RecordBatch::try_new(schema, vec![source]).unwrap();
    let output = expr.evaluate(&batch).unwrap().into_array(3).unwrap();
    let expected_nested: ArrayRef = Arc::new(StructArray::new(
        nested_target,
        vec![Arc::clone(&values)],
        nested_nulls,
    ));
    let expected = StructArray::new(target_fields, vec![expected_nested, values], parent_nulls);
    assert_eq!(
        output.as_any().downcast_ref::<StructArray>().unwrap(),
        &expected
    );
}

#[test]
fn subset_proof_rejects_missing_duplicate_and_changing_fields() {
    let field = Field::new("kept", DataType::Int32, true);
    let source = struct_type(vec![
        field.clone(),
        Field::new("removed", DataType::Int32, true),
    ]);
    for target in [
        struct_type(vec![]),
        struct_type(vec![Field::new("missing", DataType::Int32, true)]),
        struct_type(vec![field.clone(), field.clone()]),
        struct_type(vec![Field::new("kept", DataType::Int64, true)]),
        struct_type(vec![Field::new("kept", DataType::Int32, false)]),
        struct_type(vec![field.clone().with_metadata(HashMap::from([(
            "custom".into(),
            "changed".into(),
        )]))]),
    ] {
        let (schema, expr) = structural_cast(source.clone(), target);
        assert!(!is_infallible_read_adaptation(&expr, &schema), "{expr}");
    }
    let (schema, expr) = structural_cast(
        struct_type(vec![field.clone(), field.clone()]),
        struct_type(vec![field]),
    );
    assert!(!is_infallible_read_adaptation(&expr, &schema));
}

#[test]
fn list_subsets_require_unchanged_representation_and_compatible_nullability() {
    let source_inner = struct_type(vec![
        Field::new("kept", DataType::Int32, true),
        Field::new("removed", DataType::Int32, true),
    ]);
    let target_inner = struct_type(vec![Field::new("kept", DataType::Int32, true)]);
    for large in [false, true] {
        let list = |field| {
            if large {
                DataType::LargeList(Arc::new(field))
            } else {
                DataType::List(Arc::new(field))
            }
        };
        let source = list(Field::new("item", source_inner.clone(), true));
        for (nullable, expected) in [(true, true), (false, false)] {
            let target = list(Field::new("element", target_inner.clone(), nullable));
            let (schema, expr) = structural_cast(source.clone(), target);
            assert_eq!(is_infallible_read_adaptation(&expr, &schema), expected);
        }
    }
    let (schema, expr) = structural_cast(
        DataType::List(Arc::new(Field::new("item", source_inner, true))),
        DataType::LargeList(Arc::new(Field::new("item", target_inner, true))),
    );
    assert!(!is_infallible_read_adaptation(&expr, &schema));
}

fn assert_list_subset_preserves_nulls<O: OffsetSizeTrait>() {
    let kept = Arc::new(Field::new("kept", DataType::Int32, true));
    let source_fields: Fields = vec![
        Arc::clone(&kept),
        Arc::new(Field::new("removed", DataType::Int32, false)),
    ]
    .into();
    let target_fields: Fields = vec![kept].into();
    let values: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(7),
        Some(99),
        Some(13),
        None,
        Some(-2),
    ]));
    let element_nulls = Some(NullBuffer::from(vec![true, false, true, true, true]));
    let source_values = Arc::new(StructArray::new(
        source_fields.clone(),
        vec![
            Arc::clone(&values),
            Arc::new(Int32Array::from(vec![107, 199, 113, 117, 98])),
        ],
        element_nulls.clone(),
    ));
    let expected_values = Arc::new(StructArray::new(
        target_fields.clone(),
        vec![values],
        element_nulls,
    ));
    // Include a null element, a null list with a nonempty backing range, a null
    // retained leaf, and an empty list. Offsets must remain unchanged.
    let offsets = OffsetBuffer::<O>::from_lengths([2, 1, 2, 0]);
    let list_nulls = Some(NullBuffer::from(vec![true, false, true, true]));
    let source = GenericListArray::<O>::new(
        Arc::new(Field::new("item", DataType::Struct(source_fields), true)),
        offsets.clone(),
        source_values,
        list_nulls.clone(),
    );
    let expected = GenericListArray::<O>::new(
        Arc::new(Field::new("element", DataType::Struct(target_fields), true)),
        offsets,
        expected_values,
        list_nulls,
    );
    let (schema, expr) = adapt(
        Field::new("s", source.data_type().clone(), true),
        Field::new("s", expected.data_type().clone(), true),
        options(),
    );
    assert!(expr.is::<CastExpr>(), "retain DataFusion leaf clipping");
    assert!(is_infallible_read_adaptation(&expr, &schema));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(source)]).unwrap();
    let output = expr.evaluate(&batch).unwrap().into_array(4).unwrap();
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<GenericListArray<O>>()
            .unwrap(),
        &expected,
    );
}

#[test]
fn list_subsets_preserve_offsets_values_and_nulls() {
    assert_list_subset_preserves_nulls::<i32>();
    assert_list_subset_preserves_nulls::<i64>();
}

#[test]
fn variant_and_opaque_comet_casts_are_not_identity_proofs() {
    let storage = struct_type(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("metadata", DataType::Binary, false),
    ]);
    let field = Field::new("s", storage, true).with_extension_type(VariantType);
    let (schema, expr) = adapt(field.clone(), field, options());
    assert!(expr.is::<CometCastColumnExpr>());
    assert!(!is_infallible_read_adaptation(&expr, &schema));

    let source = Arc::new(Field::new("s", DataType::Int32, true));
    let target = Arc::new(Field::new("s", DataType::Int64, true));
    let expr: Arc<dyn PhysicalExpr> = Arc::new(
        CometCastColumnExpr::try_new(
            Arc::new(Column::new("s", 0)),
            Arc::clone(&source),
            target,
            None,
        )
        .unwrap(),
    );
    let schema = Schema::new(vec![source]);
    assert!(!is_infallible_read_adaptation(&expr, &schema));
}
