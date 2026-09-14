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

//! Nested array shapes shared by the hash benchmarks, pulled in with
//! `#[path = "common/hash_shapes.rs"] mod hash_shapes;`. This lives in a subdirectory so that
//! Cargo's bench auto-discovery, which only looks at `benches/*.rs`, does not treat it as a bench
//! target.
//!
//! `hash.rs` measures time and `hash_alloc.rs` measures allocation, and both have to build the
//! same inputs for the two sets of numbers to describe the same work.
#![allow(dead_code)]

use arrow::array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder, StructBuilder};
use arrow::array::{Int32Array, ListArray, MapFieldNames, StringArray, StructArray};
// Re-exported so a bench that uses these shapes does not have to import it separately.
pub use arrow::array::ArrayRef;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields};
use std::sync::Arc;

pub const NUM_ROWS: usize = 8192;

pub fn struct_fields() -> Fields {
    vec![
        Arc::new(Field::new("a", DataType::Int32, true)),
        Arc::new(Field::new("b", DataType::Utf8, true)),
    ]
    .into()
}

pub fn struct_builder() -> StructBuilder {
    StructBuilder::new(
        struct_fields(),
        vec![
            Box::new(Int32Builder::new()),
            Box::new(StringBuilder::new()),
        ],
    )
}

pub fn append_struct(sb: &mut StructBuilder, i: usize) {
    sb.field_builder::<Int32Builder>(0)
        .unwrap()
        .append_value(i as i32);
    sb.field_builder::<StringBuilder>(1)
        .unwrap()
        .append_value(format!("v{}", i % 97));
    sb.append(true);
}

/// `int32`, the cheapest leaf, as a reference point for the nested shapes.
pub fn primitive(num_rows: usize) -> ArrayRef {
    Arc::new(Int32Array::from((0..num_rows as i32).collect::<Vec<_>>()))
}

/// `utf8`: variable-width, so the hash reads from the values buffer per row.
pub fn string(num_rows: usize) -> ArrayRef {
    Arc::new(StringArray::from(
        (0..num_rows)
            .map(|i| format!("v{}", i % 97))
            .collect::<Vec<_>>(),
    ))
}

/// `struct<a: int32, b: utf8>`: hashed field by field across the whole batch.
pub fn structs(num_rows: usize) -> ArrayRef {
    let mut sb = struct_builder();
    for i in 0..num_rows {
        append_struct(&mut sb, i);
    }
    Arc::new(sb.finish())
}

/// `array<int32>`: elements are primitives, so this takes the vectorized element path.
pub fn list_of_primitive(num_rows: usize, elems: usize) -> ArrayRef {
    let mut lb = ListBuilder::new(Int32Builder::new());
    for i in 0..num_rows {
        for j in 0..elems {
            lb.values().append_value((i * 31 + j) as i32);
        }
        lb.append(true);
    }
    Arc::new(lb.finish())
}

/// `array<struct<..>>`: elements are nested, so this takes the per-element path.
pub fn list_of_struct(num_rows: usize, elems: usize) -> ArrayRef {
    let mut lb = ListBuilder::new(struct_builder());
    for i in 0..num_rows {
        for j in 0..elems {
            append_struct(lb.values(), i * 31 + j);
        }
        lb.append(true);
    }
    Arc::new(lb.finish())
}

/// `array<struct<..>>` where one row is far longer than the rest, so the element count is spread
/// very unevenly across rows rather than uniformly. The per-element path slices and re-dispatches
/// once per element, so a batch dominated by a single long list has the same total work in a very
/// different distribution, which a uniform shape cannot show.
pub fn skewed_list_of_struct(num_rows: usize, long_len: usize) -> ArrayRef {
    let mut lb = ListBuilder::new(struct_builder());
    for i in 0..num_rows {
        let len = if i == 0 { long_len } else { 1 };
        for j in 0..len {
            append_struct(lb.values(), i * 31 + j);
        }
        lb.append(true);
    }
    Arc::new(lb.finish())
}

/// `map<utf8, int32>`: keys and values are hashed entry by entry.
pub fn maps(num_rows: usize, entries: usize) -> ArrayRef {
    let mut mb = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".into(),
            key: "key".into(),
            value: "value".into(),
        }),
        StringBuilder::new(),
        Int32Builder::new(),
    );
    for i in 0..num_rows {
        for j in 0..entries {
            mb.keys().append_value(format!("k{}", (i + j) % 97));
            mb.values().append_value((i * 31 + j) as i32);
        }
        mb.append(true).unwrap();
    }
    Arc::new(mb.finish())
}

/// `struct<a: int32, m: map<utf8, int32>>`: a map inside a struct, so the struct branch recurses
/// into the map specialization rather than into a leaf.
pub fn struct_of_map(num_rows: usize, entries: usize) -> ArrayRef {
    let fields: Fields = vec![
        Arc::new(Field::new("a", DataType::Int32, true)),
        Arc::new(Field::new(
            "m",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", DataType::Utf8, false)),
                            Arc::new(Field::new("value", DataType::Int32, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        )),
    ]
    .into();
    let ints = primitive(num_rows);
    let ms = maps(num_rows, entries);
    Arc::new(StructArray::new(fields, vec![ints, ms], None))
}

/// `array<array<int32>>`: the element is a list, so the non-primitive element path recurses into
/// the vectorized leaf loop one level down.
pub fn list_of_list(num_rows: usize, outer: usize, inner: usize) -> ArrayRef {
    let mut lb = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
    for i in 0..num_rows {
        for j in 0..outer {
            for k in 0..inner {
                lb.values()
                    .values()
                    .append_value((i * 31 + j * 7 + k) as i32);
            }
            lb.values().append(true);
        }
        lb.append(true);
    }
    Arc::new(lb.finish())
}

/// `map<utf8, struct<..>>`: a struct as the map value, which the key/value specializations do not
/// cover, so the value array is hashed recursively instead.
pub fn map_of_struct(num_rows: usize, entries: usize) -> ArrayRef {
    let mut mb = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".into(),
            key: "key".into(),
            value: "value".into(),
        }),
        StringBuilder::new(),
        struct_builder(),
    );
    for i in 0..num_rows {
        for j in 0..entries {
            mb.keys().append_value(format!("k{}", (i + j) % 97));
            append_struct(mb.values(), i * 31 + j);
        }
        mb.append(true).unwrap();
    }
    Arc::new(mb.finish())
}

/// `array<struct<a: int32, m: map<..>>>`: three levels, so the per-element path recurses through a
/// struct into a map.
pub fn list_of_struct_of_map(num_rows: usize, elems: usize, entries: usize) -> ArrayRef {
    let inner = struct_of_map(num_rows * elems, entries);
    let offsets: Vec<i32> = (0..=num_rows).map(|i| (i * elems) as i32).collect();
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", inner.data_type().clone(), true)),
        OffsetBuffer::new(offsets.into()),
        inner,
        None,
    ))
}

/// `array<struct<..>>` with a large string child, so the gather copies real payload rather than
/// the two- or three-byte strings the other shapes use.
pub fn list_of_struct_big_string(num_rows: usize, elems: usize, len: usize) -> ArrayRef {
    let big = "x".repeat(len);
    let mut lb = ListBuilder::new(struct_builder());
    for i in 0..num_rows {
        for j in 0..elems {
            let s = lb.values();
            s.field_builder::<Int32Builder>(0)
                .unwrap()
                .append_value((i * 31 + j) as i32);
            s.field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(&big);
            s.append(true);
        }
        lb.append(true);
    }
    Arc::new(lb.finish())
}

/// `array<struct<..>>` where half the elements are null structs whose children still hold values,
/// so the child data under a null parent is carried through the same path.
pub fn list_of_struct_half_null(num_rows: usize, elems: usize) -> ArrayRef {
    let mut lb = ListBuilder::new(struct_builder());
    for i in 0..num_rows {
        for j in 0..elems {
            let s = lb.values();
            s.field_builder::<Int32Builder>(0)
                .unwrap()
                .append_value((i * 31 + j) as i32);
            s.field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value("payload");
            s.append((i + j) % 2 == 0);
        }
        lb.append(true);
    }
    Arc::new(lb.finish())
}

/// Short rows and then one much longer row, so all but the first pass has a single surviving row.
/// A shape that batches badly: there is nothing to gather across once the short rows finish.
pub fn list_of_struct_long_tail(num_rows: usize, tail: usize) -> ArrayRef {
    let mut lb = ListBuilder::new(struct_builder());
    for i in 0..num_rows {
        append_struct(lb.values(), i);
        lb.append(true);
    }
    for j in 0..tail {
        append_struct(lb.values(), j);
    }
    lb.append(true);
    Arc::new(lb.finish())
}

/// Deeply nested singleton lists wrapping a struct with a wide string payload. Each recursion
/// level gathers while the level above it still holds its own gather, so this is the shape that
/// shows whether peak live bytes is one gather or the sum of the nesting depth.
pub fn deep_singleton_list_of_struct_big_string(
    num_rows: usize,
    depth: usize,
    str_len: usize,
) -> ArrayRef {
    let mut sb = StructBuilder::new(
        struct_fields(),
        vec![
            Box::new(Int32Builder::new()),
            Box::new(StringBuilder::new()),
        ],
    );
    let payload = "x".repeat(str_len);
    for i in 0..num_rows {
        sb.field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(i as i32);
        sb.field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value(&payload);
        sb.append(true);
    }
    let mut current: ArrayRef = Arc::new(sb.finish());
    // One element per row at every level, so every level gathers `num_rows` elements.
    for _ in 0..depth {
        let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(1usize, num_rows));
        let field = Arc::new(Field::new("item", current.data_type().clone(), true));
        current = Arc::new(ListArray::new(field, offsets, current, None));
    }
    current
}

/// A struct whose children hold live payload but whose parent row is null. Arrow's gather copies
/// the children anyway, so the copied bytes never reach the hash.
pub fn null_parent_struct_big_string(num_rows: usize, elems: usize, str_len: usize) -> ArrayRef {
    let mut lb = ListBuilder::new(struct_builder());
    let payload = "x".repeat(str_len);
    for _ in 0..num_rows {
        for i in 0..elems {
            let sb = lb.values();
            sb.field_builder::<Int32Builder>(0)
                .unwrap()
                .append_value(i as i32);
            sb.field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(&payload);
            // Null parent, live children.
            sb.append(false);
        }
        lb.append(true);
    }
    Arc::new(lb.finish())
}

/// A struct whose child is a dictionary holding one huge, unreferenced value. `take` on a
/// dictionary copies the keys and shares the values, so the payload is never duplicated, but an
/// estimate based on the child's total memory size counts it anyway.
pub fn struct_of_dict_unreferenced_big_value(num_rows: usize, big_len: usize) -> ArrayRef {
    use arrow::array::{DictionaryArray, Int32Array as I32};
    use arrow::datatypes::Int32Type;

    // Every key points at "x"; the 8 MiB value exists in the dictionary but is never referenced.
    let values = StringArray::from(vec!["x".to_string(), "y".repeat(big_len)]);
    let keys = I32::from(vec![0i32; num_rows]);
    let dict: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).expect("dictionary"),
    );
    let ints: ArrayRef = Arc::new(I32::from_iter_values((0..num_rows).map(|i| i as i32)));

    let fields: Fields = vec![
        Arc::new(Field::new("a", DataType::Int32, true)),
        Arc::new(Field::new(
            "b",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )),
    ]
    .into();
    let structs: ArrayRef = Arc::new(StructArray::new(fields, vec![ints, dict], None));

    // One struct element per row, so the list path is taken.
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(1usize, num_rows));
    let field = Arc::new(Field::new("item", structs.data_type().clone(), true));
    Arc::new(ListArray::new(field, offsets, structs, None))
}

/// Two rows of 1,024 structs where only the first element's string is huge. An average width over
/// the child is diluted by the short elements, so a cost estimate based on it under-reads what the
/// gather will copy.
pub fn width_skewed_list_of_struct(big_len: usize) -> ArrayRef {
    let mut lb = ListBuilder::new(struct_builder());
    for _ in 0..2 {
        for i in 0..1024usize {
            let sb = lb.values();
            sb.field_builder::<Int32Builder>(0)
                .unwrap()
                .append_value(i as i32);
            let s = if i == 0 {
                "y".repeat(big_len)
            } else {
                "x".to_string()
            };
            sb.field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(&s);
            sb.append(true);
        }
        lb.append(true);
    }
    Arc::new(lb.finish())
}

/// A list sliced down to two rows whose child still backs ten million ints. `take` pre-sizes from
/// the retained child, so the gather is far larger than the two visible rows suggest.
pub fn sliced_list_retaining_big_child(kept_rows: usize, inner_elems: usize) -> ArrayRef {
    let total_rows = 1000usize;
    let values: ArrayRef = Arc::new(Int32Array::from_iter_values(
        (0..inner_elems).map(|i| i as i32),
    ));
    // First row owns nearly all the elements; the rest are empty.
    let mut lengths = vec![inner_elems - (total_rows - 1)];
    lengths.extend(std::iter::repeat_n(1usize, total_rows - 1));
    let offsets = OffsetBuffer::from_lengths(lengths);
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let inner: ArrayRef = Arc::new(ListArray::new(field, offsets, values, None));

    let outer_offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(1usize, total_rows));
    let outer_field = Arc::new(Field::new("item", inner.data_type().clone(), true));
    let outer: ArrayRef = Arc::new(ListArray::new(outer_field, outer_offsets, inner, None));
    // Slice past the huge first row: only cheap rows are visible, the buffer stays.
    outer.slice(1, kept_rows)
}

/// A single row of flat structs. Eligible for batching, but with one row there is nothing to batch
/// across, so the scheduling buffers are pure overhead.
pub fn single_row_list_of_struct(elems: usize) -> ArrayRef {
    let mut lb = ListBuilder::new(struct_builder());
    for i in 0..elems {
        let sb = lb.values();
        sb.field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(i as i32);
        sb.field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value(format!("e{i}"));
        sb.append(true);
    }
    lb.append(true);
    Arc::new(lb.finish())
}

/// Deep singleton lists over a narrow struct. The outer levels are not eligible, but recursing
/// reaches an eligible single-row flat struct at the bottom, which is where scheduler buffers get
/// allocated for a batch of one.
pub fn deep_singleton_list_narrow(num_rows: usize, depth: usize) -> ArrayRef {
    let mut sb = StructBuilder::new(
        struct_fields(),
        vec![
            Box::new(Int32Builder::new()),
            Box::new(StringBuilder::new()),
        ],
    );
    for i in 0..num_rows {
        sb.field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(i as i32);
        sb.field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("abcd");
        sb.append(true);
    }
    let mut current: ArrayRef = Arc::new(sb.finish());
    for _ in 0..depth {
        let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(1usize, num_rows));
        let field = Arc::new(Field::new("item", current.data_type().clone(), true));
        current = Arc::new(ListArray::new(field, offsets, current, None));
    }
    current
}
