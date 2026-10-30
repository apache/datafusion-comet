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

use arrow::array::{
    make_comparator, new_null_array, Array, ArrayRef, BooleanArray, DynComparator, UInt32Array,
};
use arrow::compute::{not, nullif, prep_null_mask_filter, take, SortOptions};
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::row::{OwnedRow, RowConverter, Rows, SortField};
use datafusion::common::{internal_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::physical_expr::expressions::format_state_name;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

/// Spark's `max` or `min` over a struct.
///
/// Spark orders structs field by field, with a null field below every value. Within a batch, each
/// group's best row is found with Arrow's struct comparator, which stops at the first field that
/// differs, so a wide trailing field that never decides costs nothing. Only those candidates are
/// encoded with Arrow's row format (ascending, nulls first, the same order), and each group keeps
/// the bytes of its current winner. Memory is accounted by the size of those buffers. Float and
/// double fields are not supported: both orders put `-0.0` below `0.0`, while Spark treats them
/// as equal.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkStructMinMax {
    signature: Signature,
    is_max: bool,
}

impl SparkStructMinMax {
    pub fn new(is_max: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            is_max,
        }
    }
}

impl AggregateUDFImpl for SparkStructMinMax {
    fn name(&self) -> &str {
        if self.is_max {
            "max"
        } else {
            "min"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(StructMinMaxAccumulator::try_new(
            args.return_field.data_type(),
            self.is_max,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, self.name()),
            args.return_field.data_type().clone(),
            true,
        ))])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(StructMinMaxGroupsAccumulator::try_new(
            args.return_field.data_type(),
            self.is_max,
        )?))
    }
}

/// Spark's struct order: ascending, with nulls (of the struct or of any field) first.
const SPARK_ORDER: SortOptions = SortOptions {
    descending: false,
    nulls_first: true,
};

/// Encodes struct values to Spark-ordered row bytes and decodes them back.
#[derive(Debug)]
struct StructRows {
    converter: RowConverter,
    /// The encoding of a null struct, emitted for a group that saw no value.
    null_row: OwnedRow,
    is_max: bool,
}

impl StructRows {
    fn try_new(data_type: &DataType, is_max: bool) -> Result<Self> {
        if !matches!(data_type, DataType::Struct(_)) {
            return internal_err!("Spark struct min/max expects a struct, got {data_type}");
        }
        let converter = RowConverter::new(vec![SortField::new_with_options(
            data_type.clone(),
            SPARK_ORDER,
        )])?;
        let null_row = converter
            .convert_columns(&[new_null_array(data_type, 1)])?
            .row(0)
            .owned();
        Ok(Self {
            converter,
            null_row,
            is_max,
        })
    }

    fn encode(&self, values: &ArrayRef) -> Result<Rows> {
        Ok(self.converter.convert_columns(&[Arc::clone(values)])?)
    }

    /// Compares rows of `values` with each other in Spark's struct order.
    fn comparator(&self, values: &ArrayRef) -> Result<DynComparator> {
        Ok(make_comparator(
            values.as_ref(),
            values.as_ref(),
            SPARK_ORDER,
        )?)
    }

    /// Whether a value that compares to the current one as `ordering` replaces it. Only a strictly
    /// better value does, so of equal values the first one seen is kept.
    #[inline]
    fn beats(&self, ordering: Ordering) -> bool {
        if self.is_max {
            ordering == Ordering::Greater
        } else {
            ordering == Ordering::Less
        }
    }

    /// Keeps the better of `value` and `slot`, reusing the slot's buffer. Returns the bytes the
    /// slot's buffer grew by, which is never negative since `clear` keeps the capacity.
    #[inline]
    fn update(&self, slot: &mut Option<Vec<u8>>, value: &[u8]) -> usize {
        match slot {
            None => {
                let bytes = value.to_vec();
                let added = bytes.capacity();
                *slot = Some(bytes);
                added
            }
            Some(current) if self.beats(value.cmp(current.as_slice())) => {
                let before = current.capacity();
                current.clear();
                current.extend_from_slice(value);
                current.capacity() - before
            }
            Some(_) => 0,
        }
    }

    fn decode<'a>(&self, values: impl Iterator<Item = Option<&'a [u8]>>) -> Result<ArrayRef> {
        let parser = self.converter.parser();
        let rows: Vec<_> = values
            .map(|value| match value {
                Some(bytes) => parser.parse(bytes),
                None => self.null_row.row(),
            })
            .collect();
        let mut columns = self.converter.convert_rows(rows)?;
        Ok(columns.remove(0))
    }

    fn size(&self) -> usize {
        self.converter.size() + self.null_row.as_ref().len()
    }
}

/// Whether row `i` takes part: a null struct or a row the filter drops does not.
#[inline]
fn is_skipped(values: &ArrayRef, filter: Option<&BooleanArray>, i: usize) -> bool {
    values.is_null(i) || filter.is_some_and(|f| !f.is_valid(i) || !f.value(i))
}

#[derive(Debug)]
struct StructMinMaxAccumulator {
    rows: StructRows,
    value: Option<Vec<u8>>,
}

impl StructMinMaxAccumulator {
    fn try_new(data_type: &DataType, is_max: bool) -> Result<Self> {
        Ok(Self {
            rows: StructRows::try_new(data_type, is_max)?,
            value: None,
        })
    }
}

impl Accumulator for StructMinMaxAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let cmp = self.rows.comparator(values)?;
        let mut best: Option<usize> = None;
        for i in 0..values.len() {
            if is_skipped(values, None, i) {
                continue;
            }
            if best.is_none_or(|b| self.rows.beats(cmp(i, b))) {
                best = Some(i);
            }
        }
        if let Some(best) = best {
            let encoded = self.rows.encode(&values.slice(best, 1))?;
            self.rows.update(&mut self.value, encoded.row(0).as_ref());
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let array = self.rows.decode(std::iter::once(self.value.as_deref()))?;
        ScalarValue::try_from_array(&array, 0)
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.rows.size()
            + self.value.as_ref().map_or(0, |value| value.capacity())
    }
}

#[derive(Debug)]
struct StructMinMaxGroupsAccumulator {
    rows: StructRows,
    /// The row bytes of each group's current winner, or `None` before it sees a value.
    values: Vec<Option<Vec<u8>>>,
    /// The bytes allocated by the buffers in `values`.
    value_bytes: usize,
    /// Scratch space for `update_batch`: each group's best row in the current batch, or
    /// `NO_ROW`, and the groups that have one. Reset after every batch.
    batch_best: Vec<u32>,
    touched: Vec<usize>,
}

const NO_ROW: u32 = u32::MAX;

impl StructMinMaxGroupsAccumulator {
    fn try_new(data_type: &DataType, is_max: bool) -> Result<Self> {
        Ok(Self {
            rows: StructRows::try_new(data_type, is_max)?,
            values: vec![],
            value_bytes: 0,
            batch_best: vec![],
            touched: vec![],
        })
    }
}

impl GroupsAccumulator for StructMinMaxGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let values = &values[0];
        self.values.resize(total_num_groups, None);
        self.batch_best.resize(total_num_groups, NO_ROW);

        // Find each group's best row in this batch without encoding anything.
        let cmp = self.rows.comparator(values)?;
        for (i, &group) in group_indices.iter().enumerate() {
            if is_skipped(values, opt_filter, i) {
                continue;
            }
            let best = &mut self.batch_best[group];
            if *best == NO_ROW {
                *best = i as u32;
                self.touched.push(group);
            } else if self.rows.beats(cmp(i, *best as usize)) {
                *best = i as u32;
            }
        }
        if self.touched.is_empty() {
            return Ok(());
        }

        // Encode only those rows, and keep each one that beats its group's stored winner.
        let indices: UInt32Array = self
            .touched
            .iter()
            .map(|&group| self.batch_best[group])
            .collect();
        let encoded = self.rows.encode(&take(values.as_ref(), &indices, None)?)?;
        for (k, &group) in self.touched.iter().enumerate() {
            self.value_bytes += self
                .rows
                .update(&mut self.values[group], encoded.row(k).as_ref());
            self.batch_best[group] = NO_ROW;
        }
        self.touched.clear();
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let emitted = emit_to.take_needed(&mut self.values);
        let freed: usize = emitted.iter().flatten().map(|value| value.capacity()).sum();
        self.value_bytes -= freed;
        self.rows
            .decode(emitted.iter().map(|value| value.as_deref()))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.evaluate(emit_to)?])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.update_batch(values, group_indices, None, total_num_groups)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = Arc::clone(&values[0]);
        let Some(filter) = opt_filter else {
            return Ok(vec![values]);
        };
        // A row the filter drops, or whose filter value is null, contributes nothing.
        let dropped = not(&prep_null_mask_filter(filter))?;
        Ok(vec![nullif(&values, &dropped)?])
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.rows.size()
            + self.values.capacity() * size_of::<Option<Vec<u8>>>()
            + self.value_bytes
            + self.batch_best.capacity() * size_of::<u32>()
            + self.touched.capacity() * size_of::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Int64Array, StringArray, StructArray};
    use arrow::datatypes::{Fields, Int64Type};

    fn fields(nullable: bool) -> Fields {
        Fields::from(vec![
            Field::new("k", DataType::Int64, nullable),
            Field::new("s", DataType::Utf8, nullable),
        ])
    }

    fn structs(rows: &[Option<(Option<i64>, Option<&str>)>]) -> ArrayRef {
        let k: Int64Array = rows.iter().map(|r| r.and_then(|(k, _)| k)).collect();
        let s: StringArray = rows.iter().map(|r| r.and_then(|(_, s)| s)).collect();
        let validity = rows.iter().map(Option::is_some).collect::<Vec<_>>();
        Arc::new(StructArray::new(
            fields(true),
            vec![Arc::new(k), Arc::new(s)],
            Some(validity.into()),
        ))
    }

    fn decoded(array: &ArrayRef) -> Vec<Option<(Option<i64>, Option<String>)>> {
        let array = array.as_struct();
        let k = array.column(0).as_primitive::<Int64Type>();
        let s = array.column(1).as_string::<i32>();
        (0..array.len())
            .map(|i| {
                array.is_valid(i).then(|| {
                    (
                        k.is_valid(i).then(|| k.value(i)),
                        s.is_valid(i).then(|| s.value(i).to_string()),
                    )
                })
            })
            .collect()
    }

    fn groups(is_max: bool) -> StructMinMaxGroupsAccumulator {
        StructMinMaxGroupsAccumulator::try_new(&DataType::Struct(fields(true)), is_max).unwrap()
    }

    /// Spark orders a null field below every value, at any position, and skips a null struct.
    #[test]
    fn orders_null_fields_first_like_spark() -> Result<()> {
        let input = structs(&[
            Some((Some(1), Some("z"))),
            Some((None, Some("zz"))),
            Some((Some(2), Some("a"))),
            None,
            Some((Some(2), None)),
        ]);
        for (is_max, expected) in [
            (true, Some((Some(2), Some("a".to_string())))),
            (false, Some((None, Some("zz".to_string())))),
        ] {
            let mut acc = groups(is_max);
            acc.update_batch(&[Arc::clone(&input)], &[0, 0, 0, 0, 0], None, 1)?;
            assert_eq!(decoded(&acc.evaluate(EmitTo::All)?), vec![expected]);
        }
        Ok(())
    }

    /// A group with no value, or whose rows the filter drops, emits a null struct.
    #[test]
    fn empty_and_filtered_groups_are_null() -> Result<()> {
        let input = structs(&[Some((Some(1), Some("a"))), Some((Some(5), Some("b")))]);
        let filter = BooleanArray::from(vec![Some(true), None]);
        let mut acc = groups(true);
        acc.update_batch(&[input], &[0, 1], Some(&filter), 3)?;
        assert_eq!(
            decoded(&acc.evaluate(EmitTo::All)?),
            vec![Some((Some(1), Some("a".to_string()))), None, None]
        );
        Ok(())
    }

    /// Emitting a prefix of the groups frees exactly the bytes those groups held, so ordered
    /// aggregation does not keep reserving memory for groups it has already emitted.
    #[test]
    fn emitting_groups_releases_their_bytes() -> Result<()> {
        let payload = "x".repeat(64 * 1024);
        let mut acc = groups(true);
        let empty = acc.size();
        for round in 0..3 {
            let rows: Vec<_> = (0..64)
                .map(|k| Some((Some(round * 64 + k), Some(payload.as_str()))))
                .collect();
            let group_indices: Vec<usize> = (0..64).collect();
            acc.update_batch(&[structs(&rows)], &group_indices, None, 64)?;
            assert!(acc.size() > 64 * payload.len());
            acc.evaluate(EmitTo::First(32))?;
            acc.evaluate(EmitTo::First(32))?;
            assert_eq!(acc.value_bytes, 0);
            assert!(acc.size() < empty + 64 * size_of::<Option<Vec<u8>>>());
        }
        Ok(())
    }

    /// Partial states merge into the same answer as one pass over every row.
    #[test]
    fn merge_matches_single_pass() -> Result<()> {
        let first = structs(&[Some((Some(3), Some("b"))), Some((Some(1), None))]);
        let second = structs(&[Some((Some(3), Some("c"))), None]);
        let mut left = groups(true);
        left.update_batch(&[first], &[0, 1], None, 2)?;
        let mut right = groups(true);
        right.update_batch(&[second], &[0, 1], None, 2)?;
        let mut merged = groups(true);
        merged.merge_batch(&left.state(EmitTo::All)?, &[0, 1], 2)?;
        merged.merge_batch(&right.state(EmitTo::All)?, &[0, 1], 2)?;
        assert_eq!(
            decoded(&merged.evaluate(EmitTo::All)?),
            vec![
                Some((Some(3), Some("c".to_string()))),
                Some((Some(1), None))
            ]
        );
        Ok(())
    }

    #[test]
    fn ungrouped_accumulator_matches_grouped() -> Result<()> {
        let input = structs(&[
            Some((Some(2), Some("a"))),
            None,
            Some((Some(2), Some("b"))),
            Some((None, Some("zzz"))),
        ]);
        let mut acc = StructMinMaxAccumulator::try_new(&DataType::Struct(fields(true)), true)?;
        acc.update_batch(&[Arc::clone(&input)])?;
        let state = acc.state()?;
        let mut merged = StructMinMaxAccumulator::try_new(&DataType::Struct(fields(true)), true)?;
        merged.merge_batch(&[state[0].to_array()?])?;
        let result = merged.evaluate()?.to_array()?;
        assert_eq!(
            decoded(&result),
            vec![Some((Some(2), Some("b".to_string())))]
        );

        let mut empty = StructMinMaxAccumulator::try_new(&DataType::Struct(fields(true)), true)?;
        assert_eq!(decoded(&empty.evaluate()?.to_array()?), vec![None]);
        Ok(())
    }

    /// Non-nullable fields decode with the declared type.
    #[test]
    fn keeps_non_nullable_field_types() -> Result<()> {
        let data_type = DataType::Struct(fields(false));
        let input: ArrayRef = Arc::new(StructArray::new(
            fields(false),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
            None,
        ));
        let mut acc = StructMinMaxGroupsAccumulator::try_new(&data_type, false)?;
        acc.update_batch(&[input], &[0, 0], None, 1)?;
        let result = acc.evaluate(EmitTo::All)?;
        assert_eq!(result.data_type(), &data_type);
        Ok(())
    }

    /// Nested structs with nulls at every level, used to check the two orders agree.
    fn nested_structs(n: usize) -> ArrayRef {
        // Small value ranges so that ties on the leading fields are common.
        let k: Int64Array = (0..n)
            .map(|i| (i % 7 != 0).then_some((i * 7919 % 5) as i64 - 2))
            .collect();
        let s: StringArray = (0..n)
            .map(|i| (i % 5 != 0).then(|| ["", "a", "ab", "日本", "\u{7f}"][i * 31 % 5]))
            .collect();
        let inner_v: Int64Array = (0..n)
            .map(|i| (i % 3 != 0).then_some((i * 104729 % 3) as i64))
            .collect();
        let inner_fields = Fields::from(vec![Field::new("v", DataType::Int64, true)]);
        let inner_validity: Vec<bool> = (0..n).map(|i| i % 11 != 0).collect();
        let inner = StructArray::new(
            inner_fields.clone(),
            vec![Arc::new(inner_v)],
            Some(inner_validity.into()),
        );
        let outer_fields = Fields::from(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("inner", DataType::Struct(inner_fields), true),
        ]);
        let outer_validity: Vec<bool> = (0..n).map(|i| i % 13 != 0).collect();
        Arc::new(StructArray::new(
            outer_fields,
            vec![Arc::new(k), Arc::new(s), Arc::new(inner)],
            Some(outer_validity.into()),
        ))
    }

    /// Each batch picks candidates with Arrow's comparator, and winners are kept as row bytes.
    /// The two must order every pair of values the same way.
    #[test]
    fn comparator_and_row_bytes_agree() -> Result<()> {
        let values = nested_structs(200);
        let rows = StructRows::try_new(values.data_type(), true)?;
        let cmp = rows.comparator(&values)?;
        let encoded = rows.encode(&values)?;
        for i in 0..values.len() {
            for j in 0..values.len() {
                assert_eq!(
                    cmp(i, j),
                    encoded.row(i).cmp(&encoded.row(j)),
                    "rows {i} and {j}"
                );
            }
        }
        Ok(())
    }

    /// Many rows per group in each batch, across several batches, against a brute-force answer.
    #[test]
    fn matches_brute_force_over_batches() -> Result<()> {
        let values = nested_structs(600);
        let data_type = values.data_type().clone();
        let groups: Vec<usize> = (0..values.len()).map(|i| i * 17 % 9).collect();
        let reference = StructRows::try_new(&data_type, true)?;
        let encoded = reference.encode(&values)?;
        for is_max in [true, false] {
            let mut acc = StructMinMaxGroupsAccumulator::try_new(&data_type, is_max)?;
            for start in (0..values.len()).step_by(100) {
                acc.update_batch(
                    &[values.slice(start, 100)],
                    &groups[start..start + 100],
                    None,
                    9,
                )?;
            }
            let result = acc.evaluate(EmitTo::All)?;
            let result_rows = reference.encode(&result)?;
            for group in 0..9 {
                let members =
                    (0..values.len()).filter(|&i| groups[i] == group && values.is_valid(i));
                let expected = if is_max {
                    members.max_by(|&a, &b| encoded.row(a).cmp(&encoded.row(b)))
                } else {
                    members.min_by(|&a, &b| encoded.row(a).cmp(&encoded.row(b)))
                };
                match expected {
                    Some(i) => assert_eq!(result_rows.row(group), encoded.row(i), "group {group}"),
                    None => assert!(result.is_null(group), "group {group}"),
                }
            }
        }
        Ok(())
    }

    /// Sorted aggregation emits finished groups after every batch. Struct max over 4,096 groups
    /// of 4 KiB values (16 MiB in all), read in 64-row batches, must fit a 2 MiB pool, which it
    /// only does if emitting a group releases the memory charged for it. DataFusion's own struct
    /// max fails this with `ResourcesExhausted`, because it subtracts rows rather than bytes.
    #[tokio::test]
    async fn sorted_aggregation_fits_a_small_pool() -> Result<()> {
        use arrow::array::{Int64Array, RecordBatch};
        use arrow::datatypes::Schema;
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::datasource::source::DataSourceExec;
        use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use datafusion::execution::TaskContext;
        use datafusion::logical_expr::AggregateUDF;
        use datafusion::physical_expr::aggregate::AggregateExprBuilder;
        use datafusion::physical_expr::expressions::{col, Column};
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
        use datafusion::physical_plan::aggregates::{
            AggregateExec, AggregateMode, PhysicalGroupBy,
        };
        use datafusion::physical_plan::{collect, ExecutionPlan, InputOrderMode};
        use datafusion::prelude::SessionConfig;

        const GROUPS: i64 = 4096;
        const BATCH_ROWS: i64 = 64;
        let payload = "x".repeat(4096);
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("v", DataType::Struct(fields(true)), true),
        ]));
        let batches = (0..GROUPS / BATCH_ROWS)
            .map(|b| {
                let groups = (b * BATCH_ROWS..(b + 1) * BATCH_ROWS).collect::<Vec<_>>();
                let rows: Vec<_> = groups
                    .iter()
                    .map(|&g| Some((Some(g), Some(payload.as_str()))))
                    .collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(groups)), structs(&rows)],
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
            Column::new("g", 0),
        ))])
        .unwrap();
        let source = MemorySourceConfig::try_new(&[batches], Arc::clone(&schema), None)?
            .try_with_sort_information(vec![ordering])?;
        let input: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(source);

        let max = Arc::new(
            AggregateExprBuilder::new(
                Arc::new(AggregateUDF::new_from_impl(SparkStructMinMax::new(true))),
                vec![col("v", &schema)?],
            )
            .schema(Arc::clone(&schema))
            .alias("max")
            .build()?,
        );
        // Comet splits a sort aggregate into a Partial and a Final stage. Both see sorted input,
        // so DataFusion runs its ordered streams, which emit finished groups after every batch.
        let group_by = PhysicalGroupBy::new_single(vec![(col("g", &schema)?, "g".to_string())]);
        let partial = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by.clone(),
            vec![Arc::clone(&max)],
            vec![None],
            input,
            Arc::clone(&schema),
        )?);
        let aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            group_by.as_final(),
            vec![max],
            vec![None],
            Arc::clone(&partial) as Arc<dyn ExecutionPlan>,
            Arc::clone(&schema),
        )?);
        assert_eq!(partial.input_order_mode(), &InputOrderMode::Sorted);
        // No disk, so an over-reserving accumulator fails instead of spilling.
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(2 * 1024 * 1024, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()?;
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(BATCH_ROWS as usize))
                .with_runtime(runtime),
        );
        let output = collect(aggregate, context).await?;
        assert_eq!(
            output.iter().map(|b| b.num_rows()).sum::<usize>(),
            GROUPS as usize
        );
        Ok(())
    }
}
