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

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, LargeListArray, ListArray, MapArray, RecordBatch,
    RecordBatchOptions, StringArray, StructArray, UInt64Array,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::row::{RowConverter, Rows, SortField};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::TryStreamExt;

/// Native counterpart of Iceberg's sorted changelog iterators. Input ordering and distribution
/// are supplied by create_changelog_view; state is local to one execution, not a streaming checkpoint.
#[derive(Debug)]
pub struct IcebergChangelogExec {
    input: Arc<dyn ExecutionPlan>,
    mode: u32,
    metadata: [usize; 3],
    identifiers: Vec<usize>,
    output_indices: Vec<usize>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl IcebergChangelogExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        mode: u32,
        metadata: [usize; 3],
        identifiers: Vec<usize>,
        output_indices: Vec<usize>,
    ) -> Result<Self> {
        if mode > 2
            || (mode == 1 && identifiers.is_empty())
            || metadata[0] == metadata[1]
            || metadata[0] == metadata[2]
            || metadata[1] == metadata[2]
        {
            return Err(DataFusionError::Plan(
                "Invalid Iceberg changelog mode or identifiers".into(),
            ));
        }
        let schema = input.schema();
        if identifiers
            .iter()
            .chain(&output_indices)
            .chain(&metadata)
            .any(|&i| i >= schema.fields().len())
        {
            return Err(DataFusionError::Plan(
                "Iceberg changelog column index out of range".into(),
            ));
        }
        let schema = Arc::new(schema.project(&output_indices)?);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(
                input.properties().partitioning.partition_count(),
            ),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            mode,
            metadata,
            identifiers,
            output_indices,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for IcebergChangelogExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IcebergChangelogExec: mode={}", self.mode)
    }
}

impl ExecutionPlan for IcebergChangelogExec {
    fn name(&self) -> &str {
        "IcebergChangelogExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.mode,
            self.metadata,
            self.identifiers.clone(),
            self.output_indices.clone(),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let processor = Processor::new(
            self.input.execute(partition, Arc::clone(&context))?,
            self.mode,
            self.metadata,
            &self.identifiers,
        )?;
        let size = context.session_config().batch_size();
        let output_indices = self.output_indices.clone();
        let schema = self.schema();
        let output_schema = Arc::clone(&schema);
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let stream = futures::stream::try_unfold(processor, move |mut processor| {
            let schema = Arc::clone(&output_schema);
            let indices = output_indices.clone();
            let baseline = baseline.clone();
            async move {
                let mut rows = Vec::with_capacity(size);
                while rows.len() < size {
                    match processor.next().await? {
                        Some(row) => {
                            // A sparse result must not pin thousands of full input batches.
                            let boundary = rows.first().is_some_and(|first: &ChangeRow| {
                                !Arc::ptr_eq(&first.batch, &row.batch)
                            });
                            rows.push(row);
                            if boundary {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                if rows.is_empty() {
                    return Ok(None);
                }
                let batch = collect_rows(&rows, schema, &indices, processor.source.change_index)?;
                baseline.record_output(batch.num_rows());
                Ok(Some((batch, processor)))
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Insert,
    Delete,
    Before,
    After,
}
impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Delete => "DELETE",
            Self::Before => "UPDATE_BEFORE",
            Self::After => "UPDATE_AFTER",
        }
    }
}

struct EncodedBatch {
    batch: RecordBatch,
    keys: Rows,
    identifiers: Option<Rows>,
    kinds: StringArray,
}

#[derive(Clone)]
struct ChangeRow {
    batch: Arc<EncodedBatch>,
    index: usize,
    kind: Kind,
}
impl ChangeRow {
    fn same_record(&self, other: &Self) -> bool {
        self.batch.keys.row(self.index) == other.batch.keys.row(other.index)
    }
    fn same_identifier(&self, other: &Self) -> bool {
        self.batch.identifiers.as_ref().unwrap().row(self.index)
            == other.batch.identifiers.as_ref().unwrap().row(other.index)
    }
}

// Iceberg compares external Spark values with Objects.equals. Maps compare independently of
// entry order and floating NaNs compare equal, while the signs of zero remain distinct.
fn comparison_values(array: &ArrayRef) -> Result<ArrayRef> {
    Ok(match array.data_type() {
        DataType::Float32 => Arc::new(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .iter()
                .map(|v| v.map(|v| if v.is_nan() { f32::NAN } else { v }))
                .collect::<Float32Array>(),
        ),
        DataType::Float64 => Arc::new(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|v| v.map(|v| if v.is_nan() { f64::NAN } else { v }))
                .collect::<Float64Array>(),
        ),
        DataType::Struct(fields) => {
            let values = array.as_any().downcast_ref::<StructArray>().unwrap();
            Arc::new(StructArray::new(
                fields.clone(),
                values
                    .columns()
                    .iter()
                    .map(comparison_values)
                    .collect::<Result<Vec<_>>>()?,
                values.nulls().cloned(),
            ))
        }
        DataType::List(field) => {
            let values = array.as_any().downcast_ref::<ListArray>().unwrap();
            Arc::new(ListArray::new(
                Arc::clone(field),
                values.offsets().clone(),
                comparison_values(values.values())?,
                values.nulls().cloned(),
            ))
        }
        DataType::LargeList(field) => {
            let values = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            Arc::new(LargeListArray::new(
                Arc::clone(field),
                values.offsets().clone(),
                comparison_values(values.values())?,
                values.nulls().cloned(),
            ))
        }
        DataType::Map(field, sorted) => {
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            let entries = StructArray::new(
                map.entries().fields().clone(),
                map.entries()
                    .columns()
                    .iter()
                    .map(comparison_values)
                    .collect::<Result<Vec<_>>>()?,
                None,
            );
            let converter =
                RowConverter::new(vec![SortField::new(entries.column(0).data_type().clone())])?;
            let keys = converter.convert_columns(&[Arc::clone(entries.column(0))])?;
            let mut indices = (0..entries.len()).map(|i| i as u64).collect::<Vec<_>>();
            for offsets in map.offsets().windows(2) {
                indices[offsets[0] as usize..offsets[1] as usize]
                    .sort_by(|&a, &b| keys.row(a as usize).cmp(&keys.row(b as usize)));
            }
            let entries = arrow::compute::take(&entries, &UInt64Array::from(indices), None)?;
            Arc::new(MapArray::new(
                Arc::clone(field),
                map.offsets().clone(),
                entries
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .clone(),
                map.nulls().cloned(),
                *sorted,
            ))
        }
        _ => Arc::clone(array),
    })
}

struct Source {
    input: SendableRecordBatchStream,
    current: Option<Arc<EncodedBatch>>,
    index: usize,
    change_index: usize,
    key_indices: Vec<usize>,
    identifier_indices: Vec<usize>,
    keys: RowConverter,
    identifiers: Option<RowConverter>,
}
impl Source {
    fn new(
        input: SendableRecordBatchStream,
        net: bool,
        metadata: [usize; 3],
        identifiers: &[usize],
    ) -> Result<Self> {
        let schema = input.schema();
        let [change_index, ordinal, snapshot] = metadata;
        let key_indices: Vec<_> = (0..schema.fields().len())
            .filter(|&i| i != change_index && (!net || (i != ordinal && i != snapshot)))
            .collect();
        let converter = |indices: &[usize]| {
            RowConverter::new(
                indices
                    .iter()
                    .map(|&i| SortField::new(schema.field(i).data_type().clone()))
                    .collect(),
            )
        };
        let keys = converter(&key_indices)?;
        let identifiers_converter = if identifiers.is_empty() {
            None
        } else {
            Some(converter(identifiers)?)
        };
        Ok(Self {
            input,
            current: None,
            index: 0,
            change_index,
            key_indices,
            identifier_indices: identifiers.to_vec(),
            keys,
            identifiers: identifiers_converter,
        })
    }
    async fn next(&mut self) -> Result<Option<ChangeRow>> {
        loop {
            if let Some(batch) = &self.current {
                if self.index < batch.batch.num_rows() {
                    let index = self.index;
                    self.index += 1;
                    if batch.kinds.is_null(index) {
                        return Err(DataFusionError::Execution(
                            "Change type should not be null".into(),
                        ));
                    }
                    let kind = match batch.kinds.value(index) {
                        "INSERT" => Kind::Insert,
                        "DELETE" => Kind::Delete,
                        other => {
                            return Err(DataFusionError::Execution(format!(
                                "Unexpected Iceberg change type: {other}"
                            )))
                        }
                    };
                    return Ok(Some(ChangeRow {
                        batch: Arc::clone(batch),
                        index,
                        kind,
                    }));
                }
            }
            let Some(batch) = self.input.try_next().await? else {
                return Ok(None);
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let columns = |indices: &[usize]| {
                indices
                    .iter()
                    .map(|&i| Arc::clone(batch.column(i)))
                    .collect::<Vec<_>>()
            };
            let keys = self.keys.convert_columns(
                &columns(&self.key_indices)
                    .iter()
                    .map(comparison_values)
                    .collect::<Result<Vec<_>>>()?,
            )?;
            let identifiers = self
                .identifiers
                .as_ref()
                .map(|c| -> Result<_> {
                    Ok(c.convert_columns(
                        &columns(&self.identifier_indices)
                            .iter()
                            .map(comparison_values)
                            .collect::<Result<Vec<_>>>()?,
                    )?)
                })
                .transpose()?;
            let kinds = arrow::compute::cast(batch.column(self.change_index), &DataType::Utf8)?;
            let kinds = kinds
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            self.current = Some(Arc::new(EncodedBatch {
                batch,
                keys,
                identifiers,
                kinds,
            }));
            self.index = 0;
        }
    }
}

struct Processor {
    source: Source,
    mode: u32,
    lookahead: Option<ChangeRow>,
    repeated: Option<(ChangeRow, usize)>,
    update_next: Option<ChangeRow>,
}
impl Processor {
    fn new(
        input: SendableRecordBatchStream,
        mode: u32,
        metadata: [usize; 3],
        identifiers: &[usize],
    ) -> Result<Self> {
        Ok(Self {
            source: Source::new(input, mode == 2, metadata, identifiers)?,
            mode,
            lookahead: None,
            repeated: None,
            update_next: None,
        })
    }
    async fn remove_carryovers(&mut self) -> Result<Option<ChangeRow>> {
        if let Some((row, count)) = &mut self.repeated {
            let row = row.clone();
            *count -= 1;
            if *count == 0 {
                self.repeated = None;
            }
            return Ok(Some(row));
        }
        loop {
            let current = match self.lookahead.take() {
                Some(row) => row,
                None => match self.source.next().await? {
                    Some(row) => row,
                    None => return Ok(None),
                },
            };
            if self.mode != 2 && current.kind != Kind::Delete {
                return Ok(Some(current));
            }
            let mut count = 1usize;
            while let Some(next) = self.source.next().await? {
                if !current.same_record(&next) {
                    self.lookahead = Some(next);
                    break;
                }
                if current.kind == next.kind {
                    count += 1;
                } else {
                    count -= 1;
                }
                if count == 0 {
                    break;
                }
            }
            if count > 0 {
                if count > 1 {
                    self.repeated = Some((current.clone(), count - 1));
                }
                return Ok(Some(current));
            }
        }
    }
    async fn next(&mut self) -> Result<Option<ChangeRow>> {
        if self.mode != 1 {
            return self.remove_carryovers().await;
        }
        let mut current = match self.update_next.take() {
            Some(row) => row,
            None => match self.remove_carryovers().await? {
                Some(row) => row,
                None => return Ok(None),
            },
        };
        if current.kind == Kind::Delete {
            if let Some(mut next) = self.remove_carryovers().await? {
                if current.same_identifier(&next) {
                    if next.kind != Kind::Insert {
                        return Err(DataFusionError::Execution("Cannot compute updates because there are multiple rows with the same identifier fields. Please make sure the rows are unique.".into()));
                    }
                    current.kind = Kind::Before;
                    next.kind = Kind::After;
                }
                self.update_next = Some(next);
            }
        }
        Ok(Some(current))
    }
}

fn collect_rows(
    rows: &[ChangeRow],
    schema: SchemaRef,
    output_indices: &[usize],
    change_index: usize,
) -> Result<RecordBatch> {
    let mut batches = Vec::new();
    let mut batch_indices = HashMap::new();
    let indices = rows
        .iter()
        .map(|row| {
            let next_index = batches.len();
            let batch_index = *batch_indices
                .entry(Arc::as_ptr(&row.batch) as usize)
                .or_insert_with(|| {
                    batches.push(Arc::clone(&row.batch));
                    next_index
                });
            (batch_index, row.index)
        })
        .collect::<Vec<_>>();
    let columns = output_indices
        .iter()
        .enumerate()
        .map(|(output, &input)| -> Result<ArrayRef> {
            if input == change_index {
                let values = StringArray::from_iter_values(rows.iter().map(|row| row.kind.name()));
                Ok(arrow::compute::cast(
                    &values,
                    schema.field(output).data_type(),
                )?)
            } else {
                let arrays = batches
                    .iter()
                    .map(|b| b.batch.column(input).as_ref())
                    .collect::<Vec<_>>();
                Ok(arrow::compute::interleave(&arrays, &indices)?)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{Field, Schema};

    async fn run(mode: u32, records: &[(&str, &str, i32)]) -> Result<Vec<(String, String, i32)>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("_change_type", DataType::Utf8, false),
            Field::new("_change_ordinal", DataType::Int32, false),
            Field::new("_commit_snapshot_id", DataType::Int64, false),
        ]));
        // Every row is a separate Arrow batch; pending deletes/updates must survive each boundary.
        let batches = records
            .iter()
            .map(|(value, kind, ordinal)| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(vec![1])),
                        Arc::new(StringArray::from(vec![*value])),
                        Arc::new(StringArray::from(vec![*kind])),
                        Arc::new(Int32Array::from(vec![*ordinal])),
                        Arc::new(Int64Array::from(vec![100 + i64::from(*ordinal)])),
                    ],
                )
                .map_err(DataFusionError::from)
            })
            .collect::<Vec<_>>();
        let input = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(batches),
        ));
        let ids = if mode == 1 { vec![0, 3] } else { vec![] };
        let mut processor = Processor::new(input, mode, [2, 3, 4], &ids)?;
        let mut rows = Vec::new();
        while let Some(row) = processor.next().await? {
            rows.push(row);
        }
        if rows.is_empty() {
            return Ok(vec![]);
        }
        let batch = collect_rows(&rows, schema, &[0, 1, 2, 3, 4], 2)?;
        let value = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let kind = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ordinal = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        Ok((0..batch.num_rows())
            .map(|i| {
                (
                    value.value(i).to_owned(),
                    kind.value(i).to_owned(),
                    ordinal.value(i),
                )
            })
            .collect())
    }

    #[test]
    fn comparison_matches_map_equality_and_boxed_float_equality() {
        use arrow::array::{Int32Builder, MapBuilder, StringBuilder};
        let mut maps = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        for entries in [[("a", 1), ("b", 2)], [("b", 2), ("a", 1)]] {
            for (key, value) in entries {
                maps.keys().append_value(key);
                maps.values().append_value(value);
            }
            maps.append(true).unwrap();
        }
        let maps: ArrayRef = Arc::new(maps.finish());
        let converter = RowConverter::new(vec![SortField::new(maps.data_type().clone())]).unwrap();
        let keys = converter
            .convert_columns(&[comparison_values(&maps).unwrap()])
            .unwrap();
        assert_eq!(keys.row(0), keys.row(1));
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![
            f64::NAN,
            f64::from_bits(0x7ff0000000000001),
            0.0,
            -0.0,
        ]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Float64)]).unwrap();
        let keys = converter
            .convert_columns(&[comparison_values(&floats).unwrap()])
            .unwrap();
        assert_eq!(keys.row(0), keys.row(1));
        assert_ne!(keys.row(2), keys.row(3));
    }

    #[tokio::test]
    async fn carryovers_and_updates_preserve_multiplicity_across_batches() {
        let rows = [
            ("a", "DELETE", 1),
            ("a", "DELETE", 1),
            ("a", "INSERT", 1),
            ("b", "INSERT", 1),
        ];
        assert_eq!(
            run(0, &rows).await.unwrap(),
            vec![
                ("a".into(), "DELETE".into(), 1),
                ("b".into(), "INSERT".into(), 1)
            ]
        );
        assert_eq!(
            run(1, &rows).await.unwrap(),
            vec![
                ("a".into(), "UPDATE_BEFORE".into(), 1),
                ("b".into(), "UPDATE_AFTER".into(), 1)
            ]
        );
        let error = run(1, &[("a", "DELETE", 1), ("b", "DELETE", 1)])
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("multiple rows with the same identifier"));
    }

    #[tokio::test]
    async fn net_changes_retain_the_surviving_run_snapshot() {
        assert!(run(2, &[("a", "INSERT", 0), ("a", "DELETE", 1)])
            .await
            .unwrap()
            .is_empty());
        assert!(run(0, &[("a", "DELETE", 1), ("a", "INSERT", 1)])
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            run(
                2,
                &[("a", "INSERT", 0), ("a", "DELETE", 1), ("a", "INSERT", 2)]
            )
            .await
            .unwrap(),
            vec![("a".into(), "INSERT".into(), 2)]
        );
        assert_eq!(
            run(
                2,
                &[("a", "INSERT", 0), ("a", "INSERT", 1), ("a", "DELETE", 2)]
            )
            .await
            .unwrap(),
            vec![("a".into(), "INSERT".into(), 0)]
        );
    }
}
