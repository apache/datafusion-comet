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

//! The fallback of [`super::SpillingHashJoinExec`] for a spilled bucket that is too big to join at
//! once and cannot be split, because most of its build rows share one key, or have a null key.
//!
//! The bucket's build side is joined in chunks that fit in memory, each with a full replay of the
//! bucket's probe side. A build row is in exactly one chunk and meets every probe row there, so a
//! join type that emits build rows needs nothing more. A probe row meets every chunk, so for a join
//! type that emits or marks probe rows, each chunk runs a join that records which probe rows
//! matched, and the probe rows are emitted from that record. With the build side on the left:
//!
//! | Join type                                     | Each chunk runs | After the last chunk         |
//! | --------------------------------------------- | --------------- | ---------------------------- |
//! | `Inner`, `Left`, `LeftSemi`, `LeftAnti`, `LeftMark` | the join  | nothing                      |
//! | `Right`     | `Inner`, recording matches                        | unmatched rows, null padded  |
//! | `Full`      | `Left`, recording matches                         | unmatched rows, null padded  |
//! | `RightSemi` | `RightSemi`, emitting rows not emitted before     | nothing                      |
//! | `RightAnti` | `RightSemi`, recording matches                    | unmatched rows               |
//! | `RightMark` | `RightSemi`, recording matches                    | every row with its mark      |
//!
//! A probe row is identified by its position in the replay, which is the same for every chunk,
//! through a column of row numbers appended to the replayed batches and dropped from the output.

use std::sync::Arc;

use arrow::array::{
    new_null_array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, RecordBatch,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{SchemaRef, UInt64Type};
use datafusion::common::{JoinType, Result};
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;

use super::source::{batches_stream, with_row_number_field, with_row_numbers, StreamSourceExec};
use super::spill::SpilledFile;

/// How a join type whose output depends on whether a probe row matched is run in chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tracking {
    Right,
    Full,
    RightSemi,
    RightAnti,
    RightMark,
}

impl Tracking {
    fn for_join(join_type: JoinType) -> Option<Self> {
        match join_type {
            JoinType::Right => Some(Self::Right),
            JoinType::Full => Some(Self::Full),
            JoinType::RightSemi => Some(Self::RightSemi),
            JoinType::RightAnti => Some(Self::RightAnti),
            JoinType::RightMark => Some(Self::RightMark),
            JoinType::Inner
            | JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::LeftMark => None,
        }
    }

    /// The join type each chunk runs.
    fn chunk_join_type(self) -> JoinType {
        match self {
            Self::Right => JoinType::Inner,
            Self::Full => JoinType::Left,
            Self::RightSemi | Self::RightAnti | Self::RightMark => JoinType::RightSemi,
        }
    }
}

/// A spilled bucket being joined in chunks.
pub(super) struct ChunkedPair {
    build: SendableRecordBatchStream,
    /// A build batch that was read but did not fit in the previous chunk.
    pending: Option<RecordBatch>,
    probe: Option<SpilledFile>,
    tracking: Option<(Tracking, BooleanBufferBuilder)>,
    build_schema: SchemaRef,
    probe_schema: SchemaRef,
    output_schema: SchemaRef,
    build_read_ahead: usize,
}

impl ChunkedPair {
    pub(super) fn try_new(
        build: &SpilledFile,
        probe: Option<SpilledFile>,
        join_type: JoinType,
        template: &HashJoinExec,
        build_reader: &SpillManager,
    ) -> Result<Self> {
        let tracking = Tracking::for_join(join_type).map(|tracking| {
            let rows = probe.as_ref().map_or(0, |file| file.rows);
            let mut matched = BooleanBufferBuilder::new(rows);
            matched.append_n(rows, false);
            (tracking, matched)
        });
        Ok(Self {
            build: build.read(build_reader)?,
            pending: None,
            probe,
            tracking,
            build_schema: template.left().schema(),
            probe_schema: template.right().schema(),
            output_schema: template.schema(),
            build_read_ahead: build.read_ahead_bytes(),
        })
    }

    /// The bytes of the record of matched probe rows.
    pub(super) fn bitmap_bytes(&self) -> usize {
        self.tracking
            .as_ref()
            .map_or(0, |(_, matched)| matched.len().div_ceil(8))
    }

    /// What reading the build side and replaying the probe side hold besides the chunk.
    pub(super) fn read_ahead_bytes(&self) -> usize {
        self.build_read_ahead + self.probe.as_ref().map_or(0, SpilledFile::read_ahead_bytes)
    }

    /// The next build batch, or None when every build batch has been placed in a chunk.
    pub(super) async fn next_build_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self.pending.take() {
            Some(batch) => Ok(Some(batch)),
            None => self.build.next().await.transpose(),
        }
    }

    /// Keeps a batch that did not fit for the next chunk.
    pub(super) fn put_back(&mut self, batch: RecordBatch) {
        self.pending = Some(batch);
    }

    /// The join of one chunk with a replay of the probe side.
    pub(super) fn chunk_join(
        &self,
        template: &HashJoinExec,
        chunk: Vec<RecordBatch>,
        probe_reader: &SpillManager,
    ) -> Result<HashJoinExec> {
        let build = batches_stream(Arc::clone(&self.build_schema), chunk);
        let (probe_schema, builder) = match &self.tracking {
            Some((tracking, _)) => (
                with_row_number_field(&self.probe_schema),
                template
                    .builder()
                    .reset_state()
                    .with_type(tracking.chunk_join_type()),
            ),
            None => (
                Arc::clone(&self.probe_schema),
                template.builder().reset_state(),
            ),
        };
        let probe = match &self.probe {
            Some(file) => file.read(probe_reader)?,
            None => batches_stream(Arc::clone(&self.probe_schema), vec![]),
        };
        let probe = if self.tracking.is_some() {
            with_row_numbers(probe, probe_schema)
        } else {
            probe
        };
        builder
            .with_new_children(vec![
                StreamSourceExec::new_arc(build),
                StreamSourceExec::new_arc(probe),
            ])?
            .build()
    }

    /// Turns a batch of a chunk's join into output. Returns None when it contributes nothing yet.
    pub(super) fn on_output(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        let Some((tracking, matched)) = &mut self.tracking else {
            return Ok(Some(batch));
        };
        let last = batch.num_columns() - 1;
        let numbers = batch.column(last).as_primitive::<UInt64Type>();
        match tracking {
            Tracking::Right | Tracking::Full => {
                for number in numbers.iter().flatten() {
                    matched.set_bit(number as usize, true);
                }
                Ok(Some(without_last_column(&batch, &self.output_schema)?))
            }
            Tracking::RightSemi => {
                let first_match: BooleanArray = numbers
                    .iter()
                    .map(|number| {
                        number.map(|number| {
                            let first = !matched.get_bit(number as usize);
                            matched.set_bit(number as usize, true);
                            first
                        })
                    })
                    .collect();
                let batch = filter_record_batch(&batch, &first_match)?;
                if batch.num_rows() == 0 {
                    return Ok(None);
                }
                Ok(Some(without_last_column(&batch, &self.output_schema)?))
            }
            Tracking::RightAnti | Tracking::RightMark => {
                for number in numbers.iter().flatten() {
                    matched.set_bit(number as usize, true);
                }
                Ok(None)
            }
        }
    }

    /// The stream that emits probe rows once every chunk has run, or None if the join type needs
    /// none.
    pub(super) fn final_pass(
        self,
        probe_reader: &SpillManager,
    ) -> Result<Option<SendableRecordBatchStream>> {
        let Some((tracking, mut matched)) = self.tracking else {
            return Ok(None);
        };
        let Some(probe) = &self.probe else {
            return Ok(None);
        };
        if tracking == Tracking::RightSemi {
            return Ok(None);
        }
        let matched = matched.finish();
        let build_schema = self.build_schema;
        let schema = Arc::clone(&self.output_schema);
        let mut offset = 0;
        let stream = probe.read(probe_reader)?.filter_map(move |batch| {
            let output = batch.and_then(|batch| {
                let rows = batch.num_rows();
                let matched = matched.slice(offset, rows);
                offset += rows;
                let columns: Vec<ArrayRef> = match tracking {
                    Tracking::Right | Tracking::Full => {
                        let unmatched =
                            filter_record_batch(&batch, &BooleanArray::new(!&matched, None))?;
                        let rows = unmatched.num_rows();
                        build_schema
                            .fields()
                            .iter()
                            .map(|field| new_null_array(field.data_type(), rows))
                            .chain(unmatched.columns().iter().cloned())
                            .collect()
                    }
                    Tracking::RightAnti => {
                        filter_record_batch(&batch, &BooleanArray::new(!&matched, None))?
                            .columns()
                            .to_vec()
                    }
                    Tracking::RightMark => {
                        let mut columns = batch.columns().to_vec();
                        columns.push(Arc::new(BooleanArray::new(matched, None)));
                        columns
                    }
                    Tracking::RightSemi => unreachable!("RightSemi has no final pass"),
                };
                let output = RecordBatch::try_new(Arc::clone(&schema), columns)?;
                Ok((output.num_rows() > 0).then_some(output))
            });
            futures::future::ready(output.transpose())
        });
        Ok(Some(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema,
            stream,
        ))))
    }
}

/// `batch` without its row numbers, with the schema of the join's output.
fn without_last_column(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = batch.columns()[..batch.num_columns() - 1].to_vec();
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}
