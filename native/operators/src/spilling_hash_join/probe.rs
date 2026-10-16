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

//! The probe input of the resident join of [`super::SpillingHashJoinExec`], which writes the
//! probe rows of spilled buckets to disk on the way.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::metrics::Count;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;

use super::bloom::{FilterUse, KeyFilter};
use super::routing::{take_rows, Router, NULL_KEY, NUM_BUCKETS};
use super::spill::{SpillFileWriter, SpillWriterFactory, SpilledFile};

/// Routes the probe input between the resident join and the probe files of spilled buckets.
pub(super) struct ProbeRouter {
    input: Option<SendableRecordBatchStream>,
    router: Router,
    resident: [bool; NUM_BUCKETS],
    /// The filter of each spilled bucket's build keys, if it has one.
    filters: Vec<Option<KeyFilter>>,
    uses: Vec<FilterUse>,
    writers: Vec<Option<SpillFileWriter>>,
    files: SpillWriterFactory,
    filtered_rows: Count,
}

impl ProbeRouter {
    pub(super) fn new(
        input: SendableRecordBatchStream,
        keys: Vec<PhysicalExprRef>,
        resident: [bool; NUM_BUCKETS],
        filters: Vec<Option<KeyFilter>>,
        files: SpillWriterFactory,
        filtered_rows: Count,
    ) -> Self {
        Self {
            input: Some(input),
            router: Router::new(keys),
            resident,
            filters,
            uses: (0..NUM_BUCKETS).map(|_| FilterUse::default()).collect(),
            writers: (0..NUM_BUCKETS).map(|_| None).collect(),
            files,
            filtered_rows,
        }
    }

    /// Writes the rows of spilled buckets to their files, and returns the other rows, or None if
    /// there are none or `keep_others` is false. A row that matches nothing goes with the others,
    /// because the resident join emits it as every join type requires: a row with a null key, and
    /// a row whose bucket's filter shows that no build row has its key.
    pub(super) fn route(
        &mut self,
        batch: &RecordBatch,
        keep_others: bool,
    ) -> Result<Option<RecordBatch>> {
        let buckets = self.router.route(batch, 0, true)?;
        let hashes = self.router.hashes();
        let mut others = Vec::new();
        let mut spilled = vec![Vec::new(); NUM_BUCKETS];
        let mut looked_up = [0; NUM_BUCKETS];
        let mut rejected = [0; NUM_BUCKETS];
        for (row, bucket) in buckets.iter().enumerate() {
            let index = *bucket as usize;
            if *bucket == NULL_KEY || self.resident[index] {
                others.push(row as u32);
                continue;
            }
            if let Some(filter) = &self.filters[index] {
                looked_up[index] += 1;
                if !filter.may_contain(hashes[row]) {
                    rejected[index] += 1;
                    others.push(row as u32);
                    continue;
                }
            }
            spilled[index].push(row as u32);
        }
        for bucket in 0..NUM_BUCKETS {
            if looked_up[bucket] > 0 {
                self.filtered_rows.add(rejected[bucket]);
                if !self.uses[bucket].record(looked_up[bucket], rejected[bucket]) {
                    self.filters[bucket] = None;
                }
            }
        }
        for (bucket, rows) in spilled.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            let part = take_rows(batch, rows)?;
            let writer = match self.writers[bucket].take() {
                Some(writer) => writer,
                None => self.files.create()?,
            };
            self.writers[bucket].insert(writer).append(part)?;
        }
        if !keep_others || others.is_empty() {
            return Ok(None);
        }
        Ok(Some(take_rows(batch, others)?))
    }

    /// The probe input that the resident join did not read.
    pub(super) fn take_input(&mut self) -> Option<SendableRecordBatchStream> {
        self.input.take()
    }

    /// The probe file of each bucket, if it has one. The filters are no longer needed.
    pub(super) fn finish(&mut self) -> Result<Vec<Option<SpilledFile>>> {
        self.filters.clear();
        self.writers
            .iter_mut()
            .map(|writer| writer.take().map(SpillFileWriter::finish).transpose())
            .collect()
    }
}

/// The probe input of the resident join: the rows of resident buckets and the rows with a null
/// key. It writes the rest to disk while the resident join reads it.
pub(super) struct RoutedProbeStream {
    router: Arc<Mutex<ProbeRouter>>,
    schema: SchemaRef,
}

impl RoutedProbeStream {
    pub(super) fn new_stream(
        router: Arc<Mutex<ProbeRouter>>,
        schema: SchemaRef,
    ) -> SendableRecordBatchStream {
        Box::pin(Self { router, schema })
    }
}

impl Stream for RoutedProbeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut router = self.router.lock();
        loop {
            let Some(input) = router.input.as_mut() else {
                return Poll::Ready(None);
            };
            match ready!(input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => match router.route(&batch, true) {
                    Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                    Ok(None) => {}
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    router.input = None;
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for RoutedProbeStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
