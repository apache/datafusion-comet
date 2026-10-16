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
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;

use super::routing::{take_rows, Router, NULL_KEY, NUM_BUCKETS};
use super::spill::{SpillFileWriter, SpillWriterFactory, SpilledFile};

/// Routes the probe input between the resident join and the probe files of spilled buckets.
pub(super) struct ProbeRouter {
    input: Option<SendableRecordBatchStream>,
    router: Router,
    resident: [bool; NUM_BUCKETS],
    writers: Vec<Option<SpillFileWriter>>,
    files: SpillWriterFactory,
}

impl ProbeRouter {
    pub(super) fn new(
        input: SendableRecordBatchStream,
        keys: Vec<PhysicalExprRef>,
        resident: [bool; NUM_BUCKETS],
        files: SpillWriterFactory,
    ) -> Self {
        Self {
            input: Some(input),
            router: Router::new(keys),
            resident,
            writers: (0..NUM_BUCKETS).map(|_| None).collect(),
            files,
        }
    }

    /// Writes the rows of spilled buckets to their files, and returns the other rows, or None if
    /// there are none or `keep_others` is false. A row with a null key goes with the others: it
    /// matches nothing, so the resident join emits it as every join type requires.
    pub(super) fn route(
        &mut self,
        batch: &RecordBatch,
        keep_others: bool,
    ) -> Result<Option<RecordBatch>> {
        let buckets = self.router.route(batch, 0, true)?;
        let mut others = Vec::new();
        let mut spilled = vec![Vec::new(); NUM_BUCKETS];
        for (row, bucket) in buckets.iter().enumerate() {
            if *bucket == NULL_KEY || self.resident[*bucket as usize] {
                others.push(row as u32);
            } else {
                spilled[*bucket as usize].push(row as u32);
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
            self.writers[bucket].insert(writer).append(&part)?;
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

    /// The probe file of each bucket, if it has one.
    pub(super) fn finish(&mut self) -> Result<Vec<Option<SpilledFile>>> {
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
