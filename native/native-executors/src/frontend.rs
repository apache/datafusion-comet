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

//! The Arrow Flight service that Spark drivers submit queries to.
//!
//! `GetFlightInfo` takes a [`QueryRequest`] as its command, runs the query, and returns one
//! endpoint per result partition. Each endpoint carries an opaque ticket for its `DoGet`, so the
//! driver never speaks Ballista's protocol, and says when the partition goes: Ballista deletes a
//! query's results a set time after the query ends, whether or not they have been read (see the
//! comet-scheduler binary).
//!
//! In Ballista's staged mode each endpoint names the executor that holds the partition. In its
//! pipelined mode the executors stream a result once, so the scheduler reads the result into its
//! own memory, within the limits of [`Limits`], and its endpoints have no location: the driver
//! reads them from this service.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use ballista_core::client::BallistaClient;
use ballista_core::config::{BallistaConfig, ExecutionMode};
use ballista_core::error::BallistaError;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::{
    job_status, JobStatus, PartitionLocation, RunningJob, SuccessfulJob,
};
use ballista_core::serde::scheduler::{Action as BallistaAction, ShuffleFileKind};
use ballista_core::JobId;
use ballista_scheduler::scheduler_server::SchedulerServer;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionConfig;
use datafusion_comet_proto::spark_native_executors::QueryRequest;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use futures::stream::BoxStream;
use futures::TryStreamExt;
use log::{info, warn};
use prost::Message;
use prost_types::Timestamp;
use tonic::{Request, Response, Status, Streaming};

use crate::assembly::{build_plan, check_version};

/// The Ballista session every query runs in. Every query uses Ballista's default settings, and
/// executors cache a runtime per session, so sharing one lets queries share those runtimes.
const SESSION_ID: &str = "comet";

/// How long a query may run, and how much of a pipelined query's result the scheduler buffers.
#[derive(Clone, Debug)]
pub struct Limits {
    /// The longest a query may take, from submission to its end. The scheduler fails and cancels a
    /// query that runs longer. `None` waits for as long as it takes.
    pub query_timeout: Option<Duration>,
    /// The most bytes of Arrow data the scheduler buffers for one pipelined query's result. It
    /// fails and cancels a query whose result is larger.
    pub max_result_bytes: usize,
    /// The most bytes of Arrow data the scheduler keeps for all pipelined results together. When
    /// a new result doesn't fit it drops the oldest results, even before they expire.
    pub max_stored_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            query_timeout: None,
            max_result_bytes: 1 << 30,
            max_stored_bytes: 4 << 30,
        }
    }
}

/// The Flight service of a Comet scheduler, which runs queries on the Ballista scheduler it
/// wraps.
pub struct CometFlightService {
    scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    /// The results of pipelined queries, by job id, until they expire.
    stored_results: Arc<Mutex<ResultStore>>,
    /// The session every query runs in, with the cluster's Ballista settings.
    session: SessionConfig,
    limits: Limits,
}

impl CometFlightService {
    /// A service that runs each query in a session with Ballista's defaults overridden by
    /// `ballista_settings`, such as `ballista.execution.mode=pipelined`.
    pub fn try_new(
        scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
        ballista_settings: &[(String, String)],
    ) -> Result<Self, String> {
        let session = ballista_settings.iter().fold(
            SessionConfig::new_with_ballista(),
            |session, (key, value)| session.set_str(key, value),
        );
        // Check the settings now rather than when the first query runs.
        session
            .ballista_config()
            .execution_mode()
            .map_err(|e| format!("Invalid Ballista settings: {e}"))?;
        Ok(Self {
            scheduler,
            stored_results: Arc::default(),
            session,
            limits: Limits::default(),
        })
    }

    /// The service with `limits` instead of the defaults.
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Whether queries run in Ballista's pipelined mode, which streams batches between stages
    /// instead of writing shuffle files.
    fn pipelined(&self) -> bool {
        matches!(
            self.session.ballista_config().execution_mode(),
            Ok(ExecutionMode::Pipelined)
        )
    }

    /// Keeps a pipelined query's result until it expires, as long as a staged job's. With no
    /// retention it is kept until the limit on the results kept pushes it out. Fails if the
    /// result alone is over that limit.
    fn store(&self, job_id: String, result: StoredResult) -> Result<(), Status> {
        self.stored_results.lock().unwrap().insert(
            job_id.clone(),
            Arc::new(result),
            self.limits.max_stored_bytes,
        )?;
        let retention = self
            .scheduler
            .state
            .config
            .finished_job_data_clean_up_interval_seconds;
        if retention > 0 {
            let stored = Arc::clone(&self.stored_results);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(retention)).await;
                stored.lock().unwrap().remove(&job_id);
            });
        }
        Ok(())
    }

    /// When the executors delete the results of a query that ended at `ended_at`, in milliseconds
    /// since the epoch. Ballista deletes them that long after the query ends, unless the scheduler
    /// keeps them. Ballista starts that wait only after it records the end, so they are still
    /// there until this time.
    fn results_expire_at(&self, ended_at: u64) -> Option<Timestamp> {
        let retention = self
            .scheduler
            .state
            .config
            .finished_job_data_clean_up_interval_seconds;
        if retention == 0 {
            return None;
        }
        UNIX_EPOCH
            .checked_add(Duration::from_millis(ended_at))?
            .checked_add(Duration::from_secs(retention))
            .map(Timestamp::from)
    }
}

/// Cancels its job when dropped before [`finished`](Self::finished), as when the driver
/// disconnects and the request's future is dropped, or a wait times out or a read fails.
struct JobGuard {
    scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    job_id: Option<JobId>,
}

impl JobGuard {
    /// The job has ended, so there is nothing to cancel.
    fn finished(&mut self) {
        self.job_id = None;
    }
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        let Some(job_id) = self.job_id.take() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let scheduler = self.scheduler.clone();
        runtime.spawn(async move {
            info!("Cancelling job {job_id}, which nothing is waiting for");
            if let Err(e) = scheduler.cancel_job(job_id.clone()).await {
                warn!("Cannot cancel job {job_id}: {e}");
            }
        });
    }
}

/// Waits for `wait`, the end of job `job_id`, for at most `timeout`.
async fn within<T>(
    timeout: Option<Duration>,
    job_id: &JobId,
    wait: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    let Some(timeout) = timeout else {
        return wait.await;
    };
    tokio::time::timeout(timeout, wait)
        .await
        .unwrap_or_else(|_| {
            Err(Status::deadline_exceeded(format!(
                "Job {job_id} did not finish within {} seconds \
             (COMET_SCHEDULER_QUERY_TIMEOUT_SECONDS) and was cancelled",
                timeout.as_secs()
            )))
        })
}

fn failed(job_id: &JobId, error: &str) -> Status {
    Status::internal(format!("Job {job_id} failed: {error}"))
}

/// Runs `plan` as a Ballista job on `scheduler` to completion and returns its result: where its
/// output partitions are, in partition order, and when it ended.
async fn run(
    scheduler: &SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    session: &SessionConfig,
    plan: Arc<dyn ExecutionPlan>,
    limits: &Limits,
) -> Result<SuccessfulJob, Status> {
    let (job_id, mut receiver, mut guard) = submit(scheduler, session, plan).await?;
    let wait = async {
        while let Some(status) = receiver.recv().await {
            match status.status {
                Some(job_status::Status::Successful(mut job)) => {
                    guard.finished();
                    job.partition_location.sort_by_key(|location| {
                        let partition = location.partition_id.as_ref().map(|p| p.partition_id);
                        (partition, location.map_partition_id)
                    });
                    return Ok(job);
                }
                Some(job_status::Status::Failed(job)) => {
                    guard.finished();
                    return Err(failed(&job_id, &job.error));
                }
                _ => {}
            }
        }
        guard.finished();
        Err(Status::internal(format!(
            "Job {job_id} ended without a final status"
        )))
    };
    within(limits.query_timeout, &job_id, wait).await
}

/// Submits `plan` as a Ballista job on `scheduler`, and returns its id, the channel its status
/// changes arrive on, and a guard that cancels it if the caller stops waiting.
async fn submit(
    scheduler: &SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    session: &SessionConfig,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<(JobId, tokio::sync::mpsc::Receiver<JobStatus>, JobGuard), Status> {
    let ctx = scheduler
        .state
        .session_manager
        .create_or_update_session(SESSION_ID, session)
        .await
        .map_err(|e| Status::internal(format!("Cannot create a session: {e}")))?;
    // Ballista drops a status it cannot fit in the channel, and a dropped final status would
    // leave the caller waiting for ever, so the channel is far larger than the statuses of a job.
    let (sender, receiver) = tokio::sync::mpsc::channel(1000);
    let job_id = scheduler
        .submit_physical_plan("comet", ctx, plan, Some(sender))
        .await
        .map_err(|e| Status::internal(format!("Cannot submit the query: {e}")))?;
    info!("Submitted query as job {job_id}");
    let guard = JobGuard {
        scheduler: scheduler.clone(),
        job_id: Some(job_id.clone()),
    };
    Ok((job_id, receiver, guard))
}

/// Runs `plan` as a pipelined Ballista job and reads its results into the scheduler.
///
/// A pipelined job's final stage streams each output partition to one reader, once, and can't
/// finish until it has been read. A Spark driver may read a result partition more than once,
/// since Spark can compute a partition of an RDD again, so the scheduler reads the streams itself
/// and serves them from its own Flight service for as long as a staged job's results are kept.
/// It buffers at most [`Limits::max_result_bytes`] of them, and fails the query beyond that.
///
/// The result is gathered into one partition first. A sorted query's result is already one
/// partition, the merge of its sorted partitions.
async fn run_pipelined(
    scheduler: &SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    session: &SessionConfig,
    plan: Arc<dyn ExecutionPlan>,
    limits: &Limits,
) -> Result<(String, StoredResult), Status> {
    let plan: Arc<dyn ExecutionPlan> = if plan.output_partitioning().partition_count() > 1 {
        Arc::new(CoalescePartitionsExec::new(plan))
    } else {
        plan
    };
    let schema = plan.schema();
    let config = session.ballista_config();
    let (job_id, mut receiver, mut guard) = submit(scheduler, session, plan).await?;
    let wait = async {
        let (locations, mut succeeded) = loop {
            let Some(status) = receiver.recv().await else {
                guard.finished();
                return Err(Status::internal(format!(
                    "Job {job_id} ended without reporting where its results are"
                )));
            };
            match status.status {
                Some(job_status::Status::Running(RunningJob {
                    pipelined_output_locations,
                    ..
                })) if !pipelined_output_locations.is_empty() => {
                    break (pipelined_output_locations, false)
                }
                Some(job_status::Status::Successful(SuccessfulJob {
                    pipelined_output_locations,
                    ..
                })) if !pipelined_output_locations.is_empty() => {
                    break (pipelined_output_locations, true)
                }
                Some(job_status::Status::Failed(job)) => {
                    guard.finished();
                    return Err(failed(&job_id, &job.error));
                }
                _ => {}
            }
        };
        // Every partition at once: the final stage can't finish until all of them are read.
        let buffered = AtomicUsize::new(0);
        let fetch = futures::future::try_join_all(
            locations
                .into_iter()
                .map(|location| fetch_pipelined(location, &config, &buffered, limits)),
        );
        tokio::pin!(fetch);
        // Keep reading the job's statuses meanwhile, so that its failure ends the wait, and a
        // failed read ends it at once, with the job cancelled.
        let mut watching = true;
        let partitions = loop {
            tokio::select! {
                partitions = &mut fetch => break partitions?,
                status = receiver.recv(), if watching => match status.and_then(|s| s.status) {
                    Some(job_status::Status::Successful(_)) => succeeded = true,
                    Some(job_status::Status::Failed(job)) => {
                        guard.finished();
                        return Err(failed(&job_id, &job.error));
                    }
                    Some(_) => {}
                    None => watching = false,
                },
            }
        };
        while watching && !succeeded {
            match receiver.recv().await.and_then(|status| status.status) {
                Some(job_status::Status::Successful(_)) => succeeded = true,
                Some(job_status::Status::Failed(job)) => {
                    guard.finished();
                    return Err(failed(&job_id, &job.error));
                }
                Some(_) => {}
                None => watching = false,
            }
        }
        guard.finished();
        Ok(StoredResult::new(schema, partitions))
    };
    let result = within(limits.query_timeout, &job_id, wait).await?;
    Ok((job_id.to_string(), result))
}

/// Reads one result partition of a pipelined job from the executor that streams it.
///
/// `buffered` counts the bytes read for the whole result, which may not pass the limit.
async fn fetch_pipelined(
    location: ballista_core::serde::protobuf::PipelinedPartitionLocation,
    config: &BallistaConfig,
    buffered: &AtomicUsize,
    limits: &Limits,
) -> Result<Vec<RecordBatch>, Status> {
    let location: ballista_core::serde::scheduler::PipelinedPartitionLocation = location
        .try_into()
        .map_err(|e: BallistaError| Status::internal(format!("Bad result location: {e}")))?;
    let executor = &location.executor_meta;
    let failed = |e: BallistaError| {
        Status::internal(format!(
            "Cannot read a result from {}:{}: {e}",
            executor.host, executor.port
        ))
    };
    let mut client = BallistaClient::try_new(
        &executor.host,
        executor.port,
        config.grpc_client_max_message_size(),
        false,
        None,
        config.io_retries_times() as u8,
        config.io_retry_wait_time_ms() as u64,
        0,
        0,
    )
    .await
    .map_err(failed)?;
    let mut stream = client
        .fetch_pipelined_partition(&location, config.pipelined_registration_timeout())
        .await
        .map_err(failed)?;
    let max_bytes = limits.max_result_bytes.min(limits.max_stored_bytes);
    let mut batches = vec![];
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|e| failed(BallistaError::DataFusionError(Box::new(e))))?
    {
        let size = batch.get_array_memory_size();
        if buffered
            .fetch_add(size, Ordering::Relaxed)
            .saturating_add(size)
            > max_bytes
        {
            return Err(Status::resource_exhausted(format!(
                "The query's result is larger than the {max_bytes} bytes the scheduler buffers \
                 for a pipelined query (COMET_SCHEDULER_PIPELINED_RESULT_MAX_BYTES and \
                 COMET_SCHEDULER_PIPELINED_STORE_MAX_BYTES). Run it in staged mode \
                 (ballista.execution.mode=staged) instead."
            )));
        }
        batches.push(batch);
    }
    Ok(batches)
}

/// The results of a pipelined query, which the scheduler serves itself.
struct StoredResult {
    schema: SchemaRef,
    partitions: Vec<Vec<RecordBatch>>,
    /// The Arrow memory the batches hold.
    bytes: usize,
}

impl StoredResult {
    fn new(schema: SchemaRef, partitions: Vec<Vec<RecordBatch>>) -> Self {
        let bytes = partitions
            .iter()
            .flatten()
            .map(RecordBatch::get_array_memory_size)
            .sum();
        Self {
            schema,
            partitions,
            bytes,
        }
    }
}

/// The stored results of pipelined queries, oldest first, within a limit on their total size.
#[derive(Default)]
struct ResultStore {
    results: HashMap<String, Arc<StoredResult>>,
    /// The job ids in `results`, oldest first.
    order: VecDeque<String>,
    bytes: usize,
}

impl ResultStore {
    /// Stores `result`, after dropping the oldest results as far as it takes to keep the total
    /// within `max_bytes`.
    fn insert(
        &mut self,
        job_id: String,
        result: Arc<StoredResult>,
        max_bytes: usize,
    ) -> Result<(), Status> {
        if result.bytes > max_bytes {
            return Err(Status::resource_exhausted(format!(
                "The query's result is larger than the {max_bytes} bytes the scheduler keeps for \
                 pipelined results (COMET_SCHEDULER_PIPELINED_STORE_MAX_BYTES)"
            )));
        }
        while self.bytes + result.bytes > max_bytes {
            let Some(oldest) = self.order.front().cloned() else {
                break;
            };
            warn!("Dropping the results of job {oldest} to make room for job {job_id}'s");
            self.remove(&oldest);
        }
        self.bytes += result.bytes;
        self.order.push_back(job_id.clone());
        self.results.insert(job_id, result);
        Ok(())
    }

    fn remove(&mut self, job_id: &str) {
        if let Some(result) = self.results.remove(job_id) {
            self.bytes -= result.bytes;
            self.order.retain(|id| id != job_id);
        }
    }

    fn get(&self, job_id: &str) -> Option<Arc<StoredResult>> {
        self.results.get(job_id).cloned()
    }
}

/// Prefix of a ticket for a result partition the scheduler serves itself. The rest is the job id,
/// a NUL, and the partition's index.
const STORED_RESULT_MAGIC: &str = "COMET_RESULT\0";

/// The Flight endpoint for a result partition: the executor's location, a ticket the executor's
/// Flight service reads as a Ballista partition fetch, and when the partition is deleted.
fn endpoint(
    location: &PartitionLocation,
    expires_at: Option<Timestamp>,
) -> Result<FlightEndpoint, Status> {
    let executor = location
        .executor_meta
        .as_ref()
        .ok_or_else(|| Status::internal("A result partition has no executor"))?;
    let partition = location
        .partition_id
        .as_ref()
        .ok_or_else(|| Status::internal("A result partition has no ID"))?;
    let action = BallistaAction::FetchPartition {
        job_id: partition.job_id.clone().into(),
        stage_id: partition.stage_id as usize,
        partition_id: partition.partition_id as usize,
        host: executor.host.clone(),
        port: executor.port as u16,
        file_id: location.file_id,
        layout: location.layout(),
        file_kind: ShuffleFileKind::Data,
        byte_ranges: vec![],
    };
    let action: ballista_core::serde::protobuf::Action = action
        .try_into()
        .map_err(|e| Status::internal(format!("Cannot encode a partition fetch: {e}")))?;
    let mut endpoint = FlightEndpoint::new()
        .with_ticket(Ticket::new(action.encode_to_vec()))
        .with_location(format!("grpc+tcp://{}:{}", executor.host, executor.port));
    endpoint.expiration_time = expires_at;
    Ok(endpoint)
}

fn unimplemented<T>(method: &str) -> Result<T, Status> {
    Err(Status::unimplemented(format!(
        "A Comet scheduler doesn't implement {method}"
    )))
}

#[tonic::async_trait]
impl FlightService for CometFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let query = QueryRequest::decode(descriptor.cmd.as_ref())
            .map_err(|e| Status::invalid_argument(format!("Cannot decode the query: {e}")))?;
        check_version(&query).map_err(|e| Status::failed_precondition(e.to_string()))?;
        let pipelined = self.pipelined();
        let plan = tokio::task::spawn_blocking(move || build_plan(query))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let mut info = FlightInfo::new();
        if pipelined {
            let (job_id, result) =
                run_pipelined(&self.scheduler, &self.session, plan, &self.limits).await?;
            let partitions = result.partitions.len();
            self.store(job_id.clone(), result)?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |now| now.as_millis() as u64);
            let expires_at = self.results_expire_at(now);
            // No location: the driver reads them from this service.
            for index in 0..partitions {
                let ticket = format!("{STORED_RESULT_MAGIC}{job_id}\0{index}");
                let mut endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket));
                endpoint.expiration_time = expires_at;
                info = info.with_endpoint(endpoint);
            }
        } else {
            let job = run(&self.scheduler, &self.session, plan, &self.limits).await?;
            let expires_at = self.results_expire_at(job.ended_at);
            for location in &job.partition_location {
                info = info.with_endpoint(endpoint(location, expires_at)?);
            }
        }
        Ok(Response::new(info))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        unimplemented("Handshake")
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        unimplemented("ListFlights")
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        unimplemented("PollFlightInfo")
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        unimplemented("GetSchema")
    }

    /// Serves a result partition of a pipelined query. A staged query's results are read from
    /// the executors instead.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner().ticket;
        let ticket = std::str::from_utf8(&ticket)
            .ok()
            .and_then(|ticket| ticket.strip_prefix(STORED_RESULT_MAGIC))
            .and_then(|ticket| ticket.split_once('\0'))
            .and_then(|(job_id, index)| Some((job_id.to_string(), index.parse::<usize>().ok()?)));
        let Some((job_id, index)) = ticket else {
            return unimplemented("DoGet for this ticket; fetch it from an executor");
        };
        let result = self
            .stored_results
            .lock()
            .unwrap()
            .get(&job_id)
            .ok_or_else(|| {
                Status::not_found(format!("The results of job {job_id} have expired"))
            })?;
        let batches = result.partitions.get(index).cloned().ok_or_else(|| {
            Status::not_found(format!("Job {job_id} has no result partition {index}"))
        })?;
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(Arc::clone(&result.schema))
            .build(futures::stream::iter(batches.into_iter().map(Ok)))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        unimplemented("DoPut")
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        unimplemented("DoExchange")
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        unimplemented("DoAction")
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        unimplemented("ListActions")
    }
}
