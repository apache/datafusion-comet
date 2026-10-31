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
//! endpoint per result partition. Each endpoint names the executor that holds the partition and
//! carries an opaque ticket for its `DoGet`, so the driver never speaks Ballista's protocol. It
//! also says when the partition goes: Ballista deletes a query's results a set time after the
//! query ends, whether or not they have been read (see the comet-scheduler binary).

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use ballista_core::client::BallistaClient;
use ballista_core::config::BallistaConfig;
use ballista_core::error::BallistaError;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::{job_status, PartitionLocation, SuccessfulJob};
use ballista_core::serde::scheduler::{Action as BallistaAction, ShuffleFileKind};
use ballista_scheduler::scheduler_server::SchedulerServer;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_comet_proto::spark_native_executors::QueryRequest;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use log::info;
use prost::Message;
use prost_types::Timestamp;
use tonic::{Request, Response, Status, Streaming};

use crate::assembly::{build_plan, check_version};

/// The Ballista session every query runs in. Every query uses Ballista's default settings, and
/// executors cache a runtime per session, so sharing one lets queries share those runtimes.
const SESSION_ID: &str = "comet";

/// The Flight service of a Comet scheduler, which runs queries on the Ballista scheduler it
/// wraps.
pub struct CometFlightService {
    scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
}

impl CometFlightService {
    pub fn new(scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>) -> Self {
        Self { scheduler }
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

/// Runs `plan` as a Ballista job on `scheduler` and reads the rows of all its partitions back from
/// the executors that hold them, a few partitions at a time.
async fn collect(
    scheduler: &SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<RecordBatch>, Status> {
    let job = run(scheduler, plan).await?;
    let config = BallistaConfig::default();
    let batches: Vec<Vec<RecordBatch>> = futures::stream::iter(job.partition_location)
        .map(|location| fetch(location, &config))
        .buffer_unordered(FETCH_CONCURRENCY)
        .try_collect()
        .await?;
    Ok(batches.concat())
}

/// How many sample partitions the scheduler reads at once.
const FETCH_CONCURRENCY: usize = 16;

/// Reads the rows of one partition from the executor that holds it, with Ballista's client.
async fn fetch(
    location: PartitionLocation,
    config: &BallistaConfig,
) -> Result<Vec<RecordBatch>, Status> {
    let layout = location.layout();
    let (Some(executor), Some(partition)) = (location.executor_meta, location.partition_id) else {
        return Err(Status::internal("A sample partition has no executor or ID"));
    };
    let failed = |e: BallistaError| {
        Status::internal(format!(
            "Cannot read a sample from {}:{}: {e}",
            executor.host, executor.port
        ))
    };
    let mut client = BallistaClient::try_new(
        &executor.host,
        executor.port as u16,
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
    let stream = client
        .fetch_partition(
            &executor.id,
            &partition.into(),
            location.file_id,
            layout,
            true,
        )
        .await
        .map_err(failed)?;
    datafusion::physical_plan::common::collect(stream)
        .await
        .map_err(|e| failed(BallistaError::DataFusionError(Box::new(e))))
}

/// Runs `plan` as a Ballista job on `scheduler` to completion and returns its result: where its
/// output partitions are, in partition order, and when it ended.
async fn run(
    scheduler: &SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<SuccessfulJob, Status> {
    let ctx = scheduler
        .state
        .session_manager
        .create_or_update_session(SESSION_ID, &SessionConfig::new_with_ballista())
        .await
        .map_err(|e| Status::internal(format!("Cannot create a session: {e}")))?;
    let (sender, mut receiver) = tokio::sync::mpsc::channel(16);
    let job_id = scheduler
        .submit_physical_plan("comet", ctx, plan, Some(sender))
        .await
        .map_err(|e| Status::internal(format!("Cannot submit the query: {e}")))?;
    info!("Submitted query as job {job_id}");
    while let Some(status) = receiver.recv().await {
        match status.status {
            Some(job_status::Status::Successful(mut job)) => {
                job.partition_location.sort_by_key(|location| {
                    let partition = location.partition_id.as_ref().map(|p| p.partition_id);
                    (partition, location.map_partition_id)
                });
                return Ok(job);
            }
            Some(job_status::Status::Failed(job)) => {
                return Err(Status::internal(format!(
                    "Job {job_id} failed: {}",
                    job.error
                )))
            }
            _ => {}
        }
    }
    Err(Status::internal(format!(
        "Job {job_id} ended without a final status"
    )))
}

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

/// The status for a query the scheduler couldn't plan: a sampling job's own status when that job
/// failed, and otherwise an invalid query.
fn planning_status(error: DataFusionError) -> Status {
    if let DataFusionError::External(cause) = error.find_root() {
        if let Some(status) = cause.downcast_ref::<Status>() {
            return status.clone();
        }
    }
    Status::invalid_argument(error.to_string())
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
        // A range exchange is sampled by a job of its own while the query is planned, which
        // blocks the planning thread on this runtime.
        let scheduler = self.scheduler.clone();
        let runtime = tokio::runtime::Handle::current();
        let plan = tokio::task::spawn_blocking(move || {
            let mut sampler = |plan| {
                runtime
                    .block_on(collect(&scheduler, plan))
                    .map_err(|e| DataFusionError::External(Box::new(e)))
            };
            build_plan(query, &mut sampler)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(planning_status)?;
        let job = run(&self.scheduler, plan).await?;
        let expires_at = self.results_expire_at(job.ended_at);
        let mut info = FlightInfo::new();
        for location in &job.partition_location {
            info = info.with_endpoint(endpoint(location, expires_at)?);
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

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        unimplemented("DoGet; fetch result partitions from the executors")
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
