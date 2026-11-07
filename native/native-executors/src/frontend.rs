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

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::{job_status, PartitionLocation, SuccessfulJob};
use ballista_core::serde::scheduler::{Action as BallistaAction, ShuffleFileKind};
use ballista_core::JobId;
use ballista_scheduler::scheduler_server::SchedulerServer;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_comet_proto::spark_native_executors::QueryRequest;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use futures::stream::BoxStream;
use log::{info, warn};
use prost::Message;
use prost_types::Timestamp;
use tonic::{Request, Response, Status, Streaming};

use crate::assembly::{build_plan, check_version};

/// The Ballista session every query runs in. Every query uses Ballista's default settings, and
/// executors cache a runtime per session, so sharing one lets queries share those runtimes.
const SESSION_ID: &str = "comet";

/// How long a query may run.
#[derive(Clone, Debug, Default)]
pub struct Limits {
    /// The longest a query may take, from submission to its end. The scheduler fails and cancels a
    /// query that runs longer. `None` waits for as long as it takes.
    pub query_timeout: Option<Duration>,
}

/// The Flight service of a Comet scheduler, which runs queries on the Ballista scheduler it
/// wraps.
pub struct CometFlightService {
    scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    limits: Limits,
}

impl CometFlightService {
    pub fn new(scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>) -> Self {
        Self {
            scheduler,
            limits: Limits::default(),
        }
    }

    /// The service with `limits` instead of the defaults.
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Runs `plan` to completion and returns its result: where its output partitions are, in
    /// partition order, and when it ended.
    async fn run(&self, plan: Arc<dyn ExecutionPlan>) -> Result<SuccessfulJob, Status> {
        let ctx = self
            .scheduler
            .state
            .session_manager
            .create_or_update_session(SESSION_ID, &SessionConfig::new_with_ballista())
            .await
            .map_err(|e| Status::internal(format!("Cannot create a session: {e}")))?;
        let (sender, mut receiver) = tokio::sync::mpsc::channel(16);
        let job_id = self
            .scheduler
            .submit_physical_plan("comet", ctx, plan, Some(sender))
            .await
            .map_err(|e| Status::internal(format!("Cannot submit the query: {e}")))?;
        info!("Submitted query as job {job_id}");
        let mut guard = JobGuard {
            scheduler: self.scheduler.clone(),
            job_id: Some(job_id.clone()),
        };
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
                        return Err(Status::internal(format!(
                            "Job {job_id} failed: {}",
                            job.error
                        )));
                    }
                    _ => {}
                }
            }
            guard.finished();
            Err(Status::internal(format!(
                "Job {job_id} ended without a final status"
            )))
        };
        within(self.limits.query_timeout, &job_id, wait).await
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
/// disconnects and the request's future is dropped, or a wait times out.
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
        let plan = tokio::task::spawn_blocking(move || build_plan(query))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let job = self.run(plan).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_wait_past_the_timeout_fails_as_deadline_exceeded() {
        let job_id = JobId::new("job");
        let status = within(
            Some(Duration::from_millis(10)),
            &job_id,
            std::future::pending::<Result<(), Status>>(),
        )
        .await
        .unwrap_err();
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
        assert!(
            status
                .message()
                .contains("COMET_SCHEDULER_QUERY_TIMEOUT_SECONDS"),
            "{status}"
        );
    }

    #[tokio::test]
    async fn a_wait_within_the_timeout_or_without_one_returns_its_result() {
        let job_id = JobId::new("job");
        for timeout in [None, Some(Duration::from_secs(60))] {
            let result = within(timeout, &job_id, async { Ok::<_, Status>(7) }).await;
            assert_eq!(result.unwrap(), 7);
        }
    }
}
