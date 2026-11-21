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

//! Runs queries on a cluster of the comet-scheduler and comet-executor binaries, through the
//! scheduler's Arrow Flight service, as a Spark driver does.

mod common;

use std::future::Future;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, FlightDescriptor, FlightEndpoint, FlightInfo};
use common::*;
use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::Int64Type;
use datafusion_comet_proto::spark_native_executors::{QueryRequest, Stage};
use futures::TryStreamExt;
use prost::Message;
use tonic::transport::Endpoint;
use tonic::Code;

/// A comet-scheduler and a comet-executor, stopped when dropped.
struct Cluster {
    processes: Vec<Child>,
    work_dir: PathBuf,
    flight_url: String,
    /// The scheduler's log, with this crate's messages at info level.
    scheduler_log: PathBuf,
    /// The port of Ballista's gRPC service, for executors.
    scheduler_port: u16,
}

/// Held while a cluster starts, so that two tests don't pick the same free port in between.
static STARTING: Mutex<()> = Mutex::new(());

impl Cluster {
    fn start() -> Self {
        Self::start_with(&[])
    }

    /// Starts a cluster whose scheduler also gets the settings `scheduler_env`.
    fn start_with(scheduler_env: &[(&str, String)]) -> Self {
        let _starting = STARTING.lock().unwrap_or_else(|e| e.into_inner());
        let scheduler_port = free_port();
        let flight_port = free_port();
        let executor_port = free_port();
        let executor_grpc_port = free_port();
        let mut cluster = Cluster {
            processes: vec![],
            work_dir: std::env::temp_dir().join(format!(
                "comet-native-executors-{}-{flight_port}",
                std::process::id()
            )),
            flight_url: format!("http://127.0.0.1:{flight_port}"),
            scheduler_log: std::env::temp_dir().join(format!(
                "comet-scheduler-{}-{flight_port}.log",
                std::process::id()
            )),
            scheduler_port,
        };
        let mut env = vec![
            ("COMET_SCHEDULER_BIND_HOST", "127.0.0.1".to_string()),
            ("COMET_SCHEDULER_PORT", scheduler_port.to_string()),
            ("COMET_SCHEDULER_FLIGHT_PORT", flight_port.to_string()),
        ];
        env.extend_from_slice(scheduler_env);
        let log = std::fs::File::create(&cluster.scheduler_log).unwrap();
        cluster.spawn_logging(
            env!("CARGO_BIN_EXE_comet-scheduler"),
            &env,
            "warn,comet_native_executors=info",
            Stdio::from(log),
        );
        wait_for(scheduler_port);
        wait_for(flight_port);
        let work_dir = cluster.work_dir.to_string_lossy().into_owned();
        cluster.spawn(
            env!("CARGO_BIN_EXE_comet-executor"),
            &[
                ("COMET_EXECUTOR_BIND_HOST", "127.0.0.1".to_string()),
                ("COMET_EXECUTOR_PORT", executor_port.to_string()),
                ("COMET_EXECUTOR_GRPC_PORT", executor_grpc_port.to_string()),
                ("COMET_SCHEDULER_HOST", "127.0.0.1".to_string()),
                ("COMET_SCHEDULER_PORT", scheduler_port.to_string()),
                ("COMET_EXECUTOR_TASKS", "4".to_string()),
                ("COMET_EXECUTOR_WORK_DIR", work_dir),
            ],
        );
        wait_for(executor_port);
        cluster
    }

    fn spawn(&mut self, binary: &str, env: &[(&str, String)]) {
        self.spawn_logging(binary, env, "warn", Stdio::inherit());
    }

    fn spawn_logging(&mut self, binary: &str, env: &[(&str, String)], log: &str, stderr: Stdio) {
        let child = Command::new(binary)
            .envs(env.iter().map(|(key, value)| (*key, value.as_str())))
            .env("RUST_LOG", log)
            .stdout(Stdio::null())
            .stderr(stderr)
            .spawn()
            .unwrap_or_else(|e| panic!("Cannot start {binary}: {e}"));
        self.processes.push(child);
    }

    async fn get_flight_info(&self, request: &QueryRequest) -> Result<FlightInfo, FlightError> {
        let mut scheduler = connect(&self.flight_url).await;
        within_time_limit(
            scheduler.get_flight_info(FlightDescriptor::new_cmd(request.encode_to_vec())),
        )
        .await
    }

    /// Runs `request` and returns the batches of each result partition, in partition order.
    async fn run(&self, request: &QueryRequest) -> Vec<Vec<RecordBatch>> {
        let info = self.get_flight_info(request).await.unwrap();
        let mut partitions = vec![];
        for endpoint in &info.endpoint {
            partitions.push(self.read(endpoint).await.unwrap());
        }
        partitions
    }

    /// Reads a result partition from where its endpoint says, or from the scheduler if the
    /// endpoint names no location, as for the results of a pipelined query.
    async fn read(&self, endpoint: &FlightEndpoint) -> Result<Vec<RecordBatch>, FlightError> {
        match endpoint.location.first() {
            Some(location) => read_from(&location.uri, endpoint).await,
            None => read_from(&self.flight_url, endpoint).await,
        }
    }
}

/// Reads a result partition from the executor that holds it.
async fn read(endpoint: &FlightEndpoint) -> Result<Vec<RecordBatch>, FlightError> {
    read_from(&endpoint.location[0].uri, endpoint).await
}

async fn read_from(
    location: &str,
    endpoint: &FlightEndpoint,
) -> Result<Vec<RecordBatch>, FlightError> {
    let mut server = connect(location).await;
    let ticket = endpoint.ticket.clone().unwrap();
    within_time_limit(async { server.do_get(ticket).await?.try_collect().await }).await
}

impl Drop for Cluster {
    fn drop(&mut self) {
        // The executor first, so that it doesn't report to a scheduler that has gone.
        for process in self.processes.iter_mut().rev() {
            let _ = process.kill();
            let _ = process.wait();
        }
        let _ = std::fs::remove_dir_all(&self.work_dir);
        let _ = std::fs::remove_file(&self.scheduler_log);
    }
}

/// Runs `call`, failing the test if it takes too long, as when no executor ever runs the query.
async fn within_time_limit<T>(call: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(300), call)
        .await
        .expect("The cluster took too long")
}

/// Connects to a Flight location, such as an endpoint's `grpc+tcp://host:port`.
async fn connect(location: &str) -> FlightClient {
    let uri = match location.strip_prefix("grpc+tcp://") {
        Some(address) => format!("http://{address}"),
        None => location.to_string(),
    };
    let channel = Endpoint::from_shared(uri).unwrap().connect().await.unwrap();
    FlightClient::new(channel)
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// The HTTP status of a GET of `path` on a local port.
fn http_status(port: u16, path: &str) -> u16 {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut response = vec![];
    stream.read_to_end(&mut response).unwrap();
    let response = String::from_utf8_lossy(&response);
    response
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse().ok())
        .unwrap_or_else(|| panic!("Not an HTTP response: {response}"))
}

fn wait_for(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(
            Instant::now() < deadline,
            "Nothing is listening on port {port}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn runs_a_query_across_a_hash_shuffle() {
    let cluster = Cluster::start();
    let request = query(vec![
        range_stage(1000, 4),
        reading_stage(0, hash_on_first_column(3), 3),
    ]);
    let partitions = cluster.run(&request).await;
    assert_eq!(partitions.len(), 3);
    assert_eq!(
        sorted_values::<Int64Type>(&partitions.concat()),
        (0..1000).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn runs_a_query_across_a_range_exchange() {
    let cluster = Cluster::start();
    let request = query(vec![
        range_stage(1000, 4),
        reading_stage(0, range_partitioning(4, vec![]), 4),
    ]);
    let partitions = cluster.run(&request).await;
    assert_eq!(partitions.len(), 1);
    assert_eq!(
        sorted_values::<Int64Type>(&partitions.concat()),
        (0..1000).collect::<Vec<_>>()
    );
}

/// A global sort runs as a sort of each partition and a merge, so its result comes back as one
/// partition, in order.
#[tokio::test(flavor = "multi_thread")]
async fn sorts_across_a_range_exchange_by_merging_sorted_partitions() {
    let cluster = Cluster::start();
    let order = first_column_descending(long_type());
    let request = query(vec![
        range_stage(1000, 4),
        sorting_stage(0, range_partitioning(4, vec![]), order, long_type()),
    ]);
    let partitions = cluster.run(&request).await;
    assert_eq!(partitions.len(), 1);
    let values: Vec<i64> = partitions[0]
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(values, (0..1000).rev().collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn broadcasts_a_build_side_that_measures_small_into_the_probe_side_stage() {
    // Spark's estimates make the build side worth measuring before the probe side is shuffled:
    // under Ballista's maximum estimate multiple, and the probe side many times larger. It
    // measures far under the broadcast threshold, so the join runs in the probe side's stage.
    let cluster = Cluster::start();
    let mut probe = range_stage(10_000, 4);
    probe.estimated_output_bytes = Some(1 << 40);
    let mut build = range_stage(100, 2);
    build.estimated_output_bytes = Some(1 << 30);
    let request = query(vec![probe, build, joining_stage(0, 1, 3)]);

    let partitions = cluster.run(&request).await;

    let rows = partitions.concat();
    assert_eq!(
        sorted_values::<Int64Type>(&rows),
        (0..100).collect::<Vec<_>>()
    );
    for batch in &rows {
        assert_eq!(batch.column(0), batch.column(1));
    }
    let log = std::fs::read_to_string(&cluster.scheduler_log).unwrap();
    assert!(log.contains("Holding back a join's probe side"), "{log}");
    assert!(
        log.contains("broadcasting it to the probe side's stage"),
        "{log}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn shuffles_the_probe_side_when_the_build_side_measures_large() {
    // With a broadcast threshold of one byte, a held join's build side always measures too
    // large, so the probe side's stage is released as it was and the join is shuffled.
    let cluster = Cluster::start_with(&[(
        "COMET_SCHEDULER_BALLISTA_CONFIG",
        "ballista.optimizer.broadcast_join_threshold_bytes=1".to_string(),
    )]);
    let mut probe = range_stage(10_000, 4);
    probe.estimated_output_bytes = Some(1 << 40);
    let mut build = range_stage(100, 2);
    build.estimated_output_bytes = Some(16);
    let request = query(vec![probe, build, joining_stage(0, 1, 3)]);

    let partitions = cluster.run(&request).await;

    let rows = partitions.concat();
    assert_eq!(
        sorted_values::<Int64Type>(&rows),
        (0..100).collect::<Vec<_>>()
    );
    let log = std::fs::read_to_string(&cluster.scheduler_log).unwrap();
    assert!(log.contains("Holding back a join's probe side"), "{log}");
    assert!(log.contains("shuffling the probe side"), "{log}");
}

/// The stages of `broadcasts_a_build_side_that_measures_small_into_the_probe_side_stage`, with
/// `build` for the build side and `join` for the stage that joins. Spark's estimates make the
/// build side worth measuring before the probe side is shuffled.
fn held_join_query(build: Stage, join: Stage) -> QueryRequest {
    let mut probe = range_stage(10_000, 4);
    probe.estimated_output_bytes = Some(1 << 40);
    let mut build = build;
    build.estimated_output_bytes = Some(1 << 30);
    query(vec![probe, build, join])
}

/// `id % 10` of `range(0, 100)`: a build side whose join keys repeat ten times each.
fn duplicate_keys_build_side() -> Stage {
    let key = remainder(bound(0, long_type()), long_literal(10));
    projecting_stage(range_stage(100, 2), vec![key], vec![long_type()])
}

#[tokio::test(flavor = "multi_thread")]
async fn broadcast_join_keeps_every_match_of_duplicate_build_keys() {
    let cluster = Cluster::start();
    let request = held_join_query(duplicate_keys_build_side(), joining_stage(0, 1, 3));

    let rows = cluster.run(&request).await.concat();

    // Probe ids 0..10 each meet ten build rows.
    assert_eq!(
        rows.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        100
    );
    let keys = sorted_values::<Int64Type>(&rows);
    assert_eq!(keys, (0..10).flat_map(|key| [key; 10]).collect::<Vec<_>>());
    for batch in &rows {
        assert_eq!(batch.column(0), batch.column(1));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn broadcast_join_that_is_not_the_root_of_its_block() {
    // The join's consumer then holds the join's output in a leaf and runs a projection over it.
    let cluster = Cluster::start();
    let mut join = joining_stage(0, 1, 3);
    join = projecting_stage(
        join,
        vec![bound(0, long_type()), bound(1, long_type())],
        vec![long_type(), long_type()],
    );
    let request = held_join_query(range_stage(100, 2), join);

    let rows = cluster.run(&request).await.concat();

    assert_eq!(
        sorted_values::<Int64Type>(&rows),
        (0..100).collect::<Vec<_>>()
    );
    for batch in &rows {
        assert_eq!(batch.column(0), batch.column(1));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn join_with_a_build_side_that_measures_empty_returns_no_rows() {
    let cluster = Cluster::start();
    let request = held_join_query(range_stage(0, 2), joining_stage(0, 1, 3));

    let rows = cluster.run(&request).await.concat();

    assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 0);
}

/// Settings that run queries in Ballista's pipelined mode.
fn pipelined() -> (&'static str, String) {
    (
        "COMET_SCHEDULER_BALLISTA_CONFIG",
        "ballista.execution.mode=pipelined".to_string(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn pipelined_results_are_served_by_the_scheduler_as_often_as_they_are_read() {
    let cluster = Cluster::start_with(&[pipelined()]);
    let request = query(vec![
        range_stage(1000, 4),
        reading_stage(0, hash_on_first_column(3), 3),
    ]);
    let info = cluster.get_flight_info(&request).await.unwrap();
    assert!(!info.endpoint.is_empty());
    assert!(info
        .endpoint
        .iter()
        .all(|endpoint| endpoint.location.is_empty()));

    // Spark can compute an RDD partition again, so a partition can be read more than once.
    for _ in 0..2 {
        let mut batches = vec![];
        for endpoint in &info.endpoint {
            batches.extend(cluster.read(endpoint).await.unwrap());
        }
        assert_eq!(
            sorted_values::<Int64Type>(&batches),
            (0..1000).collect::<Vec<_>>()
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pipelined_result_over_the_buffer_limit_fails_the_query() {
    let cluster = Cluster::start_with(&[
        pipelined(),
        (
            "COMET_SCHEDULER_PIPELINED_RESULT_MAX_BYTES",
            (64 << 10).to_string(),
        ),
    ]);
    // 800,000 bytes of longs.
    match cluster
        .get_flight_info(&query(vec![range_stage(100_000, 4)]))
        .await
    {
        Err(FlightError::Tonic(status)) => {
            assert_eq!(status.code(), Code::ResourceExhausted, "{status}");
            assert!(
                status
                    .message()
                    .contains("COMET_SCHEDULER_PIPELINED_RESULT_MAX_BYTES"),
                "{status}"
            );
        }
        other => panic!("Expected the query to fail, got {other:?}"),
    }
    // The cluster still runs queries that fit: the failed query's job was cancelled.
    let partitions = cluster.run(&query(vec![range_stage(100, 2)])).await;
    assert_eq!(
        sorted_values::<Int64Type>(&partitions.concat()),
        (0..100).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pipelined_results_without_retention_are_dropped_oldest_first_at_the_limit() {
    // Retention 0 never expires a result, so only the limit on the stored results drops one. Each
    // result is about 16 KB, so two don't fit.
    let cluster = Cluster::start_with(&[
        pipelined(),
        ("COMET_SCHEDULER_RESULT_RETENTION_SECONDS", "0".to_string()),
        (
            "COMET_SCHEDULER_PIPELINED_STORE_MAX_BYTES",
            (24 << 10).to_string(),
        ),
    ]);
    let request = query(vec![range_stage(2000, 2)]);
    let first = cluster.get_flight_info(&request).await.unwrap();
    let second = cluster.get_flight_info(&request).await.unwrap();

    for endpoint in &second.endpoint {
        cluster.read(endpoint).await.unwrap();
    }
    match cluster.read(&first.endpoint[0]).await {
        Err(FlightError::Tonic(status)) => assert_eq!(status.code(), Code::NotFound, "{status}"),
        other => panic!("Expected the first result to be dropped, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn refuses_a_query_larger_than_the_request_limit() {
    let cluster = Cluster::start_with(&[(
        "COMET_SCHEDULER_FLIGHT_MAX_REQUEST_BYTES",
        "4096".to_string(),
    )]);
    cluster.run(&query(vec![range_stage(10, 1)])).await;
    let mut large = query(vec![range_stage(10, 1)]);
    large.conf.insert("padding".to_string(), "x".repeat(8192));
    match cluster.get_flight_info(&large).await {
        Err(FlightError::Tonic(status)) => assert!(
            matches!(status.code(), Code::OutOfRange | Code::ResourceExhausted),
            "{status}"
        ),
        other => panic!("Expected the query to be refused, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn refuses_a_query_from_another_build() {
    let cluster = Cluster::start();
    let mut request = query(vec![range_stage(10, 1)]);
    request.comet_version = "0.0.0".to_string();
    match cluster.get_flight_info(&request).await {
        Err(FlightError::Tonic(status)) => {
            assert_eq!(status.code(), Code::FailedPrecondition, "{status}");
            assert!(status.message().contains("same Comet build"), "{status}");
        }
        other => panic!("Expected the query to be refused, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn endpoints_say_when_their_results_are_deleted() {
    // A query's results stay readable for the scheduler's retention after the query ends, whether
    // or not a client reads them, and each endpoint says until when.
    let retention = Duration::from_secs(5);
    let cluster = Cluster::start_with(&[(
        "COMET_SCHEDULER_RESULT_RETENTION_SECONDS",
        retention.as_secs().to_string(),
    )]);
    let submitted = SystemTime::now();
    let info = cluster
        .get_flight_info(&query(vec![range_stage(100, 2)]))
        .await
        .unwrap();
    assert_eq!(info.endpoint.len(), 2);
    let expiration = info.endpoint[0].expiration_time;
    assert!(info
        .endpoint
        .iter()
        .all(|endpoint| endpoint.expiration_time == expiration));
    let expires_at = SystemTime::try_from(expiration.expect("an expiration time")).unwrap();
    // The query ended between its submission and its answer.
    assert!(expires_at >= submitted + retention);
    assert!(expires_at <= SystemTime::now() + retention);

    // A client that reads one partition and then takes too long finds the other gone.
    read(&info.endpoint[0]).await.unwrap();
    tokio::time::sleep(
        expires_at
            .duration_since(SystemTime::now())
            .unwrap_or_default(),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(60);
    while read(&info.endpoint[1]).await.is_ok() {
        assert!(
            Instant::now() < deadline,
            "A result partition was still readable a minute after its endpoint expired"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_event_logs_without_serving_the_rest_api() {
    let log_dir = std::env::temp_dir().join(format!(
        "comet-native-executors-event-logs-{}-{}",
        std::process::id(),
        free_port()
    ));
    let cluster = Cluster::start_with(&[(
        "COMET_SCHEDULER_EVENT_LOG_DIR",
        log_dir.to_string_lossy().into_owned(),
    )]);
    cluster.run(&query(vec![range_stage(100, 2)])).await;

    // The scheduler renames a query's log to `<job id>.eventlog` once it has written all of it.
    let deadline = Instant::now() + Duration::from_secs(60);
    let log = loop {
        let finished = std::fs::read_dir(&log_dir).ok().and_then(|entries| {
            entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .find(|path| {
                    path.extension()
                        .is_some_and(|extension| extension == "eventlog")
                })
        });
        if let Some(log) = finished {
            break log;
        }
        assert!(Instant::now() < deadline, "No finished event log");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(!std::fs::read_to_string(&log).unwrap().is_empty());

    // Compiling the event-log writer compiles Ballista's REST API in, which lists and cancels
    // jobs without authentication on the scheduler's gRPC port. The scheduler doesn't serve it.
    assert_eq!(http_status(cluster.scheduler_port, "/api/jobs"), 404);

    drop(cluster);
    let _ = std::fs::remove_dir_all(&log_dir);
}

#[test]
fn refuses_an_event_log_dir_it_cannot_write() {
    // No one can create a directory inside a file, not even root, as in CI's container.
    let file = std::env::temp_dir().join(format!(
        "comet-native-executors-not-a-dir-{}-{}",
        std::process::id(),
        free_port()
    ));
    std::fs::write(&file, "").unwrap();
    let mut scheduler = Command::new(env!("CARGO_BIN_EXE_comet-scheduler"))
        .env("COMET_SCHEDULER_PORT", free_port().to_string())
        .env("COMET_SCHEDULER_FLIGHT_PORT", free_port().to_string())
        .env("COMET_SCHEDULER_EVENT_LOG_DIR", file.join("logs"))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = scheduler.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = scheduler.kill();
            panic!("The scheduler started with an event-log directory it can't write to");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let mut stderr = String::new();
    scheduler
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    let _ = std::fs::remove_file(&file);
    assert!(!status.success());
    assert!(stderr.contains("COMET_SCHEDULER_EVENT_LOG_DIR"), "{stderr}");
}
