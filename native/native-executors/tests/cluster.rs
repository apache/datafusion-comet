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
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, FlightDescriptor, FlightEndpoint, FlightInfo};
use common::*;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Int64Type;
use datafusion_comet_proto::spark_native_executors::QueryRequest;
use futures::TryStreamExt;
use prost::Message;
use tonic::transport::Endpoint;
use tonic::Code;

/// A comet-scheduler and a comet-executor, stopped when dropped.
struct Cluster {
    processes: Vec<Child>,
    work_dir: PathBuf,
    flight_url: String,
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
        };
        let mut env = vec![
            ("COMET_SCHEDULER_BIND_HOST", "127.0.0.1".to_string()),
            ("COMET_SCHEDULER_PORT", scheduler_port.to_string()),
            ("COMET_SCHEDULER_FLIGHT_PORT", flight_port.to_string()),
        ];
        env.extend_from_slice(scheduler_env);
        cluster.spawn(env!("CARGO_BIN_EXE_comet-scheduler"), &env);
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
        let child = Command::new(binary)
            .envs(env.iter().map(|(key, value)| (*key, value.as_str())))
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
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
            partitions.push(read(endpoint).await.unwrap());
        }
        partitions
    }
}

/// Reads a result partition from the executor that holds it.
async fn read(endpoint: &FlightEndpoint) -> Result<Vec<RecordBatch>, FlightError> {
    let mut executor = connect(&endpoint.location[0].uri).await;
    let ticket = endpoint.ticket.clone().unwrap();
    within_time_limit(async { executor.do_get(ticket).await?.try_collect().await }).await
}

impl Drop for Cluster {
    fn drop(&mut self) {
        // The executor first, so that it doesn't report to a scheduler that has gone.
        for process in self.processes.iter_mut().rev() {
            let _ = process.kill();
            let _ = process.wait();
        }
        let _ = std::fs::remove_dir_all(&self.work_dir);
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
        reading_stage(0, range_partitioning(4), 4),
    ]);
    let partitions = cluster.run(&request).await;
    assert_eq!(partitions.len(), 1);
    assert_eq!(
        sorted_values::<Int64Type>(&partitions.concat()),
        (0..1000).collect::<Vec<_>>()
    );
}

/// The scheduler samples the producer for split points, so each partition holds one range of
/// keys, in the exchange's order.
#[tokio::test(flavor = "multi_thread")]
async fn range_partitions_a_range_exchange_by_sampled_split_points() {
    let cluster = Cluster::start();
    let request = query(vec![
        range_stage(1000, 4),
        reading_stage(0, range_on_first_column_descending(4), 4),
    ]);
    let partitions = cluster.run(&request).await;
    assert_eq!(partitions.len(), 4);
    let ranges: Vec<Vec<i64>> = partitions
        .iter()
        .map(|partition| sorted_values::<Int64Type>(partition))
        .collect();
    for (index, range) in ranges.iter().enumerate() {
        assert!(!range.is_empty(), "partition {index} is empty");
    }
    for pair in ranges.windows(2) {
        assert!(
            pair[0].first() > pair[1].last(),
            "a partition holds keys below the next one's"
        );
    }
    assert_eq!(
        sorted_values::<Int64Type>(&partitions.concat()),
        (0..1000).collect::<Vec<_>>()
    );
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
