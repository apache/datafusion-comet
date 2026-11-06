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

//! The JNI client a Spark driver uses to run queries on a Comet Native Executor cluster: the
//! native methods of `org.apache.comet.NativeExecutorsClient`.
//!
//! The driver submits a query to the scheduler's Arrow Flight service, which answers with one
//! endpoint per result partition, and then reads each partition from its executor with `DoGet`.

use std::time::Duration;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightClient, FlightDescriptor, Ticket};
use datafusion_comet_proto::spark_native_executors::{
    QueryRequest, ResultEndpoint, ResultEndpoints,
};
use datafusion_comet_proto::PROTO_FINGERPRINT;
use futures::StreamExt;
use jni::objects::{JByteArray, JClass, JLongArray, JString, ReleaseMode};
use jni::sys::{jbyteArray, jlong};
use jni::EnvUnowned;
use prost::Message;
use tonic::transport::{Channel, Endpoint};
use tonic::Code;

use super::COMET_VERSION;
use crate::errors::{try_unwrap_or_throw, CometError, CometResult};
use crate::execution::jni_api::{export_batch, get_runtime};

fn client_error(context: &str, error: impl std::fmt::Display) -> CometError {
    CometError::Internal(format!("Comet Native Executors: {context}: {error}"))
}

/// How long to wait for a connection to a scheduler or an executor.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// How often to probe a connection that carries no traffic, at the TCP level and at the HTTP/2
/// level. A query can run for a long time before the scheduler answers its submission, so a
/// request has no overall timeout. These probes are what notice a scheduler or an executor that
/// died or became unreachable without closing the connection.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// How long to wait for an answer to an HTTP/2 probe before giving up on the connection.
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// The error for a failed read of a result partition. A partition the cluster has deleted gets a
/// message that names the setting to change. The scheduler says that a partition it stores has
/// expired, and an executor fails to open the file of a partition it has deleted.
fn read_error(context: &str, error: FlightError) -> CometError {
    let expired = match &error {
        FlightError::Tonic(status) => match status.code() {
            Code::NotFound => status.message().contains("expired"),
            Code::Internal => {
                let message = status.message();
                message.contains("Failed to open partition file") && message.contains("NotFound")
            }
            _ => false,
        },
        _ => false,
    };
    if expired {
        client_error(
            context,
            format!(
                "the result partition expired on the cluster; raise \
                 COMET_SCHEDULER_RESULT_RETENTION_SECONDS ({error})"
            ),
        )
    } else {
        client_error(context, error)
    }
}

/// Connects to the Flight service at `location`, a Flight location such as
/// `grpc+tcp://host:port` or an `http://` URL.
async fn connect(location: &str) -> CometResult<FlightClient> {
    let uri = match location.strip_prefix("grpc+tcp://") {
        Some(address) => format!("http://{address}"),
        None => location.to_string(),
    };
    let channel: Channel = Endpoint::from_shared(uri)
        .map_err(|e| client_error(&format!("invalid location {location}"), e))?
        .connect_timeout(CONNECT_TIMEOUT)
        .tcp_keepalive(Some(KEEP_ALIVE_INTERVAL))
        .http2_keep_alive_interval(KEEP_ALIVE_INTERVAL)
        .keep_alive_timeout(KEEP_ALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .connect()
        .await
        .map_err(|e| client_error(&format!("cannot connect to {location}"), e))?;
    // Query plans and result batches can be larger than gRPC's default 4 MiB message limit.
    let inner = FlightServiceClient::new(channel)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);
    Ok(FlightClient::new_from_inner(inner))
}

/// Submits `query`, a serialized `QueryRequest`, to the scheduler at `scheduler_url` and waits
/// for it to finish. Returns where its result partitions are.
async fn submit(scheduler_url: &str, mut query: Vec<u8>) -> CometResult<ResultEndpoints> {
    // Stamp the query with this build's version and plan format. A field that appears again
    // replaces the first, so appending them spares decoding and encoding the whole query.
    QueryRequest {
        comet_version: COMET_VERSION.to_string(),
        proto_fingerprint: PROTO_FINGERPRINT.to_string(),
        ..Default::default()
    }
    .encode(&mut query)
    .map_err(|e| client_error("bad query", e))?;
    let mut client = connect(scheduler_url).await?;
    let info = client
        .get_flight_info(FlightDescriptor::new_cmd(query))
        .await
        .map_err(|e| client_error("the query failed", e))?;
    let endpoints = info
        .endpoint
        .into_iter()
        .map(|endpoint| {
            // A partition without a location is served by the scheduler itself.
            let location = endpoint.location.first().map_or_else(
                || scheduler_url.to_string(),
                |location| location.uri.clone(),
            );
            let ticket = endpoint
                .ticket
                .map(|ticket| ticket.ticket.to_vec())
                .ok_or_else(|| client_error("bad result", "a partition has no ticket"))?;
            Ok(ResultEndpoint { location, ticket })
        })
        .collect::<CometResult<Vec<_>>>()?;
    Ok(ResultEndpoints { endpoints })
}

/// Runs a query: `query` is a serialized `QueryRequest`, which this fills in with the driver's
/// Comet version and plan format. Returns a serialized `ResultEndpoints`.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_NativeExecutorsClient_submit(
    e: EnvUnowned,
    _class: JClass,
    scheduler_url: JString,
    query: JByteArray,
) -> jbyteArray {
    try_unwrap_or_throw(&e, |env| {
        let scheduler_url = scheduler_url
            .try_to_string(env)
            .map_err(|e| client_error("bad scheduler URL", e))?;
        let query = env.convert_byte_array(query)?;
        let endpoints = get_runtime().block_on(submit(&scheduler_url, query))?;
        Ok(env
            .byte_array_from_slice(&endpoints.encode_to_vec())?
            .into_raw())
    })
}

/// Starts reading a result partition from the executor at `location`. Returns a handle for
/// `nextBatch` and `closePartition`.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_NativeExecutorsClient_openPartition(
    e: EnvUnowned,
    _class: JClass,
    location: JString,
    ticket: JByteArray,
) -> jlong {
    try_unwrap_or_throw(&e, |env| {
        let location = location
            .try_to_string(env)
            .map_err(|e| client_error("bad location", e))?;
        let ticket = env.convert_byte_array(ticket)?;
        let stream = get_runtime().block_on(async {
            connect(&location)
                .await?
                .do_get(Ticket::new(ticket))
                .await
                .map_err(|e| read_error(&format!("cannot read a partition from {location}"), e))
        })?;
        Ok(Box::into_raw(Box::new(stream)) as jlong)
    })
}

/// Moves the partition's next batch into the Arrow C Data Interface structs at `array_addrs`
/// and `schema_addrs`, one pair per column. Returns its row count, or -1 at the end.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_NativeExecutorsClient_nextBatch(
    e: EnvUnowned,
    _class: JClass,
    handle: jlong,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
) -> jlong {
    try_unwrap_or_throw(&e, |env| {
        // SAFETY: `handle` comes from `openPartition` and hasn't been closed.
        let stream = unsafe { &mut *(handle as *mut FlightRecordBatchStream) };
        let Some(batch) = get_runtime().block_on(stream.next()) else {
            return Ok(-1);
        };
        let batch = batch.map_err(|e| read_error("cannot read a partition", e))?;
        let array_addrs = unsafe { array_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };
        let schema_addrs = unsafe { schema_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };
        // SAFETY: the JVM allocated one array and one schema struct per column.
        unsafe { export_batch(&array_addrs, &schema_addrs, batch, false) }
    })
}

/// Stops reading a partition and releases its handle.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_NativeExecutorsClient_closePartition(
    e: EnvUnowned,
    _class: JClass,
    handle: jlong,
) {
    try_unwrap_or_throw(&e, |_env| {
        if handle != 0 {
            // SAFETY: `handle` comes from `openPartition`, and the JVM closes it once.
            drop(unsafe { Box::from_raw(handle as *mut FlightRecordBatchStream) });
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Status;

    fn message(status: Status) -> String {
        read_error("cannot read a partition", FlightError::from(status)).to_string()
    }

    #[test]
    fn an_expired_result_names_the_setting_to_raise() {
        for status in [
            Status::not_found("The results of job abc have expired"),
            Status::internal(
                "Ballista Error: General(\"Failed to open partition file at \\\"p\\\": \
                 Os { code: 2, kind: NotFound, message: \\\"No such file or directory\\\" }\")",
            ),
        ] {
            assert!(
                message(status).contains("COMET_SCHEDULER_RESULT_RETENTION_SECONDS"),
                "no hint"
            );
        }
    }

    #[test]
    fn other_failures_do_not_blame_expiry() {
        for status in [
            Status::not_found("Job abc has no result partition 7"),
            Status::internal("Ballista Error: something else"),
            Status::unavailable("connection reset"),
        ] {
            assert!(!message(status).contains("expired"));
        }
    }
}
