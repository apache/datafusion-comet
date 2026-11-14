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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightClient, FlightDescriptor, Ticket};
use datafusion_comet_proto::spark_native_executors::{
    QueryRequest, ResultEndpoint, ResultEndpoints,
};
use datafusion_comet_proto::PROTO_FINGERPRINT;
use futures::StreamExt;
use jni::objects::{JByteArray, JClass, JLongArray, JString};
use jni::sys::{jbyteArray, jlong};
use jni::EnvUnowned;
use prost::Message;
use tonic::transport::{Channel, Endpoint};

use super::COMET_VERSION;
use crate::errors::{try_unwrap_or_throw, CometError, CometResult};
use crate::execution::jni_api::{get_runtime, prepare_output};

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

/// Whether a partition that the cluster deletes at `expires_at_millis` (0 for never) is gone at
/// `now_millis`.
fn is_expired(expires_at_millis: i64, now_millis: i64) -> bool {
    expires_at_millis > 0 && now_millis >= expires_at_millis
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The error for a read of a result partition that the cluster deleted at `expires_at_millis`,
/// naming the setting to change.
fn expired_error(
    context: &str,
    expires_at_millis: i64,
    error: impl std::fmt::Display,
) -> CometError {
    client_error(
        context,
        format!(
            "the result partition expired on the cluster at {expires_at_millis} ms since the \
             epoch; raise COMET_SCHEDULER_RESULT_RETENTION_SECONDS ({error})"
        ),
    )
}

/// The error for a failed read of a result partition. A read that fails once the partition's
/// expiration time has passed blames the expiry, whatever the cluster's message says.
fn read_error(context: &str, expires_at_millis: i64, error: FlightError) -> CometError {
    if is_expired(expires_at_millis, now_millis()) {
        expired_error(context, expires_at_millis, error)
    } else {
        client_error(context, error)
    }
}

/// A partition stream with the time the cluster deletes the partition.
struct PartitionReader {
    stream: FlightRecordBatchStream,
    expires_at_millis: i64,
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
            let location = endpoint
                .location
                .first()
                .map(|location| location.uri.clone())
                .ok_or_else(|| client_error("bad result", "a partition has no location"))?;
            let ticket = endpoint
                .ticket
                .map(|ticket| ticket.ticket.to_vec())
                .ok_or_else(|| client_error("bad result", "a partition has no ticket"))?;
            let expiration_time_millis = endpoint
                .expiration_time
                .map(|t| t.seconds * 1000 + i64::from(t.nanos) / 1_000_000)
                .unwrap_or(0);
            Ok(ResultEndpoint {
                location,
                ticket,
                expiration_time_millis,
            })
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

/// Starts reading a result partition from the executor at `location`, which the cluster deletes
/// at `expiration_time_millis` (0 for never). Returns a handle for `nextBatch` and
/// `closePartition`.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_NativeExecutorsClient_openPartition(
    e: EnvUnowned,
    _class: JClass,
    location: JString,
    ticket: JByteArray,
    expiration_time_millis: jlong,
) -> jlong {
    try_unwrap_or_throw(&e, |env| {
        let location = location
            .try_to_string(env)
            .map_err(|e| client_error("bad location", e))?;
        let context = format!("cannot read a partition from {location}");
        if is_expired(expiration_time_millis, now_millis()) {
            return Err(expired_error(
                &context,
                expiration_time_millis,
                "not read before it expired",
            ));
        }
        let ticket = env.convert_byte_array(ticket)?;
        let stream = get_runtime().block_on(async {
            connect(&location)
                .await?
                .do_get(Ticket::new(ticket))
                .await
                .map_err(|e| read_error(&context, expiration_time_millis, e))
        })?;
        let reader = PartitionReader {
            stream,
            expires_at_millis: expiration_time_millis,
        };
        Ok(Box::into_raw(Box::new(reader)) as jlong)
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
        let reader = unsafe { &mut *(handle as *mut PartitionReader) };
        let Some(batch) = get_runtime().block_on(reader.stream.next()) else {
            return Ok(-1);
        };
        let batch = batch
            .map_err(|e| read_error("cannot read a partition", reader.expires_at_millis, e))?;
        // The JVM allocated one array and one schema struct per column.
        prepare_output(env, array_addrs, schema_addrs, batch, false)
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
            drop(unsafe { Box::from_raw(handle as *mut PartitionReader) });
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Status;

    fn message(expires_at_millis: i64) -> String {
        let status = Status::internal("Ballista Error: something");
        read_error(
            "cannot read a partition",
            expires_at_millis,
            FlightError::from(status),
        )
        .to_string()
    }

    #[test]
    fn a_read_after_expiry_names_the_setting_to_raise() {
        assert!(message(1).contains("COMET_SCHEDULER_RESULT_RETENTION_SECONDS"));
    }

    #[test]
    fn a_read_before_expiry_does_not_blame_it() {
        assert!(!message(now_millis() + 3_600_000).contains("expired"));
    }

    #[test]
    fn a_partition_without_expiry_never_expires() {
        assert!(!message(0).contains("expired"));
        assert!(!is_expired(0, i64::MAX));
    }
}
