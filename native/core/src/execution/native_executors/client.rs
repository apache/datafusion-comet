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

use arrow_flight::decode::FlightRecordBatchStream;
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

use super::COMET_VERSION;
use crate::errors::{try_unwrap_or_throw, CometError, CometResult};
use crate::execution::jni_api::{export_batch, get_runtime};

fn client_error(context: &str, error: impl std::fmt::Display) -> CometError {
    CometError::Internal(format!("Comet Native Executors: {context}: {error}"))
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
async fn submit(scheduler_url: &str, query: &[u8]) -> CometResult<ResultEndpoints> {
    let mut request = QueryRequest::decode(query).map_err(|e| client_error("bad query", e))?;
    request.comet_version = COMET_VERSION.to_string();
    request.proto_fingerprint = PROTO_FINGERPRINT.to_string();
    let mut client = connect(scheduler_url).await?;
    let info = client
        .get_flight_info(FlightDescriptor::new_cmd(request.encode_to_vec()))
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
        let endpoints = get_runtime().block_on(submit(&scheduler_url, &query))?;
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
                .map_err(|e| client_error(&format!("cannot read a partition from {location}"), e))
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
        let batch = batch.map_err(|e| client_error("cannot read a partition", e))?;
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
