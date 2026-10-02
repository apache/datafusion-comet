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

//! Scan attribution at object_store's HTTP connector boundary.
//!
//! The connector delegates client construction, requests, responses, and errors unchanged.
//! Counts include object_store retries of HTTP errors and interrupted response bodies, but
//! exclude redirects and protocol retries performed internally by reqwest, credential requests,
//! and bucket-region discovery. They are not a count of every request transmitted on the wire.

use async_trait::async_trait;
use datafusion::physical_plan::metrics::Count;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService, ReqwestConnector,
};
use object_store::{ClientOptions, GetOptions};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Scan-owned counters, carried by each GET rather than by the shared S3 client.
#[derive(Debug, Clone)]
pub(crate) struct HttpRequestMetrics {
    observed_requests: Count,
    attempts: Count,
    retries: Count,
}

impl HttpRequestMetrics {
    pub(crate) fn new(observed_requests: Count, attempts: Count, retries: Count) -> Self {
        Self {
            observed_requests,
            attempts,
            retries,
        }
    }

    /// Attach fresh state to one logical GET, preserving unrelated request extensions.
    /// object_store clones the extension across both request and response-body retries.
    /// Only an installed `ScanHttpConnector` records these counters: callers must compare
    /// observed requests with logical GETs before interpreting zero retries as full coverage.
    pub(crate) fn track(&self, options: &mut GetOptions) {
        options.extensions.insert(Arc::new(RequestState {
            metrics: self.clone(),
            observed: AtomicBool::new(false),
        }));
    }
}

#[derive(Debug)]
struct RequestState {
    metrics: HttpRequestMetrics,
    observed: AtomicBool,
}

/// A transparent wrapper around object_store's default connector and client configuration.
#[derive(Debug)]
pub(crate) struct ScanHttpConnector;

impl HttpConnector for ScanHttpConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(ScanHttpService {
            inner: ReqwestConnector::default().connect(options)?,
        }))
    }
}

#[derive(Debug)]
struct ScanHttpService {
    inner: HttpClient,
}

#[async_trait]
impl HttpService for ScanHttpService {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        if let Some(state) = request.extensions().get::<Arc<RequestState>>() {
            state.metrics.attempts.add(1);
            if !state.observed.swap(true, Ordering::Relaxed) {
                state.metrics.observed_requests.add(1);
            } else {
                state.metrics.retries.add(1);
            }
        }
        self.inner.execute(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
    use object_store::{aws::AmazonS3Builder, path::Path, BackoffConfig, ObjectStore, RetryConfig};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    const SUCCESS: &str = "HTTP/1.1 206 Partial Content\r\nContent-Length: 3\r\nContent-Range: bytes 0-2/3\r\nETag: \"test\"\r\nConnection: close\r\n\r\nabc";
    const UNAVAILABLE: &str =
        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

    // Real loopback HTTP exercises extension propagation and object_store's retry loops.
    // A bounded server lifetime also makes a missing retry fail instead of hanging the suite.
    async fn server(responses: Vec<&'static str>) -> (String, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(10), async move {
                let mut requests = Vec::new();
                for response in responses {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        let byte = socket.read_u8().await.unwrap();
                        request.push(byte);
                    }
                    requests.push(String::from_utf8(request).unwrap());
                    socket.write_all(response.as_bytes()).await.unwrap();
                    socket.shutdown().await.unwrap();
                }
                requests
            })
            .await
            .expect("HTTP test server timed out")
        });
        (endpoint, task)
    }

    fn store(endpoint: &str) -> impl ObjectStore {
        AmazonS3Builder::new()
            .with_bucket_name("test")
            .with_region("us-east-1")
            .with_endpoint(endpoint)
            .with_skip_signature(true)
            .with_client_options(
                ClientOptions::new()
                    .with_allow_http(true)
                    .with_timeout(Duration::from_secs(2)),
            )
            .with_retry(RetryConfig {
                max_retries: 2,
                retry_timeout: Duration::from_secs(5),
                backoff: BackoffConfig {
                    init_backoff: Duration::from_millis(1),
                    max_backoff: Duration::from_millis(1),
                    ..Default::default()
                },
            })
            .with_http_connector(ScanHttpConnector)
            .build()
            .unwrap()
    }

    fn metrics() -> HttpRequestMetrics {
        let metrics = ExecutionPlanMetricsSet::new();
        HttpRequestMetrics::new(
            MetricBuilder::new(&metrics).global_counter("requests"),
            MetricBuilder::new(&metrics).global_counter("attempts"),
            MetricBuilder::new(&metrics).global_counter("retries"),
        )
    }

    fn counts(metrics: &HttpRequestMetrics) -> (usize, usize, usize) {
        (
            metrics.observed_requests.value(),
            metrics.attempts.value(),
            metrics.retries.value(),
        )
    }

    async fn read(
        store: &impl ObjectStore,
        metrics: &HttpRequestMetrics,
    ) -> object_store::Result<Bytes> {
        let mut options = GetOptions {
            range: Some((0..3).into()),
            ..Default::default()
        };
        metrics.track(&mut options);
        store
            .get_opts(&Path::from("file"), options)
            .await?
            .bytes()
            .await
    }

    #[tokio::test]
    async fn counts_http_retries_and_exhaustion() {
        let too_many_requests =
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        for final_response in [SUCCESS, UNAVAILABLE] {
            let (endpoint, server) =
                server(vec![UNAVAILABLE, too_many_requests, final_response]).await;
            let metrics = metrics();
            let result = read(&store(&endpoint), &metrics).await;
            assert_eq!(result.is_ok(), final_response == SUCCESS);
            if let Ok(bytes) = result {
                assert_eq!(bytes.as_ref(), b"abc");
            }
            assert_eq!(server.await.unwrap().len(), 3);
            assert_eq!(counts(&metrics), (1, 3, 2));
        }
    }

    #[tokio::test]
    async fn body_resume_preserves_request_attribution() {
        let interrupted = "HTTP/1.1 206 Partial Content\r\nContent-Length: 3\r\nContent-Range: bytes 0-2/3\r\nETag: \"test\"\r\nConnection: close\r\n\r\na";
        let resumed = "HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 1-2/3\r\nETag: \"test\"\r\nConnection: close\r\n\r\nbc";
        let (endpoint, server) = server(vec![interrupted, resumed]).await;
        let metrics = metrics();
        assert_eq!(read(&store(&endpoint), &metrics).await.unwrap(), "abc");
        let requests = server.await.unwrap();
        assert!(requests[1].contains("range: bytes=1-2\r\n"));
        assert_eq!(counts(&metrics), (1, 2, 1));
    }

    #[tokio::test]
    async fn shared_client_keeps_scans_and_requests_separate() {
        let (endpoint, server) = server(vec![UNAVAILABLE, SUCCESS, SUCCESS, SUCCESS]).await;
        let store = store(&endpoint);
        let first = metrics();
        let second = metrics();
        let (a, b) = tokio::join!(read(&store, &first), read(&store, &second));
        assert_eq!(a.unwrap(), "abc");
        assert_eq!(b.unwrap(), "abc");
        // Either concurrent request can receive the 503. Only that request's scan retries.
        let mut concurrent = [counts(&first), counts(&second)];
        concurrent.sort_unstable();
        assert_eq!(concurrent, [(1, 1, 0), (1, 2, 1)]);
        let before = counts(&first);
        let second_before = counts(&second);
        assert_eq!(read(&store, &first).await.unwrap(), "abc");
        assert_eq!(server.await.unwrap().len(), 4);
        assert_eq!(counts(&first), (2, before.1 + 1, before.2));
        assert_eq!(counts(&second), second_before);
    }

    #[tokio::test]
    async fn native_s3_readers_attach_their_own_http_counters() {
        use crate::parquet::eager_page_index_reader_factory::{
            EagerPageIndexReaderFactory, ScanIoSource,
        };
        use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;
        use datafusion::prelude::SessionContext;
        use datafusion_datasource::PartitionedFile;
        use std::collections::HashMap;
        use url::Url;

        let (endpoint, server) = server(vec![SUCCESS, SUCCESS]).await;
        // Exercise production S3 construction as well as the scan wrapper. Construct outside
        // Tokio because the ordinary credential resolver drives its own runtime.
        let store: Arc<dyn ObjectStore> = tokio::task::spawn_blocking(move || {
            let configs = HashMap::from([
                ("fs.s3a.endpoint".to_string(), endpoint),
                (
                    "fs.s3a.endpoint.region".to_string(),
                    "us-east-1".to_string(),
                ),
                ("fs.s3a.path.style.access".to_string(), "true".to_string()),
                (
                    "fs.s3a.aws.credentials.provider".to_string(),
                    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider".to_string(),
                ),
            ]);
            let (store, _) = crate::parquet::objectstore::s3::create_store(
                &Url::parse("s3://test/file").unwrap(),
                &configs,
                Duration::from_secs(300),
            )
            .unwrap();
            Arc::from(store)
        })
        .await
        .unwrap();
        let session = SessionContext::new();
        let cache = session
            .runtime_env()
            .cache_manager
            .get_file_metadata_cache();
        let first = ExecutionPlanMetricsSet::new();
        let second = ExecutionPlanMetricsSet::new();
        let reader = |metrics: &ExecutionPlanMetricsSet| {
            EagerPageIndexReaderFactory::new(
                Arc::clone(&store),
                Arc::clone(&cache),
                ScanIoSource::ObjectStore,
                metrics,
            )
            .create_reader(0, PartitionedFile::new("file", 3), None, metrics)
            .unwrap()
        };
        let mut a = reader(&first);
        let mut b = reader(&second);
        let (a, b) = tokio::join!(a.get_bytes(0..3), b.get_bytes(0..3));
        assert_eq!(a.unwrap(), "abc");
        assert_eq!(b.unwrap(), "abc");
        assert_eq!(server.await.unwrap().len(), 2);
        for metrics in [first, second] {
            for (name, expected) in [
                ("scan_io_object_store_get_calls", 1),
                ("scan_io_http_observed_gets", 1),
                ("scan_io_http_attempts", 1),
                ("scan_io_http_retries", 0),
            ] {
                assert_eq!(
                    metrics.clone_inner().sum_by_name(name).unwrap().as_usize(),
                    expected,
                    "{name}"
                );
            }
        }
    }
}
