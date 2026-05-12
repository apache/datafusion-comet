<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Cloud Credential Providers

Comet's native S3 readers can route every credential request to a vendor-supplied Java provider
loaded via `java.util.ServiceLoader`. This page is the integration contract for vendors writing
that bridge.

If you are an operator deciding whether to enable this in your cluster, see the user guide page
on cloud credential providers instead.

## Why this SPI exists

Comet runs queries that bypass Spark's Hadoop S3A code path entirely. Native Parquet scans go
through the Rust `object_store` crate directly; native Iceberg scans go through `iceberg-rust` and
`opendal`. Neither path ever calls a Hadoop `Signer` or `AWSCredentialsProvider`. That means none
of the credential infrastructure Spark and Hadoop already configured for your cluster is reachable
from Comet's native code.

For the simple case (static credentials, EC2 instance profiles, environment variables, the default
AWS credential chain) Comet reproduces that resolution natively in
`native/core/src/parquet/objectstore/s3.rs`. No SPI is needed; the existing default chain works.

This SPI exists for the case the default chain _cannot_ express: a vendor-managed exchange that
takes a per-request path or token and returns short-lived STS credentials.

The reasons that case can't be served by an existing API:

- **`org.apache.spark.deploy.security.cloud.CloudCredentialsProvider`** yields a single auth proof
  (typically a JWT) per service name. It has no path argument and no notion of returning AWS
  credentials. It is the right place to obtain a JWT, but not to exchange one.
- **Hadoop S3A's custom signer mechanism** keeps path-aware logic inside the
  `Signer.sign(request, credentials)` call. The standard `AWSCredentialsProvider.getCredentials()`
  contract is parameterless, so vendors that need per-path STS lookup hide that lookup inside the
  signer. The credential is never returned outside the AWS SDK's signing pipeline; even running
  the signer on a synthesized request does not recover the underlying credential, because the
  secret access key is an HMAC key, not a value present in the signed output.
- **Reflecting into vendor singletons** would require Comet to encode a vendor's class name, field
  name, lazy-init lifecycle, and method signatures. Each vendor differs; each version may rename
  things. That shifts the maintenance burden from the vendor to Comet and creates silent breakage
  on upgrades.
- **A Comet-specific HTTP STS endpoint contract** would require vendors to expose and stabilize an
  HTTP API just for Comet. Most vendors ship this logic as Java code, not as a public HTTP API,
  and asking them to do otherwise is a larger change than a small Java class.

Comet's SPI is a peer to two contracts vendors with full integration coverage already implement:

| Path              | Vendor implements                                           |
| ----------------- | ----------------------------------------------------------- |
| Hadoop S3A        | `org.apache.hadoop.fs.s3a.AwsSignerInitializer` plus signer |
| Iceberg-Java      | `org.apache.iceberg.aws.AwsClientFactory`                   |
| Comet (this page) | `org.apache.comet.cloud.CometCloudCredentialProvider`       |

Adding a Comet implementation is the same shape as the first two with a smaller surface (one
method).

## SPI contract

Implement `org.apache.comet.cloud.CometCloudCredentialProvider`:

```java
package org.apache.comet.cloud;

public interface CometCloudCredentialProvider {
    CometCredentials getCredentialsForPath(String bucket, String path) throws Exception;
}
```

`getCredentialsForPath` may be invoked concurrently from many native tokio worker threads.
Implementations must be thread-safe.

The returned `CometCredentials` POJO carries:

| Field                   | Type                | Notes                                                  |
| ----------------------- | ------------------- | ------------------------------------------------------ |
| `accessKeyId`           | `String` (non-null) | Required.                                              |
| `secretAccessKey`       | `String` (non-null) | Required.                                              |
| `sessionToken`          | `String` (nullable) | Pass `null` for non-STS credentials.                   |
| `region`                | `String` (nullable) | `null` lets the native reader fall back to its config. |
| `expirationEpochMillis` | `long`              | `0` means "unknown" (see expiration semantics below).  |

### Expiration semantics

`expirationEpochMillis` is the absolute expiry of the returned credential, in milliseconds since
the Unix epoch.

- For credentials with a known expiry, set this to the actual expiry. The Iceberg path uses it to
  decide when `opendal` must re-call your provider for a fresh credential.
- `0` is treated as "unknown". The Iceberg path then defaults to a 5-minute refresh interval to
  bound how long opendal can cache a possibly-stale credential. Spark jobs running for hours or
  days with cached stale credentials would otherwise fail silently mid-task; the 5-minute floor
  is a safety net, not a recommendation. **Provide a real expiry whenever you have one.**
- The `object_store` path (raw `s3://` Parquet scans) ignores expiration today and re-fetches per
  request.

### Error semantics

- Throwing from `getCredentialsForPath` aborts the native S3 request and surfaces the exception
  message (and chained causes) to the caller.
- Returning `null` is reserved for "this provider does not handle this path"; the native caller
  treats it as an authorization failure rather than falling back to other providers.

## Discovery

Discovery is purely classpath-based. Register your implementation by adding a service file:

```
META-INF/services/org.apache.comet.cloud.CometCloudCredentialProvider
```

containing the fully-qualified name of your implementation class.

Resolution rules at JVM startup:

- **Zero impls registered** — the native readers fall through to the existing AWS credential chain
  (the same behavior as a Comet without your jar on the classpath).
- **Exactly one impl registered** — cached and used for every credential request.
- **Multiple impls registered** — `CometCloudCredentialDispatcher` throws
  `IllegalStateException` at class-load with the FQN of every impl found. The executor fails
  loudly. This is intentional; pick one bridge jar.

There is no Comet-specific config knob for selecting a provider. Discovery follows the same source
of truth Spark itself reads, so a query that falls back from Comet to Spark mid-execution sees
identical credentials.

## Threading and lifecycle

- The dispatcher caches the resolved provider in a `static final` field for the JVM lifetime. Your
  provider is constructed once per executor and reused for every request.
- The dispatcher itself caches no credentials. Every native call dispatches through JNI on a tokio
  worker thread, so any STS / token refresh logic must live inside your provider.
- Implementations should be cheap to construct (no long blocking work in the no-arg constructor)
  and thread-safe.

## Spark and AWS SDK version selection

Vendors that need to pick between Spark 3 and Spark 4 implementations, or between AWS SDK v1 and
v2 backends, do so inside their own provider class. Comet is unaware of either. Common patterns
include reflectively probing for an SDK class on the classpath, or shipping two service files (one
per Spark profile) that each register a different provider class.

## Worked example

A minimal static-credential provider, suitable for tests and development:

```java
package com.example.comet.test;

import org.apache.comet.cloud.CometCloudCredentialProvider;
import org.apache.comet.cloud.CometCredentials;

public final class StaticCometCredentialProvider implements CometCloudCredentialProvider {
    private static final String ACCESS_KEY = System.getenv("EXAMPLE_ACCESS_KEY");
    private static final String SECRET_KEY = System.getenv("EXAMPLE_SECRET_KEY");
    private static final String REGION = System.getenv().getOrDefault("EXAMPLE_REGION", "us-east-1");

    @Override
    public CometCredentials getCredentialsForPath(String bucket, String path) {
        return new CometCredentials(ACCESS_KEY, SECRET_KEY, null, REGION, 0L);
    }
}
```

Register it via `META-INF/services/org.apache.comet.cloud.CometCloudCredentialProvider`:

```
com.example.comet.test.StaticCometCredentialProvider
```

A real provider would read a JWT from `SparkConf` (typically populated by a Spark
`CloudCredentialsProvider`), call its STS-vending service with the `(bucket, path)` tuple, and
return the resulting credentials with their actual expiry.

## Where this lives in Comet

| Component                                                 | Location                                                                          |
| --------------------------------------------------------- | --------------------------------------------------------------------------------- |
| SPI interface                                             | `common/src/main/java/org/apache/comet/cloud/CometCloudCredentialProvider.java`   |
| POJO                                                      | `common/src/main/java/org/apache/comet/cloud/CometCredentials.java`               |
| Dispatcher (called from native via JNI)                   | `common/src/main/java/org/apache/comet/cloud/CometCloudCredentialDispatcher.java` |
| Rust JNI bridge and `object_store::CredentialProvider`    | `native/core/src/parquet/objectstore/comet_credential_bridge.rs`                  |
| `s3.rs` injection point (DataSourceExec / Parquet scans)  | `native/core/src/parquet/objectstore/s3.rs`                                       |
| Iceberg scan injection point (`iceberg-rust` + `opendal`) | `native/core/src/execution/operators/iceberg_scan.rs`                             |
