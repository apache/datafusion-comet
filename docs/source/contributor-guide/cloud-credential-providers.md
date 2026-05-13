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
    CometCredentials getCredentialsForPath(
        String bucket, String path, CometAccessMode mode) throws Exception;
}
```

`getCredentialsForPath` may be invoked concurrently from many native tokio worker threads.
Implementations must be thread-safe.

The `mode` is the access intent for this credential request:

| Value             | Used for                                                                |
| ----------------- | ----------------------------------------------------------------------- |
| `READ`            | All native scan paths (raw Parquet, Iceberg). Comet today only sends READ. |
| `WRITE`           | Reserved for future native write paths (Iceberg writes, native INSERT). |

The SPI does not promise that a `WRITE` credential is also read-capable; vendors that need
read-during-write workflows (multipart-completion HEAD, Iceberg manifest reads on the write path,
etc.) include the necessary read permissions in the IAM policy attached to their `WRITE`
credentials.

The returned `CometCredentials` POJO carries:

| Field                   | Type                | Notes                                                  |
| ----------------------- | ------------------- | ------------------------------------------------------ |
| `accessKeyId`           | `String` (non-null) | Required.                                              |
| `secretAccessKey`       | `String` (non-null) | Required.                                              |
| `sessionToken`          | `String` (nullable) | Pass `null` for non-STS credentials.                   |
| `region`                | `String` (nullable) | `null` lets the native reader fall back to its config. |
| `expirationEpochMillis` | `long`              | `0` means "unknown" (see expiration semantics below).  |

### Why this SPI returns or throws (no fallthrough return value)

The SPI follows the same shape as every other AWS-credential SPI in the JVM ecosystem:

| SPI                                         | Method                                       | Behavior                  |
| ------------------------------------------- | -------------------------------------------- | ------------------------- |
| AWS SDK v1 `AWSCredentialsProvider`         | `getCredentials()`                           | returns or throws         |
| AWS SDK v2 `AwsCredentialsProvider`         | `resolveCredentials()`                       | returns or throws         |
| Hadoop S3A `AWSCredentialsProvider`         | `getCredentials()`                           | returns or throws         |
| Iceberg `VendedCredentialsProvider`         | `resolveCredentials()`                       | returns or throws         |
| Iceberg `AwsClientFactory`                  | `s3()`                                       | returns a configured client |
| **Comet `CometCloudCredentialProvider`**    | `getCredentialsForPath(bucket, path, mode)`  | **returns or throws**     |

Chaining/fallback in the AWS world is a separate concern, composed *outside* the provider —
e.g. AWS SDK v2's `AwsCredentialsProviderChain.builder().credentialsProviders(...)`, or the Hadoop
S3A `fs.s3a.aws.credentials.provider` comma-separated list resolved by
`AWSCredentialsProviderChain`. Each *individual* provider is atomic ("give me a credential or
fail"); a chain class composes them.

Comet's SPI keeps to this convention. We considered an alternative `Optional<CometCredentials>`
return where empty would mean "fall through to Comet's native default AWS chain" — convenient
for path-scoped vendors but inconsistent with everything else at this layer. Composition is
cheap to do vendor-side and keeps the SPI surface narrow.

#### Pattern: vendor that handles only a subset of paths

If your provider is authoritative only for a subset of buckets/prefixes (e.g. you have indexed
policies for some paths and want others to use the host's default AWS credentials), construct
the default chain in your provider and return its credentials directly:

```java
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

public final class PathScopedProvider implements CometCloudCredentialProvider {

    private final DefaultCredentialsProvider defaultChain = DefaultCredentialsProvider.create();

    @Override
    public CometCredentials getCredentialsForPath(
            String bucket, String path, CometAccessMode mode) throws Exception {
        if (handlesPath(bucket, path)) {
            return mintFromMyVendorService(bucket, path, mode);
        }
        AwsCredentials c = defaultChain.resolveCredentials();
        String token = (c instanceof AwsSessionCredentials)
                ? ((AwsSessionCredentials) c).sessionToken()
                : null;
        return new CometCredentials(c.accessKeyId(), c.secretAccessKey(), token, null, 0L);
    }
}
```

This mirrors how Iceberg's `VendedCredentialsProvider` is implemented (it composes
`HTTPClient` + `CachedSupplier` internally; the SPI itself just returns or throws). Pick AWS SDK
v1 vs v2 to match whatever your existing signer / `AwsClientFactory` integration uses — the
choice is invisible to Comet because only `CometCredentials` (a plain POJO with strings + a long)
crosses the JNI boundary.

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
  message (and chained causes) to the caller. (See the iceberg-path note below for one current
  exception to that propagation.)
- Returning `null` is a contract violation; the native bridge surfaces it as a request failure.
  Implementations that want to defer to a default credential chain on a subset of paths should
  resolve the default chain themselves and return its credentials — see the worked pattern under
  *Why this SPI returns or throws* above.

#### Iceberg path: error message fidelity

When the bridge is wired into `iceberg-rust` (Iceberg native scans), iceberg-rust currently wraps
our credential loader in its own `ProvideCredentialChain`. That outer chain swallows errors into
"no credential" before the request reaches opendal (see `reqsign-core::ProvideCredentialChain`),
so a thrown exception surfaces as an opaque opendal/anonymous-request failure rather than your
exception message. The credential is still not issued and the request still fails — only the
message is degraded.

This is tied to opendal's chain semantics. It would resolve if iceberg-rust either (a) stops
wrapping custom loaders in its own outer chain, or (b) moves Iceberg's S3 storage backend to
`object_store` (whose `CredentialProvider` has no chain-swallow behavior — auth failures
propagate cleanly as they do on Comet's raw Parquet path today). No Comet SPI change is needed
in either case.

### Iceberg path: explicit S3 region required

When the bridge is registered, Comet wires it into `iceberg-storage-opendal` as a
`CustomAwsCredentialLoader`. opendal then requires explicit S3 region (and, for non-AWS
endpoints, an explicit endpoint) on the catalog properties — its built-in region auto-detection
only runs when no custom credential loader is configured.

In practice this means deployments using the bridge for Iceberg must set, on the Spark catalog:

```
spark.sql.catalog.<catalog>.s3.region        = us-east-1     (or your real region)
spark.sql.catalog.<catalog>.s3.endpoint      = https://...   (only for non-AWS)
spark.sql.catalog.<catalog>.s3.path-style-access = true      (only for path-style endpoints)
```

If a deployment hits `region is missing. Please find it by S3::detect_region() or set them
in env`, this is the missing config.

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

A vendor that wants to compose multiple credential sources (per-path STS for some prefixes, a
catchall provider for others) does so inside their single provider implementation — same as how
AWS SDK v1/v2 providers compose into `AwsCredentialsProviderChain` at the call site rather than
exposing chain semantics through the individual provider contract. See *Why this SPI returns or
throws* above for a worked example.

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

## Build setup

Vendor implementations need the Comet SPI classes at compile time only. The standard pattern is
a `provided`-scope Maven dependency on `comet-common`:

```xml
<dependency>
  <groupId>org.apache.datafusion</groupId>
  <artifactId>comet-common-spark${spark.version.short}_${scala.binary.version}</artifactId>
  <version>${comet.version}</version>
  <scope>provided</scope>
</dependency>
```

`provided` scope means:

- Compile-time only — your jar resolves the `CometCloudCredentialProvider` interface and
  `CometCredentials` POJO for compilation.
- No runtime bundling — your jar does not ship Comet classes; no fat-jar bloat, no version
  conflict on the executor classpath.
- The API classes are already present at runtime because Comet itself is loaded (that's the
  whole reason the bridge exists).

Same pattern vendors already use for implementing Hadoop S3A `AwsSignerInitializer` and Iceberg
`AwsClientFactory` — Comet is a third entry in the same list, not a different shape.

## Worked example

A minimal static-credential provider, suitable for tests and development:

```java
package com.example.comet.test;

import org.apache.comet.cloud.CometAccessMode;
import org.apache.comet.cloud.CometCloudCredentialProvider;
import org.apache.comet.cloud.CometCredentials;

public final class StaticCometCredentialProvider implements CometCloudCredentialProvider {
    private static final String ACCESS_KEY = System.getenv("EXAMPLE_ACCESS_KEY");
    private static final String SECRET_KEY = System.getenv("EXAMPLE_SECRET_KEY");
    private static final String REGION = System.getenv().getOrDefault("EXAMPLE_REGION", "us-east-1");

    @Override
    public CometCredentials getCredentialsForPath(
            String bucket, String path, CometAccessMode mode) {
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
