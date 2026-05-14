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

Comet's native S3 readers normally fetch credentials from the standard AWS credential chain
(static keys, instance profiles, environment variables, etc.). Some clusters use a vendor-managed
mechanism instead, where credentials are issued per request based on a JWT or per S3 path. For
those clusters, Comet supports loading a vendor-supplied bridge JAR that routes every native
credential request through the vendor's Java code.

## Do I need this?

You don't, if any of the following describe your cluster:

- You use static AWS credentials (`fs.s3a.access.key` / `fs.s3a.secret.key`).
- You use EC2 instance profiles, EKS pod identities, ECS task roles, or environment variables.
- Your S3 access works in Spark today via the default AWS credential chain.

You probably do, if any of these are true:

- You have a Hadoop S3A custom signer configured (`fs.s3a.custom.signers=...`).
- You have a Spark `CloudCredentialsProvider` that issues a JWT for a vendor STS service.
- You have a custom Iceberg `client.factory` that injects a configured S3 client.
- Spark queries against your S3 paths work, but the same queries with Comet enabled fail with 403.

## Enabling a bridge JAR

Add the vendor-supplied bridge JAR to your Spark executor classpath:

```sh
spark-submit --jars vendor-comet-bridge.jar ...
```

Or via `spark.jars`:

```
spark.jars=/path/to/vendor-comet-bridge.jar
```

Comet discovers the bridge through `META-INF/services` at executor startup. There are no Comet
config keys to set.

OSS Comet ships no vendor-specific bridges. Get one from the same vendor that supplies your
Hadoop S3A signer or Iceberg client factory. If they do not yet provide one, send them to the
"Writing a bridge" section below.

## Verification

With the bridge on the classpath, executor logs show:

- At startup: `Registered CometS3CredentialProvider: <fully.qualified.VendorClassName>`
- On first S3 access (debug level): `Fetching credentials for bucket=... path=... mode=...`

Without a bridge registered you get exactly one line at startup:

```
No CometS3CredentialProvider registered; native S3 readers will use the default AWS credential chain
```

## Troubleshooting

**`Multiple CometS3CredentialProvider impls on classpath: [...]`** at startup. Remove all but one
bridge JAR. Comet does not chain providers; it fails fast to prevent silent ambiguity.

**`No CometS3CredentialProvider registered`** combined with `403 AccessDenied`. The bridge JAR is
not on the executor classpath. Re-check `--jars` / `spark.jars`. On YARN or Kubernetes, confirm
the JAR actually reached the executor and not only the driver.

**Credentials silently going stale during long-running jobs.** The bridge caps opendal's
credential cache at 5 minutes when the vendor does not populate `expirationEpochMillis`. Ask the
vendor to return a real expiry; the 5-minute floor is a safety net, not a knob.

## Iceberg: explicit S3 region required

With the bridge registered, Comet wires a custom credential loader into `iceberg-storage-opendal`.
opendal's built-in S3 region auto-detection only runs when no custom loader is configured, so on
the bridge path the region (and endpoint for non-AWS) must be set explicitly on the Spark catalog:

```
spark.sql.catalog.<catalog>.s3.region        = us-east-1
spark.sql.catalog.<catalog>.s3.endpoint      = https://...   (non-AWS only)
spark.sql.catalog.<catalog>.s3.path-style-access = true      (path-style endpoints only)
```

If you hit `region is missing. Please find it by S3::detect_region() or set them in env`, this
is the missing config.

## Writing a bridge

Comet's native scan paths (`object_store` for raw Parquet, `opendal` via `iceberg-rust` for
Iceberg) bypass Hadoop S3A entirely. The standard `AWSCredentialsProvider.getCredentials()` has no
path argument, so vendors that issue per-path STS credentials cannot expose them through it. The
`CometS3CredentialProvider` SPI fills that gap.

Implement `org.apache.comet.cloud.s3.CometS3CredentialProvider`:

```java
package org.apache.comet.cloud.s3;

public interface CometS3CredentialProvider {
    CometS3Credentials getCredentialsForPath(
        String bucket, String path, CometS3AccessMode mode) throws Exception;
}
```

Register via `META-INF/services/org.apache.comet.cloud.s3.CometS3CredentialProvider` with the
fully-qualified class name. `getCredentialsForPath` may be invoked concurrently from many native
tokio worker threads; the implementation must be thread-safe.

### Returned fields

| Field                   | Notes                                             |
| ----------------------- | ------------------------------------------------- |
| `accessKeyId`           | Required.                                         |
| `secretAccessKey`       | Required.                                         |
| `sessionToken`          | `null` for non-STS credentials.                   |
| `expirationEpochMillis` | Absolute expiry. `0` means "unknown" (see below). |

Provide a real `expirationEpochMillis` whenever you have one. The Iceberg path uses it to decide
when `opendal` must re-call the provider for a fresh credential. `0` is treated as unknown and the
Iceberg path defaults to a 5-minute refresh to bound staleness.

### Returns or throws

The SPI follows the same shape as the other JVM AWS-credential SPIs (AWS SDK v1/v2,
Hadoop S3A, Iceberg `VendedCredentialsProvider`): return credentials or throw. There is no
"fall-through" return value. Chaining is a vendor-side concern.

If your provider is authoritative only for some paths, resolve the default AWS chain yourself for
the rest:

```java
private final DefaultCredentialsProvider defaultChain = DefaultCredentialsProvider.create();

@Override
public CometS3Credentials getCredentialsForPath(
        String bucket, String path, CometS3AccessMode mode) throws Exception {
    if (handlesPath(bucket, path)) {
        return mintFromMyVendorService(bucket, path, mode);
    }
    AwsCredentials c = defaultChain.resolveCredentials();
    String token = (c instanceof AwsSessionCredentials)
            ? ((AwsSessionCredentials) c).sessionToken()
            : null;
    return new CometS3Credentials(c.accessKeyId(), c.secretAccessKey(), token, 0L);
}
```

### Access mode

| Value   | Used for                                                                   |
| ------- | -------------------------------------------------------------------------- |
| `READ`  | All native scan paths (raw Parquet, Iceberg). Comet today only sends READ. |
| `WRITE` | Reserved for future native write paths.                                    |

A `WRITE` credential is not implicitly read-capable. Vendors that need read-during-write
workflows include the required read permissions in the IAM policy attached to their `WRITE`
credentials.

### Discovery rules

- Zero impls registered: native readers use the default AWS credential chain.
- One impl registered: cached and used for every request.
- Multiple impls registered: `CometS3CredentialDispatcher` throws `IllegalStateException` at
  class-load. Pick one bridge JAR.

### Build setup

Vendor implementations need the Comet SPI classes at compile time only. Use `provided`-scope:

```xml
<dependency>
  <groupId>org.apache.datafusion</groupId>
  <artifactId>comet-common-spark${spark.version.short}_${scala.binary.version}</artifactId>
  <version>${comet.version}</version>
  <scope>provided</scope>
</dependency>
```

### Iceberg path: error message fidelity

When the bridge is wired into `iceberg-rust`, the outer `reqsign-core::ProvideCredentialChain`
currently swallows thrown exceptions into "no credential" before the request reaches opendal. The
credential is still not issued and the request still fails, only the message is degraded to an
opaque anonymous-request failure. No Comet change fixes this; it is resolved upstream if
`iceberg-rust` stops wrapping custom loaders in its outer chain or moves its S3 backend to
`object_store`.
