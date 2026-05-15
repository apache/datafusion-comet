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

Comet's native S3 readers normally fetch credentials from the standard AWS credential chain (static keys, instance profiles, environment variables, etc.). Some clusters use a vendor-managed mechanism instead, where credentials are issued per request based on a JWT or per S3 path. For those clusters, Comet supports loading a vendor-supplied bridge class that routes every native credential request through the vendor's Java code.

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

## Enabling a bridge

A bridge is activated by naming the vendor's class in a Spark config. There is no `META-INF/services` discovery and putting a JAR on the classpath alone has no effect; the config key must be set.

For raw Parquet (the `object_store` path), set the Hadoop S3A config:

```
spark.hadoop.fs.s3a.comet.credential.provider.class=com.vendor.MyCometCredentialProvider
```

A per-bucket override is supported and follows the same shape as `fs.s3a.bucket.<name>.aws.credentials.provider`:

```
spark.hadoop.fs.s3a.bucket.<bucket>.comet.credential.provider.class=com.vendor.MyCometCredentialProvider
```

For Iceberg (the `opendal` path), set the per-catalog property under the `s3.` namespace Iceberg already uses for its S3 settings:

```
spark.sql.catalog.<catalog>.s3.comet.credential.provider.class=com.vendor.MyCometCredentialProvider
```

Add the vendor JAR to your Spark executor classpath:

```sh
spark-submit --jars vendor-comet-bridge.jar ...
```

OSS Comet ships no vendor-specific bridges. Get one from the same vendor that supplies your Hadoop S3A signer or Iceberg client factory. If they do not yet provide one, send them to the "Writing a bridge" section below.

## Verification

With the config set and the JAR on the classpath, executor logs show on first S3 access:

- Info level: `Instantiated CometS3CredentialProvider <fully.qualified.VendorClassName>`
- Debug level: `Fetching credentials via <class> for bucket=... path=... mode=...`

Without the config set, no credential-related log lines appear at startup; native readers use the default AWS credential chain.

## Troubleshooting

**`CometS3CredentialProvider class not found: <name>`**. The class named in the config is not on the executor classpath. Re-check `--jars` / `spark.jars`. On YARN or Kubernetes, confirm the JAR actually reached the executor and not only the driver.

**`<class> does not implement org.apache.comet.cloud.s3.CometS3CredentialProvider`**. The configured class exists but does not implement the SPI. Double-check the FQCN against the vendor's documentation.

**`<class> must declare a public no-arg constructor`**. Vendor classes are instantiated reflectively with `Class.forName(name).getDeclaredConstructor().newInstance()`. A non-default constructor is not supported; ask the vendor to expose a no-arg form that reads any state it needs from environment or system properties.

**`403 AccessDenied` with the bridge configured.** The provider returned credentials but they were rejected by S3. Most often a region mismatch (see Iceberg section below) or expired session token; enable debug logging on the vendor's class to confirm what it returned.

**Credentials silently going stale during long-running jobs.** The bridge caps opendal's credential cache at 5 minutes when the vendor does not populate `expirationEpochMillis`. Ask the vendor to return a real expiry; the 5-minute floor is a safety net, not a knob.

## Iceberg: explicit S3 region required

With the bridge configured, Comet wires a custom credential loader into `iceberg-storage-opendal`. opendal's built-in S3 region auto-detection only runs when no custom loader is configured, so on the bridge path the region (and endpoint for non-AWS) must be set explicitly on the Spark catalog:

```
spark.sql.catalog.<catalog>.s3.region        = us-east-1
spark.sql.catalog.<catalog>.s3.endpoint      = https://...   (non-AWS only)
spark.sql.catalog.<catalog>.s3.path-style-access = true      (path-style endpoints only)
```

If you hit `region is missing. Please find it by S3::detect_region() or set them in env`, this is the missing config.

## Writing a bridge

Comet's native scan paths (`object_store` for raw Parquet, `opendal` via `iceberg-rust` for Iceberg) bypass Hadoop S3A entirely. The standard `AWSCredentialsProvider.getCredentials()` has no path argument, so vendors that issue per-path STS credentials cannot expose them through it. The `CometS3CredentialProvider` SPI fills that gap.

Implement `org.apache.comet.cloud.s3.CometS3CredentialProvider`:

```java
package org.apache.comet.cloud.s3;

public interface CometS3CredentialProvider {
    CometS3Credentials getCredentialsForPath(
        String bucket, String path, CometS3AccessMode mode) throws Exception;
}
```

The class must have a public no-arg constructor. `getCredentialsForPath` may be invoked concurrently from many native tokio worker threads; the implementation must be thread-safe.

### Returned fields

| Field                   | Notes                                             |
| ----------------------- | ------------------------------------------------- |
| `accessKeyId`           | Required.                                         |
| `secretAccessKey`       | Required.                                         |
| `sessionToken`          | `null` for non-STS credentials.                   |
| `expirationEpochMillis` | Absolute expiry. `0` means "unknown" (see below). |

Provide a real `expirationEpochMillis` whenever you have one. The Iceberg path uses it to decide when `opendal` must re-call the provider for a fresh credential. `0` is treated as unknown and the Iceberg path defaults to a 5-minute refresh to bound staleness.

### Returns or throws

The SPI follows the same shape as the other JVM AWS-credential SPIs (AWS SDK v1/v2, Hadoop S3A, Iceberg `VendedCredentialsProvider`): return credentials or throw. There is no "fall-through" return value.

If your provider is authoritative only for some paths, resolve the default AWS chain yourself for the rest:

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

### Composing multiple credential backends

A single configured provider class is the dispatcher. If a vendor needs to route across several credential backends (per bucket, per path prefix, per tenant), the dispatch lives inside the vendor's class:

```java
public final class MyCometCredentialProvider implements CometS3CredentialProvider {
    private final ProdVendor prod = ...;
    private final StsVendor sts = ...;
    private final DefaultVendor fallback = ...;

    @Override
    public CometS3Credentials getCredentialsForPath(
            String bucket, String path, CometS3AccessMode mode) throws Exception {
        if (bucket.startsWith("data-prod-"))   return prod.fetch(bucket, path, mode);
        if (bucket.equals("partner-shared"))   return sts.assumeRole(bucket, path, mode);
        return fallback.fetch(bucket, path);
    }
}
```

Per-bucket Hadoop overrides (`fs.s3a.bucket.<name>.comet.credential.provider.class`) are also available if you prefer to ship multiple vendor classes and pick by bucket in config rather than in code.

### Access mode

| Value   | Used for                                                                   |
| ------- | -------------------------------------------------------------------------- |
| `READ`  | All native scan paths (raw Parquet, Iceberg). Comet today only sends READ. |
| `WRITE` | Reserved for future native write paths.                                    |

A `WRITE` credential is not implicitly read-capable. Vendors that need read-during-write workflows include the required read permissions in the IAM policy attached to their `WRITE` credentials.

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

When the bridge is wired into `iceberg-rust`, the outer `reqsign-core::ProvideCredentialChain` currently swallows thrown exceptions into "no credential" before the request reaches opendal. The credential is still not issued and the request still fails, only the message is degraded to an opaque anonymous-request failure. No Comet change fixes this; it is resolved upstream if `iceberg-rust` stops wrapping custom loaders in its outer chain or moves its S3 backend to `object_store`.
