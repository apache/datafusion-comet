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

# S3 Credential Providers

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

A bridge is activated by naming the vendor's class in a Spark config. Putting a JAR on the classpath alone has no effect; the config key must be set.

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

## Wire encryption

Catalog configuration you set under `spark.sql.catalog.<catalog>.*` (which may include endpoint URLs, OAuth tokens, and other vendor keys your provider reads) is sent from the driver to executors over Spark's Netty RPC channel so the provider can run there. Live AWS credentials fetched by the provider stay on the executor that fetched them.

Spark's RPC channel is plaintext by default and offers two opt-in encryption mechanisms, both off by default:

- [`spark.network.crypto.enabled`](https://spark.apache.org/docs/latest/security.html#authentication-and-encryption) for AES-based RPC encryption keyed off the auth shared secret.
- [`spark.ssl.rpc.enabled`](https://spark.apache.org/docs/latest/security.html#ssl-configuration) for TLS on the same channel (Spark 3.4+).

The same defaults apply to Hadoop delegation tokens, `CloudCredentialsProvider` JWTs, and any other secrets Spark already ships driver-to-executor, so this is a deployment-wide call rather than something specific to this SPI. See Spark's [security guide](https://spark.apache.org/docs/latest/security.html) for the full set of knobs.

## Verification

With the config set and the JAR on the classpath, executor logs show on first S3 access:

- Info level: `Instantiated CometS3CredentialProvider <fully.qualified.VendorClassName>`
- Debug level: `Fetching credentials via <class> (dispatchKey=<key>) for bucket=... path=... mode=...`

Without the config set, no credential-related log lines appear at startup; native readers use the default AWS credential chain.

## Troubleshooting

**`CometS3CredentialProvider class not found: <name>`**. The class named in the config is not on the executor classpath. Re-check `--jars` / `spark.jars`. On YARN or Kubernetes, confirm the JAR actually reached the executor and not only the driver.

**`<class> does not implement org.apache.comet.cloud.s3.CometS3CredentialProvider`**. The configured class exists but does not implement the SPI. Double-check the FQCN against the vendor's documentation.

**`<class> must declare a public no-arg constructor`**. Vendor classes are instantiated reflectively with `Class.forName(name).getDeclaredConstructor().newInstance()`. A non-default constructor is not supported; ask the vendor to expose a no-arg form that reads any state it needs from `initialize(Map)` or environment.

**`CometS3CredentialProvider <class> (dispatchKey=...) was not initialized`**. `initialize(Map)` was not called before a credential request. Comet should always invoke `ensureInitialized` synchronously when it builds the bridge at plan time, so this indicates the bridge skipped the init call (a Comet bug) or the vendor's `initialize` threw and the bridge fell through to the default chain.

**`403 AccessDenied` with the bridge configured.** The provider returned credentials but they were rejected by S3. Most often a region mismatch (see Iceberg section below) or expired session token; enable debug logging on the vendor's class to confirm what it returned.

**Credentials silently going stale during long-running jobs.** When a vendor returns `expirationEpochMillis=0`, the bridge substitutes a 5-minute expiry before handing the credential to `opendal`, so `opendal`'s cache cannot hold a stale credential indefinitely. Returning a real expiry is preferred; the 5-minute fallback is a safety net, not a knob.

## Iceberg: explicit S3 region required

With the bridge configured, Comet wires a custom credential loader into `iceberg-storage-opendal`. `opendal`'s built-in S3 region auto-detection only runs when no custom loader is configured, so on the bridge path the region (and endpoint for non-AWS) must be set explicitly on the Spark catalog:

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

public interface CometS3CredentialProvider extends AutoCloseable {
    /** Called once per (FQCN, dispatchKey, catalogProperties) before any per-request call. Optional. */
    default void initialize(java.util.Map<String, String> catalogProperties) {}

    CometS3Credentials getCredentialsForPath(CometS3CredentialContext context) throws Exception;

    /** Invoked from the dispatcher's JVM shutdown hook. Default is a no-op. */
    @Override default void close() throws Exception {}
}
```

The class must have a public no-arg constructor. `getCredentialsForPath` may be invoked concurrently from many native tokio worker threads; the implementation must be thread-safe. The supplied `CometS3CredentialContext` exposes `getBucket()`, `getPath()`, and `getMode()`; future Comet releases may add accessors here without changing the method signature, so vendors compiled against today's API stay binary-compatible.

### Lifecycle

Comet keys provider instances by `(FQCN, dispatchKey, catalogProperties)`. The dispatch key is the Spark V2 catalog name on the Iceberg path and the S3 bucket name on the Parquet path. The first time a given key is seen on an executor, Comet reflects the class, calls `initialize(Map)` exactly once, and caches the instance for the JVM lifetime. Two catalogs sharing one provider FQCN therefore get isolated instances with their own `initialize` maps. Including `catalogProperties` in the key matters in multi-tenant JVMs (Spark Connect, Thrift Server, `SparkSession.newSession()`) where two sessions can otherwise collide on the same `(FQCN, dispatchKey)` and have the second session silently use the first session's credentials.

`initialize` should be cheap and non-blocking. Defer real credential fetches (REST round-trips, STS calls) to the first `getCredentialsForPath` invocation. On the Iceberg path the supplied `catalogProperties` carries the unfiltered FileIO bag, including REST-vended fields like `credentials.uri`, OAuth tokens, and any vendor-custom keys you set on the catalog config. The map may contain secrets, so do not log it.

`close()` is invoked from a JVM shutdown hook installed by the dispatcher. The default no-op is fine for stateless providers. Override it to release HTTP clients, scheduled-refresh executors, or STS connection pools. Shutdown hooks are best-effort: a `SIGKILL` or abrupt JVM termination skips them, so do not depend on `close()` for correctness.

### Caching, refresh, and distribution

Comet does not maintain a TTL cache, broadcast catalog state, or schedule refresh. Vendors decide:

- Whether to cache credentials and for how long. Iceberg vendors get `software.amazon.awssdk.utils.cache.CachedSupplier` for free inside `VendedCredentialsProvider`; vendors with custom STS write whatever cache fits.
- When to refresh: proactive timer, on-demand at expiry, on `403` retry, etc.
- How to distribute and refresh state. `catalogProperties` is a plan-time snapshot. Comet does not re-execute the catalog or push fresh values to running scans, so any state that must refresh during a scan is the vendor's responsibility. Options: call back to a vendor service from the executor on each `getCredentialsForPath`, run your own Spark broadcast inside the class, or compose with Spark's [`HadoopDelegationTokenProvider`](https://spark.apache.org/docs/latest/security.html#kerberos) when the underlying bearer needs scheduled driver-side renewal.

When using `HadoopDelegationTokenProvider`, Spark mints and renews the token on the driver and propagates it to executors via `UserGroupInformation`. The provider reads the current value inside `getCredentialsForPath` and exchanges it for AWS credentials:

```java
@Override
public CometS3Credentials getCredentialsForPath(CometS3CredentialContext ctx) throws Exception {
    Token<?> token = UserGroupInformation.getCurrentUser()
        .getCredentials()
        .getToken(new Text("vendor-service"));
    return vendorSts.exchangeForS3Credentials(token, ctx.getBucket(), ctx.getPath(), ctx.getMode());
}
```

Spark delegation token propagation is supported on YARN and Kubernetes only. Standalone deployments need a different refresh path, typically a vendor-side service callback authenticated by long-lived state in `catalogProperties` or Hadoop conf.

`expirationEpochMillis` only matters on the Iceberg/`opendal` path. There the bridge implements `reqsign_core::ProvideCredential`, which carries an `expires_in` field that `opendal` uses to schedule the next refresh. Publish a real expiry when you have one. `0` means "unknown"; the bridge then substitutes a 5-minute expiry to bound staleness.

The Parquet/`object_store` path has no expiry concept: `object_store::CredentialProvider` returns just `AwsCredential` (key/secret/token). The bridge is passed to `with_credentials` without a TTL wrapper, so `object_store` calls into the SPI on every request and relies on the vendor's own cache for hit rates. Expiry handling is fully the vendor's responsibility: the vendor decides when its internal cache refreshes. If `object_store` receives a 403 from an expired session token, its retry layer calls `get_credential()` again, giving the vendor another chance to mint fresh credentials.

### Returned fields

| Field                   | Notes                                                                                                                     |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `accessKeyId`           | Required.                                                                                                                 |
| `secretAccessKey`       | Required.                                                                                                                 |
| `sessionToken`          | `null` for non-STS credentials.                                                                                           |
| `expirationEpochMillis` | Iceberg path only. `0` means "unknown"; the bridge substitutes a 5-minute expiry. The Parquet path has no expiry concept. |

Provide a real `expirationEpochMillis` whenever you have one on the Iceberg path. The Parquet path's `object_store::CredentialProvider` does not consume an expiry, and the bridge invokes the SPI on every `get_credential()` call.

### Returns or throws

The SPI follows the same shape as the other JVM AWS-credential SPIs (AWS SDK v1/v2, Hadoop S3A, Iceberg `VendedCredentialsProvider`): return credentials or throw. There is no "fall-through" return value.

If your provider is authoritative only for some paths, resolve the default AWS chain yourself for the rest:

```java
private final DefaultCredentialsProvider defaultChain = DefaultCredentialsProvider.create();

@Override
public CometS3Credentials getCredentialsForPath(CometS3CredentialContext ctx) throws Exception {
    if (handlesPath(ctx.getBucket(), ctx.getPath())) {
        return mintFromMyVendorService(ctx.getBucket(), ctx.getPath(), ctx.getMode());
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
    public CometS3Credentials getCredentialsForPath(CometS3CredentialContext ctx) throws Exception {
        if (ctx.getBucket().startsWith("data-prod-"))   return prod.fetch(ctx);
        if (ctx.getBucket().equals("partner-shared"))   return sts.assumeRole(ctx);
        return fallback.fetch(ctx.getBucket(), ctx.getPath());
    }
}
```

Per-bucket Hadoop overrides (`fs.s3a.bucket.<name>.comet.credential.provider.class`) are also available if you prefer to ship multiple vendor classes and pick by bucket in config rather than in code.

For Iceberg deployments where two catalogs share one provider class but need isolated state, configure the same FQCN on both catalogs and read your discriminator from `initialize`'s `catalogProperties`. Each catalog gets its own provider instance because Comet keys by `(FQCN, catalogName, catalogProperties)`:

```java
public final class MyMultiTenantProvider implements CometS3CredentialProvider {
    private volatile String tenantId;

    @Override
    public void initialize(Map<String, String> catalogProperties) {
        this.tenantId = catalogProperties.get("vendor.tenant-id");
    }

    @Override
    public CometS3Credentials getCredentialsForPath(CometS3CredentialContext ctx) {
        return mintForTenant(tenantId, ctx.getBucket(), ctx.getPath(), ctx.getMode());
    }
}
```

### Reference implementation: Iceberg REST vended credentials

For Iceberg REST catalogs that vend AWS credentials (`LoadTableResponse.credentials`), the canonical implementation wraps Iceberg's existing `VendedCredentialsProvider`:

```java
public final class IcebergRESTVendedS3Provider implements CometS3CredentialProvider {
    private volatile VendedCredentialsProvider provider;

    @Override
    public void initialize(Map<String, String> catalogProperties) {
        this.provider = VendedCredentialsProvider.create(catalogProperties);
    }

    @Override
    public CometS3Credentials getCredentialsForPath(CometS3CredentialContext ctx) {
        AwsCredentials c = provider.resolveCredentials();
        String token = (c instanceof AwsSessionCredentials)
            ? ((AwsSessionCredentials) c).sessionToken() : null;
        return new CometS3Credentials(c.accessKeyId(), c.secretAccessKey(), token, 0L);
    }
}
```

`VendedCredentialsProvider` reads `credentials.uri`, the catalog endpoint, and OAuth tokens from the supplied map (Comet forwards the unfiltered FileIO bag to `initialize`), and refreshes through its own `CachedSupplier`. Caching, refresh-near-expiry, and the REST round-trip all live in Iceberg, not in Comet. Comet ships a copy of this class under `spark/src/test` as a reference; copy it into your runtime jar alongside `iceberg-aws` and AWS SDK v2.

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
  <artifactId>comet-spark-spark${spark.version.short}_${scala.binary.version}</artifactId>
  <version>${comet.version}</version>
  <scope>provided</scope>
</dependency>
```

### Iceberg path: error message fidelity

When the bridge is wired into `iceberg-rust`, the outer `reqsign-core::ProvideCredentialChain` currently swallows thrown exceptions into "no credential" before the request reaches `opendal`. The credential is still not issued and the request still fails, only the message is degraded to an opaque anonymous-request failure. No Comet change fixes this; it is resolved upstream if `iceberg-rust` stops wrapping custom loaders in its outer chain or moves its S3 backend to `object_store`.
