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

# S3 Credential Provider SPI: Design Notes

This page captures why the `org.apache.comet.cloud.s3.CometS3CredentialProvider` SPI is shaped the way it is. The user-facing contract and operator setup live in the user guide page on S3 credential providers; this page is for maintainers and reviewers who want the design rationale.

## The gap the SPI fills

Comet's native scan paths (`object_store` for raw Parquet, `opendal` via `iceberg-rust` for Iceberg) bypass Spark's Hadoop S3A code path. That means credentials cannot flow through any of the contracts that vendors typically wire into for S3A:

- `org.apache.spark.deploy.security.cloud.CloudCredentialsProvider` yields a single JWT per service name. No path argument, no AWS credential.
- Hadoop S3A custom signers hide path-aware logic inside `Signer.sign(request, credentials)`. The credential never leaves the signing pipeline, and the underlying secret is an HMAC key that is not present in the signed output, so running the signer against a synthesized request cannot recover it.
- `AWSCredentialsProvider.getCredentials()` (AWS SDK v1) and `AwsCredentialsProvider.resolveCredentials()` (v2) are parameterless. They cannot vend per-path credentials.
- Reflecting into vendor singletons would encode per-vendor class names and lifecycles in Comet and would silently break on vendor upgrades.

A Comet-specific SPI is the narrowest fit: a single Java method that takes a `CometS3CredentialContext` (today wrapping `bucket`, `path`, and access `mode`; new fields can be added without breaking vendors compiled against earlier versions) and returns `CometS3Credentials`.

## Why config-driven activation, not `META-INF/services`

An earlier iteration used `ServiceLoader` discovery. That was rejected because:

- Peer SPIs in the same space (Hadoop `AWSCredentialsProvider`, AWS SDK v2 `AwsCredentialsProvider`, Iceberg `AwsClientFactory`, S3A custom signers) are all class-name-in-config. Vendors are already familiar with that model.
- ServiceLoader makes activation implicit on classpath presence. A vendor JAR drifting onto a cluster could silently change S3 auth behavior. The config key makes activation explicit.
- The activation key (`fs.s3a.comet.credential.provider.class`, with per-bucket override) follows the same shape as `fs.s3a.bucket.<name>.aws.credentials.provider`, so operators do not learn a new pattern.

Activation is modeled on `parquet.crypto.factory.class` (Parquet Modular Encryption KMS, see Comet [#2447](https://github.com/apache/datafusion-comet/pull/2447)): the user names a single vendor class and the vendor dispatches across multiple credential backends inside that class if they need to. This mirrors how Iceberg's `DecryptionPropertiesFactory` already behaves for Parquet keys.

## Why `(FQCN, dispatchKey, catalogProperties)` keying

Comet caches one provider instance per `(FQCN, dispatchKey, catalogProperties)` triple. The dispatch key is the Spark V2 catalog name on the Iceberg path and the bucket on the Parquet path.

- Two catalogs that share one provider class (typical in multi-tenant deployments) need isolated `initialize` maps because their `catalogProperties` differ. Without `dispatchKey`, the second `initialize` would either overwrite the first or be silently skipped.
- The bucket as `dispatchKey` for Parquet gives vendors per-bucket isolation when the same provider is named under several `fs.s3a.bucket.<name>.comet.credential.provider.class` keys.
- `catalogProperties` enters the key to handle multi-tenant JVMs (Spark Connect, Thrift Server, `SparkSession.newSession()`) where two sessions can configure the same provider class against the same `dispatchKey` but with different REST endpoints, OAuth tokens, or vendor keys. Without it the second session would silently use the first session's credentials.
- Keying solely by FQCN would force vendors to encode multi-tenant routing in static state. The triple-key keeps each call site independent.

`ensureInitialized` returns a `long` handle that the native bridge stashes and replays on every per-request call. Routing per-request lookups by handle avoids re-sending the property bag across JNI on the hot path and unambiguously selects the right provider when the same `(FQCN, dispatchKey)` pair maps to multiple instances.

## Why fresh construction in `initialize`, not probing a JVM-wide static

A provider implementation might be tempted to probe an existing static populated elsewhere (e.g. by a Hadoop S3A signer's `registerStore` callback) and reuse the credential cache that the Hadoop path uses. That fails on Comet-only executors:

- The driver JVM hits `S3AFileSystem.initialize` during analysis (raw `s3a://` paths) or during Hadoop catalog manifest reads (Iceberg with Hadoop catalog), so the static is populated there.
- The driver may not hit `S3AFileSystem` at all under Iceberg with REST catalog plus `S3FileIO`, because `S3FileIO` calls AWS SDK directly without going through the Hadoop layer. The static stays null.
- Executors with Comet-only reads never instantiate `S3AFileSystem`. The data path is `object_store` (raw Parquet) or `opendal` via `iceberg-rust` (Iceberg native scan). Neither touches Hadoop S3A. The static stays null on every executor.

Constructing a fresh provider from `catalogProperties` plus `SparkEnv` is the only strategy that works across all four cases. The trade-off is that on the driver (and any JVM where Hadoop S3A is also active), two credential caches now exist for the same identity: one inside the Hadoop signer's provider, one inside the SPI implementation's. The vendor pays for this with a small number of extra AS round-trips on cold starts and TTL boundaries. A future optional optimization could probe the static first and reuse if non-null, falling back to fresh construction otherwise.

## Why no Comet-side cache

Comet's bridge does not maintain a TTL cache, schedule refresh, or broadcast catalog state. All of that is the vendor's responsibility:

- Iceberg vendors get `software.amazon.awssdk.utils.cache.CachedSupplier` for free inside `org.apache.iceberg.aws.s3.VendedCredentialsProvider`.
- Custom-STS vendors write whatever cache fits their refresh model.
- Driver-only state is distributed via `initialize`'s `catalogProperties` (Iceberg path) or read from Hadoop conf via `SparkEnv` (Parquet path). Both are plan-time snapshots: Comet does not re-execute the catalog or push fresh values to running scans. Vendors that need a refreshing bearer compose with Spark's `HadoopDelegationTokenProvider`, which mints and renews on the driver and propagates to executors via `UserGroupInformation`. The two SPIs are orthogonal: Spark covers bearer lifecycle, this SPI covers path-aware AWS credential minting.

A Comet-side cache would have to either expose a tuning knob (TTL, max size, eviction policy) and grow over time, or be hardcoded and surprise vendors whose policies disagree. The bridge intentionally has neither and forwards every call.

## Scope-aware `object_store` registry

The base SPI hands Comet one credential per `(bucket, path)`. Vendors whose sessions are inherently scoped (STS session A covers `s3://bucket/prefix-A/**` but not `s3://bucket/prefix-B/**`) hit a subtle failure mode against the pre-scope `object_store` cache: the cache key was `(scheme://bucket, config_hash, hdfs_backend)`, so the *first* prefix's session bound at construction time was reused for every later prefix under the same bucket, and any later prefix that fell outside that session's scope 403'd on read.

The opt-in `CometS3ScopedCredentialProvider` sub-interface + a correctness safety net on top of the cache resolves this without changing the base contract:

1. **Advisory scope hint on the read path.** When Comet detects that the configured provider implements the sub-interface, it calls `getPolicyLocationsFor(context)` immediately after resolving the SPI class and before looking the store up in the cache. The returned prefixes are attached to each cache entry as a `ScopeEntry { prefixes, store }`; the outer key stays `(scheme://bucket, config_hash, hdfs_backend)`, but the value becomes `Vec<ScopeEntry>` and the read path picks the entry whose covering prefix is the *longest* match for the requested path (ties break by insertion order). An empty prefix list is treated as "catchall" and matches with effective length 0, so any properly-scoped covering entry wins — this preserves the pre-existing single-entry-per-bucket semantics for base-interface providers while letting scoped and catchall entries coexist under one key.

2. **`RetryOn403ObjectStore` safety net.** Every scoped store is wrapped in a thin `ObjectStore` decorator that intercepts `Error::PermissionDenied` (403) exactly once, invokes a caller-supplied rebuild closure, and retries the same operation against the rebuilt store. On a second 403 the error propagates. Only 403 triggers the retry: `Unauthenticated` (401) is treated as a permanent credential-config error and passes through. The rebuild closure receives the `Path` that triggered the 403, constructs a fresh bridge bound to that path, re-fires the SPI (so the vendor's `getPolicyLocationsFor` reflects the *actual* failing request), and appends a new `ScopeEntry` to the outer cache alongside the pre-existing entries. Longest-prefix match on the lookup side then routes subsequent requests inside the new scope to the fresh store, while requests inside the pre-existing scope continue to reuse the original — disjoint scoped stores on the same bucket coexist across 403 recoveries.

The wrapper's scope of interception is deliberately narrow: `put_opts`, `get_opts`, `get_ranges`, `list_with_delimiter`, `copy_opts`, `rename_opts`. Stream variants (`list`, `list_with_offset`, `delete_stream`) surface per-item errors as-is, since per-item retry would require materializing the stream and there is no observed failure mode outside `get_opts`/`get_ranges` on the parquet path. `put_multipart_opts` also skips retry — a partially-uploaded multipart cannot be transparently restarted. For `copy_opts` and `rename_opts` the read-side (source) path is what the rebuild is scoped against.

**Why not fold scope prefixes into the outer cache key.** The obvious alternative shape — extend `ObjectStoreCacheKey` to `(bucket, config_hash, hdfs_backend, Vec<String>)` — was rejected because the same underlying store is still a valid answer for any *sub*-prefix of a scope. Keying on the exact prefix list would fragment the cache along every axis vendors report and defeat sharing on the common path. Threading the prefixes onto the entry, not the key, keeps the identity of the underlying store stable while letting the read path pick the correct scope per request.

**Why the safety net at all.** The SPI is authored per-vendor; overreporting is easy to write and hard to detect at plan time. The 403-retry wrapper turns a vendor-side scope bug into a correctness-preserving performance cost (one round-trip + one new cache entry) rather than a user-visible failure. It also handles the legitimate case where a scope was accurate at plan time and expired mid-scan.

## Path-specific behavior

`object_store::CredentialProvider` and `reqsign_core::ProvideCredential` differ in what they consume:

| Concern                 | Parquet (`object_store`)                           | Iceberg (`opendal` via `reqsign-core`)                       |
| ----------------------- | -------------------------------------------------- | ------------------------------------------------------------ |
| Trait method            | `get_credential() -> AwsCredential`                | `provide_credential(...) -> Option<IcebergAwsCredential>`    |
| Returns expiry?         | No (only key/secret/token)                         | Yes (`expires_in: Option<Timestamp>`)                        |
| Comet-side TTL wrapper? | None. Bridge passed straight to `with_credentials` | None. `opendal` schedules the next refresh from `expires_in` |
| When SPI is called      | Every `get_credential()` call                      | When `expires_in` is exceeded                                |
| Vendor returns 0 expiry | Field has no use                                   | Bridge substitutes 5 minutes to bound staleness              |

The 5-minute fallback is a safety net so a vendor that omits expiry cannot leave `opendal` caching a stale token indefinitely. It is intentionally not a configuration knob.

## Property-bag handling on the Iceberg path

The full unfiltered FileIO property bag crosses JNI as `catalog_properties`. The storage-prefix filter (`s3.`/`gcs.`/`adls.`/`client.`) is applied native-side in `iceberg_scan.rs::load_file_io` immediately before `FileIOBuilder.with_prop`. This means the bridge sees `credentials.uri`, OAuth tokens, and any vendor-custom keys with no parallel field on the operator and no driver-side broadcast. Vendors set their own keys on the catalog config and read them back inside `initialize(Map)`.

`IcebergScanExec` derives a redacting `Debug` so plan dumps and tracing do not leak the property bag.

## Returns or throws, not a fall-through value

The SPI returns a `CometS3Credentials` or throws. There is no sentinel "I do not know" return. Vendors that are only authoritative for some paths resolve the default AWS chain themselves for the rest and return the result. This matches the contract on every other AWS credential SPI in the JVM ecosystem (AWS SDK v1/v2, Hadoop S3A, Iceberg `VendedCredentialsProvider`).

## Lifecycle: `AutoCloseable` plus a JVM shutdown hook

`CometS3CredentialProvider` extends `AutoCloseable` with a default no-op `close()`. The dispatcher installs a JVM shutdown hook that iterates every cached instance and calls `close()`, swallowing per-provider exceptions so a slow or buggy vendor cannot block other providers from cleaning up. Stateless providers ignore this entirely; vendors that hold long-lived resources (HTTP clients, scheduled-refresh executors, STS connection pools) override `close()` to release them. Shutdown hooks are best-effort, so a `SIGKILL` or abrupt JVM termination skips them; vendors must not rely on `close()` for correctness, only for resource hygiene.

## Iceberg path: error message fidelity caveat

When the bridge is wired into `iceberg-rust`, the outer `reqsign-core::ProvideCredentialChain` currently swallows thrown exceptions into "no credential" before the request reaches `opendal`. The credential is still not issued and the request still fails, but the message is degraded to an opaque anonymous-request failure. No Comet change fixes this; it is resolved upstream when `iceberg-rust` stops wrapping custom loaders in its outer chain or moves its S3 backend to `object_store`.
