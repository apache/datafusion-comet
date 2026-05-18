/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.cloud.s3;

import java.util.Map;

/**
 * SPI for supplying AWS credentials to Comet's native S3 readers, which bypass Spark's Hadoop S3A
 * code path and cannot reach signer-based or path-aware credential mechanisms through the standard
 * parameterless {@code AWSCredentialsProvider.getCredentials()} contract.
 *
 * <p>Peer to {@code org.apache.hadoop.fs.s3a.AwsSignerInitializer} (Hadoop S3A) and {@code
 * org.apache.iceberg.aws.AwsClientFactory} (Iceberg-Java): the same shape vendors already implement
 * for those two, with a smaller surface (one method).
 *
 * <h2>Why a new SPI?</h2>
 *
 * No existing contract carries per-path AWS credentials from vendor code to Comet's native readers:
 *
 * <ul>
 *   <li>{@code org.apache.spark.deploy.security.cloud.CloudCredentialsProvider} yields a single JWT
 *       per service name. No path argument and does not return AWS credentials.
 *   <li>Hadoop S3A custom signers hide path-aware logic inside {@code Signer.sign(request,
 *       credentials)}. Credentials never leave the signing pipeline, and the underlying secret key
 *       is an HMAC key (not present in the signed output), so running the signer on a synthesized
 *       request cannot recover it.
 *   <li>Reflecting into vendor singletons encodes per-vendor class and lifecycle details in Comet
 *       and breaks silently on vendor upgrades.
 *   <li>A Comet-specific HTTP STS endpoint would push a new public API onto every vendor; vendors
 *       ship this logic as Java code, not HTTP.
 * </ul>
 *
 * <p>Vendors register an implementation by setting {@code
 * spark.hadoop.fs.s3a.comet.credential.provider.class} (or the per-bucket form {@code
 * spark.hadoop.fs.s3a.bucket.<name>.comet.credential.provider.class}) for the Parquet path, or
 * {@code spark.sql.catalog.<catalog>.s3.comet.credential.provider.class} for the Iceberg path. The
 * class must have a public no-arg constructor.
 *
 * <h2>Lifecycle</h2>
 *
 * <p>Comet keys provider instances by {@code (FQCN, dispatchKey)}, where {@code dispatchKey} is the
 * Spark V2 catalog name on the Iceberg path and the bucket on the Parquet path. The first time a
 * given key is seen on an executor, Comet reflects the class, calls {@link #initialize(Map)} once,
 * and caches the instance. Subsequent requests for the same key reuse it. Two catalogs that share
 * one FQCN therefore get isolated instances with their own {@code initialize} maps.
 *
 * <p>{@link #initialize(Map)} should be cheap and non-blocking; defer real credential fetches to
 * the first {@link #getCredentialsForPath} call. {@link #getCredentialsForPath} may be invoked
 * concurrently from many native tokio worker threads, so implementations must be thread-safe.
 *
 * <h2>Caching, refresh, and distribution are the vendor's job</h2>
 *
 * <p>Comet does not maintain a TTL cache, broadcast catalog state, or schedule refresh. Vendors
 * decide whether to cache (e.g. by wrapping {@code
 * org.apache.iceberg.aws.s3.VendedCredentialsProvider}'s {@code CachedSupplier}), when to refresh,
 * and how to distribute driver-side state to executors (typically by reading {@link #initialize}'s
 * {@code catalogProperties}, which Comet has already serialized through the native plan op).
 *
 * <p>Returns credentials or throws; there is no fall-through return value. A provider that is only
 * authoritative for some paths should resolve the default AWS chain itself for the rest. See the
 * user guide on cloud credential providers.
 */
public interface CometS3CredentialProvider {

  /**
   * Called once per {@code (FQCN, dispatchKey)} on each executor before any {@link
   * #getCredentialsForPath} call. The {@code catalogProperties} map carries the full FileIO
   * property bag for the Iceberg path (including {@code credentials.uri}, OAuth tokens, vendor keys
   * like {@code tenant-id}) and is empty on the Parquet path. The default no-op keeps Parquet
   * vendors source-compatible.
   *
   * @param catalogProperties unfiltered FileIO/catalog properties; may contain secrets, do not log
   */
  default void initialize(Map<String, String> catalogProperties) {}

  /**
   * @param bucket S3 bucket name (no scheme, no path)
   * @param path object key or prefix, leading slash included (matches the URL path component)
   * @param mode access intent for this request
   * @return non-null credentials; {@code null} is a contract violation
   */
  CometS3Credentials getCredentialsForPath(String bucket, String path, CometS3AccessMode mode)
      throws Exception;
}
