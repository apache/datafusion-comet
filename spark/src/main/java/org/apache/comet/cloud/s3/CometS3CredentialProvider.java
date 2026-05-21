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
 * code path. Vendors implement this when path-aware or vendor-managed credential mechanisms cannot
 * be reached through the standard parameterless {@code AWSCredentialsProvider.getCredentials()}
 * contract.
 *
 * <p>Vendors register an implementation by setting {@code
 * spark.hadoop.fs.s3a.comet.credential.provider.class} (or the per-bucket form {@code
 * spark.hadoop.fs.s3a.bucket.<name>.comet.credential.provider.class}) for the Parquet path, or
 * {@code spark.sql.catalog.<catalog>.s3.comet.credential.provider.class} for the Iceberg path. The
 * class must have a public no-arg constructor.
 *
 * <p>{@link #initialize(Map)} runs once per Comet-cached instance before any {@link
 * #getCredentialsForPath} call, must be cheap and non-blocking, and may receive secrets in its map.
 * {@link #getCredentialsForPath} may be invoked concurrently from many native worker threads so
 * implementations must be thread-safe; it returns credentials or throws (no fall-through).
 *
 * <p>See the user guide on S3 credential providers for caching, refresh, and multi-tenant isolation
 * guidance.
 */
public interface CometS3CredentialProvider extends AutoCloseable {

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
   * @param context per-request context (bucket, path, access mode). Fields can be added to {@link
   *     CometS3CredentialContext} in future Comet releases without changing this method signature,
   *     so vendors compiled against today's API stay binary-compatible.
   * @return non-null credentials; {@code null} is a contract violation
   */
  CometS3Credentials getCredentialsForPath(CometS3CredentialContext context) throws Exception;

  /**
   * Releases vendor-owned resources (HTTP clients, refresh executors, STS connection pools).
   * Invoked from a best-effort JVM shutdown hook installed by the dispatcher; default no-op suits
   * stateless providers. The hook swallows exceptions thrown here.
   */
  @Override
  default void close() throws Exception {}
}
