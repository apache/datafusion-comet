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
 * <p>Comet keys provider instances by {@code (FQCN, dispatchKey)}, where {@code dispatchKey} is the
 * Spark V2 catalog name on the Iceberg path and the bucket on the Parquet path. The first time a
 * given key is seen on an executor, Comet reflects the class, calls {@link #initialize(Map)} once,
 * and caches the instance. Two catalogs that share one FQCN therefore get isolated instances with
 * their own {@code initialize} maps.
 *
 * <p>{@link #initialize(Map)} should be cheap and non-blocking; defer real credential fetches to
 * the first {@link #getCredentialsForPath} call. {@link #getCredentialsForPath} may be invoked
 * concurrently from many native worker threads, so implementations must be thread-safe.
 *
 * <p>Comet does not maintain a TTL cache, broadcast catalog state, or schedule refresh. Vendors
 * own caching, refresh, and any executor-side state distribution. Returns credentials or throws;
 * there is no fall-through return value. See the user guide on cloud credential providers for the
 * full contract and examples.
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
