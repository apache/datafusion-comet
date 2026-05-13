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

package org.apache.comet.cloud;

/**
 * SPI for supplying AWS credentials to Comet's native S3 readers.
 *
 * <p>Comet's native scan paths ({@code object_store} for raw Parquet, {@code opendal} via {@code
 * iceberg-rust} for Iceberg) bypass Spark's Hadoop S3A code path. The standard {@code
 * AWSCredentialsProvider.getCredentials()} contract has no path argument, so vendors that issue
 * per-path STS credentials cannot expose them through that interface. This SPI fills the gap.
 *
 * <p>Vendors register an implementation via {@code
 * META-INF/services/org.apache.comet.cloud.CometCloudCredentialProvider}. Comet discovers it at
 * executor startup and routes every per-request credential fetch through it.
 *
 * <p>Implementations must be thread-safe; {@link #getCredentialsForPath} may be invoked
 * concurrently from many native tokio tasks.
 *
 * <p>Contract: returns credentials or throws. There is no "fall through to the default chain"
 * return value; if a provider is registered, it is responsible for every credential fetch on the
 * paths it sees. See the contributor guide section on cloud credential providers for the rationale
 * and patterns for vendors that need to defer to a default credential chain on a subset of paths.
 */
public interface CometCloudCredentialProvider {

  /**
   * Returns credentials usable to sign an S3 request for the given path.
   *
   * @param bucket the S3 bucket name (no scheme, no path)
   * @param path the object key or prefix being accessed; the URL path of the store, leading slash
   *     included
   * @param mode the access intent for this credential request
   * @return non-null credentials
   * @throws Exception any failure surfaces to the native caller and aborts the request
   */
  CometCredentials getCredentialsForPath(String bucket, String path, CometAccessMode mode)
      throws Exception;
}
