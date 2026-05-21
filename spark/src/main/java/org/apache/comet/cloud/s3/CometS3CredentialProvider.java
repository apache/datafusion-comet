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
 * SPI for supplying AWS credentials to Comet's native S3 readers, which bypass Hadoop S3A. See the
 * user guide (operator setup, vendor contract) and the contributor-guide design notes for the
 * rationale.
 *
 * <p>{@link #getCredentialsForPath} may be invoked concurrently from many native worker threads;
 * implementations must be thread-safe. It returns credentials or throws (no fall-through).
 */
public interface CometS3CredentialProvider extends AutoCloseable {

  /**
   * Called once per Comet-cached instance before any {@link #getCredentialsForPath} call. Must be
   * cheap and non-blocking. On the Iceberg path the map carries the unfiltered FileIO bag; on the
   * Parquet path it is empty.
   *
   * @param catalogProperties may contain secrets, do not log
   */
  default void initialize(Map<String, String> catalogProperties) {}

  /**
   * @return non-null credentials; {@code null} is a contract violation
   */
  CometS3Credentials getCredentialsForPath(CometS3CredentialContext context) throws Exception;

  /**
   * Invoked from the dispatcher's best-effort JVM shutdown hook. Default no-op suits stateless
   * providers; override to release HTTP clients, refresh executors, etc.
   */
  @Override
  default void close() throws Exception {}
}
