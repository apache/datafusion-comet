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

package org.apache.comet.iceberg;

import java.io.Serializable;
import java.util.Map;

/**
 * Pluggable, catalog-scoped credential provider for Comet's native Iceberg reader.
 *
 * <p>Configure via: {@code spark.comet.scan.icebergNative.credentialProvider.class}
 */
public interface CometCredentialProvider extends Serializable {

  /**
   * Initialize this provider with catalog-level properties.
   *
   * @param catalogProperties the catalog-level FileIO properties
   */
  void initialize(Map<String, String> catalogProperties);

  /**
   * Returns {@code [accessKeyId, secretAccessKey, sessionToken, expiresAtEpochMs]}. slot 3 may be
   * {@code "-1"} for no-expiry.
   */
  String[] resolveCredentials(ResolveContext context);
}
