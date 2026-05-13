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
 * Access intent for a credential request issued by Comet's native code, passed to {@link
 * CometCloudCredentialProvider#getCredentialsForPath}.
 *
 * <p>Granularity is intentionally binary. Vendors that issue WRITE-scoped credentials are expected
 * to include READ permissions when their workflows require it (multipart-completion HEAD, Iceberg
 * manifest reads on the write path, etc.) — the SPI does not promise that a WRITE credential is
 * also read-capable; the vendor's IAM policy does.
 */
public enum CometAccessMode {
  /** GET / HEAD / LIST and equivalent. All Comet native scan paths request this today. */
  READ,
  /** PUT / POST / DELETE / multipart and equivalent. Reserved for future native write paths. */
  WRITE
}
