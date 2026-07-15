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

import java.util.Objects;

/**
 * Per-request context passed to {@link
 * CometS3CredentialProvider#getCredentialsForPath(CometS3CredentialContext)}. New fields can be
 * added here without changing the SPI method signature, so vendors compiled against earlier
 * versions stay binary-compatible.
 */
public final class CometS3CredentialContext {

  private final String bucket;
  private final String path;
  private final CometS3AccessMode mode;

  public CometS3CredentialContext(String bucket, String path, CometS3AccessMode mode) {
    this.bucket = Objects.requireNonNull(bucket, "bucket");
    this.path = Objects.requireNonNull(path, "path");
    this.mode = Objects.requireNonNull(mode, "mode");
  }

  /** S3 bucket name (no scheme, no path). */
  public String getBucket() {
    return bucket;
  }

  /** Object key or prefix, leading slash included (matches the URL path component). */
  public String getPath() {
    return path;
  }

  /** Access intent for this request. */
  public CometS3AccessMode getMode() {
    return mode;
  }

  @Override
  public String toString() {
    return "CometS3CredentialContext{bucket=" + bucket + ", path=" + path + ", mode=" + mode + "}";
  }
}
