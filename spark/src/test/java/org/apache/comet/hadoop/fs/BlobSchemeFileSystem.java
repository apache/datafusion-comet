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

package org.apache.comet.hadoop.fs;

import org.apache.hadoop.fs.s3a.S3AFileSystem;

/**
 * An {@link S3AFileSystem} that reports the {@code blob} scheme, so a test can read/write a {@code
 * blob://} path against MinIO without a real vendor connector. Bind via {@code
 * spark.hadoop.fs.blob.impl}; S3A derives the bucket from the URI host and reads the same {@code
 * fs.s3a.*} surface, so no extra config is needed beyond what the s3a suites set.
 *
 * <p>Overriding {@link #getScheme()} mirrors {@link FakeHdfsSchemeFileSystem}. {@code
 * S3AFileSystem#checkPath} compares against the initialize-time URI (scheme {@code blob}), not
 * {@code getScheme()}, so a {@code blob://} path is accepted.
 */
public class BlobSchemeFileSystem extends S3AFileSystem {

  @Override
  public String getScheme() {
    return "blob";
  }
}
