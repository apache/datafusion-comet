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

package org.apache.comet.contrib.delta

import java.net.URI

import org.apache.hadoop.fs.RawLocalFileSystem

/**
 * A local-disk-backed FileSystem that reports the `s3a` scheme, so a contrib-delta test can
 * write/read an `s3a://` path without a live S3 endpoint. Used to exercise credential-extraction
 * paths that key off the table's URI scheme (`NativeConfig.extractObjectStoreOptions` only bridges
 * `fs.s3a.*` for `s3`/`s3a` table roots), e.g. asserting the Delta CDF scan ships the bridged S3
 * credentials in its proto. `RawLocalFileSystem` ignores the authority and maps the URI path
 * component to a local path, so address a local temp dir as `s3a://fakebucket<tmpDir>`.
 *
 * Contrib-only (no core test uses it), so it lives under `contrib/delta/src/test/scala` rather
 * than core's `spark/src/test/java` alongside `FakeHdfsSchemeFileSystem`.
 */
class FakeS3aSchemeFileSystem extends RawLocalFileSystem {
  RawLocalFileSystem.useStatIfAvailable()
  override def getScheme: String = "s3a"
  override def getUri: URI = URI.create(FakeS3aSchemeFileSystem.PREFIX)
}

object FakeS3aSchemeFileSystem {
  val PREFIX = "s3a://fakebucket"
}
