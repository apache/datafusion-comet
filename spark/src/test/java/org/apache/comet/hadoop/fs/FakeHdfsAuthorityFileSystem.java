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

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;

/**
 * A local-disk-backed FileSystem for the {@code hdfs} scheme that keeps the authority it is
 * initialized with, so {@code hdfs://nn1/...} and {@code hdfs://nn2/...} are two file systems (two
 * native object stores) whose paths both resolve on the local disk.
 */
public class FakeHdfsAuthorityFileSystem extends RawLocalFileSystem {
  private static final URI DEFAULT_URI = URI.create("hdfs://fake-namenode");

  // Unset while the superclass constructor runs, which already asks for the URI.
  private URI uri;

  public FakeHdfsAuthorityFileSystem() {
    // Avoid `URI scheme is not "file"` error on
    // RawLocalFileSystem$DeprecatedRawLocalFileStatus.getOwner
    RawLocalFileSystem.useStatIfAvailable();
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    uri = URI.create("hdfs://" + name.getAuthority());
  }

  @Override
  public String getScheme() {
    return "hdfs";
  }

  @Override
  public URI getUri() {
    return uri == null ? DEFAULT_URI : uri;
  }
}
