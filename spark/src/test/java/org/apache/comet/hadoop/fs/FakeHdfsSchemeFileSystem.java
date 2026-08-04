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

import java.net.URI;

import org.apache.hadoop.fs.RawLocalFileSystem;

/**
 * A local-disk-backed FileSystem that reports the {@code hdfs} scheme, so a test can write/read an
 * {@code hdfs://} path without a live HDFS cluster. Used to assert that {@code CometScanRule} still
 * claims an {@code hdfs://} scan when {@code spark.hadoop.fs.comet.libhdfs.schemes} is unset --
 * i.e. the JVM scheme gate's default stays in lockstep with the native {@code is_hdfs_scheme}
 * default.
 */
public class FakeHdfsSchemeFileSystem extends RawLocalFileSystem {

  public static final String PREFIX = "hdfs://fake-namenode";

  public FakeHdfsSchemeFileSystem() {
    // Avoid `URI scheme is not "file"` error on
    // RawLocalFileSystem$DeprecatedRawLocalFileStatus.getOwner
    RawLocalFileSystem.useStatIfAvailable();
  }

  @Override
  public String getScheme() {
    return "hdfs";
  }

  @Override
  public URI getUri() {
    return URI.create(PREFIX);
  }
}
