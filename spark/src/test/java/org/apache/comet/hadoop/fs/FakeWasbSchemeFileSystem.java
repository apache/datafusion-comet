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
 * A local-disk-backed FileSystem that reports the {@code wasb} scheme, so a test can create and
 * read an Iceberg table under a {@code wasb://} warehouse without Azure. Used to assert that {@code
 * CometScanRule} declines a native Iceberg scan whose storage scheme object_store recognizes but
 * the native Iceberg storage factory cannot build, instead of failing at execution.
 */
public class FakeWasbSchemeFileSystem extends RawLocalFileSystem {

  public static final String PREFIX = "wasb://fake-container";

  public FakeWasbSchemeFileSystem() {
    // Avoid `URI scheme is not "file"` error on
    // RawLocalFileSystem$DeprecatedRawLocalFileStatus.getOwner
    RawLocalFileSystem.useStatIfAvailable();
  }

  @Override
  public String getScheme() {
    return "wasb";
  }

  @Override
  public URI getUri() {
    return URI.create(PREFIX);
  }
}
