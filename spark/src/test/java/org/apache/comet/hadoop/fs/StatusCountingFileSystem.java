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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

/**
 * A local-disk-backed FileSystem for the {@code statuscount} scheme that counts the {@code
 * getFileStatus} calls made on it, the metadata lookups that cost a HEAD request each on an object
 * store. Opening and listing go to the wrapped local file system, so the status lookups that the
 * local file system makes internally for those are not counted.
 */
public class StatusCountingFileSystem extends FilterFileSystem {

  public static final String SCHEME = "statuscount";
  public static final String PREFIX = SCHEME + "://bucket";

  private static final AtomicInteger GET_FILE_STATUS_CALLS = new AtomicInteger();

  public StatusCountingFileSystem() {
    super(new LocalBackend());
  }

  public static int getFileStatusCalls() {
    return GET_FILE_STATUS_CALLS.get();
  }

  public static void resetGetFileStatusCalls() {
    GET_FILE_STATUS_CALLS.set(0);
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    GET_FILE_STATUS_CALLS.incrementAndGet();
    return super.getFileStatus(f);
  }

  /** The wrapped local file system. It answers for the same scheme, so paths pass through. */
  private static class LocalBackend extends RawLocalFileSystem {

    LocalBackend() {
      // Avoid `URI scheme is not "file"` error on
      // RawLocalFileSystem$DeprecatedRawLocalFileStatus.getOwner
      RawLocalFileSystem.useStatIfAvailable();
    }

    @Override
    public String getScheme() {
      return SCHEME;
    }

    @Override
    public URI getUri() {
      return URI.create(PREFIX);
    }
  }
}
