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
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Abortable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.util.Progressable;

/**
 * A local-disk-backed FileSystem under its own scheme that records every file created through it
 * and how each file's stream ended, and can fail writes and closes on demand. Its streams can
 * abort, as S3A's do, and an aborted file is deleted, as an aborted S3A upload never becomes an
 * object.
 *
 * <p>It lets a test check that a file was created through Hadoop's FileSystem API without a remote
 * store: Comet's native writer creates only local and HDFS files itself. The recordings are static,
 * since FileSystem instances are cached and the tests run in local mode.
 */
public class RecordingFileSystem extends RawLocalFileSystem {

  public static final String SCHEME = "comet-recording";

  public static final String PREFIX = SCHEME + "://recording";

  /** The message of the {@link IOException} a failing write throws. */
  public static final String WRITE_FAILURE = "injected output stream failure";

  /** The message of the {@link IOException} a failing close throws. */
  public static final String CLOSE_FAILURE = "injected output stream close failure";

  /** The files created through this FileSystem, in order. */
  public static final List<String> created = new CopyOnWriteArrayList<>();

  /** The files whose stream was closed. */
  public static final List<String> closed = new CopyOnWriteArrayList<>();

  /** The files whose stream was aborted. */
  public static final List<String> aborted = new CopyOnWriteArrayList<>();

  /** The length of every write to a stream, in order, across all files. */
  public static final List<Integer> writeLengths = new CopyOnWriteArrayList<>();

  /** Every write fails. */
  public static volatile boolean failWrites = false;

  /** When positive, the write with this number, counting from 1 in each file, fails. */
  public static volatile int failAtWrite = 0;

  /** Every close fails, after closing the file underneath. */
  public static volatile boolean failCloses = false;

  /** Whether streams report that they can abort, as S3A's do. */
  public static volatile boolean abortable = true;

  /**
   * How many more files the failures above apply to. Each file created while it is positive takes
   * one, and files created once it reaches 0 write and close normally.
   */
  public static final AtomicInteger failingFiles = new AtomicInteger(Integer.MAX_VALUE);

  public static void reset() {
    created.clear();
    closed.clear();
    aborted.clear();
    writeLengths.clear();
    failWrites = false;
    failAtWrite = 0;
    failCloses = false;
    abortable = true;
    failingFiles.set(Integer.MAX_VALUE);
  }

  public RecordingFileSystem() {
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

  @Override
  public FSDataOutputStream create(
      Path f,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    return record(f, super.create(f, overwrite, bufferSize, replication, blockSize, progress));
  }

  private FSDataOutputStream record(Path path, FSDataOutputStream out) {
    created.add(path.toString());
    return new FSDataOutputStream(new RecordingStream(path, out), statistics);
  }

  private final class RecordingStream extends OutputStream
      implements Abortable, StreamCapabilities {
    private final Path path;
    private final OutputStream out;
    private final boolean failing = failingFiles.getAndDecrement() > 0;
    private int writes = 0;
    private boolean finished = false;

    RecordingStream(Path path, OutputStream out) {
      this.path = path;
      this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
      checkWritable(1);
      out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      checkWritable(len);
      out.write(b, off, len);
    }

    private void checkWritable(int length) throws IOException {
      writes++;
      if (failing && (failWrites || writes == failAtWrite)) {
        throw new IOException(WRITE_FAILURE);
      }
      writeLengths.add(length);
    }

    @Override
    public void close() throws IOException {
      if (!finished) {
        finished = true;
        out.close();
        if (failing && failCloses) {
          throw new IOException(CLOSE_FAILURE);
        }
        closed.add(path.toString());
      }
    }

    @Override
    public AbortableResult abort() {
      boolean alreadyFinished = finished;
      IOException cleanupFailure = null;
      if (!finished) {
        finished = true;
        try {
          out.close();
          delete(path, false);
        } catch (IOException e) {
          cleanupFailure = e;
        }
        aborted.add(path.toString());
      }
      IOException failure = cleanupFailure;
      return new AbortableResult() {
        @Override
        public boolean alreadyClosed() {
          return alreadyFinished;
        }

        @Override
        public IOException anyCleanupException() {
          return failure;
        }
      };
    }

    @Override
    public boolean hasCapability(String capability) {
      return abortable && StreamCapabilities.ABORTABLE_STREAM.equals(capability);
    }
  }
}
