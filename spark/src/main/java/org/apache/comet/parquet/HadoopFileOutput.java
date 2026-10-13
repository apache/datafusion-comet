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

package org.apache.comet.parquet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Abortable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.parquet.hadoop.ParquetOutputFormat;

/**
 * A Parquet file that Comet's native writer writes through Hadoop's FileSystem API rather than
 * creating it natively.
 *
 * <p>The native writer creates local and HDFS files itself. Any other file is created here, the way
 * Spark's own Parquet writer creates it, so the FileSystem does everything it does for a file it
 * creates. S3A's magic committer depends on that: S3A turns a file created under a {@code __magic}
 * directory into a multipart upload to the file's final destination, which it leaves pending for
 * the committer to complete at job commit. It also applies the FileSystem's own write settings,
 * such as S3A's encryption, ACL, storage class and object headers.
 *
 * <p>The native writer writes the file's bytes through {@link #write}, one call at a time, from
 * whichever thread runs the write. The JVM then {@link #close closes} the file, or {@link #abort
 * aborts} it if the write failed.
 */
public final class HadoopFileOutput {
  private final Path path;
  private final FSDataOutputStream out;

  /** What {@link #write} threw. The native writer only passes on its text. */
  private volatile Throwable failure;

  private HadoopFileOutput(Path path, FSDataOutputStream out) {
    this.path = path;
    this.out = out;
  }

  /**
   * Creates {@code file} as Spark's Parquet writer does, through parquet-mr's {@code
   * HadoopOutputFile.create}: without overwriting an existing file, and with a block size of at
   * least {@code parquet.block.size}.
   */
  public static HadoopFileOutput create(String file, Configuration conf) throws IOException {
    Path path = new Path(file);
    FileSystem fs = path.getFileSystem(conf);
    long blockSize =
        Math.max(fs.getDefaultBlockSize(path), ParquetOutputFormat.getLongBlockSize(conf));
    FSDataOutputStream out =
        fs.create(path, false, 4096, fs.getDefaultReplication(path), blockSize);
    return new HadoopFileOutput(path, out);
  }

  /** Appends the first {@code length} bytes of {@code data} to the file. Called natively. */
  public void write(byte[] data, int length) throws IOException {
    try {
      out.write(data, 0, length);
    } catch (IOException | RuntimeException | Error e) {
      if (failure == null) {
        failure = e;
      }
      throw e;
    }
  }

  /** What the file's stream threw when the native writer wrote to it, or {@code null}. */
  public Throwable failure() {
    return failure;
  }

  /**
   * Completes the file once the native writer has written all of it. For a file under an S3A magic
   * committer directory, this is where S3A records the pending upload for the committer.
   */
  public void close() throws IOException {
    out.close();
  }

  /**
   * Discards the file after its write failed, attaching anything that goes wrong to {@code cause}
   * rather than throwing it. A stream that can abort does, so that S3A abandons the upload rather
   * than completing a truncated file. Any other stream is closed, as Spark's writer closes its file
   * on failure, and the commit protocol's {@code abortTask} removes what is left of it.
   */
  public void abort(Throwable cause) {
    try {
      if (out.hasCapability(StreamCapabilities.ABORTABLE_STREAM)) {
        Abortable.AbortableResult result = out.abort();
        if (result.anyCleanupException() != null) {
          cause.addSuppressed(result.anyCleanupException());
        }
      } else {
        out.close();
      }
    } catch (Throwable t) {
      if (t != cause) {
        cause.addSuppressed(t);
      }
    }
  }

  @Override
  public String toString() {
    return "HadoopFileOutput(" + path + ")";
  }
}
