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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.VersionInfo;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * A Parquet {@link InputFile} implementation that's similar to {@link
 * org.apache.parquet.hadoop.util.HadoopInputFile}, but with optimizations introduced in Hadoop 3.x,
 * for S3 specifically.
 */
public class CometInputFile implements InputFile {
  private static final String MAJOR_MINOR_REGEX = "^(\\d+)\\.(\\d+)(\\..*)?$";
  private static final Pattern VERSION_MATCHER = Pattern.compile(MAJOR_MINOR_REGEX);

  private final FileSystem fs;
  private final FileStatus stat;
  private final Configuration conf;

  public static CometInputFile fromPath(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return new CometInputFile(fs, fs.getFileStatus(path), conf);
  }

  private CometInputFile(FileSystem fs, FileStatus stat, Configuration conf) {
    this.fs = fs;
    this.stat = stat;
    this.conf = conf;
  }

  @Override
  public long getLength() {
    return stat.getLen();
  }

  public Configuration getConf() {
    return this.conf;
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  public Path getPath() {
    return stat.getPath();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    FSDataInputStream stream;
    try {
      if (isAtLeastHadoop33()) {
        // If Hadoop version is >= 3.3.x, we'll use the 'openFile' API which can save a
        // HEAD request from cloud storages like S3
        FutureDataInputStreamBuilder inputStreamBuilder =
            fs.openFile(stat.getPath()).withFileStatus(stat);

        if (stat.getPath().toString().startsWith("s3a")) {
          // Switch to random S3 input policy so that we don't do sequential read on the entire
          // S3 object. By default, the policy is normal which does sequential read until a back
          // seek happens, which in our case will never happen.
          inputStreamBuilder =
              inputStreamBuilder.opt("fs.s3a.experimental.input.fadvise", "random");
        }
        stream = inputStreamBuilder.build().get();
      } else {
        stream = fs.open(stat.getPath());
      }
    } catch (Exception e) {
      throw new IOException("Error when opening file " + stat.getPath(), e);
    }
    return HadoopStreams.wrap(stream);
  }

  public SeekableInputStream newStream(long offset, long length) throws IOException {
    try {
      FSDataInputStream stream;
      if (isAtLeastHadoop33()) {
        FutureDataInputStreamBuilder inputStreamBuilder =
            fs.openFile(stat.getPath()).withFileStatus(stat);

        if (stat.getPath().toString().startsWith("s3a")) {
          // Switch to random S3 input policy so that we don't do sequential read on the entire
          // S3 object. By default, the policy is normal which does sequential read until a back
          // seek happens, which in our case will never happen.
          //
          // Also set read ahead length equal to the column chunk length so we don't have to open
          // multiple S3 http connections.
          inputStreamBuilder =
              inputStreamBuilder
                  .opt("fs.s3a.experimental.input.fadvise", "random")
                  .opt("fs.s3a.readahead.range", Long.toString(length));
        }

        stream = inputStreamBuilder.build().get();
      } else {
        stream = fs.open(stat.getPath());
      }
      return HadoopStreams.wrap(stream);
    } catch (Exception e) {
      throw new IOException(
          "Error when opening file " + stat.getPath() + ", offset=" + offset + ", length=" + length,
          e);
    }
  }

  @Override
  public String toString() {
    return stat.getPath().toString();
  }

  private static boolean isAtLeastHadoop33() {
    String version = VersionInfo.getVersion();
    return CometInputFile.isAtLeastHadoop33(version);
  }

  static boolean isAtLeastHadoop33(String version) {
    Matcher matcher = VERSION_MATCHER.matcher(version);
    if (matcher.matches()) {
      if (matcher.group(1).equals("3")) {
        int minorVersion = Integer.parseInt(matcher.group(2));
        return minorVersion >= 3;
      }
    }
    return false;
  }
}
