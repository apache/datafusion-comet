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
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class JniHDFSBridge {
  /**
   * Reads a byte range from a file using Hadoop FileSystem API.
   *
   * @param path The file path to read from
   * @param configs Configuration properties for the filesystem
   * @param offset Starting byte position (0-based)
   * @param len Number of bytes to read
   * @return Byte array containing the read data, or null if error occurs
   * @throws IllegalArgumentException If parameters are invalid
   */
  public static byte[] read(String path, Map<String, String> configs, long offset, int len) {
    if (path == null || path.isEmpty()) {
      throw new IllegalArgumentException("Path cannot be null or empty");
    }
    if (offset < 0) {
      throw new IllegalArgumentException("Offset cannot be negative");
    }
    if (len < 0) {
      throw new IllegalArgumentException("Length cannot be negative");
    }

    try {
      Path p = new Path(path);
      Configuration conf = new Configuration();

      // Set configurations if provided
      if (configs != null) {
        for (Map.Entry<String, String> entry : configs.entrySet()) {
          conf.set(entry.getKey(), entry.getValue());
        }
      }
      FileSystem fs = p.getFileSystem(conf);

      long fileLen = fs.getFileStatus(p).getLen();

      if (offset > fileLen) {
        throw new IOException(
            "Offset beyond file length: offset=" + offset + ", fileLen=" + fileLen);
      }

      if (len == 0) {
        return new byte[0];
      }

      // Adjust length if it exceeds remaining bytes
      if (offset + len > fileLen) {
        len = (int) (fileLen - offset);
        if (len <= 0) {
          return new byte[0];
        }
      }

      FSDataInputStream inputStream = fs.open(p);
      inputStream.seek(offset);
      byte[] buffer = new byte[len];
      int totalBytesRead = 0;
      while (totalBytesRead < len) {
        int read = inputStream.read(buffer, totalBytesRead, len - totalBytesRead);
        if (read == -1) break;
        totalBytesRead += read;
      }
      inputStream.close();

      return totalBytesRead < len ? Arrays.copyOf(buffer, totalBytesRead) : buffer;
    } catch (Exception e) {
      System.err.println("Native.read failed: " + e);
      return null;
    }
  }

  /**
   * Gets the length of a file using Hadoop FileSystem API.
   *
   * @param path The file path to check
   * @param configs Configuration properties for the filesystem
   * @return File length in bytes, or -1 if the file doesn't exist
   * @throws IllegalArgumentException If path is invalid or configs contain invalid values
   */
  public static long getLength(String path, Map<String, String> configs) {
    if (path == null || path.isEmpty()) {
      throw new IllegalArgumentException("Path cannot be null or empty");
    }

    try {
      Path p = new Path(path);
      Configuration conf = new Configuration();
      if (configs != null) {
        for (Map.Entry<String, String> entry : configs.entrySet()) {
          conf.set(entry.getKey(), entry.getValue());
        }
      }

      FileSystem fs = p.getFileSystem(conf);
      return fs.getFileStatus(p).getLen();
    } catch (Exception e) {
      System.err.println("Native.getLength failed: " + e);
      return -1;
    }
  }
}
