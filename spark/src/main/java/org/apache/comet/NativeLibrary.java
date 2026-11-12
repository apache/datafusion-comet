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

package org.apache.comet;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;

/** Owns a bundled native library until it is loaded or discarded. */
final class NativeLibrary implements AutoCloseable {
  // Use a different prefix from legacy libcomet-* files: old JVMs must not clean up this directory.
  private static final String DIRECTORY_PREFIX = "comet-native-";
  static final String WORK_DIR_PROPERTY = "comet.native.workdir";
  private final Path directory;
  private final Path library;
  private boolean retained;

  private NativeLibrary(Path directory, String libraryName) {
    this.directory = directory;
    this.library = directory.resolve(libraryName);
  }

  static NativeLibrary extract(InputStream resource, String libraryName) throws IOException {
    String root = System.getProperty(WORK_DIR_PROPERTY, System.getProperty("java.io.tmpdir"));
    return extract(resource, libraryName, Paths.get(root));
  }

  static NativeLibrary extract(InputStream resource, String libraryName, Path tempRoot)
      throws IOException {
    Path directory =
        Files.createTempDirectory(
            tempRoot.toRealPath(),
            DIRECTORY_PREFIX,
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    NativeLibrary result = new NativeLibrary(directory, libraryName);
    try {
      copy(resource, result.library);
      return result;
    } catch (IOException | RuntimeException | Error e) {
      try {
        result.close();
      } catch (IOException cleanupError) {
        e.addSuppressed(cleanupError);
      }
      throw e;
    }
  }

  static void copy(InputStream resource, Path library) throws IOException {
    // CREATE_NEW refuses existing entries, including symbolic links. Both directory and file
    // permissions are set at creation, before any library bytes are written.
    try (SeekableByteChannel channel =
            Files.newByteChannel(
                library,
                EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                PosixFilePermissions.asFileAttribute(
                    PosixFilePermissions.fromString("rw-------")));
        OutputStream output = Channels.newOutputStream(channel)) {
      resource.transferTo(output);
    }
    Files.setPosixFilePermissions(library, PosixFilePermissions.fromString("r--------"));
  }

  Path path() {
    return library;
  }

  void deleteOnExit() {
    // JVM shutdown hooks delete in reverse registration order: remove the file before its parent.
    directory.toFile().deleteOnExit();
    library.toFile().deleteOnExit();
    retained = true;
  }

  @Override
  public void close() throws IOException {
    if (!retained) {
      try {
        Files.deleteIfExists(library);
      } finally {
        Files.deleteIfExists(directory);
      }
    }
  }
}
