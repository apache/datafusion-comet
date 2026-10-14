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
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.EnumSet;
import java.util.Map;

/** Owns a bundled native library until it is loaded or discarded. */
final class NativeLibrary implements AutoCloseable {
  // Use a different prefix from legacy libcomet-* files: old JVMs must not clean up this directory.
  private static final String DIRECTORY_PREFIX = "comet-native-";
  private final Path directory;
  private final Path library;
  private boolean retained;

  private NativeLibrary(Path directory, String libraryName) {
    this.directory = directory;
    this.library = directory.resolve(libraryName);
  }

  static NativeLibrary extract(InputStream resource, String libraryName) throws IOException {
    // Spark on YARN puts java.io.tmpdir below a NodeManager-owned directory. That account can
    // replace descendants, so use a separately validated system root if the preferred root is
    // unsafe.
    return extract(
        resource,
        libraryName,
        Paths.get(System.getProperty("java.io.tmpdir")),
        Paths.get("/tmp"),
        Paths.get("/var/tmp"));
  }

  static NativeLibrary extract(InputStream resource, String libraryName, Path... tempRoots)
      throws IOException {
    Path directory = createExtractionDirectory(tempRoots);
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

  private static Path createExtractionDirectory(Path[] tempRoots) throws IOException {
    IOException failure = new IOException("No safe temporary directory for libcomet");
    for (Path tempRoot : tempRoots) {
      try {
        Path root = tempRoot.toRealPath();
        checkTempRoot(root);
        return Files.createTempDirectory(
            root,
            DIRECTORY_PREFIX,
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
      } catch (IOException e) {
        failure.addSuppressed(e);
      }
    }
    throw failure;
  }

  private static void checkTempRoot(Path root) throws IOException {
    // A private child can still be renamed in a shared, non-sticky parent. Check every ancestor
    // after resolving symlinks so another local user cannot replace any component of the load path.
    // Fail closed on filesystems where these Unix protections cannot be established.
    if (!root.getFileSystem().supportedFileAttributeViews().contains("unix")) {
      throw new IOException("Cannot securely extract libcomet on a non-Unix filesystem");
    }
    UserPrincipal owner =
        root.getFileSystem()
            .getUserPrincipalLookupService()
            .lookupPrincipalByName(System.getProperty("user.name"));
    for (Path ancestor = root; ancestor != null; ancestor = ancestor.getParent()) {
      Map<String, Object> attributes =
          Files.readAttributes(ancestor, "unix:uid,mode,owner", LinkOption.NOFOLLOW_LINKS);
      int mode = (int) attributes.get("mode");
      boolean trustedOwner =
          (int) attributes.get("uid") == 0 || owner.equals(attributes.get("owner"));
      boolean sharedWritable = (mode & 0022) != 0;
      boolean sticky = (mode & 01000) != 0;
      if (!trustedOwner || (sharedWritable && !sticky)) {
        throw new IOException("Unsafe temporary directory for libcomet: " + ancestor);
      }
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
