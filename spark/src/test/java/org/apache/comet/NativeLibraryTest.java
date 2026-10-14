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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class NativeLibraryTest {
  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private static final byte[] CONTENT = {1, 2, 3, 4};
  private static final String NAME = "libcomet.so";

  private ByteArrayInputStream resource() {
    return new ByteArrayInputStream(CONTENT);
  }

  @Test
  public void extractionIsPrivateAndReadOnly() throws Exception {
    Path library;
    Path directory;
    try (NativeLibrary extracted = NativeLibrary.extract(resource(), NAME)) {
      library = extracted.path();
      directory = library.getParent();
      assertArrayEquals(CONTENT, Files.readAllBytes(library));
      assertEquals(
          PosixFilePermissions.fromString("rwx------"), Files.getPosixFilePermissions(directory));
      assertEquals(
          PosixFilePermissions.fromString("r--------"), Files.getPosixFilePermissions(library));
      assertEquals(Files.getOwner(directory), Files.getOwner(library));
      assertFalse(Files.isSymbolicLink(directory));
      assertFalse(Files.isSymbolicLink(library));
      assertTrue(directory.getFileName().toString().startsWith("comet-native-"));
    }
    assertFalse(Files.exists(library));
    assertFalse(Files.exists(directory));
  }

  @Test
  public void extractionsDoNotSharePaths() throws Exception {
    try (NativeLibrary first = NativeLibrary.extract(resource(), NAME);
        NativeLibrary second = NativeLibrary.extract(resource(), NAME)) {
      assertNotEquals(first.path().getParent(), second.path().getParent());
    }
  }

  @Test
  public void refusesExistingLibrary() throws Exception {
    Path target = temp.newFile(NAME).toPath();
    Files.write(target, new byte[] {9});
    assertThrows(FileAlreadyExistsException.class, () -> NativeLibrary.copy(resource(), target));
    assertArrayEquals(new byte[] {9}, Files.readAllBytes(target));
  }

  @Test
  public void refusesSymlinkWithoutOverwritingTarget() throws Exception {
    Path target = temp.newFile("target").toPath();
    Files.write(target, new byte[] {9});
    Path link = temp.getRoot().toPath().resolve(NAME);
    Files.createSymbolicLink(link, target);
    assertThrows(FileAlreadyExistsException.class, () -> NativeLibrary.copy(resource(), link));
    assertTrue(Files.isSymbolicLink(link));
    assertArrayEquals(new byte[] {9}, Files.readAllBytes(target));
  }

  @Test
  public void copyFailureRemovesPartialLibraryAndDirectory() throws Exception {
    Path root = temp.newFolder("copy-failure").toPath();
    InputStream broken =
        new InputStream() {
          private int reads;

          @Override
          public int read() throws IOException {
            if (reads++ == 0) {
              return 1;
            }
            throw new IOException("broken resource");
          }
        };
    IOException error =
        assertThrows(IOException.class, () -> NativeLibrary.extract(broken, NAME, root));
    assertEquals("broken resource", error.getMessage());
    assertDirectoryEmpty(root);
  }

  @Test
  public void loadFailureRemovesLibraryAndDirectory() throws Exception {
    Path root = temp.newFolder("load-failure").toPath();
    assertThrows(
        UnsatisfiedLinkError.class,
        () -> {
          try (NativeLibrary extracted = NativeLibrary.extract(resource(), NAME, root)) {
            System.load(extracted.path().toString());
          }
        });
    assertDirectoryEmpty(root);
  }

  @Test
  public void rejectsSharedWritableTempRoot() throws Exception {
    Path root = temp.newFolder("shared").toPath();
    Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwxrwxrwx"));
    assertThrows(IOException.class, () -> NativeLibrary.extract(resource(), NAME, root));
    assertDirectoryEmpty(root);
  }

  @Test
  public void rejectsSharedWritableAncestorOfPrivateTempRoot() throws Exception {
    Path shared = temp.newFolder("shared-parent").toPath();
    Path root = Files.createDirectory(shared.resolve("private"));
    Files.setPosixFilePermissions(shared, PosixFilePermissions.fromString("rwxrwxrwx"));
    Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwx------"));
    assertThrows(IOException.class, () -> NativeLibrary.extract(resource(), NAME, root));
    assertDirectoryEmpty(root);
  }

  @Test
  public void selectsSafeAlternativeToUnsuitableRoot() throws Exception {
    Path unsafe = temp.newFolder("unsafe-preferred").toPath();
    Path safe = temp.newFolder("safe-alternative").toPath();
    Files.setPosixFilePermissions(unsafe, PosixFilePermissions.fromString("rwxrwxrwx"));
    try (NativeLibrary extracted = NativeLibrary.extract(resource(), NAME, unsafe, safe)) {
      assertEquals(safe.toRealPath(), extracted.path().getParent().getParent());
      assertArrayEquals(CONTENT, Files.readAllBytes(extracted.path()));
      assertDirectoryEmpty(unsafe);
    }
    assertDirectoryEmpty(safe);
  }

  @Test
  public void defaultExtractionFallsBackFromUnsuitableSparkTempRoot() throws Exception {
    Path unsafe = temp.newFolder("spark-temp").toPath();
    Files.setPosixFilePermissions(unsafe, PosixFilePermissions.fromString("rwxrwxrwx"));
    String previous = System.getProperty("java.io.tmpdir");
    try {
      System.setProperty("java.io.tmpdir", unsafe.toString());
      try (NativeLibrary extracted = NativeLibrary.extract(resource(), NAME)) {
        assertNotEquals(unsafe.toRealPath(), extracted.path().getParent().getParent());
        assertArrayEquals(CONTENT, Files.readAllBytes(extracted.path()));
        assertDirectoryEmpty(unsafe);
      }
    } finally {
      System.setProperty("java.io.tmpdir", previous);
    }
  }

  @Test
  public void skipsMissingRootWithoutConsumingResource() throws Exception {
    Path missing = temp.getRoot().toPath().resolve("missing");
    Path safe = temp.newFolder("existing").toPath();
    try (NativeLibrary extracted = NativeLibrary.extract(resource(), NAME, missing, safe)) {
      assertEquals(safe.toRealPath(), extracted.path().getParent().getParent());
      assertArrayEquals(CONTENT, Files.readAllBytes(extracted.path()));
      assertFalse(Files.exists(missing));
    }
  }

  @Test
  public void allUnsafeCandidatesFailBeforeReadingResource() throws Exception {
    Path first = temp.newFolder("first-unsafe").toPath();
    Path second = temp.newFolder("second-unsafe").toPath();
    Files.setPosixFilePermissions(first, PosixFilePermissions.fromString("rwxrwxrwx"));
    Files.setPosixFilePermissions(second, PosixFilePermissions.fromString("rwxrwxrwx"));
    InputStream unread =
        new InputStream() {
          @Override
          public int read() {
            throw new AssertionError("Resource read before selecting a safe root");
          }
        };
    IOException failure =
        assertThrows(IOException.class, () -> NativeLibrary.extract(unread, NAME, first, second));
    assertEquals(2, failure.getSuppressed().length);
    assertDirectoryEmpty(first);
    assertDirectoryEmpty(second);
  }

  @Test
  public void copyFailureDoesNotRetryAnotherRoot() throws Exception {
    Path first = temp.newFolder("first-safe").toPath();
    Path second = temp.newFolder("second-safe").toPath();
    int[] reads = {0};
    InputStream broken =
        new InputStream() {
          @Override
          public int read() throws IOException {
            reads[0]++;
            throw new IOException("broken resource");
          }
        };
    IOException failure =
        assertThrows(IOException.class, () -> NativeLibrary.extract(broken, NAME, first, second));
    assertEquals("broken resource", failure.getMessage());
    assertEquals(1, reads[0]);
    assertDirectoryEmpty(first);
    assertDirectoryEmpty(second);
  }

  @Test
  public void permitsStickySharedTempRoot() throws Exception {
    Path root = temp.newFolder("sticky").toPath();
    Files.setAttribute(root, "unix:mode", 01777);
    try (NativeLibrary extracted = NativeLibrary.extract(resource(), NAME, root)) {
      assertArrayEquals(CONTENT, Files.readAllBytes(extracted.path()));
    }
    assertDirectoryEmpty(root);
  }

  @Test
  public void resolvesTempRootSymlinkBeforeLoading() throws Exception {
    Path root = temp.newFolder("real").toPath();
    Path link = temp.getRoot().toPath().resolve("link");
    Files.createSymbolicLink(link, root);
    try (NativeLibrary extracted = NativeLibrary.extract(resource(), NAME, link)) {
      assertEquals(root.toRealPath(), extracted.path().getParent().getParent());
    }
  }

  @Test
  public void failsClosedWithoutUnixPermissions() throws Exception {
    Path archive = temp.getRoot().toPath().resolve("temp.zip");
    try (java.nio.file.FileSystem fs =
        FileSystems.newFileSystem(archive, Collections.singletonMap("create", "true"))) {
      assertThrows(
          IOException.class, () -> NativeLibrary.extract(resource(), NAME, fs.getPath("/")));
      assertDirectoryEmpty(fs.getPath("/"));
    }
  }

  @Test
  public void retainedLibraryIsRemovedAtJvmExit() throws Exception {
    Path marker = temp.getRoot().toPath().resolve("library-path");
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                ShutdownProbe.class.getName(),
                marker.toString())
            .inheritIO()
            .start();
    try {
      assertTrue(process.waitFor(30, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue());
      Path library = Path.of(Files.readString(marker));
      assertFalse(Files.exists(library));
      assertFalse(Files.exists(library.getParent()));
    } finally {
      process.destroyForcibly();
    }
  }

  public static class ShutdownProbe {
    public static void main(String[] args) throws Exception {
      Path library;
      try (NativeLibrary extracted =
          NativeLibrary.extract(new ByteArrayInputStream(CONTENT), NAME)) {
        library = extracted.path();
        extracted.deleteOnExit();
      }
      if (!Files.exists(library)) {
        throw new AssertionError("Loaded library was deleted before JVM shutdown");
      }
      Files.writeString(Path.of(args[0]), library.toString());
    }
  }

  private void assertDirectoryEmpty(Path directory) throws IOException {
    try (java.util.stream.Stream<Path> entries = Files.list(directory)) {
      assertEquals(0, entries.count());
    }
  }
}
