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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    try (NativeLibrary extracted =
        NativeLibrary.extract(resource(), NAME, temp.getRoot().toPath())) {
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
  public void supportsNonStickySharedTempRoot() throws Exception {
    Path root = temp.newFolder("shared").toPath();
    Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwxrwxrwx"));
    try (NativeLibrary extracted = NativeLibrary.extract(resource(), NAME, root)) {
      assertEquals(root.toRealPath(), extracted.path().getParent().getParent());
      assertEquals(
          PosixFilePermissions.fromString("rwx------"),
          Files.getPosixFilePermissions(extracted.path().getParent()));
      assertArrayEquals(CONTENT, Files.readAllBytes(extracted.path()));
    }
    assertDirectoryEmpty(root);
  }

  @Test
  public void configuredRootAndDefaultRootAreCleanedAtJvmExit() throws Exception {
    Path preferred = temp.newFolder("preferred").toPath();
    Path override = temp.newFolder("override").toPath();
    for (boolean useOverride : new boolean[] {false, true}) {
      Path marker = temp.getRoot().toPath().resolve("library-path-" + useOverride);
      runProbe(preferred, useOverride ? override : null, marker, false);
      Path library = Path.of(Files.readString(marker));
      assertEquals(
          (useOverride ? override : preferred).toRealPath(), library.getParent().getParent());
      assertFalse(Files.exists(library));
      assertFalse(Files.exists(library.getParent()));
    }
    assertDirectoryEmpty(preferred);
    assertDirectoryEmpty(override);
  }

  @Test
  public void invalidOverrideDoesNotFallBack() throws Exception {
    Path preferred = temp.newFolder("preferred").toPath();
    Path missing = temp.getRoot().toPath().resolve("missing");
    Path marker = temp.getRoot().toPath().resolve("expected-failure");
    runProbe(preferred, missing, marker, true);
    assertEquals("rejected", Files.readString(marker));
    assertFalse(Files.exists(missing));
    assertDirectoryEmpty(preferred);
  }

  private void runProbe(Path preferred, Path override, Path marker, boolean expectFailure)
      throws Exception {
    // Both system properties are confined to the child JVM, not the shared Surefire JVM.
    ProcessBuilder builder =
        new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Djava.io.tmpdir=" + preferred,
            "-cp",
            System.getProperty("java.class.path"),
            PropertyProbe.class.getName(),
            marker.toString(),
            Boolean.toString(expectFailure));
    if (override != null) {
      builder.command().add(1, "-D" + NativeLibrary.WORK_DIR_PROPERTY + "=" + override);
    }
    Process process = builder.inheritIO().start();
    try {
      assertTrue(process.waitFor(30, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue());
    } finally {
      process.destroyForcibly();
    }
  }

  public static class PropertyProbe {
    public static void main(String[] args) throws Exception {
      Path library;
      try (NativeLibrary extracted =
          NativeLibrary.extract(new ByteArrayInputStream(CONTENT), NAME)) {
        if (Boolean.parseBoolean(args[1])) {
          throw new AssertionError("Invalid configured root unexpectedly succeeded");
        }
        library = extracted.path();
        extracted.deleteOnExit();
      } catch (IOException e) {
        if (!Boolean.parseBoolean(args[1])) {
          throw e;
        }
        Files.writeString(Path.of(args[0]), "rejected");
        return;
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
