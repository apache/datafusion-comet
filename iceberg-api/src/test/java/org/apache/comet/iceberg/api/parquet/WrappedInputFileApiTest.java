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

package org.apache.comet.iceberg.api.parquet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.junit.Test;

import org.apache.parquet.io.InputFile;

import org.apache.comet.iceberg.api.AbstractApiTest;
import org.apache.comet.parquet.WrappedInputFile;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the WrappedInputFile public API. */
public class WrappedInputFileApiTest extends AbstractApiTest {

  @Test
  public void testWrappedInputFileHasIcebergApiAnnotation() {
    assertThat(hasIcebergApiAnnotation(WrappedInputFile.class)).isTrue();
  }

  @Test
  public void testWrappedInputFileIsPublic() {
    assertThat(isPublic(WrappedInputFile.class)).isTrue();
  }

  @Test
  public void testImplementsInputFile() {
    assertThat(InputFile.class.isAssignableFrom(WrappedInputFile.class)).isTrue();
  }

  @Test
  public void testConstructorExists() throws NoSuchMethodException {
    Constructor<?> constructor = WrappedInputFile.class.getConstructor(Object.class);
    assertThat(constructor).isNotNull();
    assertThat(hasIcebergApiAnnotation(constructor)).isTrue();
  }

  @Test
  public void testGetLengthMethodExists() throws NoSuchMethodException {
    Method method = WrappedInputFile.class.getMethod("getLength");
    assertThat(method).isNotNull();
    assertThat(method.getReturnType()).isEqualTo(long.class);
  }

  @Test
  public void testNewStreamMethodExists() throws NoSuchMethodException {
    Method method = WrappedInputFile.class.getMethod("newStream");
    assertThat(method).isNotNull();
    assertThat(method.getReturnType().getSimpleName()).isEqualTo("SeekableInputStream");
  }

  @Test
  public void testToStringMethodExists() throws NoSuchMethodException {
    Method method = WrappedInputFile.class.getMethod("toString");
    assertThat(method).isNotNull();
    assertThat(method.getReturnType()).isEqualTo(String.class);
  }

  @Test
  public void testCanWrapMockObject() {
    MockInputFile mockFile = new MockInputFile(100L);
    WrappedInputFile wrappedFile = new WrappedInputFile(mockFile);
    assertThat(wrappedFile).isNotNull();
  }

  @Test
  public void testGetLengthDelegatesToWrappedObject() throws IOException {
    MockInputFile mockFile = new MockInputFile(12345L);
    WrappedInputFile wrappedFile = new WrappedInputFile(mockFile);
    assertThat(wrappedFile.getLength()).isEqualTo(12345L);
  }

  @Test
  public void testToStringDelegatesToWrappedObject() {
    MockInputFile mockFile = new MockInputFile(100L);
    WrappedInputFile wrappedFile = new WrappedInputFile(mockFile);
    assertThat(wrappedFile.toString()).isEqualTo("MockInputFile");
  }

  @Test
  public void testNewStreamDelegatesToWrappedObject() throws IOException {
    MockInputFile mockFile = new MockInputFile(100L);
    WrappedInputFile wrappedFile = new WrappedInputFile(mockFile);

    // newStream should return a SeekableInputStream
    org.apache.parquet.io.SeekableInputStream stream = wrappedFile.newStream();
    assertThat(stream).isNotNull();
    stream.close();
  }

  /** Mock InputFile that simulates Iceberg's InputFile interface. */
  private static class MockInputFile {
    private final long length;

    MockInputFile(long length) {
      this.length = length;
    }

    public long getLength() {
      return length;
    }

    public InputStream newStream() {
      // Return a simple ByteArrayInputStream for testing
      byte[] data = new byte[(int) length];
      return new ByteArrayInputStream(data);
    }

    @Override
    public String toString() {
      return "MockInputFile";
    }
  }
}
