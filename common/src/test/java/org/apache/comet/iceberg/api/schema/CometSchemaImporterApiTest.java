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

package org.apache.comet.iceberg.api.schema;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.junit.Test;

import org.apache.arrow.c.AbstractCometSchemaImporter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.apache.comet.CometSchemaImporter;
import org.apache.comet.iceberg.api.AbstractApiTest;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the CometSchemaImporter public API. */
public class CometSchemaImporterApiTest extends AbstractApiTest {

  @Test
  public void testCometSchemaImporterHasIcebergApiAnnotation() {
    assertThat(hasIcebergApiAnnotation(CometSchemaImporter.class)).isTrue();
  }

  @Test
  public void testCometSchemaImporterIsPublic() {
    assertThat(isPublic(CometSchemaImporter.class)).isTrue();
  }

  @Test
  public void testExtendsAbstractCometSchemaImporter() {
    assertThat(AbstractCometSchemaImporter.class.isAssignableFrom(CometSchemaImporter.class))
        .isTrue();
  }

  @Test
  public void testConstructorExists() throws NoSuchMethodException {
    Constructor<?> constructor = CometSchemaImporter.class.getConstructor(BufferAllocator.class);
    assertThat(constructor).isNotNull();
    assertThat(hasIcebergApiAnnotation(constructor)).isTrue();
  }

  @Test
  public void testCanInstantiate() {
    try (BufferAllocator allocator = new RootAllocator()) {
      CometSchemaImporter importer = new CometSchemaImporter(allocator);
      assertThat(importer).isNotNull();
      importer.close();
    }
  }

  @Test
  public void testCloseMethodExists() throws NoSuchMethodException {
    Method method = CometSchemaImporter.class.getMethod("close");
    assertThat(method).isNotNull();
  }

  @Test
  public void testGetAllocatorMethodExists() throws NoSuchMethodException {
    Method method = CometSchemaImporter.class.getMethod("getAllocator");
    assertThat(method).isNotNull();
    assertThat(method.getReturnType()).isEqualTo(BufferAllocator.class);
  }

  @Test
  public void testGetProviderMethodExists() throws NoSuchMethodException {
    Method method = CometSchemaImporter.class.getMethod("getProvider");
    assertThat(method).isNotNull();
    assertThat(method.getReturnType().getSimpleName()).isEqualTo("CDataDictionaryProvider");
  }

  @Test
  public void testGetAllocatorReturnsCorrectValue() {
    try (BufferAllocator allocator = new RootAllocator()) {
      CometSchemaImporter importer = new CometSchemaImporter(allocator);
      assertThat(importer.getAllocator()).isSameAs(allocator);
      importer.close();
    }
  }

  @Test
  public void testGetProviderIsNotNull() {
    try (BufferAllocator allocator = new RootAllocator()) {
      CometSchemaImporter importer = new CometSchemaImporter(allocator);
      assertThat(importer.getProvider()).isNotNull();
      importer.close();
    }
  }

  @Test
  public void testAbstractCometSchemaImporterCloseMethodHasAnnotation()
      throws NoSuchMethodException {
    Method method = AbstractCometSchemaImporter.class.getMethod("close");
    assertThat(method).isNotNull();
    assertThat(hasIcebergApiAnnotation(method)).isTrue();
  }
}
