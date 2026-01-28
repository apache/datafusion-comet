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

package org.apache.comet.iceberg.api;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import org.junit.After;
import org.junit.Before;

import org.apache.comet.IcebergApi;

/**
 * Base class for Iceberg API tests. Provides common utilities for testing annotated API elements.
 */
public abstract class AbstractApiTest {

  protected Path tempDir;

  @Before
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("iceberg-api-test");
  }

  @After
  public void tearDown() throws IOException {
    if (tempDir != null && Files.exists(tempDir)) {
      Files.walk(tempDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }

  /** Checks if a class has the @IcebergApi annotation. */
  protected static boolean hasIcebergApiAnnotation(Class<?> clazz) {
    return clazz.isAnnotationPresent(IcebergApi.class);
  }

  /** Checks if a method has the @IcebergApi annotation. */
  protected static boolean hasIcebergApiAnnotation(Method method) {
    return method.isAnnotationPresent(IcebergApi.class);
  }

  /** Checks if a constructor has the @IcebergApi annotation. */
  protected static boolean hasIcebergApiAnnotation(Constructor<?> constructor) {
    return constructor.isAnnotationPresent(IcebergApi.class);
  }

  /** Checks if a field has the @IcebergApi annotation. */
  protected static boolean hasIcebergApiAnnotation(Field field) {
    return field.isAnnotationPresent(IcebergApi.class);
  }

  /** Checks if a class is public. */
  protected static boolean isPublic(Class<?> clazz) {
    return Modifier.isPublic(clazz.getModifiers());
  }

  /** Checks if a method is public. */
  protected static boolean isPublic(Method method) {
    return Modifier.isPublic(method.getModifiers());
  }

  /** Checks if a constructor is public. */
  protected static boolean isPublic(Constructor<?> constructor) {
    return Modifier.isPublic(constructor.getModifiers());
  }

  /** Checks if a field is public or protected. */
  protected static boolean isAccessible(Field field) {
    int modifiers = field.getModifiers();
    return Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers);
  }

  /** Checks if native library is available. */
  protected static boolean isNativeLibraryAvailable() {
    try {
      Class.forName("org.apache.comet.NativeBase");
      return true;
    } catch (Throwable t) {
      return false;
    }
  }

  /** Creates a temp file path for testing. */
  protected String createTempFilePath(String name) {
    return tempDir.resolve(name).toString();
  }
}
