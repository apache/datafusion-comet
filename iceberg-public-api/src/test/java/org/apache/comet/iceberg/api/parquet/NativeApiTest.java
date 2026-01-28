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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.junit.Test;

import org.apache.comet.iceberg.api.AbstractApiTest;
import org.apache.comet.parquet.Native;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the Native public API. These tests verify method signatures exist using reflection
 * since actual invocation requires native library.
 */
public class NativeApiTest extends AbstractApiTest {

  @Test
  public void testNativeClassIsPublic() {
    assertThat(isPublic(Native.class)).isTrue();
  }

  @Test
  public void testNativeClassIsFinal() {
    assertThat(Modifier.isFinal(Native.class.getModifiers())).isTrue();
  }

  @Test
  public void testResetBatchMethodExists() throws NoSuchMethodException {
    Method method = Native.class.getMethod("resetBatch", long.class);
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
    assertThat(hasIcebergApiAnnotation(method)).isTrue();
  }

  @Test
  public void testSetPositionMethodExists() throws NoSuchMethodException {
    Method method = Native.class.getMethod("setPosition", long.class, long.class, int.class);
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
    assertThat(hasIcebergApiAnnotation(method)).isTrue();
  }

  @Test
  public void testSetIsDeletedMethodExists() throws NoSuchMethodException {
    Method method = Native.class.getMethod("setIsDeleted", long.class, boolean[].class);
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
    assertThat(hasIcebergApiAnnotation(method)).isTrue();
  }

  @Test
  public void testInitColumnReaderMethodExists() throws NoSuchMethodException {
    Method method =
        Native.class.getMethod(
            "initColumnReader",
            int.class, // physicalTypeId
            int.class, // logicalTypeId
            int.class, // expectedPhysicalTypeId
            String[].class, // path
            int.class, // maxDl
            int.class, // maxRl
            int.class, // bitWidth
            int.class, // expectedBitWidth
            boolean.class, // isSigned
            int.class, // typeLength
            int.class, // precision
            int.class, // expectedPrecision
            int.class, // scale
            int.class, // expectedScale
            int.class, // tu
            boolean.class, // isAdjustedUtc
            int.class, // batchSize
            boolean.class, // useDecimal128
            boolean.class); // useLegacyDateTimestampOrNTZ
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
    assertThat(method.getReturnType()).isEqualTo(long.class);
  }

  @Test
  public void testCloseColumnReaderMethodExists() throws NoSuchMethodException {
    Method method = Native.class.getMethod("closeColumnReader", long.class);
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
  }

  @Test
  public void testReadBatchMethodExists() throws NoSuchMethodException {
    Method method = Native.class.getMethod("readBatch", long.class, int.class, int.class);
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
    assertThat(method.getReturnType()).isEqualTo(int[].class);
  }

  @Test
  public void testCurrentBatchMethodExists() throws NoSuchMethodException {
    Method method = Native.class.getMethod("currentBatch", long.class, long.class, long.class);
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
  }

  @Test
  public void testSetDictionaryPageMethodExists() throws NoSuchMethodException {
    Method method =
        Native.class.getMethod("setDictionaryPage", long.class, int.class, byte[].class, int.class);
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
  }

  @Test
  public void testSetPageV1MethodExists() throws NoSuchMethodException {
    Method method =
        Native.class.getMethod("setPageV1", long.class, int.class, byte[].class, int.class);
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
  }

  @Test
  public void testSetNullMethodExists() throws NoSuchMethodException {
    Method method = Native.class.getMethod("setNull", long.class);
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
  }

  @Test
  public void testSetBooleanMethodExists() throws NoSuchMethodException {
    Method method = Native.class.getMethod("setBoolean", long.class, boolean.class);
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
  }

  @Test
  public void testSetIntMethodExists() throws NoSuchMethodException {
    Method method = Native.class.getMethod("setInt", long.class, int.class);
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
  }

  @Test
  public void testSetLongMethodExists() throws NoSuchMethodException {
    Method method = Native.class.getMethod("setLong", long.class, long.class);
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
  }

  @Test
  public void testSetBinaryMethodExists() throws NoSuchMethodException {
    Method method = Native.class.getMethod("setBinary", long.class, byte[].class);
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
  }

  @Test
  public void testSetDecimalMethodExists() throws NoSuchMethodException {
    Method method = Native.class.getMethod("setDecimal", long.class, byte[].class);
    assertThat(method).isNotNull();
    assertThat(Modifier.isStatic(method.getModifiers())).isTrue();
    assertThat(Modifier.isNative(method.getModifiers())).isTrue();
  }
}
