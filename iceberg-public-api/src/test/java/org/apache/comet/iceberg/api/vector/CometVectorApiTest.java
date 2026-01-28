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

package org.apache.comet.iceberg.api.vector;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.junit.Test;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnVector;

import org.apache.comet.iceberg.api.AbstractApiTest;
import org.apache.comet.vector.CometVector;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the CometVector public API. */
public class CometVectorApiTest extends AbstractApiTest {

  @Test
  public void testCometVectorHasIcebergApiAnnotation() {
    assertThat(hasIcebergApiAnnotation(CometVector.class)).isTrue();
  }

  @Test
  public void testCometVectorIsPublic() {
    assertThat(isPublic(CometVector.class)).isTrue();
  }

  @Test
  public void testCometVectorIsAbstract() {
    assertThat(Modifier.isAbstract(CometVector.class.getModifiers())).isTrue();
  }

  @Test
  public void testExtendsColumnVector() {
    assertThat(ColumnVector.class.isAssignableFrom(CometVector.class)).isTrue();
  }

  @Test
  public void testPublicConstructorExists() throws NoSuchMethodException {
    Constructor<?> constructor =
        CometVector.class.getDeclaredConstructor(DataType.class, boolean.class);
    assertThat(constructor).isNotNull();
    assertThat(hasIcebergApiAnnotation(constructor)).isTrue();
    assertThat(Modifier.isPublic(constructor.getModifiers())).isTrue();
  }

  @Test
  public void testSetNumNullsMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("setNumNulls", int.class);
    assertThat(method).isNotNull();
    assertThat(hasIcebergApiAnnotation(method)).isTrue();
    assertThat(Modifier.isAbstract(method.getModifiers())).isTrue();
  }

  @Test
  public void testSetNumValuesMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("setNumValues", int.class);
    assertThat(method).isNotNull();
    assertThat(hasIcebergApiAnnotation(method)).isTrue();
    assertThat(Modifier.isAbstract(method.getModifiers())).isTrue();
  }

  @Test
  public void testNumValuesMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("numValues");
    assertThat(method).isNotNull();
    assertThat(hasIcebergApiAnnotation(method)).isTrue();
    assertThat(Modifier.isAbstract(method.getModifiers())).isTrue();
    assertThat(method.getReturnType()).isEqualTo(int.class);
  }

  @Test
  public void testGetValueVectorMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("getValueVector");
    assertThat(method).isNotNull();
    assertThat(hasIcebergApiAnnotation(method)).isTrue();
    assertThat(Modifier.isAbstract(method.getModifiers())).isTrue();
    assertThat(method.getReturnType().getSimpleName()).isEqualTo("ValueVector");
  }

  @Test
  public void testSliceMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("slice", int.class, int.class);
    assertThat(method).isNotNull();
    assertThat(hasIcebergApiAnnotation(method)).isTrue();
    assertThat(Modifier.isAbstract(method.getModifiers())).isTrue();
    assertThat(method.getReturnType()).isEqualTo(CometVector.class);
  }

  @Test
  public void testCloseMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("close");
    assertThat(method).isNotNull();
  }

  @Test
  public void testIsNullAtMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("isNullAt", int.class);
    assertThat(method).isNotNull();
    assertThat(method.getReturnType()).isEqualTo(boolean.class);
  }

  @Test
  public void testGetBooleanMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("getBoolean", int.class);
    assertThat(method).isNotNull();
    assertThat(method.getReturnType()).isEqualTo(boolean.class);
  }

  @Test
  public void testGetIntMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("getInt", int.class);
    assertThat(method).isNotNull();
    assertThat(method.getReturnType()).isEqualTo(int.class);
  }

  @Test
  public void testGetLongMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("getLong", int.class);
    assertThat(method).isNotNull();
    assertThat(method.getReturnType()).isEqualTo(long.class);
  }

  @Test
  public void testGetFloatMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("getFloat", int.class);
    assertThat(method).isNotNull();
    assertThat(method.getReturnType()).isEqualTo(float.class);
  }

  @Test
  public void testGetDoubleMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("getDouble", int.class);
    assertThat(method).isNotNull();
    assertThat(method.getReturnType()).isEqualTo(double.class);
  }

  @Test
  public void testGetBinaryMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("getBinary", int.class);
    assertThat(method).isNotNull();
    assertThat(method.getReturnType()).isEqualTo(byte[].class);
  }

  @Test
  public void testGetDecimalMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("getDecimal", int.class, int.class, int.class);
    assertThat(method).isNotNull();
    assertThat(method.getReturnType().getSimpleName()).isEqualTo("Decimal");
  }

  @Test
  public void testGetUTF8StringMethodExists() throws NoSuchMethodException {
    Method method = CometVector.class.getMethod("getUTF8String", int.class);
    assertThat(method).isNotNull();
    assertThat(method.getReturnType().getSimpleName()).isEqualTo("UTF8String");
  }
}
