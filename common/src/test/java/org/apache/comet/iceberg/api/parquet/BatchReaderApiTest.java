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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.junit.Test;

import org.apache.spark.sql.types.StructType;

import org.apache.comet.iceberg.api.AbstractApiTest;
import org.apache.comet.parquet.AbstractColumnReader;
import org.apache.comet.parquet.BatchReader;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the BatchReader public API. */
public class BatchReaderApiTest extends AbstractApiTest {

  @Test
  public void testBatchReaderHasIcebergApiAnnotation() {
    assertThat(hasIcebergApiAnnotation(BatchReader.class)).isTrue();
  }

  @Test
  public void testBatchReaderIsPublic() {
    assertThat(isPublic(BatchReader.class)).isTrue();
  }

  @Test
  public void testConstructorWithColumnReadersExists() throws NoSuchMethodException {
    Constructor<?> constructor = BatchReader.class.getConstructor(AbstractColumnReader[].class);
    assertThat(constructor).isNotNull();
    assertThat(hasIcebergApiAnnotation(constructor)).isTrue();
  }

  @Test
  public void testSetSparkSchemaMethodExists() throws NoSuchMethodException {
    Method method = BatchReader.class.getMethod("setSparkSchema", StructType.class);
    assertThat(method).isNotNull();
    assertThat(hasIcebergApiAnnotation(method)).isTrue();
  }

  @Test
  public void testGetColumnReadersMethodExists() throws NoSuchMethodException {
    Method method = BatchReader.class.getMethod("getColumnReaders");
    assertThat(method).isNotNull();
    assertThat(hasIcebergApiAnnotation(method)).isTrue();
    assertThat(method.getReturnType()).isEqualTo(AbstractColumnReader[].class);
  }

  @Test
  public void testNextBatchWithSizeMethodExists() throws NoSuchMethodException {
    Method method = BatchReader.class.getMethod("nextBatch", int.class);
    assertThat(method).isNotNull();
    assertThat(hasIcebergApiAnnotation(method)).isTrue();
    assertThat(method.getReturnType()).isEqualTo(boolean.class);
  }

  @Test
  public void testCurrentBatchMethodExists() throws NoSuchMethodException {
    Method method = BatchReader.class.getMethod("currentBatch");
    assertThat(method).isNotNull();
    assertThat(method.getReturnType().getSimpleName()).isEqualTo("ColumnarBatch");
  }

  @Test
  public void testCloseMethodExists() throws NoSuchMethodException {
    Method method = BatchReader.class.getMethod("close");
    assertThat(method).isNotNull();
  }

  @Test
  public void testImplementsCloseable() {
    assertThat(java.io.Closeable.class.isAssignableFrom(BatchReader.class)).isTrue();
  }

  @Test
  public void testExtendsRecordReader() {
    assertThat(BatchReader.class.getSuperclass().getSimpleName()).isEqualTo("RecordReader");
  }
}
