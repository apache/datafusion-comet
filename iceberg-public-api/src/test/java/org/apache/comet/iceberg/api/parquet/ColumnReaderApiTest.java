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

import org.junit.Test;

import org.apache.parquet.column.page.PageReader;

import org.apache.comet.iceberg.api.AbstractApiTest;
import org.apache.comet.parquet.ColumnReader;
import org.apache.comet.parquet.ParquetColumnSpec;
import org.apache.comet.parquet.RowGroupReader;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the ColumnReader public API. Uses reflection for signature verification. */
public class ColumnReaderApiTest extends AbstractApiTest {

  @Test
  public void testColumnReaderHasIcebergApiAnnotation() {
    assertThat(hasIcebergApiAnnotation(ColumnReader.class)).isTrue();
  }

  @Test
  public void testColumnReaderIsPublic() {
    assertThat(isPublic(ColumnReader.class)).isTrue();
  }

  @Test
  public void testSetPageReaderMethodExists() throws NoSuchMethodException {
    Method method = ColumnReader.class.getMethod("setPageReader", PageReader.class);
    assertThat(method).isNotNull();
    assertThat(hasIcebergApiAnnotation(method)).isTrue();
  }

  @Test
  public void testSetRowGroupReaderMethodExists() throws NoSuchMethodException {
    Method method =
        ColumnReader.class.getMethod(
            "setRowGroupReader", RowGroupReader.class, ParquetColumnSpec.class);
    assertThat(method).isNotNull();
    assertThat(hasIcebergApiAnnotation(method)).isTrue();
  }

  @Test
  public void testExtendsAbstractColumnReader() {
    assertThat(ColumnReader.class.getSuperclass().getSimpleName())
        .isEqualTo("AbstractColumnReader");
  }

  @Test
  public void testImplementsAutoCloseable() {
    assertThat(AutoCloseable.class.isAssignableFrom(ColumnReader.class)).isTrue();
  }

  @Test
  public void testReadBatchMethodExists() throws NoSuchMethodException {
    Method method = ColumnReader.class.getMethod("readBatch", int.class);
    assertThat(method).isNotNull();
  }

  @Test
  public void testCurrentBatchMethodExists() throws NoSuchMethodException {
    Method method = ColumnReader.class.getMethod("currentBatch");
    assertThat(method).isNotNull();
    assertThat(method.getReturnType().getSimpleName()).isEqualTo("CometVector");
  }

  @Test
  public void testCloseMethodExists() throws NoSuchMethodException {
    Method method = ColumnReader.class.getMethod("close");
    assertThat(method).isNotNull();
  }
}
