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

import java.io.IOException;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import org.apache.comet.iceberg.api.AbstractApiTest;
import org.apache.comet.iceberg.api.ParquetTestHelper;
import org.apache.comet.parquet.*;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the RowGroupReader public API. */
public class RowGroupReaderApiTest extends AbstractApiTest {

  private ParquetTestHelper helper;

  @Override
  @Before
  public void setUp() throws IOException {
    super.setUp();
    helper = new ParquetTestHelper();
  }

  @Test
  public void testGetRowCount() throws IOException {
    String filePath = createTempFilePath("test_row_count.parquet");
    helper.createSimpleParquetFile(filePath, 100);

    MockInputFile mockInputFile = new MockInputFile(filePath, helper.getConfiguration());
    WrappedInputFile wrappedFile = new WrappedInputFile(mockInputFile);

    ReadOptions readOptions = ReadOptions.builder(helper.getConfiguration()).build();

    try (FileReader reader =
        new FileReader(wrappedFile, readOptions, new HashMap<>(), null, null, null, null)) {

      RowGroupReader rowGroup = reader.readNextRowGroup();
      assertThat(rowGroup).isNotNull();
      assertThat(rowGroup.getRowCount()).isEqualTo(100);
    }
  }

  @Test
  public void testRowGroupReaderIsPageReadStore() throws IOException {
    String filePath = createTempFilePath("test_page_read_store.parquet");
    helper.createSimpleParquetFile(filePath, 100);

    MockInputFile mockInputFile = new MockInputFile(filePath, helper.getConfiguration());
    WrappedInputFile wrappedFile = new WrappedInputFile(mockInputFile);

    ReadOptions readOptions = ReadOptions.builder(helper.getConfiguration()).build();

    try (FileReader reader =
        new FileReader(wrappedFile, readOptions, new HashMap<>(), null, null, null, null)) {

      RowGroupReader rowGroup = reader.readNextRowGroup();
      assertThat(rowGroup).isNotNull();

      // RowGroupReader implements PageReadStore
      assertThat(rowGroup).isInstanceOf(org.apache.parquet.column.page.PageReadStore.class);
    }
  }

  @Test
  public void testGetRowIndexes() throws IOException {
    String filePath = createTempFilePath("test_row_indexes.parquet");
    helper.createSimpleParquetFile(filePath, 100);

    MockInputFile mockInputFile = new MockInputFile(filePath, helper.getConfiguration());
    WrappedInputFile wrappedFile = new WrappedInputFile(mockInputFile);

    ReadOptions readOptions = ReadOptions.builder(helper.getConfiguration()).build();

    try (FileReader reader =
        new FileReader(wrappedFile, readOptions, new HashMap<>(), null, null, null, null)) {

      RowGroupReader rowGroup = reader.readNextRowGroup();
      assertThat(rowGroup).isNotNull();

      // getRowIndexes() returns Optional
      assertThat(rowGroup.getRowIndexes()).isNotNull();
    }
  }

  @Test
  public void testGetRowIndexOffset() throws IOException {
    String filePath = createTempFilePath("test_row_index_offset.parquet");
    helper.createSimpleParquetFile(filePath, 100);

    MockInputFile mockInputFile = new MockInputFile(filePath, helper.getConfiguration());
    WrappedInputFile wrappedFile = new WrappedInputFile(mockInputFile);

    ReadOptions readOptions = ReadOptions.builder(helper.getConfiguration()).build();

    try (FileReader reader =
        new FileReader(wrappedFile, readOptions, new HashMap<>(), null, null, null, null)) {

      RowGroupReader rowGroup = reader.readNextRowGroup();
      assertThat(rowGroup).isNotNull();

      // getRowIndexOffset() returns Optional
      assertThat(rowGroup.getRowIndexOffset()).isNotNull();
    }
  }

  /** Mock InputFile for testing WrappedInputFile. */
  private static class MockInputFile {
    private final String filePath;
    private final org.apache.hadoop.conf.Configuration conf;

    MockInputFile(String filePath, org.apache.hadoop.conf.Configuration conf) {
      this.filePath = filePath;
      this.conf = conf;
    }

    public long getLength() throws IOException {
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(filePath);
      org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
      return fs.getFileStatus(path).getLen();
    }

    public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(filePath);
      org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
      return org.apache.parquet.hadoop.util.HadoopStreams.wrap(fs.open(path));
    }

    @Override
    public String toString() {
      return filePath;
    }
  }
}
