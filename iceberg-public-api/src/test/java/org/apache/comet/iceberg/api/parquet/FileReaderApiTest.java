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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import org.apache.comet.iceberg.api.AbstractApiTest;
import org.apache.comet.iceberg.api.ParquetTestHelper;
import org.apache.comet.parquet.*;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the FileReader public API. */
public class FileReaderApiTest extends AbstractApiTest {

  private ParquetTestHelper helper;

  @Override
  @Before
  public void setUp() throws IOException {
    super.setUp();
    helper = new ParquetTestHelper();
  }

  @Test
  public void testFileReaderConstructorWithWrappedInputFile() throws IOException {
    String filePath = createTempFilePath("test_wrapped.parquet");
    helper.createSimpleParquetFile(filePath, 100);

    // Create a mock object that has getLength and newStream methods
    MockInputFile mockInputFile = new MockInputFile(filePath, helper.getConfiguration());
    WrappedInputFile wrappedFile = new WrappedInputFile(mockInputFile);

    ReadOptions readOptions = ReadOptions.builder(helper.getConfiguration()).build();
    Map<String, String> properties = new HashMap<>();

    try (FileReader reader =
        new FileReader(wrappedFile, readOptions, properties, null, null, null, null)) {
      assertThat(reader).isNotNull();
    }
  }

  @Test
  public void testReadNextRowGroup() throws IOException {
    String filePath = createTempFilePath("test_row_group.parquet");
    helper.createSimpleParquetFile(filePath, 100);

    MockInputFile mockInputFile = new MockInputFile(filePath, helper.getConfiguration());
    WrappedInputFile wrappedFile = new WrappedInputFile(mockInputFile);

    ReadOptions readOptions = ReadOptions.builder(helper.getConfiguration()).build();

    try (FileReader reader =
        new FileReader(wrappedFile, readOptions, new HashMap<>(), null, null, null, null)) {

      RowGroupReader rowGroup = reader.readNextRowGroup();
      assertThat(rowGroup).isNotNull();
      assertThat(rowGroup.getRowCount()).isEqualTo(100);

      // Try to read another row group (should be null since small file)
      RowGroupReader nextRowGroup = reader.readNextRowGroup();
      assertThat(nextRowGroup).isNull();
    }
  }

  @Test
  public void testSkipNextRowGroup() throws IOException {
    String filePath = createTempFilePath("test_skip.parquet");
    helper.createSimpleParquetFile(filePath, 100);

    MockInputFile mockInputFile = new MockInputFile(filePath, helper.getConfiguration());
    WrappedInputFile wrappedFile = new WrappedInputFile(mockInputFile);

    ReadOptions readOptions = ReadOptions.builder(helper.getConfiguration()).build();

    try (FileReader reader =
        new FileReader(wrappedFile, readOptions, new HashMap<>(), null, null, null, null)) {

      boolean skipped = reader.skipNextRowGroup();
      assertThat(skipped).isTrue();

      // Try to skip another row group
      boolean skippedAgain = reader.skipNextRowGroup();
      assertThat(skippedAgain).isFalse();
    }
  }

  @Test
  public void testSetRequestedSchemaFromSpecs() throws IOException {
    String filePath = createTempFilePath("test_schema_spec.parquet");
    helper.createSimpleParquetFile(filePath, 100);

    MockInputFile mockInputFile = new MockInputFile(filePath, helper.getConfiguration());
    WrappedInputFile wrappedFile = new WrappedInputFile(mockInputFile);

    ReadOptions readOptions = ReadOptions.builder(helper.getConfiguration()).build();

    try (FileReader reader =
        new FileReader(wrappedFile, readOptions, new HashMap<>(), null, null, null, null)) {

      // Create column specs for only the 'id' column
      List<ParquetColumnSpec> specs = new ArrayList<>();
      specs.add(new ParquetColumnSpec(1, new String[] {"id"}, "INT32", 0, false, 0, 0, null, null));

      // Set the requested schema
      reader.setRequestedSchemaFromSpecs(specs);

      // Read row group with only the requested columns
      RowGroupReader rowGroup = reader.readNextRowGroup();
      assertThat(rowGroup).isNotNull();
    }
  }

  @Test
  public void testClose() throws IOException {
    String filePath = createTempFilePath("test_close.parquet");
    helper.createSimpleParquetFile(filePath, 100);

    MockInputFile mockInputFile = new MockInputFile(filePath, helper.getConfiguration());
    WrappedInputFile wrappedFile = new WrappedInputFile(mockInputFile);

    ReadOptions readOptions = ReadOptions.builder(helper.getConfiguration()).build();

    FileReader reader =
        new FileReader(wrappedFile, readOptions, new HashMap<>(), null, null, null, null);
    reader.close();
    // No exception should be thrown
  }

  /** Mock InputFile for testing WrappedInputFile. */
  private static class MockInputFile {
    private final String filePath;
    private final Configuration conf;

    MockInputFile(String filePath, Configuration conf) {
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
