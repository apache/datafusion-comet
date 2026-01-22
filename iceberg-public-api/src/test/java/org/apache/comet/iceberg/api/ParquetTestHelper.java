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

import java.io.IOException;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/** Helper class for creating test Parquet files. */
public class ParquetTestHelper {

  /** Schema for a simple test file with int and string columns. */
  public static final String SIMPLE_SCHEMA =
      "message test_schema {"
          + "  required int32 id;"
          + "  optional binary name (UTF8);"
          + "  optional int64 value;"
          + "}";

  /** Schema for testing decimal types. */
  public static final String DECIMAL_SCHEMA =
      "message decimal_schema {"
          + "  required int32 id;"
          + "  optional fixed_len_byte_array(16) decimal_col (DECIMAL(38,10));"
          + "}";

  /** Schema for testing timestamp types. */
  public static final String TIMESTAMP_SCHEMA =
      "message timestamp_schema {"
          + "  required int32 id;"
          + "  optional int64 ts_col (TIMESTAMP(MICROS,true));"
          + "}";

  private final Configuration conf;

  public ParquetTestHelper() {
    this.conf = new Configuration();
  }

  public ParquetTestHelper(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Creates a simple Parquet file with the given number of rows.
   *
   * @param filePath path to write the file
   * @param numRows number of rows to write
   * @return the path to the created file
   */
  public String createSimpleParquetFile(Path filePath, int numRows) throws IOException {
    return createSimpleParquetFile(filePath.toString(), numRows);
  }

  /**
   * Creates a simple Parquet file with the given number of rows.
   *
   * @param filePath path to write the file
   * @param numRows number of rows to write
   * @return the path to the created file
   */
  public String createSimpleParquetFile(String filePath, int numRows) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(SIMPLE_SCHEMA);
    return createParquetFile(
        filePath, schema, numRows, (factory, i) -> createSimpleRow(factory, i));
  }

  /**
   * Creates a Parquet file with the given schema and row creator.
   *
   * @param filePath path to write the file
   * @param schema the Parquet schema
   * @param numRows number of rows to write
   * @param rowCreator function to create each row
   * @return the path to the created file
   */
  public String createParquetFile(
      String filePath, MessageType schema, int numRows, RowCreator rowCreator) throws IOException {
    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(filePath);

    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);

    try (ParquetWriter<Group> writer =
        new ParquetWriter<>(
            hadoopPath,
            new GroupWriteSupport(),
            CompressionCodecName.UNCOMPRESSED,
            ParquetWriter.DEFAULT_BLOCK_SIZE,
            ParquetWriter.DEFAULT_PAGE_SIZE,
            ParquetWriter.DEFAULT_PAGE_SIZE,
            ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
            ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
            ParquetProperties.WriterVersion.PARQUET_1_0,
            conf)) {

      for (int i = 0; i < numRows; i++) {
        writer.write(rowCreator.createRow(factory, i));
      }
    }

    return filePath;
  }

  private Group createSimpleRow(SimpleGroupFactory factory, int rowIndex) {
    Group group = factory.newGroup();
    group.add("id", rowIndex);
    if (rowIndex % 3 != 0) { // Make some values null
      group.add("name", "name_" + rowIndex);
    }
    if (rowIndex % 2 == 0) {
      group.add("value", (long) rowIndex * 100);
    }
    return group;
  }

  /** Deletes the file at the given path if it exists. */
  public void deleteFile(String filePath) throws IOException {
    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(filePath);
    FileSystem fs = hadoopPath.getFileSystem(conf);
    if (fs.exists(hadoopPath)) {
      fs.delete(hadoopPath, false);
    }
  }

  /** Functional interface for creating rows. */
  @FunctionalInterface
  public interface RowCreator {
    Group createRow(SimpleGroupFactory factory, int rowIndex);
  }
}
