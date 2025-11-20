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

package org.apache.comet.parquet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

/**
 * Utility class for serializing and deserializing ParquetMetadata instances to/from byte arrays.
 * This uses the Parquet format's FileMetaData structure and the underlying Thrift compact protocol
 * for serialization.
 */
public class ParquetMetadataSerializer {

  private final ParquetMetadataConverter converter;

  public ParquetMetadataSerializer() {
    this.converter = new ParquetMetadataConverter();
  }

  public ParquetMetadataSerializer(ParquetMetadataConverter converter) {
    this.converter = converter;
  }

  /**
   * Serializes a ParquetMetadata instance to a byte array.
   *
   * @param metadata the ParquetMetadata to serialize
   * @return the serialized byte array
   * @throws IOException if an error occurs during serialization
   */
  public byte[] serialize(ParquetMetadata metadata) throws IOException {
    FileMetaData fileMetaData = converter.toParquetMetadata(1, metadata);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Util.writeFileMetaData(fileMetaData, outputStream);
    return outputStream.toByteArray();
  }

  /**
   * Deserializes a byte array back into a ParquetMetadata instance.
   *
   * @param bytes the serialized byte array
   * @return the deserialized ParquetMetadata
   * @throws IOException if an error occurs during deserialization
   */
  public ParquetMetadata deserialize(byte[] bytes) throws IOException {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    FileMetaData fileMetaData = Util.readFileMetaData(inputStream);
    return converter.fromParquetMetadata(fileMetaData);
  }
}
