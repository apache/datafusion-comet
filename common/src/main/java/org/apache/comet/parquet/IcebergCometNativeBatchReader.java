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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.types.StructType;

/**
 * A specialized NativeBatchReader for Iceberg that accepts ParquetMetadata as a Thrift encoded byte
 * array . This allows Iceberg to pass metadata in serialized form with a two-step initialization
 * pattern.
 */
public class IcebergCometNativeBatchReader extends NativeBatchReader {

  public IcebergCometNativeBatchReader(StructType requiredSchema) {
    super();
    this.sparkSchema = requiredSchema;
  }

  /** Initialize the reader using FileInfo instead of PartitionedFile. */
  public void init(
      Configuration conf,
      FileInfo fileInfo,
      byte[] parquetMetadataBytes,
      byte[] nativeFilter,
      int capacity,
      StructType dataSchema,
      boolean isCaseSensitive,
      boolean useFieldId,
      boolean ignoreMissingIds,
      boolean useLegacyDateTimestamp,
      StructType partitionSchema,
      InternalRow partitionValues,
      AbstractColumnReader[] preInitializedReaders,
      Map<String, SQLMetric> metrics)
      throws Throwable {

    // Set parent fields
    this.conf = conf;
    this.fileInfo = fileInfo;
    this.footer = new ParquetMetadataSerializer().deserialize(parquetMetadataBytes);
    this.nativeFilter = nativeFilter;
    this.capacity = capacity;
    this.dataSchema = dataSchema;
    this.isCaseSensitive = isCaseSensitive;
    this.useFieldId = useFieldId;
    this.ignoreMissingIds = ignoreMissingIds;
    this.useLegacyDateTimestamp = useLegacyDateTimestamp;
    this.partitionSchema = partitionSchema;
    this.partitionValues = partitionValues;
    this.preInitializedReaders = preInitializedReaders;
    this.metrics.clear();
    if (metrics != null) {
      this.metrics.putAll(metrics);
    }

    // Call parent init method
    super.init();
  }

  public StructType getSparkSchema() {
    return this.sparkSchema;
  }
}
