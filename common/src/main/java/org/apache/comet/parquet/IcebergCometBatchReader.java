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

import java.util.HashMap;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import org.apache.comet.vector.CometVector;

/**
 * This class is a public interface used by Apache Iceberg to read batches
 * using Comet
 */
public class IcebergCometBatchReader extends BatchReader {
  public IcebergCometBatchReader(int numColumns, StructType schema) {
    this.columnReaders = new AbstractColumnReader[numColumns];
    this.vectors = new CometVector[numColumns];
    this.currentBatch = new ColumnarBatch(vectors);
    this.metrics = new HashMap<>();
    this.sparkSchema = schema;
  }

  public void init(AbstractColumnReader[] columnReaders) {
    this.columnReaders = columnReaders;
    this.isInitialized = true;
  }
}
