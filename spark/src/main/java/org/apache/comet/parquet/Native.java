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

import org.apache.comet.NativeBase;

public final class Native extends NativeBase {

  ///////////// Arrow Native Parquet Reader APIs

  /**
   * Verify that object store options are valid. An exception will be thrown if the provided options
   * are not valid.
   */
  public static native void validateObjectStoreConfig(
      String filePath, Map<String, String> objectStoreOptions);

  /**
   * Initialize a record batch reader for a PartitionedFile
   *
   * @param filePath
   * @param starts
   * @param lengths
   * @return a handle to the record batch reader, used in subsequent calls.
   */
  public static native long initRecordBatchReader(
      String filePath,
      long fileSize,
      long[] starts,
      long[] lengths,
      byte[] filter,
      byte[] requiredSchema,
      byte[] dataSchema,
      String sessionTimezone,
      int batchSize,
      boolean caseSensitive,
      boolean returnNullStructIfAllFieldsMissing,
      Map<String, String> objectStoreOptions,
      CometFileKeyUnwrapper keyUnwrapper,
      Object metricsNode);

  /**
   * Read the next batch of data into memory on native side
   *
   * @param handle
   * @return the number of rows read
   */
  public static native int readNextRecordBatch(long handle);

  /**
   * Load the column corresponding to columnNum in the currently loaded record batch into JVM
   *
   * @param handle
   * @param columnNum
   * @param arrayAddr
   * @param schemaAddr
   */
  public static native void currentColumnBatch(
      long handle, int columnNum, long arrayAddr, long schemaAddr);

  /**
   * Close the record batch reader. Free the resources
   *
   * @param handle
   */
  public static native void closeRecordBatchReader(long handle);
}
