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

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.comet.NativeBase;

public final class Native extends NativeBase {
  public static int[] readBatch(long handle, int batchSize) {
    return readBatch(handle, batchSize, 0);
  }

  public static int skipBatch(long handle, int batchSize) {
    return skipBatch(handle, batchSize, false);
  }

  /** Native APIs * */

  /**
   * Creates a reader for a primitive Parquet column.
   *
   * @param physicalTypeId id for Parquet physical type
   * @param logicalTypeId id for Parquet logical type
   * @param expectedPhysicalTypeId id for Parquet physical type, converted from Spark read type.
   *     This is used for type promotion.
   * @param path the path from the root schema to the column, derived from the method
   *     'ColumnDescriptor#getPath()'.
   * @param maxDl the maximum definition level of the primitive column
   * @param maxRl the maximum repetition level of the primitive column
   * @param bitWidth (only set when logical type is INT) the bit width for the integer type (INT8,
   *     INT16, INT32, etc)
   * @param isSigned (only set when logical type is INT) whether it is signed or unsigned int.
   * @param typeLength number of bytes required to store a value of the type, only set when the
   *     physical type is FIXED_LEN_BYTE_ARRAY, otherwise it's 0.
   * @param precision (only set when logical type is DECIMAL) precision of the decimal type
   * @param expectedPrecision (only set when logical type is DECIMAL) precision of the decimal type
   *     from Spark read schema. This is used for type promotion.
   * @param scale (only set when logical type is DECIMAL) scale of the decimal type
   * @param tu (only set when logical type is TIMESTAMP) unit for the timestamp
   * @param isAdjustedUtc (only set when logical type is TIMESTAMP) whether the timestamp is
   *     adjusted to UTC or not
   * @param batchSize the batch size for the columnar read
   * @param useDecimal128 whether to always return 128 bit decimal regardless of precision
   * @param useLegacyDateTimestampOrNTZ whether to read legacy dates/timestamps as it is
   * @return a pointer to a native Parquet column reader created
   */
  public static native long initColumnReader(
      int physicalTypeId,
      int logicalTypeId,
      int expectedPhysicalTypeId,
      String[] path,
      int maxDl,
      int maxRl,
      int bitWidth,
      int expectedBitWidth,
      boolean isSigned,
      int typeLength,
      int precision,
      int expectedPrecision,
      int scale,
      int expectedScale,
      int tu,
      boolean isAdjustedUtc,
      int batchSize,
      boolean useDecimal128,
      boolean useLegacyDateTimestampOrNTZ);

  /**
   * Pass a Parquet dictionary page to the native column reader. Note this should only be called
   * once per Parquet column chunk. Otherwise it'll panic.
   *
   * @param handle the handle to the native Parquet column reader
   * @param dictionaryValueCount the number of values in this dictionary
   * @param dictionaryData the actual dictionary page data, including repetition/definition levels
   *     as well as values
   * @param encoding the encoding used by the dictionary
   */
  public static native void setDictionaryPage(
      long handle, int dictionaryValueCount, byte[] dictionaryData, int encoding);

  /**
   * Passes a Parquet data page V1 to the native column reader.
   *
   * @param handle the handle to the native Parquet column reader
   * @param pageValueCount the number of values in this data page
   * @param pageData the actual page data, which should only contain PLAIN-encoded values.
   * @param valueEncoding the encoding used by the values
   */
  public static native void setPageV1(
      long handle, int pageValueCount, byte[] pageData, int valueEncoding);

  /**
   * Passes a Parquet data page V1 to the native column reader.
   *
   * @param handle the handle to the native Parquet column reader
   * @param pageValueCount the number of values in this data page
   * @param buffer the actual page data, represented by a DirectByteBuffer.
   * @param valueEncoding the encoding used by the values
   */
  public static native void setPageBufferV1(
      long handle, int pageValueCount, ByteBuffer buffer, int valueEncoding);

  /**
   * Passes a Parquet data page V2 to the native column reader.
   *
   * @param handle the handle to the native Parquet column reader
   * @param pageValueCount the number of values in this data page
   * @param defLevelData the data for definition levels
   * @param repLevelData the data for repetition levels
   * @param valueData the data for values
   * @param valueEncoding the encoding used by the values
   */
  public static native void setPageV2(
      long handle,
      int pageValueCount,
      byte[] defLevelData,
      byte[] repLevelData,
      byte[] valueData,
      int valueEncoding);

  /**
   * Reset the current columnar batch. This will clear all the content of the batch as well as any
   * internal state such as the current offset.
   *
   * @param handle the handle to the native Parquet column reader
   */
  public static native void resetBatch(long handle);

  /**
   * Reads at most 'batchSize' number of rows from the native Parquet column reader. Returns a tuple
   * where the first element is the actual number of rows read (including both nulls and non-nulls),
   * and the second element is the number of nulls read.
   *
   * <p>If the returned value is < 'batchSize' then it means the current page has been completely
   * drained. In this case, the caller should call {@link Native#setPageV1} or {@link
   * Native#setPageV2} before the next 'readBatch' call.
   *
   * <p>Note that the current page could also be drained if the returned value = 'batchSize', i.e.,
   * the remaining number of rows in the page is exactly equal to 'batchSize'. In this case, the
   * next 'readBatch' call will return 0 and the caller should call {@link Native#setPageV1} or
   * {@link Native#setPageV2} next.
   *
   * <p>If `nullPadSize` > 0, it pads nulls into the underlying vector before the values will be
   * read into.
   *
   * @param handle the handle to the native Parquet column reader
   * @param batchSize the number of rows to be read
   * @param nullPadSize the number of nulls to pad before reading.
   * @return a tuple: (the actual number of rows read, the number of nulls read)
   */
  public static native int[] readBatch(long handle, int batchSize, int nullPadSize);

  /**
   * Skips at most 'batchSize' number of rows from the native Parquet column reader, and returns the
   * actual number of rows skipped.
   *
   * <p>If the returned value is < 'batchSize' then it means the current page has been completely
   * drained. In this case, the caller should call {@link Native#setPageV1} or {@link
   * Native#setPageV2} before the next 'skipBatch' call.
   *
   * <p>Note that the current page could also be drained if the returned value = 'batchSize', i.e.,
   * the remaining number of rows in the page is exactly equal to 'batchSize'. In this case, the
   * next 'skipBatch' call will return 0 and the caller should call {@link Native#setPageV1} or
   * {@link Native#setPageV2} next.
   *
   * @param handle the handle to the native Parquet column reader
   * @param batchSize the number of rows to skip in the current page
   * @param discard if true, discard read rows without padding nulls into the underlying vector
   * @return the actual number of rows skipped
   */
  public static native int skipBatch(long handle, int batchSize, boolean discard);

  /**
   * Returns the current batch constructed via 'readBatch'
   *
   * @param handle the handle to the native Parquet column reader
   * @param arrayAddr the memory address to the ArrowArray struct
   * @param schemaAddr the memory address to the ArrowSchema struct
   */
  public static native void currentBatch(long handle, long arrayAddr, long schemaAddr);

  /** Set methods to set a constant value for the reader, so it'll return constant vectors */
  public static native void setNull(long handle);

  public static native void setBoolean(long handle, boolean value);

  public static native void setByte(long handle, byte value);

  public static native void setShort(long handle, short value);

  public static native void setInt(long handle, int value);

  public static native void setLong(long handle, long value);

  public static native void setFloat(long handle, float value);

  public static native void setDouble(long handle, double value);

  public static native void setBinary(long handle, byte[] value);

  /** Set decimal backed by FixedLengthByteArray */
  public static native void setDecimal(long handle, byte[] value);

  /** Set position of row index vector for Iceberg Metadata Column */
  public static native void setPosition(long handle, long value, int size);

  /** Set row index vector for Spark row index metadata column and return vector size */
  public static native int setIndices(long handle, long offset, int size, long[] indices);

  /** Set deleted info for Iceberg Metadata Column */
  public static native void setIsDeleted(long handle, boolean[] isDeleted);

  /**
   * Closes the native Parquet column reader and releases all resources associated with it.
   *
   * @param handle the handle to the native Parquet column reader
   */
  public static native void closeColumnReader(long handle);

  ///////////// Arrow Native Parquet Reader APIs
  // TODO: Add partitionValues(?), improve requiredColumns to use a projection mask that corresponds
  // to arrow.
  //      Add batch size, datetimeRebaseModeSpec, metrics(how?)...

  /**
   * Verify that object store options are valid.
   *
   * @param filePath
   * @return true if the object store is supported
   */
  public static native boolean isValidObjectStore(
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
      Map<String, String> objectStoreOptions);

  // arrow native version of read batch
  /**
   * Read the next batch of data into memory on native side
   *
   * @param handle
   * @return the number of rows read
   */
  public static native int readNextRecordBatch(long handle);

  // arrow native equivalent of currentBatch. 'columnNum' is number of the column in the record
  // batch
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

  // arrow native version to close record batch reader

  /**
   * Close the record batch reader. Free the resources
   *
   * @param handle
   */
  public static native void closeRecordBatchReader(long handle);
}
