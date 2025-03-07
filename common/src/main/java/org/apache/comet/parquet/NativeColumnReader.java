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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CometSchemaImporter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.types.DataType;

import org.apache.comet.vector.*;

// TODO: extend ColumnReader instead of AbstractColumnReader to reduce code duplication
public class NativeColumnReader extends AbstractColumnReader {
  protected static final Logger LOG = LoggerFactory.getLogger(NativeColumnReader.class);
  protected final BufferAllocator ALLOCATOR = new RootAllocator();

  /**
   * The current Comet vector holding all the values read by this column reader. Owned by this
   * reader and MUST be closed after use.
   */
  private CometDecodedVector currentVector;

  /** Dictionary values for this column. Only set if the column is using dictionary encoding. */
  protected CometDictionary dictionary;

  /**
   * The number of values in the current batch, used when we are skipping importing of Arrow
   * vectors, in which case we'll simply update the null count of the existing vectors.
   */
  int currentNumValues;

  /**
   * Whether the last loaded vector contains any null value. This is used to determine if we can
   * skip vector reloading. If the flag is false, Arrow C API will skip to import the validity
   * buffer, and therefore we cannot skip vector reloading.
   */
  boolean hadNull;

  private final CometSchemaImporter importer;
  private final NativeUtil nativeUtil;

  private ArrowArray array = null;
  private ArrowSchema schema = null;

  private long nativeBatchHandle = 0xDEADBEEFL;
  private final int columnNum;

  public NativeColumnReader(
      long nativeBatchHandle,
      int columnNum,
      DataType type,
      Type fieldType,
      ColumnDescriptor descriptor,
      CometSchemaImporter importer,
      NativeUtil nativeUtil,
      int batchSize,
      boolean useDecimal128,
      boolean useLegacyDateTimestamp) {
    super(type, fieldType, descriptor, useDecimal128, useLegacyDateTimestamp);
    assert batchSize > 0 : "Batch size must be positive, found " + batchSize;
    this.batchSize = batchSize;
    this.nativeUtil = nativeUtil;
    this.importer = importer;
    this.nativeBatchHandle = nativeBatchHandle;
    this.columnNum = columnNum;
    initNative();
  }

  @Override
  // Override in order to avoid creation of JVM side column readers
  protected void initNative() {
    LOG.debug(
        "Native column reader {} is initialized", String.join(".", this.type.catalogString()));
    nativeHandle = 0;
  }

  @Override
  public void readBatch(int total) {
    LOG.debug("Reading column batch of size = {}", total);

    this.currentNumValues = total;
  }

  /** Returns the {@link CometVector} read by this reader. */
  @Override
  public CometVector currentBatch() {
    return loadVector();
  }

  @Override
  public void close() {
    if (currentVector != null) {
      currentVector.close();
      currentVector = null;
    }
    super.close();
  }

  /** Returns a decoded {@link CometDecodedVector Comet vector}. */
  public CometDecodedVector loadVector() {

    LOG.debug("Loading vector for next batch");

    // Close the previous vector first to release struct memory allocated to import Arrow array &
    // schema from native side, through the C data interface
    if (currentVector != null) {
      currentVector.close();
    }

    // TODO: ARROW NATIVE : Handle Uuid?

    array = ArrowArray.allocateNew(ALLOCATOR);
    schema = ArrowSchema.allocateNew(ALLOCATOR);

    long arrayAddr = array.memoryAddress();
    long schemaAddr = schema.memoryAddress();

    Native.currentColumnBatch(nativeBatchHandle, columnNum, arrayAddr, schemaAddr);

    ArrowArray[] arrays = {array};
    ArrowSchema[] schemas = {schema};

    CometDecodedVector cometVector =
        (CometDecodedVector)
            scala.collection.JavaConverters.seqAsJavaList(nativeUtil.importVector(arrays, schemas))
                .get(0);

    // Update whether the current vector contains any null values. This is used in the following
    // batch(s) to determine whether we can skip loading the native vector.
    hadNull = cometVector.hasNull();

    currentVector = cometVector;
    return currentVector;
  }
}
