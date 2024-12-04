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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.*;
import org.apache.parquet.schema.LogicalTypeAnnotation;
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

  private ArrowArray array = null;
  private ArrowSchema schema = null;

  private long nativeBatchHandle = 0xDEADBEEFL;
  private final int columnNum;

  public NativeColumnReader(
      long nativeBatchHandle,
      int columnNum,
      DataType type,
      ColumnDescriptor descriptor,
      CometSchemaImporter importer,
      int batchSize,
      boolean useDecimal128,
      boolean useLegacyDateTimestamp) {
    super(type, descriptor, useDecimal128, useLegacyDateTimestamp);
    assert batchSize > 0 : "Batch size must be positive, found " + batchSize;
    this.batchSize = batchSize;
    this.importer = importer;
    this.nativeBatchHandle = nativeBatchHandle;
    this.columnNum = columnNum;
    initNative();
  }

  @Override
  // Override in order to avoid creation of JVM side column readers
  protected void initNative() {
    LOG.debug(
        "Native column reader " + String.join(".", this.descriptor.getPath()) + " is initialized");
    nativeHandle = 0;
  }

  @Override
  public void readBatch(int total) {
    LOG.debug("Reading column batch of size = " + total);

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

    LogicalTypeAnnotation logicalTypeAnnotation =
        descriptor.getPrimitiveType().getLogicalTypeAnnotation();
    boolean isUuid =
        logicalTypeAnnotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;

    array = ArrowArray.allocateNew(ALLOCATOR);
    schema = ArrowSchema.allocateNew(ALLOCATOR);

    long arrayAddr = array.memoryAddress();
    long schemaAddr = schema.memoryAddress();

    Native.currentColumnBatch(nativeBatchHandle, columnNum, arrayAddr, schemaAddr);

    FieldVector vector = importer.importVector(array, schema);

    DictionaryEncoding dictionaryEncoding = vector.getField().getDictionary();

    CometPlainVector cometVector = new CometPlainVector(vector, useDecimal128);

    // Update whether the current vector contains any null values. This is used in the following
    // batch(s) to determine whether we can skip loading the native vector.
    hadNull = cometVector.hasNull();

    if (dictionaryEncoding == null) {
      if (dictionary != null) {
        // This means the column was using dictionary encoding but now has fall-back to plain
        // encoding, on the native side. Setting 'dictionary' to null here, so we can use it as
        // a condition to check if we can re-use vector later.
        dictionary = null;
      }
      // Either the column is not dictionary encoded, or it was using dictionary encoding but
      // a new data page has switched back to use plain encoding. For both cases we should
      // return plain vector.
      currentVector = cometVector;
      return currentVector;
    }

    // We should already re-initiate `CometDictionary` here because `Data.importVector` API will
    // release the previous dictionary vector and create a new one.
    Dictionary arrowDictionary = importer.getProvider().lookup(dictionaryEncoding.getId());
    CometPlainVector dictionaryVector =
        new CometPlainVector(arrowDictionary.getVector(), useDecimal128, isUuid);
    if (dictionary != null) {
      dictionary.setDictionaryVector(dictionaryVector);
    } else {
      dictionary = new CometDictionary(dictionaryVector);
    }

    currentVector =
        new CometDictionaryVector(
            cometVector, dictionary, importer.getProvider(), useDecimal128, false, isUuid);

    currentVector =
        new CometDictionaryVector(cometVector, dictionary, importer.getProvider(), useDecimal128);
    return currentVector;
  }
}
