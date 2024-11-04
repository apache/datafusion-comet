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

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.TimestampNTZType$;

import org.apache.comet.CometConf;
import org.apache.comet.vector.CometVector;

/** Base class for Comet Parquet column reader implementations. */
public abstract class AbstractColumnReader implements AutoCloseable {
  protected static final Logger LOG = LoggerFactory.getLogger(AbstractColumnReader.class);

  /** The Spark data type. */
  protected final DataType type;

  /** Parquet column descriptor. */
  protected final ColumnDescriptor descriptor;

  /**
   * Whether to always return 128 bit decimals, regardless of its precision. If false, this will
   * return 32, 64 or 128 bit decimals depending on the precision.
   */
  protected final boolean useDecimal128;

  /**
   * Whether to return dates/timestamps that were written with legacy hybrid (Julian + Gregorian)
   * calendar as it is. If this is true, Comet will return them as it is, instead of rebasing them
   * to the new Proleptic Gregorian calendar. If this is false, Comet will throw exceptions when
   * seeing these dates/timestamps.
   */
  protected final boolean useLegacyDateTimestamp;

  /** The size of one batch, gets updated by 'readBatch' */
  protected int batchSize;

  /** A pointer to the native implementation of ColumnReader. */
  protected long nativeHandle;

  public AbstractColumnReader(
      DataType type,
      ColumnDescriptor descriptor,
      boolean useDecimal128,
      boolean useLegacyDateTimestamp) {
    this.type = type;
    this.descriptor = descriptor;
    this.useDecimal128 = useDecimal128;
    this.useLegacyDateTimestamp = useLegacyDateTimestamp;
    TypeUtil.checkParquetType(descriptor, type);
  }

  public ColumnDescriptor getDescriptor() {
    return descriptor;
  }

  /**
   * Set the batch size of this reader to be 'batchSize'. Also initializes the native column reader.
   */
  public void setBatchSize(int batchSize) {
    assert nativeHandle == 0
        : "Native column reader shouldn't be initialized before " + "'setBatchSize' is called";
    this.batchSize = batchSize;
    initNative();
  }

  /**
   * Reads a batch of 'total' new rows.
   *
   * @param total the total number of rows to read
   */
  public abstract void readBatch(int total);

  /** Returns the {@link CometVector} read by this reader. */
  public abstract CometVector currentBatch();

  @Override
  public void close() {
    if (nativeHandle != 0) {
      LOG.debug("Closing the column reader");
      Native.closeColumnReader(nativeHandle);
      nativeHandle = 0;
    }
  }

  protected void initNative() {
    LOG.debug("initializing the native column reader");
    DataType readType = (boolean) CometConf.COMET_SCHEMA_EVOLUTION_ENABLED().get() ? type : null;
    boolean useLegacyDateTimestampOrNTZ =
        useLegacyDateTimestamp || type == TimestampNTZType$.MODULE$;
    nativeHandle =
        Utils.initColumnReader(
            descriptor, readType, batchSize, useDecimal128, useLegacyDateTimestampOrNTZ);
  }
}
