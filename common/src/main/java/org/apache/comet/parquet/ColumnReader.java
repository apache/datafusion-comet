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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CometSchemaImporter;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.spark.sql.types.DataType;

import org.apache.comet.CometConf;
import org.apache.comet.vector.CometDecodedVector;
import org.apache.comet.vector.CometDictionary;
import org.apache.comet.vector.CometDictionaryVector;
import org.apache.comet.vector.CometPlainVector;
import org.apache.comet.vector.CometVector;

public class ColumnReader extends AbstractColumnReader {
  protected static final Logger LOG = LoggerFactory.getLogger(ColumnReader.class);

  /**
   * The current Comet vector holding all the values read by this column reader. Owned by this
   * reader and MUST be closed after use.
   */
  private CometDecodedVector currentVector;

  /** Dictionary values for this column. Only set if the column is using dictionary encoding. */
  protected CometDictionary dictionary;

  /** Reader for dictionary & data pages in the current column chunk. */
  protected PageReader pageReader;

  /** Whether the first data page has been loaded. */
  private boolean firstPageLoaded = false;

  /**
   * The number of nulls in the current batch, used when we are skipping importing of Arrow vectors,
   * in which case we'll simply update the null count of the existing vectors.
   */
  int currentNumNulls;

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

  public ColumnReader(
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
    initNative();
  }

  /**
   * Set the page reader for a new column chunk to read. Expects to call `readBatch` after this.
   *
   * @param pageReader the page reader for the new column chunk
   */
  public void setPageReader(PageReader pageReader) throws IOException {
    this.pageReader = pageReader;

    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
    if (dictionaryPage != null) {
      LOG.debug("dictionary page encoding = {}", dictionaryPage.getEncoding());
      Native.setDictionaryPage(
          nativeHandle,
          dictionaryPage.getDictionarySize(),
          dictionaryPage.getBytes().toByteArray(),
          dictionaryPage.getEncoding().ordinal());
    }
  }

  @Override
  public void readBatch(int total) {
    LOG.debug("Start to batch of size = " + total);

    if (!firstPageLoaded) {
      readPage();
      firstPageLoaded = true;
    }

    // Now first reset the current columnar batch so that it can be used to fill in a new batch
    // of values. Then, keep reading more data pages (via 'readBatch') until the current batch is
    // full, or we have read 'total' number of values.
    Native.resetBatch(nativeHandle);

    int left = total, nullsRead = 0;
    while (left > 0) {
      int[] array = Native.readBatch(nativeHandle, left);
      int valuesRead = array[0];
      nullsRead += array[1];
      if (valuesRead < left) {
        readPage();
      }
      left -= valuesRead;
    }

    this.currentNumValues = total;
    this.currentNumNulls = nullsRead;
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
    // Only re-use Comet vector iff:
    //   1. if we're not using dictionary encoding, since with dictionary encoding, the native
    //      side may fallback to plain encoding and the underlying memory address for the vector
    //      will change as result.
    //   2. if the column type is of fixed width, in other words, string/binary are not supported
    //      since the native side may resize the vector and therefore change memory address.
    //   3. if the last loaded vector contains null values: if values of last vector are all not
    //      null, Arrow C data API will skip loading the native validity buffer, therefore we
    //      should not re-use the vector in that case.
    //   4. if the last loaded vector doesn't contain any null value, but the current vector also
    //      are all not null, which means we can also re-use the loaded vector.
    //   5. if the new number of value is the same or smaller
    if ((hadNull || currentNumNulls == 0)
        && currentVector != null
        && dictionary == null
        && currentVector.isFixedLength()
        && currentVector.numValues() >= currentNumValues) {
      currentVector.setNumNulls(currentNumNulls);
      currentVector.setNumValues(currentNumValues);
      return currentVector;
    }

    LOG.debug("Reloading vector");

    // Close the previous vector first to release struct memory allocated to import Arrow array &
    // schema from native side, through the C data interface
    if (currentVector != null) {
      currentVector.close();
    }

    long[] addresses = Native.currentBatch(nativeHandle);

    try (ArrowArray array = ArrowArray.wrap(addresses[0]);
        ArrowSchema schema = ArrowSchema.wrap(addresses[1])) {
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

      // There is dictionary from native side but the Java side dictionary hasn't been
      // initialized yet.
      Dictionary arrowDictionary = importer.getProvider().lookup(dictionaryEncoding.getId());
      CometPlainVector dictionaryVector =
          new CometPlainVector(arrowDictionary.getVector(), useDecimal128);
      dictionary = new CometDictionary(dictionaryVector);

      currentVector =
          new CometDictionaryVector(cometVector, dictionary, importer.getProvider(), useDecimal128);
      return currentVector;
    }
  }

  protected void readPage() {
    DataPage page = pageReader.readPage();
    if (page == null) {
      throw new RuntimeException("overreading: returned DataPage is null");
    }
    ;
    int pageValueCount = page.getValueCount();
    page.accept(
        new DataPage.Visitor<Void>() {
          @Override
          public Void visit(DataPageV1 dataPageV1) {
            LOG.debug("data page encoding = {}", dataPageV1.getValueEncoding());
            if (dataPageV1.getDlEncoding() != Encoding.RLE
                && descriptor.getMaxDefinitionLevel() != 0) {
              throw new UnsupportedOperationException(
                  "Unsupported encoding: " + dataPageV1.getDlEncoding());
            }
            if (!isValidValueEncoding(dataPageV1.getValueEncoding())) {
              throw new UnsupportedOperationException(
                  "Unsupported value encoding: " + dataPageV1.getValueEncoding());
            }
            try {
              boolean useDirectBuffer =
                  (Boolean) CometConf.COMET_PARQUET_ENABLE_DIRECT_BUFFER().get();
              if (useDirectBuffer) {
                ByteBuffer buffer = dataPageV1.getBytes().toByteBuffer();
                Native.setPageBufferV1(
                    nativeHandle, pageValueCount, buffer, dataPageV1.getValueEncoding().ordinal());
              } else {
                byte[] array = dataPageV1.getBytes().toByteArray();
                Native.setPageV1(
                    nativeHandle, pageValueCount, array, dataPageV1.getValueEncoding().ordinal());
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            return null;
          }

          @Override
          public Void visit(DataPageV2 dataPageV2) {
            if (!isValidValueEncoding(dataPageV2.getDataEncoding())) {
              throw new UnsupportedOperationException(
                  "Unsupported encoding: " + dataPageV2.getDataEncoding());
            }
            try {
              Native.setPageV2(
                  nativeHandle,
                  pageValueCount,
                  dataPageV2.getDefinitionLevels().toByteArray(),
                  dataPageV2.getRepetitionLevels().toByteArray(),
                  dataPageV2.getData().toByteArray(),
                  dataPageV2.getDataEncoding().ordinal());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            return null;
          }
        });
  }

  @SuppressWarnings("deprecation")
  private boolean isValidValueEncoding(Encoding encoding) {
    switch (encoding) {
      case PLAIN:
      case RLE_DICTIONARY:
      case PLAIN_DICTIONARY:
        return true;
      default:
        return false;
    }
  }
}
