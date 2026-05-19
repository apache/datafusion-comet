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
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.ModuleCipherFactory;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.ParquetDecodingException;

public class ColumnPageReader implements PageReader {
  private final CompressionCodecFactory.BytesInputDecompressor decompressor;
  private final long valueCount;
  private final Queue<DataPage> compressedPages;
  private final DictionaryPage compressedDictionaryPage;

  private final OffsetIndex offsetIndex;
  private final long rowCount;
  private int pageIndex = 0;

  private final BlockCipher.Decryptor blockDecryptor;
  private final byte[] dataPageAAD;
  private final byte[] dictionaryPageAAD;

  ColumnPageReader(
      CompressionCodecFactory.BytesInputDecompressor decompressor,
      List<DataPage> compressedPages,
      DictionaryPage compressedDictionaryPage,
      OffsetIndex offsetIndex,
      long rowCount,
      BlockCipher.Decryptor blockDecryptor,
      byte[] fileAAD,
      int rowGroupOrdinal,
      int columnOrdinal) {
    this.decompressor = decompressor;
    this.compressedPages = new ArrayDeque<>(compressedPages);
    this.compressedDictionaryPage = compressedDictionaryPage;
    long count = 0;
    for (DataPage p : compressedPages) {
      count += p.getValueCount();
    }
    this.valueCount = count;
    this.offsetIndex = offsetIndex;
    this.rowCount = rowCount;
    this.blockDecryptor = blockDecryptor;

    if (blockDecryptor != null) {
      dataPageAAD =
          AesCipher.createModuleAAD(
              fileAAD, ModuleCipherFactory.ModuleType.DataPage, rowGroupOrdinal, columnOrdinal, 0);
      dictionaryPageAAD =
          AesCipher.createModuleAAD(
              fileAAD,
              ModuleCipherFactory.ModuleType.DictionaryPage,
              rowGroupOrdinal,
              columnOrdinal,
              -1);
    } else {
      dataPageAAD = null;
      dictionaryPageAAD = null;
    }
  }

  @Override
  public long getTotalValueCount() {
    return valueCount;
  }

  /** Returns the total value count of the current page. */
  public int getPageValueCount() {
    return compressedPages.element().getValueCount();
  }

  /** Skips the current page so it won't be returned by {@link #readPage()} */
  public void skipPage() {
    compressedPages.poll();
    pageIndex++;
  }

  @Override
  public DataPage readPage() {
    final DataPage compressedPage = compressedPages.poll();
    if (compressedPage == null) {
      return null;
    }
    final int currentPageIndex = pageIndex++;

    if (null != blockDecryptor) {
      AesCipher.quickUpdatePageAAD(dataPageAAD, getPageOrdinal(currentPageIndex));
    }

    return compressedPage.accept(
        new DataPage.Visitor<DataPage>() {
          @Override
          public DataPage visit(DataPageV1 dataPageV1) {
            try {
              BytesInput bytes = dataPageV1.getBytes();
              if (null != blockDecryptor) {
                bytes = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dataPageAAD));
              }
              BytesInput decompressed =
                  decompressor.decompress(bytes, dataPageV1.getUncompressedSize());

              final DataPageV1 decompressedPage;
              if (offsetIndex == null) {
                decompressedPage =
                    new DataPageV1(
                        decompressed,
                        dataPageV1.getValueCount(),
                        dataPageV1.getUncompressedSize(),
                        dataPageV1.getStatistics(),
                        dataPageV1.getRlEncoding(),
                        dataPageV1.getDlEncoding(),
                        dataPageV1.getValueEncoding());
              } else {
                long firstRowIndex = offsetIndex.getFirstRowIndex(currentPageIndex);
                decompressedPage =
                    new DataPageV1(
                        decompressed,
                        dataPageV1.getValueCount(),
                        dataPageV1.getUncompressedSize(),
                        firstRowIndex,
                        Math.toIntExact(
                            offsetIndex.getLastRowIndex(currentPageIndex, rowCount)
                                - firstRowIndex
                                + 1),
                        dataPageV1.getStatistics(),
                        dataPageV1.getRlEncoding(),
                        dataPageV1.getDlEncoding(),
                        dataPageV1.getValueEncoding());
              }
              if (dataPageV1.getCrc().isPresent()) {
                decompressedPage.setCrc(dataPageV1.getCrc().getAsInt());
              }
              return decompressedPage;
            } catch (IOException e) {
              throw new ParquetDecodingException("could not decompress page", e);
            }
          }

          @Override
          public DataPage visit(DataPageV2 dataPageV2) {
            if (!dataPageV2.isCompressed() && offsetIndex == null && null == blockDecryptor) {
              return dataPageV2;
            }
            BytesInput pageBytes = dataPageV2.getData();

            if (null != blockDecryptor) {
              try {
                pageBytes =
                    BytesInput.from(blockDecryptor.decrypt(pageBytes.toByteArray(), dataPageAAD));
              } catch (IOException e) {
                throw new ParquetDecodingException(
                    "could not convert page ByteInput to byte array", e);
              }
            }
            if (dataPageV2.isCompressed()) {
              int uncompressedSize =
                  Math.toIntExact(
                      dataPageV2.getUncompressedSize()
                          - dataPageV2.getDefinitionLevels().size()
                          - dataPageV2.getRepetitionLevels().size());
              try {
                pageBytes = decompressor.decompress(pageBytes, uncompressedSize);
              } catch (IOException e) {
                throw new ParquetDecodingException("could not decompress page", e);
              }
            }

            if (offsetIndex == null) {
              return DataPageV2.uncompressed(
                  dataPageV2.getRowCount(),
                  dataPageV2.getNullCount(),
                  dataPageV2.getValueCount(),
                  dataPageV2.getRepetitionLevels(),
                  dataPageV2.getDefinitionLevels(),
                  dataPageV2.getDataEncoding(),
                  pageBytes,
                  dataPageV2.getStatistics());
            } else {
              return DataPageV2.uncompressed(
                  dataPageV2.getRowCount(),
                  dataPageV2.getNullCount(),
                  dataPageV2.getValueCount(),
                  offsetIndex.getFirstRowIndex(currentPageIndex),
                  dataPageV2.getRepetitionLevels(),
                  dataPageV2.getDefinitionLevels(),
                  dataPageV2.getDataEncoding(),
                  pageBytes,
                  dataPageV2.getStatistics());
            }
          }
        });
  }

  @Override
  public DictionaryPage readDictionaryPage() {
    if (compressedDictionaryPage == null) {
      return null;
    }
    try {
      BytesInput bytes = compressedDictionaryPage.getBytes();
      if (null != blockDecryptor) {
        bytes = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dictionaryPageAAD));
      }
      DictionaryPage decompressedPage =
          new DictionaryPage(
              decompressor.decompress(bytes, compressedDictionaryPage.getUncompressedSize()),
              compressedDictionaryPage.getDictionarySize(),
              compressedDictionaryPage.getEncoding());
      if (compressedDictionaryPage.getCrc().isPresent()) {
        decompressedPage.setCrc(compressedDictionaryPage.getCrc().getAsInt());
      }
      return decompressedPage;
    } catch (IOException e) {
      throw new ParquetDecodingException("Could not decompress dictionary page", e);
    }
  }

  private int getPageOrdinal(int currentPageIndex) {
    return offsetIndex == null ? currentPageIndex : offsetIndex.getPageOrdinal(currentPageIndex);
  }
}
