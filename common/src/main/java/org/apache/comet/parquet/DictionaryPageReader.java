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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.InternalColumnDecryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ModuleCipherFactory;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.SeekableInputStream;

import org.apache.comet.util.JavaUtils;

public class DictionaryPageReader implements DictionaryPageReadStore {
  private final Map<String, Optional<DictionaryPage>> cache;
  private final InternalFileDecryptor fileDecryptor;
  private final SeekableInputStream inputStream;
  private final ParquetReadOptions options;
  private final Map<String, ColumnChunkMetaData> columns;

  DictionaryPageReader(
      BlockMetaData block,
      InternalFileDecryptor fileDecryptor,
      SeekableInputStream inputStream,
      ParquetReadOptions options) {
    this.columns = new HashMap<>();
    this.cache = JavaUtils.newConcurrentHashMap();
    this.fileDecryptor = fileDecryptor;
    this.inputStream = inputStream;
    this.options = options;

    for (ColumnChunkMetaData column : block.getColumns()) {
      columns.put(column.getPath().toDotString(), column);
    }
  }

  @Override
  public DictionaryPage readDictionaryPage(ColumnDescriptor descriptor) {
    String dotPath = String.join(".", descriptor.getPath());
    ColumnChunkMetaData column = columns.get(dotPath);

    if (column == null) {
      throw new ParquetDecodingException("Failed to load dictionary, unknown column: " + dotPath);
    }

    return cache
        .computeIfAbsent(
            dotPath,
            key -> {
              try {
                final DictionaryPage dict =
                    column.hasDictionaryPage() ? readDictionary(column) : null;

                // Copy the dictionary to ensure it can be reused if it is returned
                // more than once. This can happen when a DictionaryFilter has two or
                // more predicates for the same column. Cache misses as well.
                return (dict != null) ? Optional.of(reusableCopy(dict)) : Optional.empty();
              } catch (IOException e) {
                throw new ParquetDecodingException("Failed to read dictionary", e);
              }
            })
        .orElse(null);
  }

  DictionaryPage readDictionary(ColumnChunkMetaData meta) throws IOException {
    if (!meta.hasDictionaryPage()) {
      return null;
    }

    if (inputStream.getPos() != meta.getStartingPos()) {
      inputStream.seek(meta.getStartingPos());
    }

    boolean encryptedColumn = false;
    InternalColumnDecryptionSetup columnDecryptionSetup = null;
    byte[] dictionaryPageAAD = null;
    BlockCipher.Decryptor pageDecryptor = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      columnDecryptionSetup = fileDecryptor.getColumnSetup(meta.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        encryptedColumn = true;
      }
    }

    PageHeader pageHeader;
    if (!encryptedColumn) {
      pageHeader = Util.readPageHeader(inputStream);
    } else {
      byte[] dictionaryPageHeaderAAD =
          AesCipher.createModuleAAD(
              fileDecryptor.getFileAAD(),
              ModuleCipherFactory.ModuleType.DictionaryPageHeader,
              meta.getRowGroupOrdinal(),
              columnDecryptionSetup.getOrdinal(),
              -1);
      pageHeader =
          Util.readPageHeader(
              inputStream, columnDecryptionSetup.getMetaDataDecryptor(), dictionaryPageHeaderAAD);
      dictionaryPageAAD =
          AesCipher.createModuleAAD(
              fileDecryptor.getFileAAD(),
              ModuleCipherFactory.ModuleType.DictionaryPage,
              meta.getRowGroupOrdinal(),
              columnDecryptionSetup.getOrdinal(),
              -1);
      pageDecryptor = columnDecryptionSetup.getDataDecryptor();
    }

    if (!pageHeader.isSetDictionary_page_header()) {
      return null;
    }

    DictionaryPage compressedPage =
        readCompressedDictionary(pageHeader, inputStream, pageDecryptor, dictionaryPageAAD);
    CompressionCodecFactory.BytesInputDecompressor decompressor =
        options.getCodecFactory().getDecompressor(meta.getCodec());

    return new DictionaryPage(
        decompressor.decompress(compressedPage.getBytes(), compressedPage.getUncompressedSize()),
        compressedPage.getDictionarySize(),
        compressedPage.getEncoding());
  }

  private DictionaryPage readCompressedDictionary(
      PageHeader pageHeader,
      SeekableInputStream fin,
      BlockCipher.Decryptor pageDecryptor,
      byte[] dictionaryPageAAD)
      throws IOException {
    DictionaryPageHeader dictHeader = pageHeader.getDictionary_page_header();

    int uncompressedPageSize = pageHeader.getUncompressed_page_size();
    int compressedPageSize = pageHeader.getCompressed_page_size();

    byte[] dictPageBytes = new byte[compressedPageSize];
    fin.readFully(dictPageBytes);

    BytesInput bin = BytesInput.from(dictPageBytes);

    if (null != pageDecryptor) {
      bin = BytesInput.from(pageDecryptor.decrypt(bin.toByteArray(), dictionaryPageAAD));
    }

    return new DictionaryPage(
        bin,
        uncompressedPageSize,
        dictHeader.getNum_values(),
        org.apache.parquet.column.Encoding.valueOf(dictHeader.getEncoding().name()));
  }

  private static DictionaryPage reusableCopy(DictionaryPage dict) throws IOException {
    return new DictionaryPage(
        BytesInput.from(dict.getBytes().toByteArray()),
        dict.getDictionarySize(),
        dict.getEncoding());
  }
}
