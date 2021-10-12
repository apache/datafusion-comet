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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.InternalColumnDecryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ModuleCipherFactory;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.SeekableInputStream;

public class BloomFilterReader implements FilterPredicate.Visitor<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(BloomFilterReader.class);
  private static final boolean BLOCK_MIGHT_MATCH = false;
  private static final boolean BLOCK_CANNOT_MATCH = true;

  private final Map<ColumnPath, ColumnChunkMetaData> columns;
  private final Map<ColumnPath, BloomFilter> cache = new HashMap<>();
  private final InternalFileDecryptor fileDecryptor;
  private final SeekableInputStream inputStream;

  BloomFilterReader(
      BlockMetaData block, InternalFileDecryptor fileDecryptor, SeekableInputStream inputStream) {
    this.columns = new HashMap<>();
    for (ColumnChunkMetaData column : block.getColumns()) {
      columns.put(column.getPath(), column);
    }
    this.fileDecryptor = fileDecryptor;
    this.inputStream = inputStream;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.Eq<T> eq) {
    T value = eq.getValue();

    if (value == null) {
      // the bloom filter bitset contains only non-null values so isn't helpful. this
      // could check the column stats, but the StatisticsFilter is responsible
      return BLOCK_MIGHT_MATCH;
    }

    Operators.Column<T> filterColumn = eq.getColumn();
    ColumnChunkMetaData meta = columns.get(filterColumn.getColumnPath());
    if (meta == null) {
      // the column isn't in this file so all values are null, but the value
      // must be non-null because of the above check.
      return BLOCK_CANNOT_MATCH;
    }

    try {
      BloomFilter bloomFilter = readBloomFilter(meta);
      if (bloomFilter != null && !bloomFilter.findHash(bloomFilter.hash(value))) {
        return BLOCK_CANNOT_MATCH;
      }
    } catch (RuntimeException e) {
      LOG.warn(e.getMessage());
      return BLOCK_MIGHT_MATCH;
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.NotEq<T> notEq) {
    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.Lt<T> lt) {
    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.LtEq<T> ltEq) {
    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.Gt<T> gt) {
    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.GtEq<T> gtEq) {
    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public Boolean visit(Operators.And and) {
    return and.getLeft().accept(this) || and.getRight().accept(this);
  }

  @Override
  public Boolean visit(Operators.Or or) {
    return or.getLeft().accept(this) && or.getRight().accept(this);
  }

  @Override
  public Boolean visit(Operators.Not not) {
    throw new IllegalArgumentException(
        "This predicate "
            + not
            + " contains a not! Did you forget"
            + " to run this predicate through LogicalInverseRewriter?");
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(
      Operators.UserDefined<T, U> udp) {
    return visit(udp, false);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(
      Operators.LogicalNotUserDefined<T, U> udp) {
    return visit(udp.getUserDefined(), true);
  }

  private <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(
      Operators.UserDefined<T, U> ud, boolean inverted) {
    return BLOCK_MIGHT_MATCH;
  }

  BloomFilter readBloomFilter(ColumnChunkMetaData meta) {
    if (cache.containsKey(meta.getPath())) {
      return cache.get(meta.getPath());
    }
    try {
      if (!cache.containsKey(meta.getPath())) {
        BloomFilter bloomFilter = readBloomFilterInternal(meta);
        if (bloomFilter == null) {
          return null;
        }

        cache.put(meta.getPath(), bloomFilter);
      }
      return cache.get(meta.getPath());
    } catch (IOException e) {
      LOG.error("Failed to read Bloom filter data", e);
    }

    return null;
  }

  private BloomFilter readBloomFilterInternal(ColumnChunkMetaData meta) throws IOException {
    long bloomFilterOffset = meta.getBloomFilterOffset();
    if (bloomFilterOffset < 0) {
      return null;
    }

    // Prepare to decrypt Bloom filter (for encrypted columns)
    BlockCipher.Decryptor bloomFilterDecryptor = null;
    byte[] bloomFilterHeaderAAD = null;
    byte[] bloomFilterBitsetAAD = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      InternalColumnDecryptionSetup columnDecryptionSetup =
          fileDecryptor.getColumnSetup(meta.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        bloomFilterDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
        bloomFilterHeaderAAD =
            AesCipher.createModuleAAD(
                fileDecryptor.getFileAAD(),
                ModuleCipherFactory.ModuleType.BloomFilterHeader,
                meta.getRowGroupOrdinal(),
                columnDecryptionSetup.getOrdinal(),
                -1);
        bloomFilterBitsetAAD =
            AesCipher.createModuleAAD(
                fileDecryptor.getFileAAD(),
                ModuleCipherFactory.ModuleType.BloomFilterBitset,
                meta.getRowGroupOrdinal(),
                columnDecryptionSetup.getOrdinal(),
                -1);
      }
    }

    // Read Bloom filter data header.
    inputStream.seek(bloomFilterOffset);
    BloomFilterHeader bloomFilterHeader;
    try {
      bloomFilterHeader =
          Util.readBloomFilterHeader(inputStream, bloomFilterDecryptor, bloomFilterHeaderAAD);
    } catch (IOException e) {
      LOG.warn("read no bloom filter");
      return null;
    }

    int numBytes = bloomFilterHeader.getNumBytes();
    if (numBytes <= 0 || numBytes > BlockSplitBloomFilter.UPPER_BOUND_BYTES) {
      LOG.warn("the read bloom filter size is wrong, size is {}", bloomFilterHeader.getNumBytes());
      return null;
    }

    if (!bloomFilterHeader.getHash().isSetXXHASH()
        || !bloomFilterHeader.getAlgorithm().isSetBLOCK()
        || !bloomFilterHeader.getCompression().isSetUNCOMPRESSED()) {
      LOG.warn(
          "the read bloom filter is not supported yet,  algorithm = {}, hash = {}, "
              + "compression = {}",
          bloomFilterHeader.getAlgorithm(),
          bloomFilterHeader.getHash(),
          bloomFilterHeader.getCompression());
      return null;
    }

    byte[] bitset;
    if (null == bloomFilterDecryptor) {
      bitset = new byte[numBytes];
      inputStream.readFully(bitset);
    } else {
      bitset = bloomFilterDecryptor.decrypt(inputStream, bloomFilterBitsetAAD);
      if (bitset.length != numBytes) {
        throw new ParquetCryptoRuntimeException("Wrong length of decrypted bloom filter bitset");
      }
    }
    return new BlockSplitBloomFilter(bitset);
  }
}
