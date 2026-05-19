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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.InternalColumnDecryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ModuleCipherFactory;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.SeekableInputStream;

class ColumnIndexReader implements ColumnIndexStore {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnIndexReader.class);

  // Used for columns are not in this parquet file
  private static final IndexStore MISSING_INDEX_STORE =
      new IndexStore() {
        @Override
        public ColumnIndex getColumnIndex() {
          return null;
        }

        @Override
        public OffsetIndex getOffsetIndex() {
          return null;
        }
      };

  private static final ColumnIndexReader EMPTY =
      new ColumnIndexReader(new BlockMetaData(), Collections.emptySet(), null, null) {
        @Override
        public ColumnIndex getColumnIndex(ColumnPath column) {
          return null;
        }

        @Override
        public OffsetIndex getOffsetIndex(ColumnPath column) {
          throw new MissingOffsetIndexException(column);
        }
      };

  private final InternalFileDecryptor fileDecryptor;
  private final SeekableInputStream inputStream;
  private final Map<ColumnPath, IndexStore> store;

  /**
   * Creates a column index store which lazily reads column/offset indexes for the columns in paths.
   * Paths are the set of columns used for the projection.
   */
  static ColumnIndexReader create(
      BlockMetaData block,
      Set<ColumnPath> paths,
      InternalFileDecryptor fileDecryptor,
      SeekableInputStream inputStream) {
    try {
      return new ColumnIndexReader(block, paths, fileDecryptor, inputStream);
    } catch (MissingOffsetIndexException e) {
      return EMPTY;
    }
  }

  private ColumnIndexReader(
      BlockMetaData block,
      Set<ColumnPath> paths,
      InternalFileDecryptor fileDecryptor,
      SeekableInputStream inputStream) {
    this.fileDecryptor = fileDecryptor;
    this.inputStream = inputStream;
    Map<ColumnPath, IndexStore> store = new HashMap<>();
    for (ColumnChunkMetaData column : block.getColumns()) {
      ColumnPath path = column.getPath();
      if (paths.contains(path)) {
        store.put(path, new IndexStoreImpl(column));
      }
    }
    this.store = store;
  }

  @Override
  public ColumnIndex getColumnIndex(ColumnPath column) {
    return store.getOrDefault(column, MISSING_INDEX_STORE).getColumnIndex();
  }

  @Override
  public OffsetIndex getOffsetIndex(ColumnPath column) {
    return store.getOrDefault(column, MISSING_INDEX_STORE).getOffsetIndex();
  }

  private interface IndexStore {
    ColumnIndex getColumnIndex();

    OffsetIndex getOffsetIndex();
  }

  private class IndexStoreImpl implements IndexStore {
    private final ColumnChunkMetaData meta;
    private ColumnIndex columnIndex;
    private boolean columnIndexRead;
    private final OffsetIndex offsetIndex;

    IndexStoreImpl(ColumnChunkMetaData meta) {
      this.meta = meta;
      OffsetIndex oi;
      try {
        oi = readOffsetIndex(meta);
      } catch (IOException e) {
        // If the I/O issue still stands it will fail the reading later;
        // otherwise we fail the filtering only with a missing offset index.
        LOG.warn("Unable to read offset index for column {}", meta.getPath(), e);
        oi = null;
      }
      if (oi == null) {
        throw new MissingOffsetIndexException(meta.getPath());
      }
      offsetIndex = oi;
    }

    @Override
    public ColumnIndex getColumnIndex() {
      if (!columnIndexRead) {
        try {
          columnIndex = readColumnIndex(meta);
        } catch (IOException e) {
          // If the I/O issue still stands it will fail the reading later;
          // otherwise we fail the filtering only with a missing column index.
          LOG.warn("Unable to read column index for column {}", meta.getPath(), e);
        }
        columnIndexRead = true;
      }
      return columnIndex;
    }

    @Override
    public OffsetIndex getOffsetIndex() {
      return offsetIndex;
    }
  }

  // Visible for testing
  ColumnIndex readColumnIndex(ColumnChunkMetaData column) throws IOException {
    IndexReference ref = column.getColumnIndexReference();
    if (ref == null) {
      return null;
    }
    inputStream.seek(ref.getOffset());

    BlockCipher.Decryptor columnIndexDecryptor = null;
    byte[] columnIndexAAD = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      InternalColumnDecryptionSetup columnDecryptionSetup =
          fileDecryptor.getColumnSetup(column.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        columnIndexDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
        columnIndexAAD =
            AesCipher.createModuleAAD(
                fileDecryptor.getFileAAD(),
                ModuleCipherFactory.ModuleType.ColumnIndex,
                column.getRowGroupOrdinal(),
                columnDecryptionSetup.getOrdinal(),
                -1);
      }
    }
    return ParquetMetadataConverter.fromParquetColumnIndex(
        column.getPrimitiveType(),
        Util.readColumnIndex(inputStream, columnIndexDecryptor, columnIndexAAD));
  }

  // Visible for testing
  OffsetIndex readOffsetIndex(ColumnChunkMetaData column) throws IOException {
    IndexReference ref = column.getOffsetIndexReference();
    if (ref == null) {
      return null;
    }
    inputStream.seek(ref.getOffset());

    BlockCipher.Decryptor offsetIndexDecryptor = null;
    byte[] offsetIndexAAD = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      InternalColumnDecryptionSetup columnDecryptionSetup =
          fileDecryptor.getColumnSetup(column.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        offsetIndexDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
        offsetIndexAAD =
            AesCipher.createModuleAAD(
                fileDecryptor.getFileAAD(),
                ModuleCipherFactory.ModuleType.OffsetIndex,
                column.getRowGroupOrdinal(),
                columnDecryptionSetup.getOrdinal(),
                -1);
      }
    }
    return ParquetMetadataConverter.fromParquetOffsetIndex(
        Util.readOffsetIndex(inputStream, offsetIndexDecryptor, offsetIndexAAD));
  }
}
