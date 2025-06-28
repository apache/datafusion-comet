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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;

public class RowGroupReader implements PageReadStore {
  private final Map<ColumnDescriptor, PageReader> readers = new HashMap<>();
  private final long rowCount;
  private final RowRanges rowRanges;
  private final long rowIndexOffset;

  public RowGroupReader(long rowCount, long rowIndexOffset) {
    this.rowCount = rowCount;
    this.rowRanges = null;
    this.rowIndexOffset = rowIndexOffset;
  }

  RowGroupReader(RowRanges rowRanges) {
    this.rowRanges = rowRanges;
    this.rowCount = rowRanges.rowCount();
    this.rowIndexOffset = -1;
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor path) {
    final PageReader pageReader = readers.get(path);
    if (pageReader == null) {
      throw new IllegalArgumentException(
          path + " is not found: " + readers.keySet() + " " + rowCount);
    }
    return pageReader;
  }

  @Override
  public Optional<PrimitiveIterator.OfLong> getRowIndexes() {
    return rowRanges == null ? Optional.empty() : Optional.of(rowRanges.iterator());
  }

  @Override
  public Optional<Long> getRowIndexOffset() {
    return this.rowIndexOffset < 0L ? Optional.empty() : Optional.of(this.rowIndexOffset);
  }

  void addColumn(ColumnDescriptor path, ColumnPageReader reader) {
    if (readers.put(path, reader) != null) {
      throw new IllegalStateException(path + " was already added");
    }
  }
}
