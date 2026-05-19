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

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;

public class IndexFilter {
  private final RowRanges rowRanges;
  private final OffsetIndex offsetIndex;
  private final long totalRowCount;

  public IndexFilter(RowRanges rowRanges, OffsetIndex offsetIndex, long totalRowCount) {
    this.rowRanges = rowRanges;
    this.offsetIndex = offsetIndex;
    this.totalRowCount = totalRowCount;
  }

  OffsetIndex filterOffsetIndex() {
    List<Integer> indexMap = new ArrayList<>();
    for (int i = 0, n = offsetIndex.getPageCount(); i < n; ++i) {
      long from = offsetIndex.getFirstRowIndex(i);
      if (rowRanges.isOverlapping(from, offsetIndex.getLastRowIndex(i, totalRowCount))) {
        indexMap.add(i);
      }
    }

    int[] indexArray = new int[indexMap.size()];
    for (int i = 0; i < indexArray.length; i++) {
      indexArray[i] = indexMap.get(i);
    }
    return new FilteredOffsetIndex(offsetIndex, indexArray);
  }

  List<OffsetRange> calculateOffsetRanges(OffsetIndex filteredOffsetIndex, ColumnChunkMetaData cm) {
    List<OffsetRange> ranges = new ArrayList<>();
    long firstPageOffset = offsetIndex.getOffset(0);
    int n = filteredOffsetIndex.getPageCount();

    if (n > 0) {
      OffsetRange currentRange = null;

      // Add a range for the dictionary page if required
      long rowGroupOffset = cm.getStartingPos();
      if (rowGroupOffset < firstPageOffset) {
        currentRange = new OffsetRange(rowGroupOffset, (int) (firstPageOffset - rowGroupOffset));
        ranges.add(currentRange);
      }

      for (int i = 0; i < n; ++i) {
        long offset = filteredOffsetIndex.getOffset(i);
        int length = filteredOffsetIndex.getCompressedPageSize(i);
        if (currentRange == null || !currentRange.extend(offset, length)) {
          currentRange = new OffsetRange(offset, length);
          ranges.add(currentRange);
        }
      }
    }
    return ranges;
  }

  private static class FilteredOffsetIndex implements OffsetIndex {
    private final OffsetIndex offsetIndex;
    private final int[] indexMap;

    private FilteredOffsetIndex(OffsetIndex offsetIndex, int[] indexMap) {
      this.offsetIndex = offsetIndex;
      this.indexMap = indexMap;
    }

    @Override
    public int getPageOrdinal(int pageIndex) {
      return indexMap[pageIndex];
    }

    @Override
    public int getPageCount() {
      return indexMap.length;
    }

    @Override
    public long getOffset(int pageIndex) {
      return offsetIndex.getOffset(indexMap[pageIndex]);
    }

    @Override
    public int getCompressedPageSize(int pageIndex) {
      return offsetIndex.getCompressedPageSize(indexMap[pageIndex]);
    }

    @Override
    public long getFirstRowIndex(int pageIndex) {
      return offsetIndex.getFirstRowIndex(indexMap[pageIndex]);
    }

    @Override
    public long getLastRowIndex(int pageIndex, long totalRowCount) {
      int nextIndex = indexMap[pageIndex] + 1;
      return (nextIndex >= offsetIndex.getPageCount()
              ? totalRowCount
              : offsetIndex.getFirstRowIndex(nextIndex))
          - 1;
    }
  }

  static class OffsetRange {
    final long offset;
    long length;

    private OffsetRange(long offset, int length) {
      this.offset = offset;
      this.length = length;
    }

    private boolean extend(long offset, int length) {
      if (this.offset + this.length == offset) {
        this.length += length;
        return true;
      } else {
        return false;
      }
    }
  }
}
