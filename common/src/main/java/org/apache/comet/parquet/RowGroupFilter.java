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

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter;
import org.apache.parquet.filter2.compat.FilterCompat.Visitor;
import org.apache.parquet.filter2.dictionarylevel.DictionaryFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.SchemaCompatibilityValidator;
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;

public class RowGroupFilter implements Visitor<List<BlockMetaData>> {
  private final List<BlockMetaData> blocks;
  private final MessageType schema;
  private final List<FilterLevel> levels;
  private final FileReader reader;

  public enum FilterLevel {
    STATISTICS,
    DICTIONARY,
    BLOOMFILTER
  }

  public static List<BlockMetaData> filterRowGroups(
      List<FilterLevel> levels, Filter filter, List<BlockMetaData> blocks, FileReader reader) {
    return filter.accept(new RowGroupFilter(levels, blocks, reader));
  }

  private RowGroupFilter(List<FilterLevel> levels, List<BlockMetaData> blocks, FileReader reader) {
    this.levels = levels;
    this.blocks = blocks;
    this.reader = reader;
    this.schema = reader.getFileMetaData().getSchema();
  }

  @Override
  public List<BlockMetaData> visit(FilterCompat.FilterPredicateCompat filterPredicateCompat) {
    FilterPredicate filterPredicate = filterPredicateCompat.getFilterPredicate();

    // check that the schema of the filter matches the schema of the file
    SchemaCompatibilityValidator.validate(filterPredicate, schema);

    List<BlockMetaData> filteredBlocks = new ArrayList<>();

    for (BlockMetaData block : blocks) {
      boolean drop = false;

      if (levels.contains(FilterLevel.STATISTICS)) {
        drop = StatisticsFilter.canDrop(filterPredicate, block.getColumns());
      }

      if (!drop && levels.contains(FilterLevel.DICTIONARY)) {
        drop =
            DictionaryFilter.canDrop(
                filterPredicate,
                block.getColumns(),
                new DictionaryPageReader(
                    block,
                    reader.getFileMetaData().getFileDecryptor(),
                    reader.getInputStream(),
                    reader.getOptions()));
      }

      if (!drop && levels.contains(FilterLevel.BLOOMFILTER)) {
        drop =
            filterPredicate.accept(
                new BloomFilterReader(
                    block, reader.getFileMetaData().getFileDecryptor(), reader.getInputStream()));
      }

      if (!drop) {
        filteredBlocks.add(block);
      }
    }

    return filteredBlocks;
  }

  @Override
  public List<BlockMetaData> visit(
      FilterCompat.UnboundRecordFilterCompat unboundRecordFilterCompat) {
    return blocks;
  }

  @Override
  public List<BlockMetaData> visit(NoOpFilter noOpFilter) {
    return blocks;
  }
}
