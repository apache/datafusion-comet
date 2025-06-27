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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

import org.apache.comet.CometConf;

import static org.apache.parquet.column.Encoding.*;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.MAX_STATS_SIZE;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import static org.apache.comet.parquet.TypeUtil.isSpark40Plus;

@SuppressWarnings("deprecation")
public class TestFileReader {
  private static final MessageType SCHEMA =
      MessageTypeParser.parseMessageType(
          ""
              + "message m {"
              + "  required group a {"
              + "    required binary b;"
              + "  }"
              + "  required group c {"
              + "    required int64 d;"
              + "  }"
              + "}");

  private static final MessageType SCHEMA2 =
      MessageTypeParser.parseMessageType(
          ""
              + "message root { "
              + "required int32 id;"
              + "required binary name(UTF8); "
              + "required int32 num; "
              + "required binary comment(UTF8);"
              + "}");

  private static final MessageType PROJECTED_SCHEMA2 =
      MessageTypeParser.parseMessageType(
          ""
              + "message root { "
              + "required int32 id;"
              + "required binary name(UTF8); "
              + "required binary comment(UTF8);"
              + "}");

  private static final String[] PATH1 = {"a", "b"};
  private static final ColumnDescriptor C1 = SCHEMA.getColumnDescription(PATH1);
  private static final String[] PATH2 = {"c", "d"};
  private static final ColumnDescriptor C2 = SCHEMA.getColumnDescription(PATH2);

  private static final byte[] BYTES1 = {0, 1, 2, 3};
  private static final byte[] BYTES2 = {1, 2, 3, 4};
  private static final byte[] BYTES3 = {2, 3, 4, 5};
  private static final byte[] BYTES4 = {3, 4, 5, 6};
  private static final CompressionCodecName CODEC = CompressionCodecName.UNCOMPRESSED;

  private static final org.apache.parquet.column.statistics.Statistics<?> EMPTY_STATS =
      org.apache.parquet.column.statistics.Statistics.getBuilderForReading(
              Types.required(PrimitiveTypeName.BINARY).named("test_binary"))
          .build();

  @Rule public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testEnableReadParallel() {
    Configuration configuration = new Configuration();
    ReadOptions options = ReadOptions.builder(configuration).build();

    assertFalse(FileReader.shouldReadParallel(options, "hdfs"));
    assertFalse(FileReader.shouldReadParallel(options, "file"));
    assertFalse(FileReader.shouldReadParallel(options, null));
    assertTrue(FileReader.shouldReadParallel(options, "s3a"));

    options = ReadOptions.builder(configuration).enableParallelIO(false).build();
    assertFalse(FileReader.shouldReadParallel(options, "s3a"));
  }

  @Test
  public void testReadWrite() throws Exception {
    File testFile = temp.newFile();
    testFile.delete();

    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();

    // Start a Parquet file with 2 row groups, each with 2 column chunks
    ParquetFileWriter w = new ParquetFileWriter(configuration, SCHEMA, path);
    w.start();
    w.startBlock(3);
    w.startColumn(C1, 5, CODEC);
    long c1Starts = w.getPos();
    long c1p1Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), EMPTY_STATS, 2, RLE, RLE, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES1), EMPTY_STATS, 3, RLE, RLE, PLAIN);
    w.endColumn();
    long c1Ends = w.getPos();
    w.startColumn(C2, 6, CODEC);
    long c2Starts = w.getPos();
    w.writeDictionaryPage(new DictionaryPage(BytesInput.from(BYTES2), 4, RLE_DICTIONARY));
    long c2p1Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES2), EMPTY_STATS, 2, RLE, RLE, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES2), EMPTY_STATS, 3, RLE, RLE, PLAIN);
    w.writeDataPage(1, 4, BytesInput.from(BYTES2), EMPTY_STATS, 1, RLE, RLE, PLAIN);
    w.endColumn();
    long c2Ends = w.getPos();
    w.endBlock();
    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    w.writeDataPage(7, 4, BytesInput.from(BYTES3), EMPTY_STATS, 7, RLE, RLE, PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    w.writeDataPage(8, 4, BytesInput.from(BYTES4), EMPTY_STATS, 8, RLE, RLE, PLAIN);
    w.endColumn();
    w.endBlock();
    w.end(new HashMap<>());

    InputFile file = HadoopInputFile.fromPath(path, configuration);
    ParquetReadOptions options = ParquetReadOptions.builder().build();
    ReadOptions cometOptions = ReadOptions.builder(configuration).build();

    try (FileReader reader = new FileReader(file, options, cometOptions)) {
      ParquetMetadata readFooter = reader.getFooter();
      assertEquals("footer: " + readFooter, 2, readFooter.getBlocks().size());
      BlockMetaData rowGroup = readFooter.getBlocks().get(0);
      assertEquals(c1Ends - c1Starts, rowGroup.getColumns().get(0).getTotalSize());
      assertEquals(c2Ends - c2Starts, rowGroup.getColumns().get(1).getTotalSize());
      assertEquals(c2Ends - c1Starts, rowGroup.getTotalByteSize());

      assertEquals(c1Starts, rowGroup.getColumns().get(0).getStartingPos());
      assertEquals(0, rowGroup.getColumns().get(0).getDictionaryPageOffset());
      assertEquals(c1p1Starts, rowGroup.getColumns().get(0).getFirstDataPageOffset());
      assertEquals(c2Starts, rowGroup.getColumns().get(1).getStartingPos());
      assertEquals(c2Starts, rowGroup.getColumns().get(1).getDictionaryPageOffset());
      assertEquals(c2p1Starts, rowGroup.getColumns().get(1).getFirstDataPageOffset());

      HashSet<Encoding> expectedEncoding = new HashSet<>();
      expectedEncoding.add(PLAIN);
      expectedEncoding.add(RLE);
      assertEquals(expectedEncoding, rowGroup.getColumns().get(0).getEncodings());
    }

    // read first block of col #1
    try (FileReader r = new FileReader(file, options, cometOptions)) {
      r.setRequestedSchema(Arrays.asList(SCHEMA.getColumnDescription(PATH1)));
      PageReadStore pages = r.readNextRowGroup();
      assertEquals(3, pages.getRowCount());
      validateContains(pages, PATH1, 2, BytesInput.from(BYTES1));
      validateContains(pages, PATH1, 3, BytesInput.from(BYTES1));
      assertTrue(r.skipNextRowGroup());
      assertNull(r.readNextRowGroup());
    }

    // read all blocks of col #1 and #2
    try (FileReader r = new FileReader(file, options, cometOptions)) {
      r.setRequestedSchema(
          Arrays.asList(SCHEMA.getColumnDescription(PATH1), SCHEMA.getColumnDescription(PATH2)));
      PageReadStore pages = r.readNextRowGroup();
      assertEquals(3, pages.getRowCount());
      validateContains(pages, PATH1, 2, BytesInput.from(BYTES1));
      validateContains(pages, PATH1, 3, BytesInput.from(BYTES1));
      validateContains(pages, PATH2, 2, BytesInput.from(BYTES2));
      validateContains(pages, PATH2, 3, BytesInput.from(BYTES2));
      validateContains(pages, PATH2, 1, BytesInput.from(BYTES2));

      pages = r.readNextRowGroup();
      assertEquals(4, pages.getRowCount());

      validateContains(pages, PATH1, 7, BytesInput.from(BYTES3));
      validateContains(pages, PATH2, 8, BytesInput.from(BYTES4));

      assertNull(r.readNextRowGroup());
    }
  }

  @Test
  public void testBloomFilterReadWrite() throws Exception {
    MessageType schema =
        MessageTypeParser.parseMessageType("message test { required binary foo; }");
    File testFile = temp.newFile();
    testFile.delete();
    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();
    configuration.set("parquet.bloom.filter.column.names", "foo");
    String[] colPath = {"foo"};

    ColumnDescriptor col = schema.getColumnDescription(colPath);
    BinaryStatistics stats1 = new BinaryStatistics();
    ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
    w.start();
    w.startBlock(3);
    w.startColumn(col, 5, CODEC);
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), stats1, 2, RLE, RLE, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES1), stats1, 2, RLE, RLE, PLAIN);
    w.endColumn();
    BloomFilter blockSplitBloomFilter = new BlockSplitBloomFilter(0);
    blockSplitBloomFilter.insertHash(blockSplitBloomFilter.hash(Binary.fromString("hello")));
    blockSplitBloomFilter.insertHash(blockSplitBloomFilter.hash(Binary.fromString("world")));
    addBloomFilter(w, "foo", blockSplitBloomFilter);
    w.endBlock();
    w.end(new HashMap<>());

    InputFile file = HadoopInputFile.fromPath(path, configuration);
    ParquetReadOptions options = ParquetReadOptions.builder().build();
    ReadOptions cometOptions = ReadOptions.builder(configuration).build();

    try (FileReader r = new FileReader(file, options, cometOptions)) {
      ParquetMetadata footer = r.getFooter();
      r.setRequestedSchema(Arrays.asList(schema.getColumnDescription(colPath)));
      BloomFilterReader bloomFilterReader =
          new BloomFilterReader(
              footer.getBlocks().get(0),
              r.getFileMetaData().getFileDecryptor(),
              r.getInputStream());
      BloomFilter bloomFilter =
          bloomFilterReader.readBloomFilter(footer.getBlocks().get(0).getColumns().get(0));
      assertTrue(bloomFilter.findHash(blockSplitBloomFilter.hash(Binary.fromString("hello"))));
      assertTrue(bloomFilter.findHash(blockSplitBloomFilter.hash(Binary.fromString("world"))));
    }
  }

  @Test
  public void testReadWriteDataPageV2() throws Exception {
    File testFile = temp.newFile();
    testFile.delete();

    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();

    ParquetFileWriter w = new ParquetFileWriter(configuration, SCHEMA, path);
    w.start();
    w.startBlock(14);

    BytesInput repLevels = BytesInput.fromInt(2);
    BytesInput defLevels = BytesInput.fromInt(1);
    BytesInput data = BytesInput.fromInt(3);
    BytesInput data2 = BytesInput.fromInt(10);

    org.apache.parquet.column.statistics.Statistics<?> statsC1P1 = createStatistics("s", "z", C1);
    org.apache.parquet.column.statistics.Statistics<?> statsC1P2 = createStatistics("b", "d", C1);

    w.startColumn(C1, 6, CODEC);
    long c1Starts = w.getPos();
    w.writeDataPageV2(4, 1, 3, repLevels, defLevels, PLAIN, data, 4, statsC1P1);
    w.writeDataPageV2(3, 0, 3, repLevels, defLevels, PLAIN, data, 4, statsC1P2);
    w.endColumn();
    long c1Ends = w.getPos();

    w.startColumn(C2, 5, CODEC);
    long c2Starts = w.getPos();
    w.writeDataPageV2(5, 2, 3, repLevels, defLevels, PLAIN, data2, 4, EMPTY_STATS);
    w.writeDataPageV2(2, 0, 2, repLevels, defLevels, PLAIN, data2, 4, EMPTY_STATS);
    w.endColumn();
    long c2Ends = w.getPos();

    w.endBlock();
    w.end(new HashMap<>());

    InputFile file = HadoopInputFile.fromPath(path, configuration);
    ParquetReadOptions options = ParquetReadOptions.builder().build();
    ReadOptions cometOptions = ReadOptions.builder(configuration).build();

    try (FileReader reader = new FileReader(file, options, cometOptions)) {
      ParquetMetadata footer = reader.getFooter();
      assertEquals("footer: " + footer, 1, footer.getBlocks().size());
      assertEquals(c1Ends - c1Starts, footer.getBlocks().get(0).getColumns().get(0).getTotalSize());
      assertEquals(c2Ends - c2Starts, footer.getBlocks().get(0).getColumns().get(1).getTotalSize());
      assertEquals(c2Ends - c1Starts, footer.getBlocks().get(0).getTotalByteSize());

      // check for stats
      org.apache.parquet.column.statistics.Statistics<?> expectedStats =
          createStatistics("b", "z", C1);
      assertStatsValuesEqual(
          expectedStats, footer.getBlocks().get(0).getColumns().get(0).getStatistics());

      HashSet<Encoding> expectedEncoding = new HashSet<>();
      expectedEncoding.add(PLAIN);
      assertEquals(expectedEncoding, footer.getBlocks().get(0).getColumns().get(0).getEncodings());
    }

    try (FileReader r = new FileReader(file, options, cometOptions)) {
      r.setRequestedSchema(
          Arrays.asList(SCHEMA.getColumnDescription(PATH1), SCHEMA.getColumnDescription(PATH2)));
      PageReadStore pages = r.readNextRowGroup();
      assertEquals(14, pages.getRowCount());
      validateV2Page(
          pages,
          PATH1,
          3,
          4,
          1,
          repLevels.toByteArray(),
          defLevels.toByteArray(),
          data.toByteArray(),
          12);
      validateV2Page(
          pages,
          PATH1,
          3,
          3,
          0,
          repLevels.toByteArray(),
          defLevels.toByteArray(),
          data.toByteArray(),
          12);
      validateV2Page(
          pages,
          PATH2,
          3,
          5,
          2,
          repLevels.toByteArray(),
          defLevels.toByteArray(),
          data2.toByteArray(),
          12);
      validateV2Page(
          pages,
          PATH2,
          2,
          2,
          0,
          repLevels.toByteArray(),
          defLevels.toByteArray(),
          data2.toByteArray(),
          12);
      assertNull(r.readNextRowGroup());
    }
  }

  @Test
  public void testColumnIndexFilter() throws Exception {
    File testFile = temp.newFile();
    testFile.delete();

    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();

    ParquetFileWriter w = new ParquetFileWriter(configuration, SCHEMA, path);

    w.start();
    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), EMPTY_STATS, 2, RLE, RLE, PLAIN);
    w.writeDataPage(2, 4, BytesInput.from(BYTES2), EMPTY_STATS, 2, RLE, RLE, PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    // the first page contains one matching record
    w.writeDataPage(1, 4, BytesInput.from(BYTES3), statsC2(2L), 1, RLE, RLE, PLAIN);
    // all the records of the second page are larger than 2, so should be filtered out
    w.writeDataPage(3, 4, BytesInput.from(BYTES4), statsC2(3L, 4L, 5L), 3, RLE, RLE, PLAIN);
    w.endColumn();
    w.endBlock();

    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), EMPTY_STATS, 2, RLE, RLE, PLAIN);
    w.writeDataPage(2, 4, BytesInput.from(BYTES2), EMPTY_STATS, 2, RLE, RLE, PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    // the first page should be filtered out
    w.writeDataPage(1, 4, BytesInput.from(BYTES3), statsC2(4L), 1, RLE, RLE, PLAIN);
    // the second page will be read since it contains matching record
    w.writeDataPage(3, 4, BytesInput.from(BYTES4), statsC2(0L, 1L, 3L), 3, RLE, RLE, PLAIN);
    w.endColumn();
    w.endBlock();

    w.end(new HashMap<>());

    // set a simple equality filter in the ParquetInputFormat
    Operators.LongColumn c2 = FilterApi.longColumn("c.d");
    FilterPredicate p = FilterApi.eq(c2, 2L);
    ParquetInputFormat.setFilterPredicate(configuration, p);
    InputFile file = HadoopInputFile.fromPath(path, configuration);
    ParquetReadOptions options = HadoopReadOptions.builder(configuration).build();
    ReadOptions cometOptions = ReadOptions.builder(configuration).build();

    try (FileReader r = new FileReader(file, options, cometOptions)) {
      assertEquals(4, r.getFilteredRecordCount());
      PageReadStore readStore = r.readNextFilteredRowGroup();

      PageReader c1Reader = readStore.getPageReader(C1);
      List<DataPage> c1Pages = new ArrayList<>();
      DataPage page;
      while ((page = c1Reader.readPage()) != null) {
        c1Pages.add(page);
      }
      // second page of c1 should be filtered out
      assertEquals(1, c1Pages.size());
      validatePage(c1Pages.get(0), 2, BytesInput.from(BYTES1));

      PageReader c2Reader = readStore.getPageReader(C2);
      List<DataPage> c2Pages = new ArrayList<>();
      while ((page = c2Reader.readPage()) != null) {
        c2Pages.add(page);
      }
      assertEquals(1, c2Pages.size());
      validatePage(c2Pages.get(0), 1, BytesInput.from(BYTES3));

      // test the second row group
      readStore = r.readNextFilteredRowGroup();
      assertNotNull(readStore);

      c1Reader = readStore.getPageReader(C1);
      c1Pages.clear();
      while ((page = c1Reader.readPage()) != null) {
        c1Pages.add(page);
      }
      // all pages of c1 should be retained
      assertEquals(2, c1Pages.size());
      validatePage(c1Pages.get(0), 2, BytesInput.from(BYTES1));
      validatePage(c1Pages.get(1), 2, BytesInput.from(BYTES2));

      c2Reader = readStore.getPageReader(C2);
      c2Pages.clear();
      while ((page = c2Reader.readPage()) != null) {
        c2Pages.add(page);
      }
      assertEquals(1, c2Pages.size());
      validatePage(c2Pages.get(0), 3, BytesInput.from(BYTES4));
    }
  }

  @Test
  public void testColumnIndexReadWrite() throws Exception {
    File testFile = temp.newFile();
    testFile.delete();

    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();

    ParquetFileWriter w = new ParquetFileWriter(configuration, SCHEMA, path);
    w.start();
    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    w.writeDataPage(7, 4, BytesInput.from(BYTES3), EMPTY_STATS, RLE, RLE, PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    w.writeDataPage(8, 4, BytesInput.from(BYTES4), EMPTY_STATS, RLE, RLE, PLAIN);
    w.endColumn();
    w.endBlock();
    w.startBlock(4);
    w.startColumn(C1, 5, CODEC);
    long c1p1Starts = w.getPos();
    w.writeDataPage(
        2, 4, BytesInput.from(BYTES1), statsC1(null, Binary.fromString("aaa")), 1, RLE, RLE, PLAIN);
    long c1p2Starts = w.getPos();
    w.writeDataPage(
        3,
        4,
        BytesInput.from(BYTES1),
        statsC1(Binary.fromString("bbb"), Binary.fromString("ccc")),
        3,
        RLE,
        RLE,
        PLAIN);
    w.endColumn();
    long c1Ends = w.getPos();
    w.startColumn(C2, 6, CODEC);
    long c2p1Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES2), statsC2(117L, 100L), 1, RLE, RLE, PLAIN);
    long c2p2Starts = w.getPos();
    w.writeDataPage(3, 4, BytesInput.from(BYTES2), statsC2(null, null, null), 2, RLE, RLE, PLAIN);
    long c2p3Starts = w.getPos();
    w.writeDataPage(1, 4, BytesInput.from(BYTES2), statsC2(0L), 1, RLE, RLE, PLAIN);
    w.endColumn();
    long c2Ends = w.getPos();
    w.endBlock();
    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    w.writeDataPage(
        7,
        4,
        BytesInput.from(BYTES3),
        // Creating huge stats so the column index will reach the limit and won't be written
        statsC1(
            Binary.fromConstantByteArray(new byte[(int) MAX_STATS_SIZE]),
            Binary.fromConstantByteArray(new byte[1])),
        4,
        RLE,
        RLE,
        PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    w.writeDataPage(8, 4, BytesInput.from(BYTES4), EMPTY_STATS, RLE, RLE, PLAIN);
    w.endColumn();
    w.endBlock();
    w.end(new HashMap<>());

    InputFile file = HadoopInputFile.fromPath(path, configuration);
    ParquetReadOptions options = ParquetReadOptions.builder().build();
    ReadOptions cometOptions = ReadOptions.builder(configuration).build();

    try (FileReader reader = new FileReader(file, options, cometOptions)) {
      ParquetMetadata footer = reader.getFooter();
      assertEquals(3, footer.getBlocks().size());
      BlockMetaData blockMeta = footer.getBlocks().get(1);
      assertEquals(2, blockMeta.getColumns().size());

      ColumnIndexReader indexReader = reader.getColumnIndexReader(1);
      ColumnIndex columnIndex = indexReader.readColumnIndex(blockMeta.getColumns().get(0));
      assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
      assertEquals(Arrays.asList(1L, 0L), columnIndex.getNullCounts());
      assertEquals(Arrays.asList(false, false), columnIndex.getNullPages());
      List<ByteBuffer> minValues = columnIndex.getMinValues();
      assertEquals(2, minValues.size());
      List<ByteBuffer> maxValues = columnIndex.getMaxValues();
      assertEquals(2, maxValues.size());
      assertEquals("aaa", new String(minValues.get(0).array(), StandardCharsets.UTF_8));
      assertEquals("aaa", new String(maxValues.get(0).array(), StandardCharsets.UTF_8));
      assertEquals("bbb", new String(minValues.get(1).array(), StandardCharsets.UTF_8));
      assertEquals("ccc", new String(maxValues.get(1).array(), StandardCharsets.UTF_8));

      columnIndex = indexReader.readColumnIndex(blockMeta.getColumns().get(1));
      assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
      assertEquals(Arrays.asList(0L, 3L, 0L), columnIndex.getNullCounts());
      assertEquals(Arrays.asList(false, true, false), columnIndex.getNullPages());
      minValues = columnIndex.getMinValues();
      assertEquals(3, minValues.size());
      maxValues = columnIndex.getMaxValues();
      assertEquals(3, maxValues.size());
      assertEquals(100, BytesUtils.bytesToLong(minValues.get(0).array()));
      assertEquals(117, BytesUtils.bytesToLong(maxValues.get(0).array()));
      assertEquals(0, minValues.get(1).array().length);
      assertEquals(0, maxValues.get(1).array().length);
      assertEquals(0, BytesUtils.bytesToLong(minValues.get(2).array()));
      assertEquals(0, BytesUtils.bytesToLong(maxValues.get(2).array()));

      OffsetIndex offsetIndex = indexReader.readOffsetIndex(blockMeta.getColumns().get(0));
      assertEquals(2, offsetIndex.getPageCount());
      assertEquals(c1p1Starts, offsetIndex.getOffset(0));
      assertEquals(c1p2Starts, offsetIndex.getOffset(1));
      assertEquals(c1p2Starts - c1p1Starts, offsetIndex.getCompressedPageSize(0));
      assertEquals(c1Ends - c1p2Starts, offsetIndex.getCompressedPageSize(1));
      assertEquals(0, offsetIndex.getFirstRowIndex(0));
      assertEquals(1, offsetIndex.getFirstRowIndex(1));

      offsetIndex = indexReader.readOffsetIndex(blockMeta.getColumns().get(1));
      assertEquals(3, offsetIndex.getPageCount());
      assertEquals(c2p1Starts, offsetIndex.getOffset(0));
      assertEquals(c2p2Starts, offsetIndex.getOffset(1));
      assertEquals(c2p3Starts, offsetIndex.getOffset(2));
      assertEquals(c2p2Starts - c2p1Starts, offsetIndex.getCompressedPageSize(0));
      assertEquals(c2p3Starts - c2p2Starts, offsetIndex.getCompressedPageSize(1));
      assertEquals(c2Ends - c2p3Starts, offsetIndex.getCompressedPageSize(2));
      assertEquals(0, offsetIndex.getFirstRowIndex(0));
      assertEquals(1, offsetIndex.getFirstRowIndex(1));
      assertEquals(3, offsetIndex.getFirstRowIndex(2));

      if (!isSpark40Plus()) {
        assertNull(indexReader.readColumnIndex(footer.getBlocks().get(2).getColumns().get(0)));
      }
    }
  }

  // Test reader with merging of scan ranges enabled
  @Test
  public void testWriteReadMergeScanRange() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(CometConf.COMET_IO_MERGE_RANGES().key(), Boolean.toString(true));
    // Set the merge range delta so small that ranges do not get merged
    conf.set(CometConf.COMET_IO_MERGE_RANGES_DELTA().key(), Integer.toString(1024));
    testReadWrite(conf, 2, 1024);
    // Set the merge range delta so large that all ranges get merged
    conf.set(CometConf.COMET_IO_MERGE_RANGES_DELTA().key(), Integer.toString(1024 * 1024));
    testReadWrite(conf, 2, 1024);
  }

  // `addBloomFilter` is package-private in Parquet, so this uses reflection to access it
  private void addBloomFilter(ParquetFileWriter w, String s, BloomFilter filter) throws Exception {
    Method method =
        ParquetFileWriter.class.getDeclaredMethod(
            "addBloomFilter", String.class, BloomFilter.class);
    method.setAccessible(true);
    method.invoke(w, s, filter);
  }

  private void validateContains(PageReadStore pages, String[] path, int values, BytesInput bytes)
      throws IOException {
    PageReader pageReader = pages.getPageReader(SCHEMA.getColumnDescription(path));
    DataPage page = pageReader.readPage();
    validatePage(page, values, bytes);
  }

  private void validatePage(DataPage page, int values, BytesInput bytes) throws IOException {
    assertEquals(values, page.getValueCount());
    assertArrayEquals(bytes.toByteArray(), ((DataPageV1) page).getBytes().toByteArray());
  }

  private void validateV2Page(
      PageReadStore pages,
      String[] path,
      int values,
      int rows,
      int nullCount,
      byte[] repetition,
      byte[] definition,
      byte[] data,
      int uncompressedSize)
      throws IOException {
    PageReader pageReader = pages.getPageReader(SCHEMA.getColumnDescription(path));
    DataPageV2 page = (DataPageV2) pageReader.readPage();
    assertEquals(values, page.getValueCount());
    assertEquals(rows, page.getRowCount());
    assertEquals(nullCount, page.getNullCount());
    assertEquals(uncompressedSize, page.getUncompressedSize());
    assertArrayEquals(repetition, page.getRepetitionLevels().toByteArray());
    assertArrayEquals(definition, page.getDefinitionLevels().toByteArray());
    assertArrayEquals(data, page.getData().toByteArray());
  }

  private Statistics<?> createStatistics(String min, String max, ColumnDescriptor col) {
    return Statistics.getBuilderForReading(col.getPrimitiveType())
        .withMin(Binary.fromString(min).getBytes())
        .withMax(Binary.fromString(max).getBytes())
        .withNumNulls(0)
        .build();
  }

  public static void assertStatsValuesEqual(Statistics<?> expected, Statistics<?> actual) {
    if (expected == actual) {
      return;
    }
    if (expected == null || actual == null) {
      assertEquals(expected, actual);
    }
    Assert.assertArrayEquals(expected.getMaxBytes(), actual.getMaxBytes());
    Assert.assertArrayEquals(expected.getMinBytes(), actual.getMinBytes());
    Assert.assertEquals(expected.getNumNulls(), actual.getNumNulls());
  }

  private Statistics<?> statsC1(Binary... values) {
    Statistics<?> stats = Statistics.createStats(C1.getPrimitiveType());
    for (Binary value : values) {
      if (value == null) {
        stats.incrementNumNulls();
      } else {
        stats.updateStats(value);
      }
    }
    return stats;
  }

  /**
   * Generates arbitrary data for simple schemas, writes the data to a file and also returns the
   * data.
   *
   * @return array of data pages for each column
   */
  private HashMap<String, byte[][]> generateAndWriteData(
      Configuration configuration,
      Path path,
      MessageType schema,
      int numPages,
      int numRecordsPerPage)
      throws IOException {

    HashMap<String, byte[][]> dataPages = new HashMap<>();

    Generator generator = new Generator();
    ParquetFileWriter writer = new ParquetFileWriter(configuration, schema, path);
    writer.start();
    writer.startBlock((long) numPages * numRecordsPerPage);
    for (ColumnDescriptor colDesc : schema.getColumns()) {
      writer.startColumn(colDesc, (long) numPages * numRecordsPerPage, CODEC);
      String type = colDesc.getPrimitiveType().getName();
      byte[][] allPages = new byte[numPages][];
      byte[] data;
      for (int i = 0; i < numPages; i++) {
        data = generator.generateValues(numRecordsPerPage, type);
        writer.writeDataPage(
            numRecordsPerPage,
            data.length,
            BytesInput.from(data),
            EMPTY_STATS,
            numRecordsPerPage,
            RLE,
            RLE,
            PLAIN);
        allPages[i] = data;
      }
      dataPages.put(String.join(".", colDesc.getPath()), allPages);
      writer.endColumn();
    }
    writer.endBlock();
    writer.end(new HashMap<>());
    return dataPages;
  }

  private void readAndValidatePageData(
      InputFile inputFile,
      ParquetReadOptions options,
      ReadOptions cometOptions,
      MessageType schema,
      HashMap<String, byte[][]> expected,
      int expectedValuesPerPage)
      throws IOException {
    try (FileReader fileReader = new FileReader(inputFile, options, cometOptions)) {
      fileReader.setRequestedSchema(schema.getColumns());
      PageReadStore pages = fileReader.readNextRowGroup();
      for (ColumnDescriptor colDesc : schema.getColumns()) {
        byte[][] allExpectedPages = expected.get(String.join(".", colDesc.getPath()));
        PageReader pageReader = pages.getPageReader(colDesc);
        for (byte[] expectedPage : allExpectedPages) {
          DataPage page = pageReader.readPage();
          validatePage(page, expectedValuesPerPage, BytesInput.from(expectedPage));
        }
      }
    }
  }

  public void testReadWrite(Configuration configuration, int numPages, int numRecordsPerPage)
      throws Exception {
    File testFile = temp.newFile();
    testFile.delete();

    Path path = new Path(testFile.toURI());
    HashMap<String, byte[][]> dataPages =
        generateAndWriteData(configuration, path, SCHEMA2, numPages, numRecordsPerPage);
    InputFile file = HadoopInputFile.fromPath(path, configuration);
    ParquetReadOptions options = ParquetReadOptions.builder().build();
    ReadOptions cometOptions = ReadOptions.builder(configuration).build();

    readAndValidatePageData(
        file, options, cometOptions, PROJECTED_SCHEMA2, dataPages, numRecordsPerPage);
  }

  static class Generator {

    static Random random = new Random(1729);
    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz -";
    private static final int STR_MIN_SIZE = 5;
    private static final int STR_MAX_SIZE = 30;

    private byte[] getString(int minSize, int maxSize) {
      int size = random.nextInt(maxSize - minSize) + minSize;
      byte[] str = new byte[size];
      for (int i = 0; i < size; ++i) {
        str[i] = (byte) ALPHABET.charAt(random.nextInt(ALPHABET.length()));
      }
      return str;
    }

    private byte[] generateValues(int numValues, String type) throws IOException {

      if (type.equals("int32")) {
        byte[] data = new byte[4 * numValues];
        random.nextBytes(data);
        return data;
      } else {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (int i = 0; i < numValues; i++) {
          outputStream.write(getString(STR_MIN_SIZE, STR_MAX_SIZE));
        }
        return outputStream.toByteArray();
      }
    }
  }

  private Statistics<?> statsC2(Long... values) {
    Statistics<?> stats = Statistics.createStats(C2.getPrimitiveType());
    for (Long value : values) {
      if (value == null) {
        stats.incrementNumNulls();
      } else {
        stats.updateStats(value);
      }
    }
    return stats;
  }
}
