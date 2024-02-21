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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalColumnDecryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ModuleCipherFactory;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.FileCryptoMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.spark.sql.execution.metric.SQLMetric;

import static org.apache.parquet.hadoop.ParquetFileWriter.EFMAGIC;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

import static org.apache.comet.parquet.RowGroupFilter.FilterLevel.BLOOMFILTER;
import static org.apache.comet.parquet.RowGroupFilter.FilterLevel.DICTIONARY;
import static org.apache.comet.parquet.RowGroupFilter.FilterLevel.STATISTICS;

/**
 * A Parquet file reader. Mostly followed {@code ParquetFileReader} in {@code parquet-mr}, but with
 * customizations & optimizations for Comet.
 */
public class FileReader implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FileReader.class);

  private final ParquetMetadataConverter converter;
  protected final SeekableInputStream f;
  private final InputFile file;
  private final Map<String, SQLMetric> metrics;
  private final Map<ColumnPath, ColumnDescriptor> paths = new HashMap<>();
  private final FileMetaData fileMetaData; // may be null
  private final List<BlockMetaData> blocks;
  private final List<ColumnIndexReader> blockIndexStores;
  private final List<RowRanges> blockRowRanges;
  private final CRC32 crc;
  private final ParquetMetadata footer;

  /**
   * Read configurations come from two options: - options: these are options defined & specified
   * from 'parquet-mr' library - cometOptions: these are Comet-specific options, for the features
   * introduced in Comet's Parquet implementation
   */
  private final ParquetReadOptions options;

  private final ReadOptions cometOptions;

  private int currentBlock = 0;
  private RowGroupReader currentRowGroup = null;
  private InternalFileDecryptor fileDecryptor;

  public FileReader(InputFile file, ParquetReadOptions options, ReadOptions cometOptions)
      throws IOException {
    this(file, null, options, cometOptions, null);
  }

  public FileReader(
      InputFile file,
      ParquetReadOptions options,
      ReadOptions cometOptions,
      Map<String, SQLMetric> metrics)
      throws IOException {
    this(file, null, options, cometOptions, metrics);
  }

  public FileReader(
      InputFile file,
      ParquetMetadata footer,
      ParquetReadOptions options,
      ReadOptions cometOptions,
      Map<String, SQLMetric> metrics)
      throws IOException {
    this.converter = new ParquetMetadataConverter(options);
    this.file = file;
    this.f = file.newStream();
    this.options = options;
    this.cometOptions = cometOptions;
    this.metrics = metrics;
    if (footer == null) {
      try {
        footer = readFooter(file, options, f, converter);
      } catch (Exception e) {
        // In case that reading footer throws an exception in the constructor, the new stream
        // should be closed. Otherwise, there's no way to close this outside.
        f.close();
        throw e;
      }
    }
    this.footer = footer;
    this.fileMetaData = footer.getFileMetaData();
    this.fileDecryptor = fileMetaData.getFileDecryptor(); // must be called before filterRowGroups!
    if (null != fileDecryptor && fileDecryptor.plaintextFile()) {
      this.fileDecryptor = null; // Plaintext file. No need in decryptor
    }

    this.blocks = filterRowGroups(footer.getBlocks());
    this.blockIndexStores = listWithNulls(this.blocks.size());
    this.blockRowRanges = listWithNulls(this.blocks.size());
    for (ColumnDescriptor col : footer.getFileMetaData().getSchema().getColumns()) {
      paths.put(ColumnPath.get(col.getPath()), col);
    }
    this.crc = options.usePageChecksumVerification() ? new CRC32() : null;
  }

  /** Returns the footer of the Parquet file being read. */
  public ParquetMetadata getFooter() {
    return this.footer;
  }

  /** Returns the metadata of the Parquet file being read. */
  public FileMetaData getFileMetaData() {
    return this.fileMetaData;
  }

  /** Returns the input stream of the Parquet file being read. */
  public SeekableInputStream getInputStream() {
    return this.f;
  }

  /** Returns the Parquet options for reading the file. */
  public ParquetReadOptions getOptions() {
    return this.options;
  }

  /** Returns all the row groups of this reader (after applying row group filtering). */
  public List<BlockMetaData> getRowGroups() {
    return blocks;
  }

  /** Sets the projected columns to be read later via {@link #readNextRowGroup()} */
  public void setRequestedSchema(List<ColumnDescriptor> projection) {
    paths.clear();
    for (ColumnDescriptor col : projection) {
      paths.put(ColumnPath.get(col.getPath()), col);
    }
  }

  /**
   * Gets the total number of records across all row groups (after applying row group filtering).
   */
  public long getRecordCount() {
    long total = 0;
    for (BlockMetaData block : blocks) {
      total += block.getRowCount();
    }
    return total;
  }

  /**
   * Gets the total number of records across all row groups (after applying both row group filtering
   * and page-level column index filtering).
   */
  public long getFilteredRecordCount() {
    if (!options.useColumnIndexFilter()
        || !FilterCompat.isFilteringRequired(options.getRecordFilter())) {
      return getRecordCount();
    }
    long total = 0;
    for (int i = 0, n = blocks.size(); i < n; ++i) {
      total += getRowRanges(i).rowCount();
    }
    return total;
  }

  /** Skips the next row group. Returns false if there's no row group to skip. Otherwise, true. */
  public boolean skipNextRowGroup() {
    return advanceToNextBlock();
  }

  /**
   * Returns the next row group to read (after applying row group filtering), or null if there's no
   * more row group.
   */
  public PageReadStore readNextRowGroup() throws IOException {
    if (currentBlock == blocks.size()) {
      return null;
    }
    BlockMetaData block = blocks.get(currentBlock);
    if (block.getRowCount() == 0) {
      throw new RuntimeException("Illegal row group of 0 rows");
    }
    this.currentRowGroup = new RowGroupReader(block.getRowCount());
    // prepare the list of consecutive parts to read them in one scan
    List<ConsecutivePartList> allParts = new ArrayList<>();
    ConsecutivePartList currentParts = null;
    for (ColumnChunkMetaData mc : block.getColumns()) {
      ColumnPath pathKey = mc.getPath();
      ColumnDescriptor columnDescriptor = paths.get(pathKey);
      if (columnDescriptor != null) {
        BenchmarkCounter.incrementTotalBytes(mc.getTotalSize());
        long startingPos = mc.getStartingPos();
        boolean mergeRanges = cometOptions.isIOMergeRangesEnabled();
        int mergeRangeDelta = cometOptions.getIOMergeRangesDelta();

        // start a new list if -
        //   it is the first part or
        //   the part is consecutive or
        //   the part is not consecutive but within the merge range
        if (currentParts == null
            || (!mergeRanges && currentParts.endPos() != startingPos)
            || (mergeRanges && startingPos - currentParts.endPos() > mergeRangeDelta)) {
          currentParts = new ConsecutivePartList(startingPos);
          allParts.add(currentParts);
        }
        // if we are in a consecutive part list and there is a gap in between the parts,
        // we treat the gap as a skippable chunk
        long delta = startingPos - currentParts.endPos();
        if (mergeRanges && delta > 0 && delta <= mergeRangeDelta) {
          // add a chunk that will be skipped because it has no column descriptor
          currentParts.addChunk(new ChunkDescriptor(null, null, startingPos, delta));
        }
        currentParts.addChunk(
            new ChunkDescriptor(columnDescriptor, mc, startingPos, mc.getTotalSize()));
      }
    }
    // actually read all the chunks
    return readChunks(block, allParts, new ChunkListBuilder());
  }

  /**
   * Returns the next row group to read (after applying both row group filtering and page level
   * column index filtering), or null if there's no more row group.
   */
  public PageReadStore readNextFilteredRowGroup() throws IOException {
    if (currentBlock == blocks.size()) {
      return null;
    }
    if (!options.useColumnIndexFilter()
        || !FilterCompat.isFilteringRequired(options.getRecordFilter())) {
      return readNextRowGroup();
    }
    BlockMetaData block = blocks.get(currentBlock);
    if (block.getRowCount() == 0) {
      throw new RuntimeException("Illegal row group of 0 rows");
    }
    ColumnIndexStore ciStore = getColumnIndexReader(currentBlock);
    RowRanges rowRanges = getRowRanges(currentBlock);
    long rowCount = rowRanges.rowCount();
    if (rowCount == 0) {
      // There are no matching rows -> skipping this row-group
      advanceToNextBlock();
      return readNextFilteredRowGroup();
    }
    if (rowCount == block.getRowCount()) {
      // All rows are matching -> fall back to the non-filtering path
      return readNextRowGroup();
    }

    this.currentRowGroup = new RowGroupReader(rowRanges);
    // prepare the list of consecutive parts to read them in one scan
    ChunkListBuilder builder = new ChunkListBuilder();
    List<ConsecutivePartList> allParts = new ArrayList<>();
    ConsecutivePartList currentParts = null;
    for (ColumnChunkMetaData mc : block.getColumns()) {
      ColumnPath pathKey = mc.getPath();
      ColumnDescriptor columnDescriptor = paths.get(pathKey);
      if (columnDescriptor != null) {
        OffsetIndex offsetIndex = ciStore.getOffsetIndex(mc.getPath());
        IndexFilter indexFilter = new IndexFilter(rowRanges, offsetIndex, block.getRowCount());
        OffsetIndex filteredOffsetIndex = indexFilter.filterOffsetIndex();
        for (IndexFilter.OffsetRange range :
            indexFilter.calculateOffsetRanges(filteredOffsetIndex, mc)) {
          BenchmarkCounter.incrementTotalBytes(range.length);
          long startingPos = range.offset;
          // first part or not consecutive => new list
          if (currentParts == null || currentParts.endPos() != startingPos) {
            currentParts = new ConsecutivePartList(startingPos);
            allParts.add(currentParts);
          }
          ChunkDescriptor chunkDescriptor =
              new ChunkDescriptor(columnDescriptor, mc, startingPos, range.length);
          currentParts.addChunk(chunkDescriptor);
          builder.setOffsetIndex(chunkDescriptor, filteredOffsetIndex);
        }
      }
    }
    // actually read all the chunks
    return readChunks(block, allParts, builder);
  }

  // Visible for testing
  ColumnIndexReader getColumnIndexReader(int blockIndex) {
    ColumnIndexReader ciStore = blockIndexStores.get(blockIndex);
    if (ciStore == null) {
      ciStore = ColumnIndexReader.create(blocks.get(blockIndex), paths.keySet(), fileDecryptor, f);
      blockIndexStores.set(blockIndex, ciStore);
    }
    return ciStore;
  }

  private PageReadStore readChunks(
      BlockMetaData block, List<ConsecutivePartList> allParts, ChunkListBuilder builder)
      throws IOException {
    for (ConsecutivePartList consecutiveChunks : allParts) {
      if (shouldReadParallel()) {
        consecutiveChunks.readAllParallel(builder);
      } else {
        consecutiveChunks.readAll(f, builder);
      }
    }
    for (Chunk chunk : builder.build()) {
      readChunkPages(chunk, block);
    }

    advanceToNextBlock();

    return currentRowGroup;
  }

  private boolean shouldReadParallel() {
    if (file instanceof CometInputFile) {
      URI uri = ((CometInputFile) file).getPath().toUri();
      return shouldReadParallel(cometOptions, uri.getScheme());
    }

    return false;
  }

  static boolean shouldReadParallel(ReadOptions options, String scheme) {
    return options.isParallelIOEnabled() && shouldReadParallelForScheme(scheme);
  }

  private static boolean shouldReadParallelForScheme(String scheme) {
    if (scheme == null) {
      return false;
    }

    switch (scheme) {
      case "s3a":
        // Only enable parallel read for S3, so far.
        return true;
      default:
        return false;
    }
  }

  private void readChunkPages(Chunk chunk, BlockMetaData block) throws IOException {
    if (fileDecryptor == null || fileDecryptor.plaintextFile()) {
      currentRowGroup.addColumn(chunk.descriptor.col, chunk.readAllPages());
      return;
    }
    // Encrypted file
    ColumnPath columnPath = ColumnPath.get(chunk.descriptor.col.getPath());
    InternalColumnDecryptionSetup columnDecryptionSetup = fileDecryptor.getColumnSetup(columnPath);
    if (!columnDecryptionSetup.isEncrypted()) { // plaintext column
      currentRowGroup.addColumn(chunk.descriptor.col, chunk.readAllPages());
    } else { // encrypted column
      currentRowGroup.addColumn(
          chunk.descriptor.col,
          chunk.readAllPages(
              columnDecryptionSetup.getMetaDataDecryptor(),
              columnDecryptionSetup.getDataDecryptor(),
              fileDecryptor.getFileAAD(),
              block.getOrdinal(),
              columnDecryptionSetup.getOrdinal()));
    }
  }

  private boolean advanceToNextBlock() {
    if (currentBlock == blocks.size()) {
      return false;
    }
    // update the current block and instantiate a dictionary reader for it
    ++currentBlock;
    return true;
  }

  public long[] getRowIndices() {
    long[] rowIndices = new long[blocks.size() * 2];
    for (int i = 0, n = blocks.size(); i < n; i++) {
      BlockMetaData block = blocks.get(i);
      rowIndices[i * 2] = getRowIndexOffset(block);
      rowIndices[i * 2 + 1] = block.getRowCount();
    }
    return rowIndices;
  }

  // Uses reflection to get row index offset from a Parquet block metadata.
  //
  // The reason reflection is used here is that some Spark versions still depend on a
  // Parquet version where the method `getRowIndexOffset` is not public.
  private long getRowIndexOffset(BlockMetaData metaData) {
    try {
      Method method = BlockMetaData.class.getMethod("getRowIndexOffset");
      method.setAccessible(true);
      return (long) method.invoke(metaData);
    } catch (Exception e) {
      throw new RuntimeException("Error when calling getRowIndexOffset", e);
    }
  }

  private RowRanges getRowRanges(int blockIndex) {
    Preconditions.checkState(
        FilterCompat.isFilteringRequired(options.getRecordFilter()),
        "Should not be invoked if filter is null or NOOP");
    RowRanges rowRanges = blockRowRanges.get(blockIndex);
    if (rowRanges == null) {
      rowRanges =
          ColumnIndexFilter.calculateRowRanges(
              options.getRecordFilter(),
              getColumnIndexReader(blockIndex),
              paths.keySet(),
              blocks.get(blockIndex).getRowCount());
      blockRowRanges.set(blockIndex, rowRanges);
    }
    return rowRanges;
  }

  private static ParquetMetadata readFooter(
      InputFile file,
      ParquetReadOptions options,
      SeekableInputStream f,
      ParquetMetadataConverter converter)
      throws IOException {
    long fileLen = file.getLength();
    String filePath = file.toString();
    LOG.debug("File length {}", fileLen);

    int FOOTER_LENGTH_SIZE = 4;

    // MAGIC + data + footer + footerIndex + MAGIC
    if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) {
      throw new RuntimeException(
          filePath + " is not a Parquet file (length is too low: " + fileLen + ")");
    }

    // Read footer length and magic string - with a single seek
    byte[] magic = new byte[MAGIC.length];
    long fileMetadataLengthIndex = fileLen - magic.length - FOOTER_LENGTH_SIZE;
    LOG.debug("reading footer index at {}", fileMetadataLengthIndex);
    f.seek(fileMetadataLengthIndex);
    int fileMetadataLength = BytesUtils.readIntLittleEndian(f);
    f.readFully(magic);

    boolean encryptedFooterMode;
    if (Arrays.equals(MAGIC, magic)) {
      encryptedFooterMode = false;
    } else if (Arrays.equals(EFMAGIC, magic)) {
      encryptedFooterMode = true;
    } else {
      throw new RuntimeException(
          filePath
              + " is not a Parquet file. Expected magic number "
              + "at tail, but found "
              + Arrays.toString(magic));
    }

    long fileMetadataIndex = fileMetadataLengthIndex - fileMetadataLength;
    LOG.debug("read footer length: {}, footer index: {}", fileMetadataLength, fileMetadataIndex);
    if (fileMetadataIndex < magic.length || fileMetadataIndex >= fileMetadataLengthIndex) {
      throw new RuntimeException(
          "corrupted file: the footer index is not within the file: " + fileMetadataIndex);
    }
    f.seek(fileMetadataIndex);

    FileDecryptionProperties fileDecryptionProperties = options.getDecryptionProperties();
    InternalFileDecryptor fileDecryptor = null;
    if (null != fileDecryptionProperties) {
      fileDecryptor = new InternalFileDecryptor(fileDecryptionProperties);
    }

    // Read all the footer bytes in one time to avoid multiple read operations,
    // since it can be pretty time consuming for a single read operation in HDFS.
    byte[] footerBytes = new byte[fileMetadataLength];
    f.readFully(footerBytes);
    ByteBuffer footerBytesBuffer = ByteBuffer.wrap(footerBytes);
    LOG.debug("Finished to read all footer bytes.");
    InputStream footerBytesStream = ByteBufferInputStream.wrap(footerBytesBuffer);

    // Regular file, or encrypted file with plaintext footer
    if (!encryptedFooterMode) {
      return converter.readParquetMetadata(
          footerBytesStream, options.getMetadataFilter(), fileDecryptor, false, fileMetadataLength);
    }

    // Encrypted file with encrypted footer
    if (fileDecryptor == null) {
      throw new ParquetCryptoRuntimeException(
          "Trying to read file with encrypted footer. " + "No keys available");
    }
    FileCryptoMetaData fileCryptoMetaData = Util.readFileCryptoMetaData(footerBytesStream);
    fileDecryptor.setFileCryptoMetaData(
        fileCryptoMetaData.getEncryption_algorithm(), true, fileCryptoMetaData.getKey_metadata());
    // footer length is required only for signed plaintext footers
    return converter.readParquetMetadata(
        footerBytesStream, options.getMetadataFilter(), fileDecryptor, true, 0);
  }

  private List<BlockMetaData> filterRowGroups(List<BlockMetaData> blocks) {
    FilterCompat.Filter recordFilter = options.getRecordFilter();
    if (FilterCompat.isFilteringRequired(recordFilter)) {
      // set up data filters based on configured levels
      List<RowGroupFilter.FilterLevel> levels = new ArrayList<>();

      if (options.useStatsFilter()) {
        levels.add(STATISTICS);
      }

      if (options.useDictionaryFilter()) {
        levels.add(DICTIONARY);
      }

      if (options.useBloomFilter()) {
        levels.add(BLOOMFILTER);
      }
      return RowGroupFilter.filterRowGroups(levels, recordFilter, blocks, this);
    }

    return blocks;
  }

  private static <T> List<T> listWithNulls(int size) {
    return Stream.generate(() -> (T) null).limit(size).collect(Collectors.toList());
  }

  public void closeStream() throws IOException {
    if (f != null) {
      f.close();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (f != null) {
        f.close();
      }
    } finally {
      options.getCodecFactory().release();
    }
  }

  /**
   * Builder to concatenate the buffers of the discontinuous parts for the same column. These parts
   * are generated as a result of the column-index based filtering when some pages might be skipped
   * at reading.
   */
  private class ChunkListBuilder {
    private class ChunkData {
      final List<ByteBuffer> buffers = new ArrayList<>();
      OffsetIndex offsetIndex;
    }

    private final Map<ChunkDescriptor, ChunkData> map = new HashMap<>();

    void add(ChunkDescriptor descriptor, List<ByteBuffer> buffers) {
      ChunkListBuilder.ChunkData data = map.get(descriptor);
      if (data == null) {
        data = new ChunkData();
        map.put(descriptor, data);
      }
      data.buffers.addAll(buffers);
    }

    void setOffsetIndex(ChunkDescriptor descriptor, OffsetIndex offsetIndex) {
      ChunkData data = map.get(descriptor);
      if (data == null) {
        data = new ChunkData();
        map.put(descriptor, data);
      }
      data.offsetIndex = offsetIndex;
    }

    List<Chunk> build() {
      List<Chunk> chunks = new ArrayList<>();
      for (Map.Entry<ChunkDescriptor, ChunkListBuilder.ChunkData> entry : map.entrySet()) {
        ChunkDescriptor descriptor = entry.getKey();
        ChunkData data = entry.getValue();
        chunks.add(new Chunk(descriptor, data.buffers, data.offsetIndex));
      }
      return chunks;
    }
  }

  /** The data for a column chunk */
  private class Chunk {
    private final ChunkDescriptor descriptor;
    private final ByteBufferInputStream stream;
    final OffsetIndex offsetIndex;

    /**
     * @param descriptor descriptor for the chunk
     * @param buffers ByteBuffers that contain the chunk
     * @param offsetIndex the offset index for this column; might be null
     */
    Chunk(ChunkDescriptor descriptor, List<ByteBuffer> buffers, OffsetIndex offsetIndex) {
      this.descriptor = descriptor;
      this.stream = ByteBufferInputStream.wrap(buffers);
      this.offsetIndex = offsetIndex;
    }

    protected PageHeader readPageHeader(BlockCipher.Decryptor blockDecryptor, byte[] pageHeaderAAD)
        throws IOException {
      return Util.readPageHeader(stream, blockDecryptor, pageHeaderAAD);
    }

    /**
     * Calculate checksum of input bytes, throw decoding exception if it does not match the provided
     * reference crc
     */
    private void verifyCrc(int referenceCrc, byte[] bytes, String exceptionMsg) {
      crc.reset();
      crc.update(bytes);
      if (crc.getValue() != ((long) referenceCrc & 0xffffffffL)) {
        throw new ParquetDecodingException(exceptionMsg);
      }
    }

    private ColumnPageReader readAllPages() throws IOException {
      return readAllPages(null, null, null, -1, -1);
    }

    private ColumnPageReader readAllPages(
        BlockCipher.Decryptor headerBlockDecryptor,
        BlockCipher.Decryptor pageBlockDecryptor,
        byte[] aadPrefix,
        int rowGroupOrdinal,
        int columnOrdinal)
        throws IOException {
      List<DataPage> pagesInChunk = new ArrayList<>();
      DictionaryPage dictionaryPage = null;
      PrimitiveType type =
          fileMetaData.getSchema().getType(descriptor.col.getPath()).asPrimitiveType();

      long valuesCountReadSoFar = 0;
      int dataPageCountReadSoFar = 0;
      byte[] dataPageHeaderAAD = null;
      if (null != headerBlockDecryptor) {
        dataPageHeaderAAD =
            AesCipher.createModuleAAD(
                aadPrefix,
                ModuleCipherFactory.ModuleType.DataPageHeader,
                rowGroupOrdinal,
                columnOrdinal,
                getPageOrdinal(dataPageCountReadSoFar));
      }
      while (hasMorePages(valuesCountReadSoFar, dataPageCountReadSoFar)) {
        byte[] pageHeaderAAD = dataPageHeaderAAD;
        if (null != headerBlockDecryptor) {
          // Important: this verifies file integrity (makes sure dictionary page had not been
          // removed)
          if (null == dictionaryPage && descriptor.metadata.hasDictionaryPage()) {
            pageHeaderAAD =
                AesCipher.createModuleAAD(
                    aadPrefix,
                    ModuleCipherFactory.ModuleType.DictionaryPageHeader,
                    rowGroupOrdinal,
                    columnOrdinal,
                    -1);
          } else {
            int pageOrdinal = getPageOrdinal(dataPageCountReadSoFar);
            AesCipher.quickUpdatePageAAD(dataPageHeaderAAD, pageOrdinal);
          }
        }

        PageHeader pageHeader = readPageHeader(headerBlockDecryptor, pageHeaderAAD);
        int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        int compressedPageSize = pageHeader.getCompressed_page_size();
        final BytesInput pageBytes;
        switch (pageHeader.type) {
          case DICTIONARY_PAGE:
            // there is only one dictionary page per column chunk
            if (dictionaryPage != null) {
              throw new ParquetDecodingException(
                  "more than one dictionary page in column " + descriptor.col);
            }
            pageBytes = this.readAsBytesInput(compressedPageSize);
            if (options.usePageChecksumVerification() && pageHeader.isSetCrc()) {
              verifyCrc(
                  pageHeader.getCrc(),
                  pageBytes.toByteArray(),
                  "could not verify dictionary page integrity, CRC checksum verification failed");
            }
            DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
            dictionaryPage =
                new DictionaryPage(
                    pageBytes,
                    uncompressedPageSize,
                    dicHeader.getNum_values(),
                    converter.getEncoding(dicHeader.getEncoding()));
            // Copy crc to new page, used for testing
            if (pageHeader.isSetCrc()) {
              dictionaryPage.setCrc(pageHeader.getCrc());
            }
            break;

          case DATA_PAGE:
            DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
            pageBytes = this.readAsBytesInput(compressedPageSize);
            if (options.usePageChecksumVerification() && pageHeader.isSetCrc()) {
              verifyCrc(
                  pageHeader.getCrc(),
                  pageBytes.toByteArray(),
                  "could not verify page integrity, CRC checksum verification failed");
            }
            DataPageV1 dataPageV1 =
                new DataPageV1(
                    pageBytes,
                    dataHeaderV1.getNum_values(),
                    uncompressedPageSize,
                    converter.fromParquetStatistics(
                        getFileMetaData().getCreatedBy(), dataHeaderV1.getStatistics(), type),
                    converter.getEncoding(dataHeaderV1.getRepetition_level_encoding()),
                    converter.getEncoding(dataHeaderV1.getDefinition_level_encoding()),
                    converter.getEncoding(dataHeaderV1.getEncoding()));
            // Copy crc to new page, used for testing
            if (pageHeader.isSetCrc()) {
              dataPageV1.setCrc(pageHeader.getCrc());
            }
            pagesInChunk.add(dataPageV1);
            valuesCountReadSoFar += dataHeaderV1.getNum_values();
            ++dataPageCountReadSoFar;
            break;

          case DATA_PAGE_V2:
            DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
            int dataSize =
                compressedPageSize
                    - dataHeaderV2.getRepetition_levels_byte_length()
                    - dataHeaderV2.getDefinition_levels_byte_length();
            pagesInChunk.add(
                new DataPageV2(
                    dataHeaderV2.getNum_rows(),
                    dataHeaderV2.getNum_nulls(),
                    dataHeaderV2.getNum_values(),
                    this.readAsBytesInput(dataHeaderV2.getRepetition_levels_byte_length()),
                    this.readAsBytesInput(dataHeaderV2.getDefinition_levels_byte_length()),
                    converter.getEncoding(dataHeaderV2.getEncoding()),
                    this.readAsBytesInput(dataSize),
                    uncompressedPageSize,
                    converter.fromParquetStatistics(
                        getFileMetaData().getCreatedBy(), dataHeaderV2.getStatistics(), type),
                    dataHeaderV2.isIs_compressed()));
            valuesCountReadSoFar += dataHeaderV2.getNum_values();
            ++dataPageCountReadSoFar;
            break;

          default:
            LOG.debug(
                "skipping page of type {} of size {}", pageHeader.getType(), compressedPageSize);
            stream.skipFully(compressedPageSize);
            break;
        }
      }
      if (offsetIndex == null && valuesCountReadSoFar != descriptor.metadata.getValueCount()) {
        // Would be nice to have a CorruptParquetFileException or something as a subclass?
        throw new IOException(
            "Expected "
                + descriptor.metadata.getValueCount()
                + " values in column chunk at "
                + file
                + " offset "
                + descriptor.metadata.getFirstDataPageOffset()
                + " but got "
                + valuesCountReadSoFar
                + " values instead over "
                + pagesInChunk.size()
                + " pages ending at file offset "
                + (descriptor.fileOffset + stream.position()));
      }
      CompressionCodecFactory.BytesInputDecompressor decompressor =
          options.getCodecFactory().getDecompressor(descriptor.metadata.getCodec());
      return new ColumnPageReader(
          decompressor,
          pagesInChunk,
          dictionaryPage,
          offsetIndex,
          blocks.get(currentBlock).getRowCount(),
          pageBlockDecryptor,
          aadPrefix,
          rowGroupOrdinal,
          columnOrdinal);
    }

    private boolean hasMorePages(long valuesCountReadSoFar, int dataPageCountReadSoFar) {
      return offsetIndex == null
          ? valuesCountReadSoFar < descriptor.metadata.getValueCount()
          : dataPageCountReadSoFar < offsetIndex.getPageCount();
    }

    private int getPageOrdinal(int dataPageCountReadSoFar) {
      if (null == offsetIndex) {
        return dataPageCountReadSoFar;
      }

      return offsetIndex.getPageOrdinal(dataPageCountReadSoFar);
    }

    /**
     * @param size the size of the page
     * @return the page
     * @throws IOException if there is an error while reading from the file stream
     */
    public BytesInput readAsBytesInput(int size) throws IOException {
      return BytesInput.from(stream.sliceBuffers(size));
    }
  }

  /**
   * Describes a list of consecutive parts to be read at once. A consecutive part may contain whole
   * column chunks or only parts of them (some pages).
   */
  private class ConsecutivePartList {
    private final long offset;
    private final List<ChunkDescriptor> chunks = new ArrayList<>();
    private long length;
    private final SQLMetric fileReadTimeMetric;
    private final SQLMetric fileReadSizeMetric;
    private final SQLMetric readThroughput;

    /**
     * Constructor
     *
     * @param offset where the first chunk starts
     */
    ConsecutivePartList(long offset) {
      if (metrics != null) {
        this.fileReadTimeMetric = metrics.get("ParquetInputFileReadTime");
        this.fileReadSizeMetric = metrics.get("ParquetInputFileReadSize");
        this.readThroughput = metrics.get("ParquetInputFileReadThroughput");
      } else {
        this.fileReadTimeMetric = null;
        this.fileReadSizeMetric = null;
        this.readThroughput = null;
      }
      this.offset = offset;
    }

    /**
     * Adds a chunk to the list. It must be consecutive to the previous chunk.
     *
     * @param descriptor a chunk descriptor
     */
    public void addChunk(ChunkDescriptor descriptor) {
      chunks.add(descriptor);
      length += descriptor.size;
    }

    private List<ByteBuffer> allocateReadBuffers() {
      int fullAllocations = Math.toIntExact(length / options.getMaxAllocationSize());
      int lastAllocationSize = Math.toIntExact(length % options.getMaxAllocationSize());

      int numAllocations = fullAllocations + (lastAllocationSize > 0 ? 1 : 0);
      List<ByteBuffer> buffers = new ArrayList<>(numAllocations);

      for (int i = 0; i < fullAllocations; i += 1) {
        buffers.add(options.getAllocator().allocate(options.getMaxAllocationSize()));
      }

      if (lastAllocationSize > 0) {
        buffers.add(options.getAllocator().allocate(lastAllocationSize));
      }
      return buffers;
    }

    /**
     * @param f file to read the chunks from
     * @param builder used to build chunk list to read the pages for the different columns
     * @throws IOException if there is an error while reading from the stream
     */
    public void readAll(SeekableInputStream f, ChunkListBuilder builder) throws IOException {
      f.seek(offset);

      List<ByteBuffer> buffers = allocateReadBuffers();
      long startNs = System.nanoTime();

      for (ByteBuffer buffer : buffers) {
        f.readFully(buffer);
        buffer.flip();
      }
      setReadMetrics(startNs);

      // report in a counter the data we just scanned
      BenchmarkCounter.incrementBytesRead(length);
      ByteBufferInputStream stream = ByteBufferInputStream.wrap(buffers);
      for (int i = 0; i < chunks.size(); i++) {
        ChunkDescriptor descriptor = chunks.get(i);
        if (descriptor.col != null) {
          builder.add(descriptor, stream.sliceBuffers(descriptor.size));
        } else {
          stream.skipFully(descriptor.size);
        }
      }
    }

    /**
     * Api to read a consecutive range from the Parquet file in parallel. This is identical to
     * {@link #readAll(SeekableInputStream, ChunkListBuilder) readAll}, except that the consecutive
     * range is split into multiple smaller ranges and read in parallel. The parallelism can be set
     * by specifying the threadpool size via {@link
     * ReadOptions.Builder#withParallelIOThreadPoolSize(int)}.
     *
     * @param builder used to build chunk list to read the pages for the different columns
     * @throws IOException if there is an error while reading from the stream
     */
    public void readAllParallel(ChunkListBuilder builder) throws IOException {

      List<ByteBuffer> buffers = allocateReadBuffers();
      long startNs = System.nanoTime();

      int nThreads = cometOptions.parallelIOThreadPoolSize();
      ExecutorService threadPool = CometFileReaderThreadPool.getOrCreateThreadPool(nThreads);
      List<Future<Void>> futures = new ArrayList<>();

      long currentOffset = this.offset;
      int buffersPerThread = buffers.size() / nThreads;
      int remaining = buffers.size() % nThreads;
      // offset in input file each thread seeks to before beginning read
      long[] offsets = new long[nThreads];
      // index of buffer where each thread will start writing data
      int[] bufferIndexes = new int[nThreads];
      //  number of buffers for each thread to fill
      int[] numBuffers = new int[nThreads];

      int bufferNum = 0;
      for (int i = 0; i < nThreads; i++) {
        int nBuffers = 0;
        offsets[i] = currentOffset;
        bufferIndexes[i] = bufferNum;
        nBuffers = buffersPerThread;
        for (int j = 0; j < buffersPerThread; j++) {
          currentOffset += buffers.get(bufferNum).capacity();
          bufferNum++;
        }
        if (remaining > 0) {
          remaining--;
          currentOffset += buffers.get(bufferNum).capacity();
          bufferNum++;
          nBuffers++;
        }
        numBuffers[i] = nBuffers;
      }
      for (int n = 0; n < nThreads; n++) {
        int threadIndex = n;
        long pos = offsets[threadIndex];
        int bufferIndex = bufferIndexes[threadIndex];
        int nBuffers = numBuffers[threadIndex];
        if (nBuffers == 0) {
          continue;
        }

        // Find the total number of bytes to read for the current thread
        long tmp = 0;
        for (int i = 0; i < nBuffers; i++) {
          int bufNo = bufferIndex + i;
          if (bufNo >= buffers.size()) break;
          tmp += buffers.get(bufNo).capacity();
        }
        final long length = tmp;

        futures.add(
            threadPool.submit(
                () -> {
                  SeekableInputStream inputStream = null;
                  try {
                    if (file instanceof CometInputFile) {
                      inputStream = (((CometInputFile) file).newStream(pos, length));
                    } else {
                      inputStream = file.newStream();
                    }

                    inputStream.seek(pos);
                    long curPos = pos;
                    for (int i = 0; i < nBuffers; i++) {
                      int bufNo = bufferIndex + i;
                      if (bufNo >= buffers.size()) {
                        break;
                      }
                      ByteBuffer buffer = buffers.get(bufNo);
                      LOG.debug(
                          "Thread: {} Offset: {} Buffer: {} Size: {}",
                          threadIndex,
                          curPos,
                          bufNo,
                          buffer.capacity());
                      curPos += buffer.capacity();
                      inputStream.readFully(buffer);
                      buffer.flip();
                    } // for
                  } finally {
                    if (inputStream != null) {
                      inputStream.close();
                    }
                  }

                  return null;
                }));
      }

      for (Future<Void> future : futures) {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }

      setReadMetrics(startNs);

      ByteBufferInputStream stream;
      stream = ByteBufferInputStream.wrap(buffers);
      // report in a counter the data we just scanned
      BenchmarkCounter.incrementBytesRead(length);
      for (int i = 0; i < chunks.size(); i++) {
        ChunkDescriptor descriptor = chunks.get(i);
        if (descriptor.col != null) {
          builder.add(descriptor, stream.sliceBuffers(descriptor.size));
        } else {
          stream.skipFully(descriptor.size);
        }
      }
    }

    private void setReadMetrics(long startNs) {
      long totalFileReadTimeNs = System.nanoTime() - startNs;
      double sizeInMb = ((double) length) / (1024 * 1024);
      double timeInSec = ((double) totalFileReadTimeNs) / 1000_0000_0000L;
      double throughput = sizeInMb / timeInSec;
      LOG.debug(
          "Comet: File Read stats:  Length: {} MB, Time: {} secs, throughput: {} MB/sec ",
          sizeInMb,
          timeInSec,
          throughput);
      if (fileReadTimeMetric != null) {
        fileReadTimeMetric.add(totalFileReadTimeNs);
      }
      if (fileReadSizeMetric != null) {
        fileReadSizeMetric.add(length);
      }
      if (readThroughput != null) {
        readThroughput.set(throughput);
      }
    }

    /**
     * End position of the last byte of these chunks
     *
     * @return the position following the last byte of these chunks
     */
    public long endPos() {
      return offset + length;
    }
  }

  /** Information needed to read a column chunk or a part of it. */
  private static class ChunkDescriptor {
    private final ColumnDescriptor col;
    private final ColumnChunkMetaData metadata;
    private final long fileOffset;
    private final long size;

    /**
     * @param col column this chunk is part of
     * @param metadata metadata for the column
     * @param fileOffset offset in the file where this chunk starts
     * @param size size of the chunk
     */
    ChunkDescriptor(
        ColumnDescriptor col, ColumnChunkMetaData metadata, long fileOffset, long size) {
      this.col = col;
      this.metadata = metadata;
      this.fileOffset = fileOffset;
      this.size = size;
    }

    @Override
    public int hashCode() {
      return col.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj instanceof ChunkDescriptor) {
        return col.equals(((ChunkDescriptor) obj).col);
      } else {
        return false;
      }
    }
  }
}
