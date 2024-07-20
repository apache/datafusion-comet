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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import scala.Option;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.c.CometSchemaImporter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.comet.parquet.CometParquetReadSupport;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.AccumulatorV2;

import org.apache.comet.CometConf;
import org.apache.comet.shims.ShimBatchReader;
import org.apache.comet.shims.ShimFileFormat;
import org.apache.comet.vector.CometVector;

/**
 * A vectorized Parquet reader that reads a Parquet file in a batched fashion.
 *
 * <p>Example of how to use this:
 *
 * <pre>
 *   BatchReader reader = new BatchReader(parquetFile, batchSize);
 *   try {
 *     reader.init();
 *     while (reader.readBatch()) {
 *       ColumnarBatch batch = reader.currentBatch();
 *       // consume the batch
 *     }
 *   } finally { // resources associated with the reader should be released
 *     reader.close();
 *   }
 * </pre>
 */
public class BatchReader extends RecordReader<Void, ColumnarBatch> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FileReader.class);
  protected static final BufferAllocator ALLOCATOR = new RootAllocator();

  private Configuration conf;
  private int capacity;
  private boolean isCaseSensitive;
  private boolean useFieldId;
  private boolean ignoreMissingIds;
  private StructType partitionSchema;
  private InternalRow partitionValues;
  private PartitionedFile file;
  private final Map<String, SQLMetric> metrics;

  private long rowsRead;
  private StructType sparkSchema;
  private MessageType requestedSchema;
  private CometVector[] vectors;
  private AbstractColumnReader[] columnReaders;
  private CometSchemaImporter importer;
  private ColumnarBatch currentBatch;
  private Future<Option<Throwable>> prefetchTask;
  private LinkedBlockingQueue<Pair<PageReadStore, Long>> prefetchQueue;
  private FileReader fileReader;
  private boolean[] missingColumns;
  private boolean isInitialized;
  private ParquetMetadata footer;

  /** The total number of rows across all row groups of the input split. */
  private long totalRowCount;

  /**
   * The total number of rows loaded so far, including all the rows from row groups that we've
   * processed and the current row group.
   */
  private long totalRowsLoaded;

  /**
   * Whether the native scan should always return decimal represented by 128 bits, regardless of its
   * precision. Normally, this should be true if native execution is enabled, since Arrow compute
   * kernels doesn't support 32 and 64 bit decimals yet.
   */
  private boolean useDecimal128;

  /** Whether to use the lazy materialization reader for reading columns. */
  private boolean useLazyMaterialization;

  /**
   * Whether to return dates/timestamps that were written with legacy hybrid (Julian + Gregorian)
   * calendar as it is. If this is true, Comet will return them as it is, instead of rebasing them
   * to the new Proleptic Gregorian calendar. If this is false, Comet will throw exceptions when
   * seeing these dates/timestamps.
   */
  private boolean useLegacyDateTimestamp;

  /** The TaskContext object for executing this task. */
  private final TaskContext taskContext;

  // Only for testing
  public BatchReader(String file, int capacity) {
    this(file, capacity, null, null);
  }

  // Only for testing
  public BatchReader(
      String file, int capacity, StructType partitionSchema, InternalRow partitionValues) {
    this(new Configuration(), file, capacity, partitionSchema, partitionValues);
  }

  // Only for testing
  public BatchReader(
      Configuration conf,
      String file,
      int capacity,
      StructType partitionSchema,
      InternalRow partitionValues) {
    conf.set("spark.sql.parquet.binaryAsString", "false");
    conf.set("spark.sql.parquet.int96AsTimestamp", "false");
    conf.set("spark.sql.caseSensitive", "false");
    conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "true");
    conf.set("spark.sql.legacy.parquet.nanosAsLong", "false");

    this.conf = conf;
    this.capacity = capacity;
    this.isCaseSensitive = false;
    this.useFieldId = false;
    this.ignoreMissingIds = false;
    this.partitionSchema = partitionSchema;
    this.partitionValues = partitionValues;

    this.file = ShimBatchReader.newPartitionedFile(partitionValues, file);
    this.metrics = new HashMap<>();

    this.taskContext = TaskContext$.MODULE$.get();
  }

  public BatchReader(AbstractColumnReader[] columnReaders) {
    // Todo: set useDecimal128 and useLazyMaterialization
    int numColumns = columnReaders.length;
    this.columnReaders = new AbstractColumnReader[numColumns];
    vectors = new CometVector[numColumns];
    currentBatch = new ColumnarBatch(vectors);
    // This constructor is used by Iceberg only. The columnReaders are
    // initialized in Iceberg, so no need to call the init()
    isInitialized = true;
    this.taskContext = TaskContext$.MODULE$.get();
    this.metrics = new HashMap<>();
  }

  BatchReader(
      Configuration conf,
      PartitionedFile inputSplit,
      ParquetMetadata footer,
      int capacity,
      StructType sparkSchema,
      boolean isCaseSensitive,
      boolean useFieldId,
      boolean ignoreMissingIds,
      boolean useLegacyDateTimestamp,
      StructType partitionSchema,
      InternalRow partitionValues,
      Map<String, SQLMetric> metrics) {
    this.conf = conf;
    this.capacity = capacity;
    this.sparkSchema = sparkSchema;
    this.isCaseSensitive = isCaseSensitive;
    this.useFieldId = useFieldId;
    this.ignoreMissingIds = ignoreMissingIds;
    this.useLegacyDateTimestamp = useLegacyDateTimestamp;
    this.partitionSchema = partitionSchema;
    this.partitionValues = partitionValues;
    this.file = inputSplit;
    this.footer = footer;
    this.metrics = metrics;
    this.taskContext = TaskContext$.MODULE$.get();
  }

  /**
   * Initialize this reader. The reason we don't do it in the constructor is that we want to close
   * any resource hold by this reader when error happens during the initialization.
   */
  public void init() throws URISyntaxException, IOException {
    useDecimal128 =
        conf.getBoolean(
            CometConf.COMET_USE_DECIMAL_128().key(),
            (Boolean) CometConf.COMET_USE_DECIMAL_128().defaultValue().get());
    useLazyMaterialization =
        conf.getBoolean(
            CometConf.COMET_USE_LAZY_MATERIALIZATION().key(),
            (Boolean) CometConf.COMET_USE_LAZY_MATERIALIZATION().defaultValue().get());

    long start = file.start();
    long length = file.length();
    String filePath = file.filePath().toString();

    ParquetReadOptions.Builder builder = HadoopReadOptions.builder(conf, new Path(filePath));

    if (start >= 0 && length >= 0) {
      builder = builder.withRange(start, start + length);
    }
    ParquetReadOptions readOptions = builder.build();

    // TODO: enable off-heap buffer when they are ready
    ReadOptions cometReadOptions = ReadOptions.builder(conf).build();

    Path path = new Path(new URI(filePath));
    fileReader =
        new FileReader(
            CometInputFile.fromPath(path, conf), footer, readOptions, cometReadOptions, metrics);
    requestedSchema = fileReader.getFileMetaData().getSchema();
    MessageType fileSchema = requestedSchema;

    if (sparkSchema == null) {
      sparkSchema = new ParquetToSparkSchemaConverter(conf).convert(requestedSchema);
    } else {
      requestedSchema =
          CometParquetReadSupport.clipParquetSchema(
              requestedSchema, sparkSchema, isCaseSensitive, useFieldId, ignoreMissingIds);
      if (requestedSchema.getColumns().size() != sparkSchema.size()) {
        throw new IllegalArgumentException(
            String.format(
                "Spark schema has %d columns while " + "Parquet schema has %d columns",
                sparkSchema.size(), requestedSchema.getColumns().size()));
      }
    }

    totalRowCount = fileReader.getRecordCount();
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    int numColumns = columns.size();
    if (partitionSchema != null) numColumns += partitionSchema.size();
    columnReaders = new AbstractColumnReader[numColumns];

    // Initialize missing columns and use null vectors for them
    missingColumns = new boolean[columns.size()];
    List<String[]> paths = requestedSchema.getPaths();
    StructField[] nonPartitionFields = sparkSchema.fields();
    ShimFileFormat.findRowIndexColumnIndexInSchema(sparkSchema);
    for (int i = 0; i < requestedSchema.getFieldCount(); i++) {
      Type t = requestedSchema.getFields().get(i);
      Preconditions.checkState(
          t.isPrimitive() && !t.isRepetition(Type.Repetition.REPEATED),
          "Complex type is not supported");
      String[] colPath = paths.get(i);
      if (nonPartitionFields[i].name().equals(ShimFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME())) {
        // Values of ROW_INDEX_TEMPORARY_COLUMN_NAME column are always populated with
        // generated row indexes, rather than read from the file.
        // TODO(SPARK-40059): Allow users to include columns named
        //                    FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME in their schemas.
        long[] rowIndices = fileReader.getRowIndices();
        columnReaders[i] = new RowIndexColumnReader(nonPartitionFields[i], capacity, rowIndices);
        missingColumns[i] = true;
      } else if (fileSchema.containsPath(colPath)) {
        ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
        if (!fd.equals(columns.get(i))) {
          throw new UnsupportedOperationException("Schema evolution is not supported");
        }
        missingColumns[i] = false;
      } else {
        if (columns.get(i).getMaxDefinitionLevel() == 0) {
          throw new IOException(
              "Required column '"
                  + Arrays.toString(colPath)
                  + "' is missing"
                  + " in data file "
                  + filePath);
        }
        ConstantColumnReader reader =
            new ConstantColumnReader(nonPartitionFields[i], capacity, useDecimal128);
        columnReaders[i] = reader;
        missingColumns[i] = true;
      }
    }

    // Initialize constant readers for partition columns
    if (partitionSchema != null) {
      StructField[] partitionFields = partitionSchema.fields();
      for (int i = columns.size(); i < columnReaders.length; i++) {
        int fieldIndex = i - columns.size();
        StructField field = partitionFields[fieldIndex];
        ConstantColumnReader reader =
            new ConstantColumnReader(field, capacity, partitionValues, fieldIndex, useDecimal128);
        columnReaders[i] = reader;
      }
    }

    vectors = new CometVector[numColumns];
    currentBatch = new ColumnarBatch(vectors);
    fileReader.setRequestedSchema(requestedSchema.getColumns());

    // For test purpose only
    // If the last external accumulator is `NumRowGroupsAccumulator`, the row group number to read
    // will be updated to the accumulator. So we can check if the row groups are filtered or not
    // in test case.
    // Note that this tries to get thread local TaskContext object, if this is called at other
    // thread, it won't update the accumulator.
    if (taskContext != null) {
      Option<AccumulatorV2<?, ?>> accu = getTaskAccumulator(taskContext.taskMetrics());
      if (accu.isDefined() && accu.get().getClass().getSimpleName().equals("NumRowGroupsAcc")) {
        @SuppressWarnings("unchecked")
        AccumulatorV2<Integer, Integer> intAccum = (AccumulatorV2<Integer, Integer>) accu.get();
        intAccum.add(fileReader.getRowGroups().size());
      }
    }

    // Pre-fetching
    boolean preFetchEnabled =
        conf.getBoolean(
            CometConf.COMET_SCAN_PREFETCH_ENABLED().key(),
            (boolean) CometConf.COMET_SCAN_PREFETCH_ENABLED().defaultValue().get());

    if (preFetchEnabled) {
      LOG.info("Prefetch enabled for BatchReader.");
      this.prefetchQueue = new LinkedBlockingQueue<>();
    }

    isInitialized = true;
    synchronized (this) {
      // if prefetch is enabled, `init()` is called in separate thread. When
      // `BatchReader.nextBatch()` is called asynchronously, it is possibly that
      // `init()` is not called or finished. We need to hold on `nextBatch` until
      // initialization of `BatchReader` is done. Once we are close to finish
      // initialization, we notify the waiting thread of `nextBatch` to continue.
      notifyAll();
    }
  }

  public void setSparkSchema(StructType schema) {
    this.sparkSchema = schema;
  }

  public AbstractColumnReader[] getColumnReaders() {
    return columnReaders;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    // Do nothing. The initialization work is done in 'init' already.
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    return nextBatch();
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public ColumnarBatch getCurrentValue() {
    return currentBatch();
  }

  @Override
  public float getProgress() {
    return (float) rowsRead / totalRowCount;
  }

  /**
   * Returns the current columnar batch being read.
   *
   * <p>Note that this must be called AFTER {@link BatchReader#nextBatch()}.
   */
  public ColumnarBatch currentBatch() {
    return currentBatch;
  }

  // Only for testing
  public Future<Option<Throwable>> getPrefetchTask() {
    return this.prefetchTask;
  }

  // Only for testing
  public LinkedBlockingQueue<Pair<PageReadStore, Long>> getPrefetchQueue() {
    return this.prefetchQueue;
  }

  /**
   * Loads the next batch of rows.
   *
   * @return true if there are no more rows to read, false otherwise.
   */
  public boolean nextBatch() throws IOException {
    if (this.prefetchTask == null) {
      Preconditions.checkState(isInitialized, "init() should be called first!");
    } else {
      // If prefetch is enabled, this reader will be initialized asynchronously from a
      // different thread. Wait until it is initialized
      while (!isInitialized) {
        synchronized (this) {
          try {
            // Wait until initialization of current `BatchReader` is finished (i.e., `init()`),
            // is done. It is possibly that `init()` is done after entering this while loop,
            // so a short timeout is given.
            wait(100);

            // Checks if prefetch task is finished. If so, tries to get exception if any.
            if (prefetchTask.isDone()) {
              Option<Throwable> exception = prefetchTask.get();
              if (exception.isDefined()) {
                throw exception.get();
              }
            }
          } catch (RuntimeException e) {
            // Spark will check certain exception e.g. `SchemaColumnConvertNotSupportedException`.
            throw e;
          } catch (Throwable e) {
            throw new IOException(e);
          }
        }
      }
    }

    if (rowsRead >= totalRowCount) return false;
    boolean hasMore;

    try {
      hasMore = loadNextRowGroupIfNecessary();
    } catch (RuntimeException e) {
      // Spark will check certain exception e.g. `SchemaColumnConvertNotSupportedException`.
      throw e;
    } catch (Throwable e) {
      throw new IOException(e);
    }

    if (!hasMore) return false;
    int batchSize = (int) Math.min(capacity, totalRowsLoaded - rowsRead);

    return nextBatch(batchSize);
  }

  public boolean nextBatch(int batchSize) {
    long totalDecodeTime = 0, totalLoadTime = 0;
    for (int i = 0; i < columnReaders.length; i++) {
      AbstractColumnReader reader = columnReaders[i];
      long startNs = System.nanoTime();
      reader.readBatch(batchSize);
      totalDecodeTime += System.nanoTime() - startNs;
      startNs = System.nanoTime();
      vectors[i] = reader.currentBatch();
      totalLoadTime += System.nanoTime() - startNs;
    }

    SQLMetric decodeMetric = metrics.get("ParquetNativeDecodeTime");
    if (decodeMetric != null) {
      decodeMetric.add(totalDecodeTime);
    }
    SQLMetric loadMetric = metrics.get("ParquetNativeLoadTime");
    if (loadMetric != null) {
      loadMetric.add(totalLoadTime);
    }

    currentBatch.setNumRows(batchSize);
    rowsRead += batchSize;
    return true;
  }

  @Override
  public void close() throws IOException {
    if (columnReaders != null) {
      for (AbstractColumnReader reader : columnReaders) {
        if (reader != null) {
          reader.close();
        }
      }
    }
    if (fileReader != null) {
      fileReader.close();
      fileReader = null;
    }
    if (importer != null) {
      importer.close();
      importer = null;
    }
  }

  @SuppressWarnings("deprecation")
  private boolean loadNextRowGroupIfNecessary() throws Throwable {
    // More rows can be read from loaded row group. No need to load next one.
    if (rowsRead != totalRowsLoaded) return true;

    SQLMetric rowGroupTimeMetric = metrics.get("ParquetLoadRowGroupTime");
    SQLMetric numRowGroupsMetric = metrics.get("ParquetRowGroups");
    long startNs = System.nanoTime();

    PageReadStore rowGroupReader = null;
    if (prefetchTask != null && prefetchQueue != null) {
      // Wait for pre-fetch task to finish.
      Pair<PageReadStore, Long> rowGroupReaderPair = prefetchQueue.take();
      rowGroupReader = rowGroupReaderPair.getLeft();

      // Update incremental byte read metric. Because this metric in Spark is maintained
      // by thread local variable, we need to manually update it.
      // TODO: We may expose metrics from `FileReader` and get from it directly.
      long incBytesRead = rowGroupReaderPair.getRight();
      FileSystem.getAllStatistics().stream()
          .forEach(statistic -> statistic.incrementBytesRead(incBytesRead));
    } else {
      rowGroupReader = fileReader.readNextRowGroup();
    }

    if (rowGroupTimeMetric != null) {
      rowGroupTimeMetric.add(System.nanoTime() - startNs);
    }
    if (rowGroupReader == null) {
      return false;
    }
    if (numRowGroupsMetric != null) {
      numRowGroupsMetric.add(1);
    }

    if (importer != null) importer.close();
    importer = new CometSchemaImporter(ALLOCATOR);

    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      if (missingColumns[i]) continue;
      if (columnReaders[i] != null) columnReaders[i].close();
      // TODO: handle tz, datetime & int96 rebase
      // TODO: consider passing page reader via ctor - however we need to fix the shading issue
      //   from Iceberg side.
      DataType dataType = sparkSchema.fields()[i].dataType();
      ColumnReader reader =
          Utils.getColumnReader(
              dataType,
              columns.get(i),
              importer,
              capacity,
              useDecimal128,
              useLazyMaterialization,
              useLegacyDateTimestamp);
      reader.setPageReader(rowGroupReader.getPageReader(columns.get(i)));
      columnReaders[i] = reader;
    }
    totalRowsLoaded += rowGroupReader.getRowCount();
    return true;
  }

  // Submits a prefetch task for this reader.
  public void submitPrefetchTask(ExecutorService threadPool) {
    this.prefetchTask = threadPool.submit(new PrefetchTask());
  }

  // A task for prefetching parquet row groups.
  private class PrefetchTask implements Callable<Option<Throwable>> {
    private long getBytesRead() {
      return FileSystem.getAllStatistics().stream()
          .mapToLong(s -> s.getThreadStatistics().getBytesRead())
          .sum();
    }

    @Override
    public Option<Throwable> call() throws Exception {
      // Gets the bytes read so far.
      long baseline = getBytesRead();

      try {
        init();

        while (true) {
          PageReadStore rowGroupReader = fileReader.readNextRowGroup();

          if (rowGroupReader == null) {
            // Reaches the end of row groups.
            return Option.empty();
          } else {
            long incBytesRead = getBytesRead() - baseline;

            prefetchQueue.add(Pair.of(rowGroupReader, incBytesRead));
          }
        }
      } catch (Throwable e) {
        // Returns exception thrown from the reader. The reader will re-throw it.
        return Option.apply(e);
      } finally {
        if (fileReader != null) {
          fileReader.closeStream();
        }
      }
    }
  }

  // Signature of externalAccums changed from returning a Buffer to returning a Seq. If comet is
  // expecting a Buffer but the Spark version returns a Seq or vice versa, we get a
  // method not found exception.
  @SuppressWarnings("unchecked")
  private Option<AccumulatorV2<?, ?>> getTaskAccumulator(TaskMetrics taskMetrics) {
    Method externalAccumsMethod;
    try {
      externalAccumsMethod = TaskMetrics.class.getDeclaredMethod("externalAccums");
      externalAccumsMethod.setAccessible(true);
      String returnType = externalAccumsMethod.getReturnType().getName();
      if (returnType.equals("scala.collection.mutable.Buffer")) {
        return ((Buffer<AccumulatorV2<?, ?>>) externalAccumsMethod.invoke(taskMetrics))
            .lastOption();
      } else if (returnType.equals("scala.collection.Seq")) {
        return ((Seq<AccumulatorV2<?, ?>>) externalAccumsMethod.invoke(taskMetrics)).lastOption();
      } else {
        return Option.apply(null); // None
      }
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      return Option.apply(null); // None
    }
  }
}
