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
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.util.*;

import scala.Option;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.c.CometSchemaImporter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.comet.parquet.CometParquetReadSupport;
import org.apache.spark.sql.comet.util.Utils$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.AccumulatorV2;

import org.apache.comet.CometConf;
import org.apache.comet.shims.ShimBatchReader;
import org.apache.comet.shims.ShimFileFormat;
import org.apache.comet.vector.CometVector;
import org.apache.comet.vector.NativeUtil;

import static org.apache.comet.parquet.TypeUtil.isEqual;

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
public class NativeBatchReader extends RecordReader<Void, ColumnarBatch> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(NativeBatchReader.class);
  protected static final BufferAllocator ALLOCATOR = new RootAllocator();
  private NativeUtil nativeUtil = new NativeUtil();

  private Configuration conf;
  private int capacity;
  private boolean isCaseSensitive;
  private boolean useFieldId;
  private boolean ignoreMissingIds;
  private StructType partitionSchema;
  private InternalRow partitionValues;
  private PartitionedFile file;
  private final Map<String, SQLMetric> metrics;

  private StructType sparkSchema;
  private StructType dataSchema;
  private MessageType requestedSchema;
  private CometVector[] vectors;
  private AbstractColumnReader[] columnReaders;
  private CometSchemaImporter importer;
  private ColumnarBatch currentBatch;
  //  private FileReader fileReader;
  private boolean[] missingColumns;
  private boolean isInitialized;
  private ParquetMetadata footer;
  private byte[] nativeFilter;

  /**
   * Whether the native scan should always return decimal represented by 128 bits, regardless of its
   * precision. Normally, this should be true if native execution is enabled, since Arrow compute
   * kernels doesn't support 32 and 64 bit decimals yet.
   */
  // TODO: (ARROW NATIVE)
  private boolean useDecimal128;

  /**
   * Whether to return dates/timestamps that were written with legacy hybrid (Julian + Gregorian)
   * calendar as it is. If this is true, Comet will return them as it is, instead of rebasing them
   * to the new Proleptic Gregorian calendar. If this is false, Comet will throw exceptions when
   * seeing these dates/timestamps.
   */
  // TODO: (ARROW NATIVE)
  private boolean useLegacyDateTimestamp;

  /** The TaskContext object for executing this task. */
  private final TaskContext taskContext;

  private long totalRowCount = 0;
  private long handle;

  // Only for testing
  public NativeBatchReader(String file, int capacity) {
    this(file, capacity, null, null);
  }

  // Only for testing
  public NativeBatchReader(
      String file, int capacity, StructType partitionSchema, InternalRow partitionValues) {
    this(new Configuration(), file, capacity, partitionSchema, partitionValues);
  }

  // Only for testing
  public NativeBatchReader(
      Configuration conf,
      String file,
      int capacity,
      StructType partitionSchema,
      InternalRow partitionValues) {

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

  public NativeBatchReader(AbstractColumnReader[] columnReaders) {
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

  NativeBatchReader(
      Configuration conf,
      PartitionedFile inputSplit,
      ParquetMetadata footer,
      byte[] nativeFilter,
      int capacity,
      StructType sparkSchema,
      StructType dataSchema,
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
    this.dataSchema = dataSchema;
    this.isCaseSensitive = isCaseSensitive;
    this.useFieldId = useFieldId;
    this.ignoreMissingIds = ignoreMissingIds;
    this.useLegacyDateTimestamp = useLegacyDateTimestamp;
    this.partitionSchema = partitionSchema;
    this.partitionValues = partitionValues;
    this.file = inputSplit;
    this.footer = footer;
    this.nativeFilter = nativeFilter;
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

    long start = file.start();
    long length = file.length();
    String filePath = file.filePath().toString();
    long fileSize = file.fileSize();

    ParquetReadOptions.Builder builder = HadoopReadOptions.builder(conf, new Path(filePath));

    if (start >= 0 && length >= 0) {
      builder = builder.withRange(start, start + length);
    }
    ParquetReadOptions readOptions = builder.build();

    // TODO: enable off-heap buffer when they are ready
    ReadOptions cometReadOptions = ReadOptions.builder(conf).build();

    Path path = new Path(new URI(filePath));
    try (FileReader fileReader =
        new FileReader(
            CometInputFile.fromPath(path, conf), footer, readOptions, cometReadOptions, metrics)) {

      requestedSchema = footer.getFileMetaData().getSchema();
      MessageType fileSchema = requestedSchema;

      if (sparkSchema == null) {
        sparkSchema = new ParquetToSparkSchemaConverter(conf).convert(requestedSchema);
      } else {
        requestedSchema =
            CometParquetReadSupport.clipParquetSchema(
                requestedSchema, sparkSchema, isCaseSensitive, useFieldId, ignoreMissingIds);
        if (requestedSchema.getFieldCount() != sparkSchema.size()) {
          throw new IllegalArgumentException(
              String.format(
                  "Spark schema has %d columns while " + "Parquet schema has %d columns",
                  sparkSchema.size(), requestedSchema.getColumns().size()));
        }
      }

      String timeZoneId = conf.get("spark.sql.session.timeZone");
      // Native code uses "UTC" always as the timeZoneId when converting from spark to arrow schema.
      Schema arrowSchema = Utils$.MODULE$.toArrowSchema(sparkSchema, "UTC");
      byte[] serializedRequestedArrowSchema = serializeArrowSchema(arrowSchema);
      Schema dataArrowSchema = Utils$.MODULE$.toArrowSchema(dataSchema, "UTC");
      byte[] serializedDataArrowSchema = serializeArrowSchema(dataArrowSchema);

      // Create Column readers
      List<Type> fields = requestedSchema.getFields();
      List<Type> fileFields = fileSchema.getFields();
      int numColumns = fields.size();
      if (partitionSchema != null) numColumns += partitionSchema.size();
      columnReaders = new AbstractColumnReader[numColumns];

      // Initialize missing columns and use null vectors for them
      missingColumns = new boolean[numColumns];
      StructField[] nonPartitionFields = sparkSchema.fields();
      boolean hasRowIndexColumn = false;
      // Ranges of rows to read (needed iff row indexes are being read)
      List<BlockMetaData> blocks =
          FileReader.filterRowGroups(readOptions, footer.getBlocks(), fileReader);
      totalRowCount = fileReader.getFilteredRecordCount();
      if (totalRowCount == 0) {
        // all the data is filtered out.
        isInitialized = true;
        return;
      }
      long[] starts = new long[blocks.size()];
      long[] lengths = new long[blocks.size()];
      starts = new long[blocks.size()];
      lengths = new long[blocks.size()];
      int blockIndex = 0;
      for (BlockMetaData block : blocks) {
        long blockStart = block.getStartingPos();
        long blockLength = block.getCompressedSize();
        starts[blockIndex] = blockStart;
        lengths[blockIndex] = blockLength;
        blockIndex++;
      }
      for (int i = 0; i < fields.size(); i++) {
        Type field = fields.get(i);
        Optional<Type> optFileField =
            fileFields.stream().filter(f -> f.getName().equals(field.getName())).findFirst();
        if (nonPartitionFields[i].name().equals(ShimFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME())) {
          // Values of ROW_INDEX_TEMPORARY_COLUMN_NAME column are always populated with
          // generated row indexes, rather than read from the file.
          // TODO(SPARK-40059): Allow users to include columns named
          //                    FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME in their schemas.
          long[] rowIndices = FileReader.getRowIndices(blocks);
          columnReaders[i] = new RowIndexColumnReader(nonPartitionFields[i], capacity, rowIndices);
          hasRowIndexColumn = true;
          missingColumns[i] = true;
        } else if (optFileField.isPresent()) {
          // The column we are reading may be a complex type in which case we check if each field in
          // the requested type is in the file type (and the same data type)
          if (!isEqual(field, optFileField.get())) {
            throw new UnsupportedOperationException("Schema evolution is not supported");
          }
          missingColumns[i] = false;
        } else {
          if (field.getRepetition() == Type.Repetition.REQUIRED) {
            throw new IOException(
                "Required column '"
                    + field.getName()
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
        for (int i = fields.size(); i < columnReaders.length; i++) {
          int fieldIndex = i - fields.size();
          StructField field = partitionFields[fieldIndex];
          ConstantColumnReader reader =
              new ConstantColumnReader(field, capacity, partitionValues, fieldIndex, useDecimal128);
          columnReaders[i] = reader;
        }
      }

      vectors = new CometVector[numColumns];
      currentBatch = new ColumnarBatch(vectors);

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
          // TODO: Get num_row_groups from native
          // intAccum.add(fileReader.getRowGroups().size());
        }
      }

      int batchSize =
          conf.getInt(
              CometConf.COMET_BATCH_SIZE().key(),
              (Integer) CometConf.COMET_BATCH_SIZE().defaultValue().get());
      this.handle =
          Native.initRecordBatchReader(
              filePath,
              fileSize,
              starts,
              lengths,
              hasRowIndexColumn ? null : nativeFilter,
              serializedRequestedArrowSchema,
              serializedDataArrowSchema,
              timeZoneId,
              batchSize);
    }
    isInitialized = true;
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
    return 0;
  }

  /**
   * Returns the current columnar batch being read.
   *
   * <p>Note that this must be called AFTER {@link NativeBatchReader#nextBatch()}.
   */
  public ColumnarBatch currentBatch() {
    return currentBatch;
  }

  /**
   * Loads the next batch of rows. This is called by Spark _and_ Iceberg
   *
   * @return true if there are no more rows to read, false otherwise.
   */
  public boolean nextBatch() throws IOException {
    Preconditions.checkState(isInitialized, "init() should be called first!");

    //    if (rowsRead >= totalRowCount) return false;

    if (totalRowCount == 0) return false;

    int batchSize;

    try {
      batchSize = loadNextBatch();
    } catch (RuntimeException e) {
      // Spark will check certain exception e.g. `SchemaColumnConvertNotSupportedException`.
      throw e;
    } catch (Throwable e) {
      throw new IOException(e);
    }

    if (batchSize == 0) return false;

    long totalDecodeTime = 0, totalLoadTime = 0;
    for (int i = 0; i < columnReaders.length; i++) {
      AbstractColumnReader reader = columnReaders[i];
      long startNs = System.nanoTime();
      // TODO: read from native reader
      reader.readBatch(batchSize);
      //      totalDecodeTime += System.nanoTime() - startNs;
      //      startNs = System.nanoTime();
      vectors[i] = reader.currentBatch();
      totalLoadTime += System.nanoTime() - startNs;
    }

    // TODO: (ARROW NATIVE) Add Metrics
    //    SQLMetric decodeMetric = metrics.get("ParquetNativeDecodeTime");
    //    if (decodeMetric != null) {
    //      decodeMetric.add(totalDecodeTime);
    //    }
    SQLMetric loadMetric = metrics.get("ParquetNativeLoadTime");
    if (loadMetric != null) {
      loadMetric.add(totalLoadTime);
    }

    currentBatch.setNumRows(batchSize);
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
    if (importer != null) {
      importer.close();
      importer = null;
    }
    nativeUtil.close();
    if (this.handle > 0) {
      Native.closeRecordBatchReader(this.handle);
      this.handle = 0;
    }
  }

  @SuppressWarnings("deprecation")
  private int loadNextBatch() throws Throwable {
    long startNs = System.nanoTime();

    int batchSize = Native.readNextRecordBatch(this.handle);
    if (batchSize == 0) {
      return batchSize;
    }
    if (importer != null) importer.close();
    importer = new CometSchemaImporter(ALLOCATOR);

    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    List<Type> fields = requestedSchema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      if (!missingColumns[i]) {
        if (columnReaders[i] != null) columnReaders[i].close();
        // TODO: (ARROW NATIVE) handle tz, datetime & int96 rebase
        DataType dataType = sparkSchema.fields()[i].dataType();
        Type field = fields.get(i);
        NativeColumnReader reader =
            new NativeColumnReader(
                this.handle,
                i,
                dataType,
                field,
                null,
                importer,
                nativeUtil,
                capacity,
                useDecimal128,
                useLegacyDateTimestamp);
        columnReaders[i] = reader;
      }
    }
    return batchSize;
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

  private byte[] serializeArrowSchema(Schema schema) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    WriteChannel writeChannel = new WriteChannel(Channels.newChannel(out));
    MessageSerializer.serialize(writeChannel, schema);
    return out.toByteArray();
  }
}
