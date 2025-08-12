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
import java.nio.channels.Channels;
import java.util.*;
import java.util.stream.Collectors;

import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.comet.parquet.CometParquetReadSupport;
import org.apache.spark.sql.comet.util.Utils$;
import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.parquet.ParquetColumn;
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.AccumulatorV2;

import org.apache.comet.CometConf;
import org.apache.comet.CometSchemaImporter;
import org.apache.comet.objectstore.NativeConfig;
import org.apache.comet.shims.ShimBatchReader;
import org.apache.comet.shims.ShimFileFormat;
import org.apache.comet.vector.CometVector;
import org.apache.comet.vector.NativeUtil;

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
  MessageType fileSchema;
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

  private ParquetColumn parquetColumn;

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

  private NativeBatchReader(AbstractColumnReader[] columnReaders) {
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
  public void init() throws Throwable {

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

    Map<String, String> objectStoreOptions =
        JavaConverters.mapAsJavaMap(NativeConfig.extractObjectStoreOptions(conf, file.pathUri()));

    // TODO: enable off-heap buffer when they are ready
    ReadOptions cometReadOptions = ReadOptions.builder(conf).build();

    Path path = new Path(new URI(filePath));
    try (FileReader fileReader =
        new FileReader(
            CometInputFile.fromPath(path, conf), footer, readOptions, cometReadOptions, metrics)) {

      requestedSchema = footer.getFileMetaData().getSchema();
      fileSchema = requestedSchema;

      if (sparkSchema == null) {
        ParquetToSparkSchemaConverter converter = new ParquetToSparkSchemaConverter(conf);
        sparkSchema = converter.convert(requestedSchema);
      } else {
        requestedSchema =
            CometParquetReadSupport.clipParquetSchema(
                requestedSchema, sparkSchema, isCaseSensitive, useFieldId, ignoreMissingIds);
        if (requestedSchema.getFieldCount() != sparkSchema.size()) {
          throw new IllegalArgumentException(
              String.format(
                  "Spark schema has %d columns while " + "Parquet schema has %d columns",
                  sparkSchema.size(), requestedSchema.getFieldCount()));
        }
      }

      boolean caseSensitive =
          conf.getBoolean(
              SQLConf.CASE_SENSITIVE().key(),
              (boolean) SQLConf.CASE_SENSITIVE().defaultValue().get());
      // rename spark fields based on field_id so name of spark schema field matches the parquet
      // field name
      if (useFieldId && ParquetUtils.hasFieldIds(sparkSchema)) {
        sparkSchema =
            getSparkSchemaByFieldId(sparkSchema, requestedSchema.asGroupType(), caseSensitive);
      }
      this.parquetColumn = getParquetColumn(requestedSchema, this.sparkSchema);

      String timeZoneId = conf.get("spark.sql.session.timeZone");
      // Native code uses "UTC" always as the timeZoneId when converting from spark to arrow schema.
      Schema arrowSchema = Utils$.MODULE$.toArrowSchema(sparkSchema, "UTC");
      byte[] serializedRequestedArrowSchema = serializeArrowSchema(arrowSchema);
      Schema dataArrowSchema = Utils$.MODULE$.toArrowSchema(dataSchema, "UTC");
      byte[] serializedDataArrowSchema = serializeArrowSchema(dataArrowSchema);

      // Create Column readers
      List<Type> fields = requestedSchema.getFields();
      List<Type> fileFields = fileSchema.getFields();
      ParquetColumn[] parquetFields =
          JavaConverters.seqAsJavaList(parquetColumn.children()).toArray(new ParquetColumn[0]);
      int numColumns = fields.size();
      if (partitionSchema != null) numColumns += partitionSchema.size();
      columnReaders = new AbstractColumnReader[numColumns];

      // Initialize missing columns and use null vectors for them
      missingColumns = new boolean[numColumns];
      // We do not need the column index of the row index; but this method has the
      // side effect of throwing an exception if a column with the same name is
      // found which we do want (spark unit tests explicitly test for that).
      ShimFileFormat.findRowIndexColumnIndexInSchema(sparkSchema);
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
          // This makes the same check as Spark's VectorizedParquetReader
          checkColumn(parquetFields[i]);
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
          if (field.isPrimitive()) {
            ConstantColumnReader reader =
                new ConstantColumnReader(nonPartitionFields[i], capacity, useDecimal128);
            columnReaders[i] = reader;
            missingColumns[i] = true;
          } else {
            // the column requested is not in the file, but the native reader can handle that
            // and will return nulls for all rows requested
            missingColumns[i] = false;
          }
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
          intAccum.add(blocks.size());
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
              batchSize,
              caseSensitive,
              objectStoreOptions);
    }
    isInitialized = true;
  }

  private ParquetColumn getParquetColumn(MessageType schema, StructType sparkSchema) {
    // We use a different config from the config that is passed in.
    // This follows the setting  used in Spark's SpecificParquetRecordReaderBase
    Configuration config = new Configuration();
    config.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING().key(), false);
    config.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), false);
    config.setBoolean(SQLConf.CASE_SENSITIVE().key(), false);
    config.setBoolean(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED().key(), false);
    config.setBoolean(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG().key(), false);
    ParquetToSparkSchemaConverter converter = new ParquetToSparkSchemaConverter(config);
    return converter.convertParquetColumn(schema, Option.apply(sparkSchema));
  }

  private Map<Integer, List<Type>> getIdToParquetFieldMap(GroupType type) {
    return type.getFields().stream()
        .filter(f -> f.getId() != null)
        .collect(Collectors.groupingBy(f -> f.getId().intValue()));
  }

  private Map<String, List<Type>> getCaseSensitiveParquetFieldMap(GroupType schema) {
    return schema.getFields().stream().collect(Collectors.toMap(Type::getName, Arrays::asList));
  }

  private Map<String, List<Type>> getCaseInsensitiveParquetFieldMap(GroupType schema) {
    return schema.getFields().stream()
        .collect(Collectors.groupingBy(f -> f.getName().toLowerCase(Locale.ROOT)));
  }

  private Type getMatchingParquetFieldById(
      StructField f,
      Map<Integer, List<Type>> idToParquetFieldMap,
      Map<String, List<Type>> nameToParquetFieldMap,
      boolean isCaseSensitive) {
    List<Type> matched = null;
    int fieldId = 0;
    if (ParquetUtils.hasFieldId(f)) {
      fieldId = ParquetUtils.getFieldId(f);
      matched = idToParquetFieldMap.get(fieldId);
    } else {
      String fieldName = isCaseSensitive ? f.name() : f.name().toLowerCase(Locale.ROOT);
      matched = nameToParquetFieldMap.get(fieldName);
    }

    if (matched == null || matched.isEmpty()) {
      return null;
    }
    if (matched.size() > 1) {
      // Need to fail if there is ambiguity, i.e. more than one field is matched
      String parquetTypesString =
          matched.stream().map(Type::getName).collect(Collectors.joining("[", ", ", "]"));
      throw QueryExecutionErrors.foundDuplicateFieldInFieldIdLookupModeError(
          fieldId, parquetTypesString);
    } else {
      return matched.get(0);
    }
  }

  // Derived from CometParquetReadSupport.matchFieldId
  private String getMatchingNameById(
      StructField f,
      Map<Integer, List<Type>> idToParquetFieldMap,
      Map<String, List<Type>> nameToParquetFieldMap,
      boolean isCaseSensitive) {
    Type matched =
        getMatchingParquetFieldById(f, idToParquetFieldMap, nameToParquetFieldMap, isCaseSensitive);

    // When there is no ID match, we use a fake name to avoid a name match by accident
    // We need this name to be unique as well, otherwise there will be type conflicts
    if (matched == null) {
      return CometParquetReadSupport.generateFakeColumnName();
    } else {
      return matched.getName();
    }
  }

  // clip ParquetGroup Type
  private StructType getSparkSchemaByFieldId(
      StructType schema, GroupType parquetSchema, boolean caseSensitive) {
    StructType newSchema = new StructType();
    Map<Integer, List<Type>> idToParquetFieldMap = getIdToParquetFieldMap(parquetSchema);
    Map<String, List<Type>> nameToParquetFieldMap =
        caseSensitive
            ? getCaseSensitiveParquetFieldMap(parquetSchema)
            : getCaseInsensitiveParquetFieldMap(parquetSchema);
    for (StructField f : schema.fields()) {
      DataType newDataType;
      String fieldName = isCaseSensitive ? f.name() : f.name().toLowerCase(Locale.ROOT);
      List<Type> parquetFieldList = nameToParquetFieldMap.get(fieldName);
      if (parquetFieldList == null) {
        newDataType = f.dataType();
      } else {
        Type fieldType = parquetFieldList.get(0);
        if (f.dataType() instanceof StructType) {
          newDataType =
              getSparkSchemaByFieldId(
                  (StructType) f.dataType(), fieldType.asGroupType(), caseSensitive);
        } else {
          newDataType = getSparkTypeByFieldId(f.dataType(), fieldType, caseSensitive);
        }
      }
      String matchedName =
          getMatchingNameById(f, idToParquetFieldMap, nameToParquetFieldMap, isCaseSensitive);
      StructField newField = f.copy(matchedName, newDataType, f.nullable(), f.metadata());
      newSchema = newSchema.add(newField);
    }
    return newSchema;
  }

  private static boolean isPrimitiveCatalystType(DataType dataType) {
    return !(dataType instanceof ArrayType)
        && !(dataType instanceof MapType)
        && !(dataType instanceof StructType);
  }

  private DataType getSparkTypeByFieldId(
      DataType dataType, Type parquetType, boolean caseSensitive) {
    DataType newDataType;
    if (dataType instanceof StructType) {
      newDataType =
          getSparkSchemaByFieldId((StructType) dataType, parquetType.asGroupType(), caseSensitive);
    } else if (dataType instanceof ArrayType
        && !isPrimitiveCatalystType(((ArrayType) dataType).elementType())) {

      newDataType =
          getSparkArrayTypeByFieldId(
              (ArrayType) dataType, parquetType.asGroupType(), caseSensitive);
    } else if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      DataType keyType = mapType.keyType();
      DataType valueType = mapType.valueType();
      DataType newKeyType;
      DataType newValueType;
      Type parquetMapType = parquetType.asGroupType().getFields().get(0);
      Type parquetKeyType = parquetMapType.asGroupType().getType("key");
      Type parquetValueType = parquetMapType.asGroupType().getType("value");
      if (keyType instanceof StructType) {
        newKeyType =
            getSparkSchemaByFieldId(
                (StructType) keyType, parquetKeyType.asGroupType(), caseSensitive);
      } else {
        newKeyType = keyType;
      }
      if (valueType instanceof StructType) {
        newValueType =
            getSparkSchemaByFieldId(
                (StructType) valueType, parquetValueType.asGroupType(), caseSensitive);
      } else {
        newValueType = valueType;
      }
      newDataType = new MapType(newKeyType, newValueType, mapType.valueContainsNull());
    } else {
      newDataType = dataType;
    }
    return newDataType;
  }

  private DataType getSparkArrayTypeByFieldId(
      ArrayType arrayType, GroupType parquetList, boolean caseSensitive) {
    DataType newDataType;
    DataType elementType = arrayType.elementType();
    DataType newElementType;
    Type parquetElementType;
    if (parquetList.getLogicalTypeAnnotation() == null
        && parquetList.isRepetition(Type.Repetition.REPEATED)) {
      parquetElementType = parquetList;
    } else {
      // we expect only non-primitive types here (see clipParquetListTypes for related logic)
      GroupType repeatedGroup = parquetList.asGroupType().getType(0).asGroupType();
      if (repeatedGroup.getFieldCount() > 1
          || Objects.equals(repeatedGroup.getName(), "array")
          || Objects.equals(repeatedGroup.getName(), parquetList.getName() + "_tuple")) {
        parquetElementType = repeatedGroup;
      } else {
        parquetElementType = repeatedGroup.getType(0);
      }
    }
    if (elementType instanceof StructType) {
      newElementType =
          getSparkSchemaByFieldId(
              (StructType) elementType, parquetElementType.asGroupType(), caseSensitive);
    } else {
      newElementType = getSparkTypeByFieldId(elementType, parquetElementType, caseSensitive);
    }
    newDataType = new ArrayType(newElementType, arrayType.containsNull());
    return newDataType;
  }

  private void checkParquetType(ParquetColumn column) throws IOException {
    String[] path = JavaConverters.seqAsJavaList(column.path()).toArray(new String[0]);
    if (containsPath(fileSchema, path)) {
      if (column.isPrimitive()) {
        ColumnDescriptor desc = column.descriptor().get();
        ColumnDescriptor fd = fileSchema.getColumnDescription(desc.getPath());
        TypeUtil.checkParquetType(fd, column.sparkType());
      } else {
        for (ParquetColumn childColumn : JavaConverters.seqAsJavaList(column.children())) {
          checkColumn(childColumn);
        }
      }
    } else { // A missing column which is either primitive or complex
      if (column.required()) {
        // Column is missing in data but the required data is non-nullable. This file is invalid.
        throw new IOException(
            "Required column is missing in data file. Col: " + Arrays.toString(path));
      }
    }
  }

  /**
   * Checks whether the given 'path' exists in 'parquetType'. The difference between this and {@link
   * MessageType#containsPath(String[])} is that the latter only support paths to leaf From Spark:
   * VectorizedParquetRecordReader Check whether a column from requested schema is missing from the
   * file schema, or whether it conforms to the type of the file schema.
   */
  private void checkColumn(ParquetColumn column) throws IOException {
    String[] path = JavaConverters.seqAsJavaList(column.path()).toArray(new String[0]);
    if (containsPath(fileSchema, path)) {
      if (column.isPrimitive()) {
        ColumnDescriptor desc = column.descriptor().get();
        ColumnDescriptor fd = fileSchema.getColumnDescription(desc.getPath());
        if (!fd.equals(desc)) {
          throw new UnsupportedOperationException("Schema evolution not supported.");
        }
      } else {
        for (ParquetColumn childColumn : JavaConverters.seqAsJavaList(column.children())) {
          checkColumn(childColumn);
        }
      }
    } else { // A missing column which is either primitive or complex
      if (column.required()) {
        // Column is missing in data but the required data is non-nullable. This file is invalid.
        throw new IOException(
            "Required column is missing in data file. Col: " + Arrays.toString(path));
      }
    }
  }

  /**
   * Checks whether the given 'path' exists in 'parquetType'. The difference between this and {@link
   * MessageType#containsPath(String[])} is that the latter only support paths to leaf nodes, while
   * this support paths both to leaf and non-leaf nodes.
   */
  private boolean containsPath(Type parquetType, String[] path) {
    return containsPath(parquetType, path, 0);
  }

  private boolean containsPath(Type parquetType, String[] path, int depth) {
    if (path.length == depth) return true;
    if (parquetType instanceof GroupType) {
      String fieldName = path[depth];
      GroupType parquetGroupType = (GroupType) parquetType;
      if (parquetGroupType.containsField(fieldName)) {
        return containsPath(parquetGroupType.getType(fieldName), path, depth + 1);
      }
    }
    return false;
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

    for (ParquetColumn childColumn : JavaConverters.seqAsJavaList(parquetColumn.children())) {
      checkParquetType(childColumn);
    }

    int batchSize = Native.readNextRecordBatch(this.handle);
    if (batchSize == 0) {
      return batchSize;
    }
    if (importer != null) importer.close();
    importer = new CometSchemaImporter(ALLOCATOR);

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
