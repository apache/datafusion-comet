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

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.spark.sql.types.*;

import org.apache.comet.CometSchemaImporter;

public class Utils {

  /** This method is called from Apache Iceberg. */
  public static ColumnReader getColumnReader(
      DataType type,
      ParquetColumnSpec columnSpec,
      CometSchemaImporter importer,
      int batchSize,
      boolean useDecimal128,
      boolean useLazyMaterialization) {

    ColumnDescriptor descriptor = buildColumnDescriptor(columnSpec);
    return getColumnReader(
        type, descriptor, importer, batchSize, useDecimal128, useLazyMaterialization, true);
  }

  public static ColumnReader getColumnReader(
      DataType type,
      ColumnDescriptor descriptor,
      CometSchemaImporter importer,
      int batchSize,
      boolean useDecimal128,
      boolean useLazyMaterialization) {
    // TODO: support `useLegacyDateTimestamp` for Iceberg
    return getColumnReader(
        type, descriptor, importer, batchSize, useDecimal128, useLazyMaterialization, true);
  }

  public static ColumnReader getColumnReader(
      DataType type,
      ColumnDescriptor descriptor,
      CometSchemaImporter importer,
      int batchSize,
      boolean useDecimal128,
      boolean useLazyMaterialization,
      boolean useLegacyDateTimestamp) {
    if (useLazyMaterialization && supportLazyMaterialization(type)) {
      return new LazyColumnReader(
          type, descriptor, importer, batchSize, useDecimal128, useLegacyDateTimestamp);
    } else {
      return new ColumnReader(
          type, descriptor, importer, batchSize, useDecimal128, useLegacyDateTimestamp);
    }
  }

  private static boolean supportLazyMaterialization(DataType type) {
    return (type instanceof StringType || type instanceof BinaryType);
  }

  /**
   * Initialize the Comet native Parquet reader.
   *
   * @param descriptor the Parquet column descriptor for the column to be read
   * @param readType the Spark read type used for type promotion. Null if promotion is not enabled.
   * @param batchSize the batch size, i.e., maximum number of elements per record batch
   * @param useDecimal128 whether to always represent decimals using 128 bits. If false, the native
   *     reader may represent decimals using 32 or 64 bits, depending on the precision.
   * @param useLegacyDateTimestampOrNTZ whether to read dates/timestamps that were written in the
   *     legacy hybrid Julian + Gregorian calendar as it is. If false, throw exceptions instead. If
   *     the spark type is TimestampNTZ, this should be true.
   */
  public static long initColumnReader(
      ColumnDescriptor descriptor,
      DataType readType,
      int batchSize,
      boolean useDecimal128,
      boolean useLegacyDateTimestampOrNTZ) {
    PrimitiveType primitiveType = descriptor.getPrimitiveType();
    int primitiveTypeId = getPhysicalTypeId(primitiveType.getPrimitiveTypeName());
    LogicalTypeAnnotation annotation = primitiveType.getLogicalTypeAnnotation();

    // Process logical type information

    int bitWidth = -1;
    boolean isSigned = false;
    if (annotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
      LogicalTypeAnnotation.IntLogicalTypeAnnotation intAnnotation =
          (LogicalTypeAnnotation.IntLogicalTypeAnnotation) annotation;
      bitWidth = intAnnotation.getBitWidth();
      isSigned = intAnnotation.isSigned();
    }

    int precision, scale;
    precision = scale = -1;
    if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalAnnotation =
          (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) annotation;
      precision = decimalAnnotation.getPrecision();
      scale = decimalAnnotation.getScale();
    }

    int tu = -1;
    boolean isAdjustedUtc = false;
    if (annotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampAnnotation =
          (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) annotation;
      tu = getTimeUnitId(timestampAnnotation.getUnit());
      isAdjustedUtc = timestampAnnotation.isAdjustedToUTC();
    }

    TypePromotionInfo promotionInfo;
    if (readType != null) {
      promotionInfo = new TypePromotionInfo(readType);
    } else {
      // If type promotion is not enable, we'll just use the Parquet primitive type and precision.
      promotionInfo = new TypePromotionInfo(primitiveTypeId, precision, scale, bitWidth);
    }

    return Native.initColumnReader(
        primitiveTypeId,
        getLogicalTypeId(annotation),
        promotionInfo.physicalTypeId,
        descriptor.getPath(),
        descriptor.getMaxDefinitionLevel(),
        descriptor.getMaxRepetitionLevel(),
        bitWidth,
        promotionInfo.bitWidth,
        isSigned,
        primitiveType.getTypeLength(),
        precision,
        promotionInfo.precision,
        scale,
        promotionInfo.scale,
        tu,
        isAdjustedUtc,
        batchSize,
        useDecimal128,
        useLegacyDateTimestampOrNTZ);
  }

  static class TypePromotionInfo {
    // The Parquet physical type ID converted from the Spark read schema, or the original Parquet
    // physical type ID if type promotion is not enabled.
    int physicalTypeId;
    // Decimal precision from the Spark read schema, or -1 if it's not decimal type.
    int precision;
    // Decimal scale from the Spark read schema, or -1 if it's not decimal type.
    int scale;
    // Integer bit width from the Spark read schema, or -1 if it's not integer type.
    int bitWidth;

    TypePromotionInfo(int physicalTypeId, int precision, int scale, int bitWidth) {
      this.physicalTypeId = physicalTypeId;
      this.precision = precision;
      this.scale = scale;
      this.bitWidth = bitWidth;
    }

    TypePromotionInfo(DataType sparkReadType) {
      // Create a dummy `StructField` from the input Spark type. We don't care about
      // field name, nullability and metadata.
      StructField f = new StructField("f", sparkReadType, false, Metadata.empty());
      ColumnDescriptor descriptor = TypeUtil.convertToParquet(f);
      PrimitiveType primitiveType = descriptor.getPrimitiveType();
      int physicalTypeId = getPhysicalTypeId(primitiveType.getPrimitiveTypeName());
      LogicalTypeAnnotation annotation = primitiveType.getLogicalTypeAnnotation();
      int precision = -1;
      int scale = -1;
      int bitWidth = -1;
      if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalAnnotation =
            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) annotation;
        precision = decimalAnnotation.getPrecision();
        scale = decimalAnnotation.getScale();
      }
      if (annotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
        LogicalTypeAnnotation.IntLogicalTypeAnnotation intAnnotation =
            (LogicalTypeAnnotation.IntLogicalTypeAnnotation) annotation;
        bitWidth = intAnnotation.getBitWidth();
      }
      this.physicalTypeId = physicalTypeId;
      this.precision = precision;
      this.scale = scale;
      this.bitWidth = bitWidth;
    }
  }

  /**
   * Maps the input Parquet physical type 'typeName' to an integer representing it. This is used for
   * serialization between the Java and native side.
   *
   * @param typeName enum for the Parquet physical type
   * @return an integer representing the input physical type
   */
  static int getPhysicalTypeId(PrimitiveType.PrimitiveTypeName typeName) {
    switch (typeName) {
      case BOOLEAN:
        return 0;
      case INT32:
        return 1;
      case INT64:
        return 2;
      case INT96:
        return 3;
      case FLOAT:
        return 4;
      case DOUBLE:
        return 5;
      case BINARY:
        return 6;
      case FIXED_LEN_BYTE_ARRAY:
        return 7;
    }
    throw new IllegalArgumentException("Invalid Parquet physical type: " + typeName);
  }

  /**
   * Maps the input Parquet logical type 'annotation' to an integer representing it. This is used
   * for serialization between the Java and native side.
   *
   * @param annotation the Parquet logical type annotation
   * @return an integer representing the input logical type
   */
  static int getLogicalTypeId(LogicalTypeAnnotation annotation) {
    if (annotation == null) {
      return -1; // No logical type associated
    } else if (annotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
      return 0;
    } else if (annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
      return 1;
    } else if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      return 2;
    } else if (annotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
      return 3;
    } else if (annotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      return 4;
    } else if (annotation instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
      return 5;
    } else if (annotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
      return 6;
    }

    throw new UnsupportedOperationException("Unsupported Parquet logical type " + annotation);
  }

  static int getTimeUnitId(LogicalTypeAnnotation.TimeUnit tu) {
    switch (tu) {
      case MILLIS:
        return 0;
      case MICROS:
        return 1;
      case NANOS:
        return 2;
      default:
        throw new UnsupportedOperationException("Unsupported TimeUnit " + tu);
    }
  }

  public static ColumnDescriptor buildColumnDescriptor(ParquetColumnSpec columnSpec) {
    PrimitiveType.PrimitiveTypeName primType =
        PrimitiveType.PrimitiveTypeName.valueOf(columnSpec.getPhysicalType());

    Type.Repetition repetition;
    if (columnSpec.getMaxRepetitionLevel() > 0) {
      repetition = Type.Repetition.REPEATED;
    } else if (columnSpec.getMaxDefinitionLevel() > 0) {
      repetition = Type.Repetition.OPTIONAL;
    } else {
      repetition = Type.Repetition.REQUIRED;
    }

    String name = columnSpec.getPath()[columnSpec.getPath().length - 1];
    // Reconstruct the logical type from parameters
    LogicalTypeAnnotation logicalType = null;
    if (columnSpec.getLogicalTypeName() != null) {
      logicalType =
          reconstructLogicalType(
              columnSpec.getLogicalTypeName(), columnSpec.getLogicalTypeParams());
    }

    PrimitiveType primitiveType;
    if (primType == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
      primitiveType =
          Types.primitive(primType, repetition)
              .length(columnSpec.getTypeLength())
              .as(logicalType)
              .named(name);
    } else {
      primitiveType = Types.primitive(primType, repetition).as(logicalType).named(name);
    }

    MessageType schema = new MessageType("root", primitiveType);
    return schema.getColumnDescription(columnSpec.getPath());
  }

  private static LogicalTypeAnnotation reconstructLogicalType(
      String logicalTypeName, java.util.Map<String, String> params) {

    switch (logicalTypeName) {
        // MAP
      case "MapLogicalTypeAnnotation":
        return LogicalTypeAnnotation.mapType();

        // LIST
      case "ListLogicalTypeAnnotation":
        return LogicalTypeAnnotation.listType();

        // STRING
      case "StringLogicalTypeAnnotation":
        return LogicalTypeAnnotation.stringType();

        // MAP_KEY_VALUE
      case "MapKeyValueLogicalTypeAnnotation":
        return LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance();

        // ENUM
      case "EnumLogicalTypeAnnotation":
        return LogicalTypeAnnotation.enumType();

        // DECIMAL
      case "DecimalLogicalTypeAnnotation":
        int scale = Integer.parseInt(params.get("scale"));
        int precision = Integer.parseInt(params.get("precision"));
        return LogicalTypeAnnotation.decimalType(scale, precision);

        // DATE
      case "DateLogicalTypeAnnotation":
        return LogicalTypeAnnotation.dateType();

        // TIME
      case "TimeLogicalTypeAnnotation":
        boolean isUTC = Boolean.parseBoolean(params.getOrDefault("isAdjustedToUTC", "true"));
        String timeUnitStr = params.getOrDefault("unit", "MICROS");

        LogicalTypeAnnotation.TimeUnit timeUnit;
        switch (timeUnitStr) {
          case "MILLIS":
            timeUnit = LogicalTypeAnnotation.TimeUnit.MILLIS;
            break;
          case "MICROS":
            timeUnit = LogicalTypeAnnotation.TimeUnit.MICROS;
            break;
          case "NANOS":
            timeUnit = LogicalTypeAnnotation.TimeUnit.NANOS;
            break;
          default:
            timeUnit = LogicalTypeAnnotation.TimeUnit.MICROS;
        }
        return LogicalTypeAnnotation.timeType(isUTC, timeUnit);

        // TIMESTAMP
      case "TimestampLogicalTypeAnnotation":
        boolean isAdjustedToUTC = Boolean.parseBoolean(params.get("isAdjustedToUTC"));
        String unitStr = params.getOrDefault("unit", "MICROS");

        LogicalTypeAnnotation.TimeUnit unit;
        switch (unitStr) {
          case "MILLIS":
            unit = LogicalTypeAnnotation.TimeUnit.MILLIS;
            break;
          case "MICROS":
            unit = LogicalTypeAnnotation.TimeUnit.MICROS;
            break;
          case "NANOS":
            unit = LogicalTypeAnnotation.TimeUnit.NANOS;
            break;
          default:
            unit = LogicalTypeAnnotation.TimeUnit.MICROS;
        }
        return LogicalTypeAnnotation.timestampType(isAdjustedToUTC, unit);

        // INTEGER
      case "IntLogicalTypeAnnotation":
        boolean isSigned = Boolean.parseBoolean(params.get("isSigned"));
        int bitWidth = Integer.parseInt(params.get("bitWidth"));
        return LogicalTypeAnnotation.intType(bitWidth, isSigned);

        // JSON
      case "JsonLogicalTypeAnnotation":
        return LogicalTypeAnnotation.jsonType();

        // BSON
      case "BsonLogicalTypeAnnotation":
        return LogicalTypeAnnotation.bsonType();

        // UUID
      case "UUIDLogicalTypeAnnotation":
        return LogicalTypeAnnotation.uuidType();

        // INTERVAL
      case "IntervalLogicalTypeAnnotation":
        return LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance();

      default:
        throw new IllegalArgumentException("Unknown logical type: " + logicalTypeName);
    }
  }
}
