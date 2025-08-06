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

import java.util.Arrays;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.LogicalTypeAnnotation.*;
import org.apache.spark.package$;
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.*;

import org.apache.comet.CometConf;

import static org.apache.comet.parquet.Utils.descriptorToParquetColumnSpec;

public class TypeUtil {

  /**
   * Converts the input Spark 'field' into a Parquet column descriptor.
   * @deprecated since 0.10.0, will be removed in 0.11.0.
   * @see <a href="https://github.com/apache/datafusion-comet/issues/2079">Comet Issue #2079</a>
   */
public  static ColumnDescriptor convertToParquet(StructField field) {
    Type.Repetition repetition;
    int maxDefinitionLevel;
    if (field.nullable()) {
      repetition = Type.Repetition.OPTIONAL;
      maxDefinitionLevel = 1;
    } else {
      repetition = Type.Repetition.REQUIRED;
      maxDefinitionLevel = 0;
    }
    String[] path = new String[] {field.name()};

    DataType type = field.dataType();

    Types.PrimitiveBuilder<PrimitiveType> builder = null;
    // Only partition column can be `NullType`, which also uses `ConstantColumnReader`. Here we
    // piggy-back onto Parquet boolean type for constant vector of null values, we don't really
    // care what Parquet type it is.
    if (type == DataTypes.BooleanType || type == DataTypes.NullType) {
      builder = Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition);
    } else if (type == DataTypes.IntegerType || type instanceof YearMonthIntervalType) {
      builder =
          Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
              .as(LogicalTypeAnnotation.intType(32, true));
    } else if (type == DataTypes.DateType) {
      builder =
          Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
              .as(LogicalTypeAnnotation.dateType());
    } else if (type == DataTypes.ByteType) {
      builder =
          Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
              .as(LogicalTypeAnnotation.intType(8, true));
    } else if (type == DataTypes.ShortType) {
      builder =
          Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
              .as(LogicalTypeAnnotation.intType(16, true));
    } else if (type == DataTypes.LongType) {
      builder = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition);
    } else if (type == DataTypes.BinaryType) {
      builder = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition);
    } else if (type == DataTypes.StringType
        || (type.sameType(DataTypes.StringType) && isSpark40Plus())) {
      builder =
          Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
              .as(LogicalTypeAnnotation.stringType());
    } else if (type == DataTypes.FloatType) {
      builder = Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition);
    } else if (type == DataTypes.DoubleType) {
      builder = Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition);
    } else if (type == DataTypes.TimestampType) {
      builder =
          Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
              .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS));
    } else if (type == TimestampNTZType$.MODULE$) {
      builder =
          Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
              .as(LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS));
    } else if (type instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) type;
      builder =
          Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
              .length(16) // always store as Decimal128
              .as(LogicalTypeAnnotation.decimalType(decimalType.scale(), decimalType.precision()));
    }
    if (builder == null) {
      throw new UnsupportedOperationException("Unsupported input Spark type: " + type);
    }

    return new ColumnDescriptor(path, builder.named(field.name()), 0, maxDefinitionLevel);
  }

  public static ParquetColumnSpec convertToParquetSpec(StructField field) {
    return descriptorToParquetColumnSpec(convertToParquet(field));
  }

  /**
   * Check whether the Parquet 'descriptor' and Spark read type 'sparkType' are compatible. If not,
   * throw exception.
   *
   * <p>This mostly follows the logic in Spark's
   * ParquetVectorUpdaterFactory#getUpdater(ColumnDescriptor, DataType)
   *
   * @param descriptor descriptor for a Parquet primitive column
   * @param sparkType Spark read type
   */
  public static void checkParquetType(ColumnDescriptor descriptor, DataType sparkType) {
    PrimitiveType.PrimitiveTypeName typeName = descriptor.getPrimitiveType().getPrimitiveTypeName();
    LogicalTypeAnnotation logicalTypeAnnotation =
        descriptor.getPrimitiveType().getLogicalTypeAnnotation();
    boolean allowTypePromotion = (boolean) CometConf.COMET_SCHEMA_EVOLUTION_ENABLED().get();

    if (sparkType instanceof NullType) {
      return;
    }

    switch (typeName) {
      case BOOLEAN:
        if (sparkType == DataTypes.BooleanType) return;
        break;
      case INT32:
        if (sparkType == DataTypes.IntegerType || canReadAsIntDecimal(descriptor, sparkType)) {
          return;
        } else if (sparkType == DataTypes.LongType
            && isUnsignedIntTypeMatched(logicalTypeAnnotation, 32)) {
          // In `ParquetToSparkSchemaConverter`, we map parquet UINT32 to our LongType.
          // For unsigned int32, it stores as plain signed int32 in Parquet when dictionary
          // fallbacks. We read them as long values.
          return;
        } else if (sparkType == DataTypes.LongType && allowTypePromotion) {
          // In Comet we allow schema evolution from int to long, if
          // `spark.comet.schemaEvolution.enabled` is enabled.
          return;
        } else if (sparkType == DataTypes.ByteType || sparkType == DataTypes.ShortType) {
          return;
        } else if (sparkType == DataTypes.DateType) {
          // TODO: use dateTimeRebaseMode from Spark side
          return;
        } else if (sparkType instanceof YearMonthIntervalType) {
          return;
        } else if (sparkType == DataTypes.DoubleType && isSpark40Plus()) {
          return;
        } else if (sparkType == TimestampNTZType$.MODULE$
            && isSpark40Plus()
            && logicalTypeAnnotation instanceof DateLogicalTypeAnnotation) {
          return;
        }
        break;
      case INT64:
        if (sparkType == DataTypes.LongType || canReadAsLongDecimal(descriptor, sparkType)) {
          return;
        } else if (isLongDecimal(sparkType)
            && isUnsignedIntTypeMatched(logicalTypeAnnotation, 64)) {
          // In `ParquetToSparkSchemaConverter`, we map parquet UINT64 to our Decimal(20, 0).
          // For unsigned int64, it stores as plain signed int64 in Parquet when dictionary
          // fallbacks. We read them as decimal values.
          return;
        } else if (isTimestampTypeMatched(logicalTypeAnnotation, TimeUnit.MICROS)
            && (sparkType == TimestampNTZType$.MODULE$ || sparkType == DataTypes.TimestampType)) {
          validateTimestampType(logicalTypeAnnotation, sparkType);
          // TODO: use dateTimeRebaseMode from Spark side
          return;
        } else if (isTimestampTypeMatched(logicalTypeAnnotation, TimeUnit.MILLIS)
            && (sparkType == TimestampNTZType$.MODULE$ || sparkType == DataTypes.TimestampType)) {
          validateTimestampType(logicalTypeAnnotation, sparkType);
          return;
        }
        break;
      case INT96:
        if (sparkType == TimestampNTZType$.MODULE$) {
          if (isSpark40Plus()) return; // Spark 4.0+ supports Timestamp NTZ with INT96
          convertErrorForTimestampNTZ(typeName.name());
        } else if (sparkType == DataTypes.TimestampType) {
          return;
        }
        break;
      case FLOAT:
        if (sparkType == DataTypes.FloatType) return;
        // In Comet we allow schema evolution from float to double, if
        // `spark.comet.schemaEvolution.enabled` is enabled.
        if (sparkType == DataTypes.DoubleType && allowTypePromotion) return;
        break;
      case DOUBLE:
        if (sparkType == DataTypes.DoubleType) return;
        break;
      case BINARY:
        if (sparkType == DataTypes.StringType
            || sparkType == DataTypes.BinaryType
            || canReadAsBinaryDecimal(descriptor, sparkType)) {
          return;
        }

        if (sparkType.sameType(DataTypes.StringType) && isSpark40Plus()) {
          LogicalTypeAnnotation lta = descriptor.getPrimitiveType().getLogicalTypeAnnotation();
          if (lta instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
            return;
          }
        }
        break;
      case FIXED_LEN_BYTE_ARRAY:
        if (canReadAsIntDecimal(descriptor, sparkType)
            || canReadAsLongDecimal(descriptor, sparkType)
            || canReadAsBinaryDecimal(descriptor, sparkType)
            || sparkType == DataTypes.BinaryType
            // for uuid, since iceberg maps uuid to StringType
            || sparkType == DataTypes.StringType
                && logicalTypeAnnotation
                    instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
          return;
        }
        break;
      default:
        break;
    }

    throw new SchemaColumnConvertNotSupportedException(
        Arrays.toString(descriptor.getPath()),
        descriptor.getPrimitiveType().getPrimitiveTypeName().toString(),
        sparkType.catalogString());
  }

  private static void validateTimestampType(
      LogicalTypeAnnotation logicalTypeAnnotation, DataType sparkType) {
    assert (logicalTypeAnnotation instanceof TimestampLogicalTypeAnnotation);
    // Throw an exception if the Parquet type is TimestampLTZ and the Catalyst type is TimestampNTZ.
    // This is to avoid mistakes in reading the timestamp values.
    if (((TimestampLogicalTypeAnnotation) logicalTypeAnnotation).isAdjustedToUTC()
        && sparkType == TimestampNTZType$.MODULE$
        && !isSpark40Plus()) {
      convertErrorForTimestampNTZ("int64 time(" + logicalTypeAnnotation + ")");
    }
  }

  private static void convertErrorForTimestampNTZ(String parquetType) {
    throw new RuntimeException(
        "Unable to create Parquet converter for data type "
            + TimestampNTZType$.MODULE$.json()
            + " whose Parquet type is "
            + parquetType);
  }

  private static boolean canReadAsIntDecimal(ColumnDescriptor descriptor, DataType dt) {
    if (!DecimalType.is32BitDecimalType(dt) && !(isSpark40Plus() && dt instanceof DecimalType))
      return false;
    return isDecimalTypeMatched(descriptor, dt);
  }

  private static boolean canReadAsLongDecimal(ColumnDescriptor descriptor, DataType dt) {
    if (!DecimalType.is64BitDecimalType(dt) && !(isSpark40Plus() && dt instanceof DecimalType))
      return false;
    return isDecimalTypeMatched(descriptor, dt);
  }

  private static boolean canReadAsBinaryDecimal(ColumnDescriptor descriptor, DataType dt) {
    if (!DecimalType.isByteArrayDecimalType(dt)) return false;
    return isDecimalTypeMatched(descriptor, dt);
  }

  private static boolean isLongDecimal(DataType dt) {
    if (dt instanceof DecimalType) {
      DecimalType d = (DecimalType) dt;
      return d.precision() == 20 && d.scale() == 0;
    }
    return false;
  }

  private static boolean isDecimalTypeMatched(ColumnDescriptor descriptor, DataType dt) {
    DecimalType d = (DecimalType) dt;
    LogicalTypeAnnotation typeAnnotation = descriptor.getPrimitiveType().getLogicalTypeAnnotation();
    if (typeAnnotation instanceof DecimalLogicalTypeAnnotation) {
      DecimalLogicalTypeAnnotation decimalType = (DecimalLogicalTypeAnnotation) typeAnnotation;
      // It's OK if the required decimal precision is larger than or equal to the physical decimal
      // precision in the Parquet metadata, as long as the decimal scale is the same.
      return (decimalType.getPrecision() <= d.precision() && decimalType.getScale() == d.scale())
          || (isSpark40Plus()
              && (!SQLConf.get().parquetVectorizedReaderEnabled()
                  || (decimalType.getScale() <= d.scale()
                      && decimalType.getPrecision() - decimalType.getScale()
                          <= d.precision() - d.scale())));
    } else if (isSpark40Plus()) {
      boolean isNullTypeAnnotation = typeAnnotation == null;
      boolean isIntTypeAnnotation = typeAnnotation instanceof IntLogicalTypeAnnotation;
      if (!SQLConf.get().parquetVectorizedReaderEnabled()) {
        return isNullTypeAnnotation || isIntTypeAnnotation;
      } else if (isNullTypeAnnotation
          || (isIntTypeAnnotation && ((IntLogicalTypeAnnotation) typeAnnotation).isSigned())) {
        PrimitiveType.PrimitiveTypeName typeName =
            descriptor.getPrimitiveType().getPrimitiveTypeName();
        int integerPrecision = d.precision() - d.scale();
        switch (typeName) {
          case INT32:
            return integerPrecision >= DecimalType$.MODULE$.IntDecimal().precision();
          case INT64:
            return integerPrecision >= DecimalType$.MODULE$.LongDecimal().precision();
        }
      }
    }
    return false;
  }

  private static boolean isTimestampTypeMatched(
      LogicalTypeAnnotation logicalTypeAnnotation, LogicalTypeAnnotation.TimeUnit unit) {
    return logicalTypeAnnotation instanceof TimestampLogicalTypeAnnotation
        && ((TimestampLogicalTypeAnnotation) logicalTypeAnnotation).getUnit() == unit;
  }

  private static boolean isUnsignedIntTypeMatched(
      LogicalTypeAnnotation logicalTypeAnnotation, int bitWidth) {
    return logicalTypeAnnotation instanceof IntLogicalTypeAnnotation
        && !((IntLogicalTypeAnnotation) logicalTypeAnnotation).isSigned()
        && ((IntLogicalTypeAnnotation) logicalTypeAnnotation).getBitWidth() == bitWidth;
  }

  static boolean isSpark40Plus() {
    return package$.MODULE$.SPARK_VERSION().compareTo("4.0") >= 0;
  }
}
