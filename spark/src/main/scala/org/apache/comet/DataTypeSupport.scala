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

package org.apache.comet

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.types._

import org.apache.comet.DataTypeSupport.{hasDuplicateFieldNames, ARRAY_ELEMENT, MAP_KEY, MAP_VALUE}

trait DataTypeSupport {

  /**
   * Checks if this schema is supported by checking if each field in the schema is supported.
   *
   * @param schema
   *   the schema to check the fields of
   * @return
   *   true if all fields in the schema are supported
   */
  def isSchemaSupported(schema: StructType, fallbackReasons: ListBuffer[String]): Boolean = {
    schema.fields.forall(f => isTypeSupported(f.dataType, f.name, fallbackReasons))
  }

  /**
   * Determine if Comet supports a data type. This method can be overridden by specific operators
   * as needed.
   */
  def isTypeSupported(
      dt: DataType,
      name: String,
      fallbackReasons: ListBuffer[String]): Boolean = {

    dt match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          BinaryType | StringType | _: DecimalType | DateType | TimestampType | TimestampNTZType |
          CalendarIntervalType =>
        true
      case StructType(fields) if hasDuplicateFieldNames(fields) =>
        // Java Arrow keys struct children by name, so a struct with duplicate field names
        // cannot cross the JVM Arrow boundary intact
        fallbackReasons += s"Unsupported ${name}: struct with duplicate field names"
        false
      case StructType(fields) =>
        fields.nonEmpty && fields.forall(f =>
          isTypeSupported(f.dataType, f.name, fallbackReasons))
      case ArrayType(elementType, _) =>
        isTypeSupported(elementType, ARRAY_ELEMENT, fallbackReasons)
      case MapType(keyType, valueType, _) =>
        isTypeSupported(keyType, MAP_KEY, fallbackReasons) && isTypeSupported(
          valueType,
          MAP_VALUE,
          fallbackReasons)
      case _ =>
        fallbackReasons += s"Unsupported ${name} of type ${dt}"
        false
    }
  }
}

object DataTypeSupport {
  val ARRAY_ELEMENT = "array element"
  val MAP_KEY = "map key"
  val MAP_VALUE = "map value"

  def isComplexType(dt: DataType): Boolean = dt match {
    case _: StructType | _: ArrayType | _: MapType => true
    case _ => false
  }

  /** True when two of `fields` carry byte-identical names. */
  def hasDuplicateFieldNames(fields: Array[StructField]): Boolean =
    fields.map(_.name).distinct.length != fields.length

  /**
   * True when two of `fields` declare the same Parquet field id.
   *
   * Deliberately not Spark's check: `ParquetReadSupport.matchIdField` raises when one *requested*
   * id is carried by several fields *in the file*. The two coincide only when the requested
   * schema equals the file schema, which is exactly when DataFusion's opener skips the expression
   * adapter that would have validated the lookup (#5801). So this can decline a read Spark would
   * have accepted, costing native execution but not correctness; it cannot report a duplicate
   * Spark would not. File-side ambiguity is left to #5786.
   *
   * Only meaningful under `spark.sql.parquet.fieldId.read.enabled`; callers gate on that.
   */
  def hasDuplicateFieldIds(fields: Array[StructField]): Boolean = {
    val ids = fields.flatMap(fieldId)
    ids.distinct.length != ids.length
  }

  private def fieldId(field: StructField): Option[Int] = {
    if (!ParquetUtils.hasFieldId(field)) None
    else {
      // A malformed id is not this check's business to report -- getFieldId raises on one -- so
      // treat it as absent and let the reader complain about it.
      try Some(ParquetUtils.getFieldId(field))
      catch { case _: IllegalArgumentException => None }
    }
  }

  /**
   * `dt` with every array/map/struct nullability flag forced to `true` at all nesting levels (map
   * key fields stay non-null per Arrow's map invariant). Re-derives Spark's `private[spark]`
   * `DataType.asNullable`, used as a common cast target to unify types whose Comet runtime
   * nullability exceeds Spark's Catalyst nullability.
   */
  def deepNullable(dt: DataType): DataType = dt match {
    case ArrayType(et, _) => ArrayType(deepNullable(et), containsNull = true)
    case MapType(kt, vt, _) =>
      MapType(deepNullable(kt), deepNullable(vt), valueContainsNull = true)
    case StructType(fields) =>
      StructType(fields.map(f => f.copy(dataType = deepNullable(f.dataType), nullable = true)))
    case other => other
  }

  def hasTemporalType(t: DataType): Boolean = t match {
    case DataTypes.DateType | DataTypes.TimestampType | DataTypes.TimestampNTZType =>
      true
    case t: StructType => t.exists(f => hasTemporalType(f.dataType))
    case t: ArrayType => hasTemporalType(t.elementType)
    case t: MapType => hasTemporalType(t.keyType) || hasTemporalType(t.valueType)
    case _ => false
  }

}
