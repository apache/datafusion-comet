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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.DataTypeSupport.{usingParquetExecWithIncompatTypes, ARRAY_ELEMENT, MAP_KEY, MAP_VALUE}

trait DataTypeSupport {

  /**
   * This can be overridden to provide support for specific data types that aren't universally
   * supported. This will only be called for non-universally supported types.
   *
   * @param dt
   *   the data type to check
   * @return
   *   true if the datatype is supported
   */
  def isAdditionallySupported(
      dt: DataType,
      name: String,
      fallbackReasons: ListBuffer[String]): Boolean = false

  private def isGloballySupported(dt: DataType, fallbackReasons: ListBuffer[String]): Boolean =
    dt match {
      case ByteType | ShortType if usingParquetExecWithIncompatTypes(SQLConf.get) =>
        fallbackReasons += s"${CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.key} is false"
        false
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          BinaryType | StringType | _: DecimalType | DateType | TimestampType =>
        true
      case t: DataType if t.typeName == "timestamp_ntz" =>
        true
      case _ => false
    }

  /**
   * Checks if this schema is supported by checking if each field in the struct is supported.
   *
   * @param struct
   *   the struct to check the fields of
   * @return
   *   true if all fields in the struct are supported
   */
  def isSchemaSupported(struct: StructType, fallbackReasons: ListBuffer[String]): Boolean = {
    struct.fields.forall(f => isTypeSupported(f.dataType, f.name, fallbackReasons))
  }

  def isTypeSupported(
      dt: DataType,
      name: String,
      fallbackReasons: ListBuffer[String]): Boolean = {
    if (isGloballySupported(dt, fallbackReasons) || isAdditionallySupported(
        dt,
        name,
        fallbackReasons)) {
      // If complex types are supported, we additionally want to recurse into their children
      dt match {
        case StructType(fields) =>
          fields.forall(f => isTypeSupported(f.dataType, f.name, fallbackReasons))
        case ArrayType(elementType, _) =>
          isTypeSupported(elementType, ARRAY_ELEMENT, fallbackReasons)
        case MapType(keyType, valueType, _) =>
          isTypeSupported(keyType, MAP_KEY, fallbackReasons) && isTypeSupported(
            valueType,
            MAP_VALUE,
            fallbackReasons)
        // Not a complex type
        case _ => true
      }
    } else {
      fallbackReasons += s"Unsupported ${name} of type ${dt}"
      false
    }
  }
}

object DataTypeSupport {
  val ARRAY_ELEMENT = "array element"
  val MAP_KEY = "map key"
  val MAP_VALUE = "map value"

  def usingParquetExecWithIncompatTypes(conf: SQLConf): Boolean = {
    CometSparkSessionExtensions.usingDataFusionParquetExec(conf) &&
    !CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.get(conf)
  }
}
