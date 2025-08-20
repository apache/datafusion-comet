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

import org.apache.spark.sql.types._

import org.apache.comet.DataTypeSupport.{ARRAY_ELEMENT, MAP_KEY, MAP_VALUE}

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
          BinaryType | StringType | _: DecimalType | DateType | TimestampType |
          TimestampNTZType =>
        true
      case StructType(fields) =>
        fields.nonEmpty && fields.forall(f => isTypeSupported(f.dataType, f.name, fallbackReasons))
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
}
