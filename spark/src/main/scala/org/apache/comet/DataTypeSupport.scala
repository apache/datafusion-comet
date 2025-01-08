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

import org.apache.spark.sql.types._

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
  def isAdditionallySupported(dt: DataType): Boolean = false

  private def isGloballySupported(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
        BinaryType | StringType | _: DecimalType | DateType | TimestampType =>
      true
    case t: DataType if t.typeName == "timestamp_ntz" =>
      true
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
  def isSchemaSupported(struct: StructType): Boolean = {
    struct.fields.map(_.dataType).forall(isTypeSupported)
  }

  def isTypeSupported(dt: DataType): Boolean = {
    if (isGloballySupported(dt) || isAdditionallySupported(dt)) {
      // If complex types are supported, we additionally want to recurse into their children
      dt match {
        case StructType(fields) => fields.map(_.dataType).forall(isTypeSupported)
        case ArrayType(elementType, _) => isTypeSupported(elementType)
        case MapType(keyType, valueType, _) =>
          isTypeSupported(keyType) && isTypeSupported(valueType)
        // Not a complex type
        case _ => true
      }
    } else {
      false
    }
  }
}
