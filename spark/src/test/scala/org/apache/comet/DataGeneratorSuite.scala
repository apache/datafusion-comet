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

import scala.util.Random

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import org.apache.comet.testing.{FuzzDataGenerator, SchemaGenOptions}

class DataGeneratorSuite extends CometTestBase {

  test("generate nested schema has at least minDepth levels") {
    val minDepth = 3
    val numCols = 4
    val schema = FuzzDataGenerator.generateNestedSchema(
      new Random(42),
      numCols,
      minDepth = minDepth,
      maxDepth = minDepth + 1,
      options = SchemaGenOptions(generateMap = true, generateArray = true, generateStruct = true))
    assert(schema.fields.length == numCols)

    def calculateDepth(dataType: DataType): Int = {
      dataType match {
        case ArrayType(elementType, _) => 1 + calculateDepth(elementType)
        case StructType(fields) =>
          if (fields.isEmpty) 1
          else 1 + fields.map(f => calculateDepth(f.dataType)).max
        case MapType(k, v, _) =>
          calculateDepth(k).max(calculateDepth(v))
        case _ =>
          // primitive type
          1
      }
    }

    val actualDepth = schema.fields.map(f => calculateDepth(f.dataType)).max
    assert(
      actualDepth >= minDepth,
      s"Generated schema depth $actualDepth is less than required minimum depth $minDepth")
  }

  test("test configurable stringGen in row generator") {
    val gen = DataGenerator.DEFAULT
    val chars = "abcde"
    val maxLen = 10
    val stringGen = () => gen.generateString(chars, maxLen)
    val numRows = 100
    val schema = new StructType().add("a", "string")
    var numNulls = 0
    gen
      .generateRows(numRows, schema, Some(stringGen))
      .foreach(row => {
        if (row.getString(0) != null) {
          assert(row.getString(0).forall(chars.toSeq.contains))
          assert(row.getString(0).length <= maxLen)
        } else {
          numNulls += 1
        }
      })
    // 0.1 null probability
    assert(numNulls >= 0.05 * numRows && numNulls <= 0.15 * numRows)
  }

}
