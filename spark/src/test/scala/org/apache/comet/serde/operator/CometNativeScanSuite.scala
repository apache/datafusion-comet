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

package org.apache.comet.serde.operator

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{LongType, StringType}

class CometNativeScanSuite extends AnyFunSuite {

  import CometNativeScan.{constantMetadataFieldPrefix, uniqueConstantMetadataFields}

  test("uniqueConstantMetadataFields prefixes metadata column names") {
    val fields = uniqueConstantMetadataFields(
      Seq(
        AttributeReference("file_size", LongType)(),
        AttributeReference("file_path", StringType)()),
      Set("id", "part"))
    assert(fields.map(_.name) ==
      Seq(s"${constantMetadataFieldPrefix}file_size", s"${constantMetadataFieldPrefix}file_path"))
    assert(fields.map(_.dataType) == Seq(LongType, StringType))
  }

  test("uniqueConstantMetadataFields uniquifies against colliding column names") {
    // DataFusion substitutes partition constants by name, so a user or partition column
    // that already uses the prefixed name must not receive the metadata value.
    val colliding = s"${constantMetadataFieldPrefix}file_size"
    val fields = uniqueConstantMetadataFields(
      Seq(AttributeReference("file_size", LongType)()),
      Set("id", colliding, colliding + "_"))
    assert(fields.map(_.name) == Seq(colliding + "__"))
  }
}
