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

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Contains, EndsWith, EqualTo, In, InSet, IsNull, Literal, Not, Or, StartsWith}
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType}

import org.apache.comet.parquet.CometParquetUtils

class CometNativeScanSuite extends AnyFunSuite {

  import CometNativeScan.{constantMetadataFieldPrefix, uniqueConstantMetadataFields}

  test("native data schema preserves required root field ids and table projection indexes") {
    val nestedType = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
    val root = StructField("renamed", nestedType, nullable = false)
    val dataSchema =
      StructType(Seq(StructField("unrequested", LongType), root, StructField("id", IntegerType)))
    val partitionSchema = StructType(Seq(StructField("part", StringType)))

    Seq(true, false).foreach { hasRequiredId =>
      val metadata = new MetadataBuilder()
      if (hasRequiredId) {
        metadata.putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 2L)
      }
      val requiredRoot = root.copy(
        dataType = StructType(Seq(nestedType("a"))),
        nullable = true,
        metadata = metadata.build())
      val requiredSchema = StructType(Seq(dataSchema("id"), requiredRoot))
      val output = (requiredSchema.fields ++ partitionSchema.fields).map { field =>
        AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
      }.toSeq
      val conf = new SQLConf
      conf.setConf(SQLConf.PARQUET_FIELD_ID_READ_ENABLED, true)
      val common = CometNativeScan
        .buildNativeScanCommon(
          source = "test",
          output = output,
          requiredSchema = requiredSchema,
          dataSchema = dataSchema,
          partitionSchema = partitionSchema,
          fileConstantMetadataColumns = Seq.empty,
          dataFilters = Seq.empty,
          objectStoreOptions = Map.empty,
          hadoopConf = new Configuration(false),
          conf = conf)
        .get
        .build()

      val serializedRoot = common.getDataSchema(1)
      val fieldIdKey = CometParquetUtils.PARQUET_FIELD_ID_META_KEY
      assert(serializedRoot.getMetadataMap.containsKey(fieldIdKey) == hasRequiredId)
      if (hasRequiredId) {
        assert(serializedRoot.getMetadataMap.get(fieldIdKey) == "2")
      }
      // Replacing the whole field with requiredRoot would lose the table's type and nullability.
      assert(serializedRoot.getDataType == schema2Proto(Seq(root)).head.getDataType)
      assert(!serializedRoot.getNullable)
      assert(
        common.getDataSchemaList.asScala.map(_.getName).toSeq ==
          Seq("unrequested", "renamed", "id"))
      assert(common.getRequiredSchemaList.asScala.toSeq == schema2Proto(requiredSchema))
      assert(common.getProjectionVectorList.asScala.toSeq == Seq(2L, 1L, 3L))
    }
  }

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

  test("Parquet pushdown settings preserve eligible conjunctions without weakening NOT") {
    val conf = new SQLConf
    conf.setConf(SQLConf.PARQUET_FILTER_PUSHDOWN_DATE_ENABLED, false)
    val date = AttributeReference("date", DateType)()
    val id = AttributeReference("id", IntegerType)()
    val disabled = EqualTo(date, Literal.create(0, DateType))
    val enabled = EqualTo(id, Literal(1))
    val predicates = Seq(
      disabled -> None,
      IsNull(date) -> None,
      And(disabled, enabled) -> Some(enabled),
      And(enabled, disabled) -> Some(enabled),
      Or(disabled, enabled) -> None,
      Or(And(disabled, enabled), enabled) -> Some(Or(enabled, enabled)),
      Not(And(disabled, enabled)) -> None,
      Not(enabled) -> Some(Not(enabled)))
    predicates.foreach { case (predicate, expected) =>
      assert(CometNativeScan.filterForParquetPushdown(predicate, conf) == expected)
    }
    conf.setConf(SQLConf.PARQUET_FILTER_PUSHDOWN_DATE_ENABLED, true)
    val combined = Not(And(disabled, enabled))
    assert(CometNativeScan.filterForParquetPushdown(combined, conf).contains(combined))
  }

  test("Parquet pushdown settings apply to string and IN predicates") {
    val conf = new SQLConf
    conf.setConf(SQLConf.PARQUET_FILTER_PUSHDOWN_STRING_PREDICATE_ENABLED, false)
    val str = AttributeReference("str", StringType)()
    Seq(StartsWith(str, Literal("a")), EndsWith(str, Literal("a")), Contains(str, Literal("a")))
      .foreach { predicate =>
        assert(CometNativeScan.filterForParquetPushdown(predicate, conf).isEmpty)
      }
    val id = AttributeReference("id", IntegerType)()
    val predicates = Seq(In(id, Seq(Literal(1), Literal(2))), InSet(id, Set[Any](1, 2)))
    conf.setConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD, 0)
    predicates.foreach { predicate =>
      assert(CometNativeScan.filterForParquetPushdown(predicate, conf).isEmpty)
    }
    conf.setConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD, 1)
    predicates.foreach { predicate =>
      assert(CometNativeScan.filterForParquetPushdown(predicate, conf).contains(predicate))
      assert(CometNativeScan.filterForParquetPushdown(Not(predicate), conf).isEmpty)
    }
  }
}
