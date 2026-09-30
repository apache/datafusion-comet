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

import java.util.Collections

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.comet.CometNativeColumnarToRowExec
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.{ArrayType, BinaryType, Metadata, StructField, StructType}

import org.apache.comet.rules.CometScanTypeChecker
import org.apache.comet.serde.{CometAttributeReference, QueryPlanSerde, Unsupported}

class CometVariantTypeSuite extends AnyFunSuite {
  private val storageType = StructType(
    Seq(
      StructField("value", BinaryType, nullable = false),
      StructField("metadata", BinaryType, nullable = false)))

  private def variantField(extensionName: Option[String]): Field = {
    val metadata = extensionName
      .map(name =>
        Collections.singletonMap(ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME, name))
      .getOrElse(Collections.emptyMap[String, String]())
    val children = Seq(
      Field.notNullable("value", ArrowType.Binary.INSTANCE),
      Field.notNullable("metadata", ArrowType.Binary.INSTANCE))
    new Field(
      "v",
      new FieldType(true, ArrowType.Struct.INSTANCE, null, metadata),
      children.asJava)
  }

  test("Variant identity requires the canonical Arrow extension marker") {
    val marked = variantField(Some("arrow.parquet.variant"))
    val unmarked = variantField(None)
    val wrongMarker = variantField(Some("example.variant"))

    assert(Utils.fromArrowField(unmarked) == storageType)
    assert(Utils.fromArrowField(wrongMarker) == storageType)

    Utils.variantType match {
      case Some(variantType) =>
        assert(Utils.fromArrowField(marked) == variantType)
        assert(QueryPlanSerde.serializeDataType(variantType).get.getTypeIdValue == 21)
        assert(QueryPlanSerde.serializeDataType(ArrayType(variantType)).isDefined)
        assert(!QueryPlanSerde.supportedDataType(variantType))
        assert(!QueryPlanSerde.supportedDataType(ArrayType(variantType), allowComplex = true))
        assert(
          !CometNativeColumnarToRowExec.supportsSchema(
            StructType(Seq(StructField("v", variantType)))))
        assert(
          !CometNativeColumnarToRowExec.supportsSchema(
            StructType(Seq(StructField("nested", ArrayType(variantType))))))
        assert(!CometScanTypeChecker().isTypeSupported(variantType, "v", ListBuffer.empty))
        assert(
          !CometScanTypeChecker()
            .isTypeSupported(ArrayType(variantType), "nested", ListBuffer.empty))
        assert(Utils.containsVariantType(ArrayType(variantType)))
        assert(
          CometAttributeReference
            .getSupportLevel(AttributeReference("v", variantType)())
            .isInstanceOf[Unsupported])
        assert(
          CometAttributeReference
            .getSupportLevel(AttributeReference("nested", ArrayType(variantType))())
            .isInstanceOf[Unsupported])
      case None =>
        assert(Utils.fromArrowField(marked) == storageType)
    }
  }

  test("whole-value scan requests require Spark's complete fullVariant metadata") {
    Utils.variantType.foreach { variantType =>
      val request = """{"path":"$","failOnError":true,"timeZoneId":"UTC"}"""
      val metadata = Metadata.fromJson(s"""{"__VARIANT_METADATA_KEY":$request}""")
      val child = StructField("0", variantType, metadata = metadata)
      val whole = StructType(Seq(child))
      assert(Utils.isWholeVariantStruct(whole))
      val arrowVariant = variantField(Some("arrow.parquet.variant"))
      val arrowMetadata = arrowVariant.getMetadata.asScala.toMap +
        ("__VARIANT_METADATA_KEY" -> request)
      val arrowChild = new Field(
        "0",
        new FieldType(true, arrowVariant.getType, null, arrowMetadata.asJava),
        arrowVariant.getChildren)
      val arrowWhole =
        new Field("v", FieldType.nullable(ArrowType.Struct.INSTANCE), Seq(arrowChild).asJava)
      assert(Utils.fromArrowField(arrowWhole) == whole)
      val serialized = QueryPlanSerde
        .serializeDataType(whole)
        .get
        .getTypeInfo
        .getStruct
        .getFieldMetadata(0)
        .getMetadataMap
        .get("__VARIANT_METADATA_KEY")
      assert(Metadata.fromJson(serialized) == Metadata.fromJson(request))
      assert(!Utils.isWholeVariantStruct(StructType(Seq(child.copy(metadata = Metadata.empty)))))
      assert(!Utils.isWholeVariantStruct(StructType(Seq(child.copy(name = "1")))))
      assert(!Utils.isWholeVariantStruct(StructType(Seq(child, child.copy(name = "1")))))
      assert(!Utils.isWholeVariantStruct(StructType(Seq(child.copy(dataType = BinaryType)))))
      for (changed <- Seq(
          request.replace("$", "$.a"),
          request.replace("true", "false"),
          request.replace("UTC", "America/Los_Angeles"),
          "{}")) {
        val other = Metadata.fromJson(s"""{"__VARIANT_METADATA_KEY":$changed}""")
        assert(!Utils.isWholeVariantStruct(StructType(Seq(child.copy(metadata = other)))))
      }
    }
  }
}
