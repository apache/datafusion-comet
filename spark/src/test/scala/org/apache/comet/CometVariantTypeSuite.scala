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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GetStructField}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.comet.CometNativeColumnarToRowExec
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, StructField, StructType}

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

  test("Variant predicate serialization is scoped to supported inputs and Spark versions") {
    assume(Utils.variantType.isDefined, "VariantType requires Spark 4.0+")
    val variantType = Utils.variantType.get
    val evaluator = Class.forName(
      "org.apache.spark.sql.catalyst.expressions.variant.VariantExpressionEvalUtils$")
    val input = AttributeReference("v", variantType)()
    for ((method, supported) <- Seq(
        "isVariantNull" -> true,
        "isValidVariant" -> CometSparkSessionExtensions.isSpark42Plus)) {
      val expression = StaticInvoke(
        evaluator,
        BooleanType,
        method,
        Seq(input),
        propagateNull = method == "isValidVariant",
        returnNullable = false)
      assert(
        QueryPlanSerde.exprToProto(expression, Seq(input), binding = true).isDefined == supported)
      assert(
        QueryPlanSerde
          .exprToProto(
            expression.copy(propagateNull = !expression.propagateNull),
            Seq(input),
            binding = true)
          .isEmpty)
      val lookalike = AttributeReference("v", storageType)()
      assert(QueryPlanSerde
        .exprToProto(expression.copy(arguments = Seq(lookalike)), Seq(lookalike), binding = true)
        .isEmpty)
      val parent = AttributeReference("s", StructType(Seq(StructField("v", variantType))))()
      assert(
        QueryPlanSerde
          .exprToProto(
            expression.copy(arguments = Seq(GetStructField(parent, 0))),
            Seq(parent),
            binding = true)
          .isEmpty)
    }
    assert(QueryPlanSerde.exprToProto(input, Seq(input), binding = true).isEmpty)
  }
}
