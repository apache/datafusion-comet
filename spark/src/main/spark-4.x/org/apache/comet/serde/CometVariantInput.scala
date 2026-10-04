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

package org.apache.comet.serde

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal}
import org.apache.spark.sql.types.VariantType
import org.apache.spark.unsafe.types.VariantVal

import com.google.protobuf.ByteString

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

/** Only explicit Variant consumers may serialize these inputs. */
object CometVariantInput {
  def convert(child: Expression, inputs: Seq[Attribute], binding: Boolean): Option[Expr] =
    child match {
      case attr: AttributeReference if attr.dataType == VariantType =>
        CometAttributeReference.convert(attr, inputs, binding)
      case Literal(value, VariantType) =>
        val literal = LiteralOuterClass.Literal
          .newBuilder()
          .setDatatype(serializeDataType(VariantType).get)
          .setIsNull(value == null)
        if (value != null) {
          val variant = value.asInstanceOf[VariantVal]
          literal.setVariantVal(
            LiteralOuterClass.VariantLiteral
              .newBuilder()
              .setValue(ByteString.copyFrom(variant.getValue))
              .setMetadata(ByteString.copyFrom(variant.getMetadata)))
        }
        Some(Expr.newBuilder().setLiteral(literal).build())
      case _ =>
        withFallbackReason(
          child,
          "Native Variant expressions require a top-level column or literal")
        None
    }
}
