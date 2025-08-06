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

package org.apache.comet.parquet

import java.io.ByteArrayOutputStream

import scala.collection.JavaConverters._

import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType

import org.apache.comet.parquet.SourceFilterSerde.{createBinaryExpr, createNameExpr, createUnaryExpr, createValueExpr}
import org.apache.comet.serde.ExprOuterClass
import org.apache.comet.serde.QueryPlanSerde.scalarFunctionExprToProto

/**
 * Utility class for creating native filters for Comet's native reader. This functionality is
 * Comet-specific and not available in Spark's native ParquetFilters.
 *
 * The native filters are serialized into protobuf format and passed to Comet's native Rust reader
 * for efficient filter evaluation.
 */
object CometNativeFilters {

  /**
   * Creates native filters from Spark Filter objects for Comet's native reader.
   *
   * @param predicates
   *   Sequence of Spark Filter objects
   * @param dataSchema
   *   The data schema for field resolution
   * @param nameToParquetField
   *   Map of field names to parquet field info (currently unused but kept for API compatibility)
   * @return
   *   Optional byte array containing serialized protobuf filters
   */
  def createNativeFilters(
      predicates: Seq[sources.Filter],
      dataSchema: StructType,
      nameToParquetField: Map[String, Any]): Option[Array[Byte]] = {
    predicates.reduceOption(sources.And).flatMap(createNativeFilter(_, dataSchema)).map { expr =>
      val outputStream = new ByteArrayOutputStream()
      expr.writeTo(outputStream)
      outputStream.close()
      outputStream.toByteArray
    }
  }

  /**
   * Converts a single Spark Filter to a native protobuf expression.
   */
  private def createNativeFilter(
      predicate: sources.Filter,
      dataSchema: StructType): Option[ExprOuterClass.Expr] = {

    def nameUnaryExpr(name: String)(
        f: (ExprOuterClass.Expr.Builder, ExprOuterClass.UnaryExpr) => ExprOuterClass.Expr.Builder)
        : Option[ExprOuterClass.Expr] = {
      createNameExpr(name, dataSchema).map { case (_, childExpr) =>
        createUnaryExpr(childExpr, f)
      }
    }

    def nameValueBinaryExpr(name: String, value: Any)(
        f: (
            ExprOuterClass.Expr.Builder,
            ExprOuterClass.BinaryExpr) => ExprOuterClass.Expr.Builder)
        : Option[ExprOuterClass.Expr] = {
      createNameExpr(name, dataSchema).flatMap { case (dataType, childExpr) =>
        createValueExpr(value, dataType).map(createBinaryExpr(childExpr, _, f))
      }
    }

    predicate match {
      case sources.IsNull(name) =>
        nameUnaryExpr(name) { (builder, unaryExpr) =>
          builder.setIsNull(unaryExpr)
        }
      case sources.IsNotNull(name) =>
        nameUnaryExpr(name) { (builder, unaryExpr) =>
          builder.setIsNotNull(unaryExpr)
        }

      case sources.EqualTo(name, value) =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setEq(binaryExpr)
        }

      case sources.Not(sources.EqualTo(name, value)) =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setNeq(binaryExpr)
        }

      case sources.EqualNullSafe(name, value) =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setEqNullSafe(binaryExpr)
        }

      case sources.Not(sources.EqualNullSafe(name, value)) =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setNeqNullSafe(binaryExpr)
        }

      case sources.LessThan(name, value) if value != null =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setLt(binaryExpr)
        }

      case sources.LessThanOrEqual(name, value) if value != null =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setLtEq(binaryExpr)
        }

      case sources.GreaterThan(name, value) if value != null =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setGt(binaryExpr)
        }

      case sources.GreaterThanOrEqual(name, value) if value != null =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setGtEq(binaryExpr)
        }

      case sources.And(lhs, rhs) =>
        (createNativeFilter(lhs, dataSchema), createNativeFilter(rhs, dataSchema)) match {
          case (Some(leftExpr), Some(rightExpr)) =>
            Some(
              createBinaryExpr(
                leftExpr,
                rightExpr,
                (builder, binaryExpr) => builder.setAnd(binaryExpr)))
          case _ => None
        }

      case sources.Or(lhs, rhs) =>
        (createNativeFilter(lhs, dataSchema), createNativeFilter(rhs, dataSchema)) match {
          case (Some(leftExpr), Some(rightExpr)) =>
            Some(
              createBinaryExpr(
                leftExpr,
                rightExpr,
                (builder, binaryExpr) => builder.setOr(binaryExpr)))
          case _ => None
        }

      case sources.Not(pred) =>
        val childExpr = createNativeFilter(pred, dataSchema)
        childExpr.map { expr =>
          createUnaryExpr(expr, (builder, unaryExpr) => builder.setNot(unaryExpr))
        }

      case sources.In(name, values) if values.nonEmpty =>
        createNameExpr(name, dataSchema).flatMap { case (dataType, nameExpr) =>
          val valueExprs = values.flatMap(createValueExpr(_, dataType))
          if (valueExprs.length != values.length) {
            None
          } else {
            val builder = ExprOuterClass.In.newBuilder()
            builder.setInValue(nameExpr)
            builder.addAllLists(valueExprs.toSeq.asJava)
            builder.setNegated(false)
            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setIn(builder)
                .build())
          }
        }

      case sources.StringStartsWith(attribute, prefix) =>
        val attributeExpr = createNameExpr(attribute, dataSchema)
        val prefixExpr = attributeExpr.flatMap { case (dataType, _) =>
          createValueExpr(prefix, dataType)
        }
        scalarFunctionExprToProto("starts_with", Some(attributeExpr.get._2), prefixExpr)

      case sources.StringEndsWith(attribute, suffix) =>
        val attributeExpr = createNameExpr(attribute, dataSchema)
        val suffixExpr = attributeExpr.flatMap { case (dataType, _) =>
          createValueExpr(suffix, dataType)
        }
        scalarFunctionExprToProto("ends_with", Some(attributeExpr.get._2), suffixExpr)

      case sources.StringContains(attribute, value) =>
        val attributeExpr = createNameExpr(attribute, dataSchema)
        val valueExpr = attributeExpr.flatMap { case (dataType, _) =>
          createValueExpr(value, dataType)
        }
        scalarFunctionExprToProto("contains", Some(attributeExpr.get._2), valueExpr)

      case _ => None
    }
  }
}
