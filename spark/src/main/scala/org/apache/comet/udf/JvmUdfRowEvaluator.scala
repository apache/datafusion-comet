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

package org.apache.comet.udf

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.comet.execution.arrow.CometArrowConverters
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometArrowAllocator
import org.apache.comet.util.ClassLoaders
import org.apache.comet.vector.CometVector

/**
 * Evaluates a vectorized UDF on one row, for the places Spark evaluates a call itself rather than
 * Comet: an operator Comet does not run, an expression the codegen dispatcher runs, and the few
 * places Spark evaluates expressions while planning. It writes the arguments into one-row Arrow
 * vectors, calls the UDF with `numRows = 1`, and reads the single result back.
 *
 * The UDF instance is created on first use and kept for the life of this evaluator, which belongs
 * to one copy of the expression.
 */
private[udf] final class JvmUdfRowEvaluator(
    className: String,
    argumentTypes: Seq[DataType],
    returnType: DataType) {

  private lazy val udf: CometUDF =
    ClassLoaders
      .loadClass(className)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[CometUDF]

  private val argumentSchema =
    StructType(argumentTypes.zipWithIndex.map { case (t, i) => StructField(s"arg$i", t) })

  // The Arrow field the result must match, as `JvmScalarUdfExpr` checks on the native path.
  private val returnField = Utils.toArrowField("result", returnType, nullable = true, "UTC")

  def evaluate(arguments: InternalRow): Any = {
    val batch = CometArrowConverters
      .rowToArrowBatchIter(
        Iterator.single(arguments),
        argumentSchema,
        1,
        "UTC",
        CometArrowAllocator)
      .next()
    try {
      val inputs = Array.tabulate[ValueVector](batch.numCols()) { i =>
        batch.column(i).asInstanceOf[CometVector].getValueVector
      }
      val result = udf.evaluate(inputs, 1)
      try {
        read(result)
      } finally {
        // An input is closed with the batch.
        if (!inputs.exists(_ eq result)) {
          result.close()
        }
      }
    } finally {
      batch.close()
    }
  }

  private def read(result: ValueVector): Any = {
    if (result.getValueCount != 1) {
      throw new IllegalStateException(
        s"CometUDF $className returned ${result.getValueCount} rows, expected 1")
    }
    if (!matches(result.getField, returnField, nameMatters = false)) {
      throw new IllegalStateException(
        s"CometUDF $className returned ${result.getField} but its declared return type is " +
          s"$returnField")
    }
    val row = new ColumnarBatch(Array(CometVector.getVector(result, null)), 1).getRow(0)
    // Copied, because the vectors the row reads are closed once this returns.
    row.copy().get(0, returnType)
  }

  /**
   * Whether `actual` can be read as `expected`, under the rules the native result check applies:
   * the Arrow types match, except that a `Null` type stands for any type, and nested nullability
   * and the names of a list's element and a map's entries, key and value do not matter. A
   * struct's field names do.
   */
  private def matches(
      actual: Field,
      expected: Field,
      nameMatters: Boolean,
      mapEntries: Boolean = false): Boolean =
    actual.getType.isInstanceOf[ArrowType.Null] || {
      val actualChildren = actual.getChildren.asScala
      val expectedChildren = expected.getChildren.asScala
      actual.getType == expected.getType &&
      (!nameMatters || actual.getName == expected.getName) &&
      actualChildren.size == expectedChildren.size &&
      actualChildren.zip(expectedChildren).forall { case (a, e) =>
        expected.getType match {
          case _: ArrowType.Map => matches(a, e, nameMatters = false, mapEntries = true)
          case _: ArrowType.Struct => matches(a, e, nameMatters = !mapEntries)
          case _ => matches(a, e, nameMatters = false)
        }
      }
    }
}
