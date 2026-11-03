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

package org.apache.spark.sql.comet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Generator, GenericInternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

/** Catalyst generators for tests, registered as SQL functions. */
object CometTestGenerators {
  def register(spark: SparkSession, name: String, build: Seq[Expression] => Generator): Unit =
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier(name),
      new ExpressionInfo(name, name),
      (children: Seq[Expression]) => build(children))

  def registerReusedBuffer(spark: SparkSession, name: String): Unit =
    register(spark, name, ReusedBufferGenerator(_))
}

/**
 * Emits the bytes of its argument three times, `x1`, `x2` and `x3`, through one row and one byte
 * array it overwrites between rows, as a generator that avoids allocating a row per output does.
 * Only a copy of the bytes made before the iterator advances keeps the three rows apart.
 */
case class ReusedBufferGenerator(children: Seq[Expression])
    extends Generator
    with CodegenFallback {

  override def elementSchema: StructType = StructType(Seq(StructField("b", BinaryType)))

  override def eval(input: InternalRow): Iterator[InternalRow] = {
    val prefix = children.head.eval(input) match {
      case null => Array.empty[Byte]
      case bytes: Array[Byte] => bytes
    }
    val buffer = new Array[Byte](prefix.length + 1)
    prefix.copyToArray(buffer)
    val row = new GenericInternalRow(Array[Any](buffer))
    Iterator.tabulate(3) { i =>
      buffer(buffer.length - 1) = ('1' + i).toByte
      row
    }
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ReusedBufferGenerator = copy(children = newChildren)
}
