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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

/**
 * A call to a native UDF registered through [[CometNativeUDF]]. The function `register` installs
 * in the session builds one for each call it resolves.
 *
 * Comet replaces it with a `NativeScalarUdf` that runs the function named `name` in the library
 * at `libraryPath`. Spark has no way to evaluate it, so `eval` throws, which makes a fallback
 * visible rather than silent. Spark evaluates a call where Comet does not take the operator
 * holding it, and while planning in a few places: over local data, in a filter on partition
 * columns, and to sample the keys of a global sort.
 *
 * `argumentTypes` is the registered signature, one type per child. Spark's analyzer checks each
 * call against it, disregarding nullability, and inserts no casts.
 */
case class NativeUdfCall(
    name: String,
    libraryPath: String,
    argumentTypes: Seq[DataType],
    dataType: DataType,
    udfDeterministic: Boolean,
    children: Seq[Expression])
    extends Expression
    with ExpectsInputTypes
    with CodegenFallback {

  override def inputTypes: Seq[DataType] = argumentTypes

  override def nullable: Boolean = true

  override lazy val deterministic: Boolean =
    udfDeterministic && children.forall(_.deterministic)

  override def eval(input: InternalRow): Any = throw new CometUdfNotEvaluatedException(name)

  override def prettyName: String = name

  override def toString: String = s"$name(${children.mkString(", ")})"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}
