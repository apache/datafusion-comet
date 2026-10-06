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

package org.apache.comet.codegen

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.catalyst.expressions.{Expression, Nondeterministic, UnaryExpression, Unevaluable}
import org.apache.spark.sql.types.DataType

/**
 * Ships one occurrence of a non-deterministic `child` to the dispatcher. The dispatcher caches a
 * kernel instance per serialized expression, and a non-deterministic kernel keeps state across
 * batches (`monotonically_increasing_id`'s counter, `rand`'s generator). Two identical
 * occurrences in one plan bind to the same ordinals and serialize to the same bytes, so without a
 * distinct `occurrence` the second would continue the first one's state, where Spark gives each
 * its own. The dispatcher unwraps this before it compiles `child`, so it never reaches generated
 * code.
 */
private[comet] case class DispatchOccurrence(child: Expression, occurrence: Long)
    extends UnaryExpression
    with Unevaluable {
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override protected def withNewChildInternal(newChild: Expression): DispatchOccurrence =
    copy(child = newChild)
}

private[comet] object DispatchOccurrence {
  private val nextOccurrence = new AtomicLong()

  /**
   * `expr` as the dispatcher should receive it: tagged when it keeps per-occurrence state.
   * Deterministic trees are returned as is, so identical ones keep sharing one compiled kernel.
   */
  def tag(expr: Expression): Expression =
    if (expr.exists(_.isInstanceOf[Nondeterministic])) {
      DispatchOccurrence(expr, nextOccurrence.getAndIncrement())
    } else {
      expr
    }

  /** The expression to compile from what the dispatcher received. */
  def untag(expr: Expression): Expression = expr match {
    case DispatchOccurrence(child, _) => child
    case other => other
  }
}
