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

package org.apache.comet.iceberg

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.comet.IcebergWriteExec
import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.types.BinaryType

/** Logical anchor for the writer. See `IcebergWriteStrategy` for the rationale. */
case class IcebergWriteLogical(
    child: LogicalPlan,
    // Driver-side only: AQE re-planning is driver-local and write commands aren't cached.
    @transient batchWrite: BatchWrite,
    @transient write: Write,
    replaceDataDispatch: Option[ReplaceDataDispatchInfo] = None)
    extends UnaryNode {

  // Owns the commit-message attribute so the physical writer keeps the same exprId across
  // AQE re-plans.
  override val output: Seq[Attribute] = Seq(
    AttributeReference(IcebergWriteExec.CommitMessageColumn, BinaryType, nullable = false)())

  override protected def withNewChildInternal(newChild: LogicalPlan): IcebergWriteLogical =
    copy(child = newChild)
}
