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

import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, WriteDelta}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.{DELETE, MERGE, UPDATE}

/** Spark 4.2 exposes the row-level fields directly; keep this extraction fully typed. */
private[iceberg] object IcebergDeltaLogicalFieldsShim extends IcebergDeltaLogicalFieldsShimApi {
  override def extract(plan: LogicalPlan): Option[DeltaLogicalFields] = plan match {
    case delta: WriteDelta =>
      val command = delta.operation.command match {
        case DELETE => Some(DeltaDelete)
        case UPDATE => Some(DeltaUpdate)
        case MERGE => Some(DeltaMerge)
        case _ => None
      }
      command.map { value =>
        val relation: NamedRelation = delta.originalTable
        DeltaLogicalFields(
          query = delta.query,
          originalTable = relation,
          projections = delta.projections,
          write = delta.write,
          command = Some(value))
      }
    case _ => None
  }
}
