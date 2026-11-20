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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.connector.write.Write

sealed trait DeltaCommand extends Product with Serializable
case object DeltaDelete extends DeltaCommand
case object DeltaUpdate extends DeltaCommand
case object DeltaMerge extends DeltaCommand

case class DeltaLogicalFields(
    query: LogicalPlan,
    originalTable: NamedRelation,
    projections: WriteDeltaProjections,
    write: Option[Write],
    command: Option[DeltaCommand] = None)

private[iceberg] trait IcebergDeltaLogicalShimApi {
  def extract(plan: LogicalPlan): Option[DeltaLogicalFields]
}

/** Version-specific extraction of Spark's public WriteDelta logical node. */
private[iceberg] trait IcebergDeltaLogicalFieldsShimApi {
  def extract(plan: LogicalPlan): Option[DeltaLogicalFields]
}
