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

package org.apache.spark.sql.comet.shims

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.execution.datasources.LogicalRelation

object ShimCometTPCQueryBenchmarkBase {
  def collectQueryRelations(plan: LogicalPlan): Set[String] = {
    val queryRelations = scala.collection.mutable.HashSet[String]()
    plan.foreach {
      case SubqueryAlias(alias, _: LogicalRelation) =>
        queryRelations.add(alias.name)
      case LogicalRelation(_, _, Some(catalogTable), _) =>
        queryRelations.add(catalogTable.identifier.table)
      case HiveTableRelation(tableMeta, _, _, _, _) =>
        queryRelations.add(tableMeta.identifier.table)
      case _ =>
    }
    queryRelations.toSet
  }
}
