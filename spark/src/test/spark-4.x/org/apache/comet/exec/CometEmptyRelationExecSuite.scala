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

package org.apache.comet.exec

import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.comet.{CometBroadcastHashJoinExec, CometEmptyRelationExec, CometHashAggregateExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometEmptyRelationExecSuite extends CometTestBase {

  test("EmptyRelationExec discovered by AQE feeds native aggregates and Spark existence joins") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_CONVERT_FROM_SPARK_PLAN_ENABLED.key -> "false") {
      withParquetTable(Seq((1, 2), (2, 3)), "aqe_empty_input") {
        // Retain Spark's Range input so AQE can infer emptiness from its completed shuffle.
        // After replacement, the supported parents must start native execution at the new leaf.
        val empty = "(SELECT CAST(id % 2 AS INT) AS k, sum(id) AS v FROM range(0, 10, 1, 2) " +
          "WHERE id < 0 GROUP BY id % 2)"
        val aggregate = s"SELECT count(*), sum(v) FROM $empty"
        val (_, aggregatePlan) = checkSparkAnswer(aggregate)
        checkAnswer(sql(aggregate), Seq(Row(0L, null)))
        assert(
          collect(aggregatePlan) { case e: CometEmptyRelationExec => e }.nonEmpty,
          aggregatePlan.toString)
        assert(
          collect(aggregatePlan) { case a: CometHashAggregateExec => a }.nonEmpty,
          aggregatePlan.toString)

        // Existence joins retain Spark's fallback and preserve probe rows with false markers.
        val existence = "SELECT l._1, EXISTS (SELECT /*+ BROADCAST(r) */ 1 FROM " +
          s"$empty r WHERE r.k = l._1) AS matched FROM aqe_empty_input l"
        val (_, joinPlan) = checkSparkAnswer(existence)
        checkAnswer(sql(existence), Seq(Row(1, false), Row(2, false)))
        assert(
          collect(joinPlan) { case e: CometEmptyRelationExec => e }.nonEmpty,
          joinPlan.toString)
        assert(
          collect(joinPlan) { case j: BroadcastHashJoinExec => j }.nonEmpty,
          joinPlan.toString)
        assert(collect(joinPlan) { case j: CometBroadcastHashJoinExec => j }.isEmpty)
      }
    }
  }
}
