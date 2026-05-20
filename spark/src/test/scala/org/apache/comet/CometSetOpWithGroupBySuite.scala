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

package org.apache.comet

import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.comet.CometUnionExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

/**
 * Regression test for issue #4122: on Spark 4.1 (SPARK-52921), EXCEPT ALL / INTERSECT ALL whose
 * sides are themselves GROUP BY aggregates are lowered to a plan where the union inherits a hash
 * partitioning from its shuffled children, so the downstream final aggregate skips its shuffle.
 * If Comet's columnar Union concatenates partitions it breaks that partitioning invariant and the
 * resulting sums/counts collapse two sides into the wrong partitions.
 */
class CometSetOpWithGroupBySuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("issue #4122: EXCEPT ALL with GROUP BY under both sides") {
    withTempView("tab3", "tab4") {
      sql("""CREATE TEMPORARY VIEW tab3 AS SELECT * FROM VALUES
            |  (1, 2), (1, 2), (1, 3), (2, 3), (2, 2) AS tab3(k, v)""".stripMargin)
      sql("""CREATE TEMPORARY VIEW tab4 AS SELECT * FROM VALUES
            |  (1, 2), (2, 3), (2, 2), (2, 2), (2, 20) AS tab4(k, v)""".stripMargin)

      val df = sql("""SELECT v FROM tab3 GROUP BY v
                     |EXCEPT ALL
                     |SELECT k FROM tab4 GROUP BY k""".stripMargin)
      checkSparkAnswer(df)
      assertContainsCometUnion(df)
    }
  }

  test("issue #4122: INTERSECT ALL with GROUP BY under both sides") {
    withTempView("tab1", "tab2") {
      sql("""CREATE TEMPORARY VIEW tab1 AS SELECT * FROM VALUES
            |  (1, 2), (1, 2), (1, 3), (1, 3), (2, 3),
            |  (CAST(null AS INT), CAST(null AS INT)),
            |  (CAST(null AS INT), CAST(null AS INT)) AS tab1(k, v)""".stripMargin)
      sql("""CREATE TEMPORARY VIEW tab2 AS SELECT * FROM VALUES
            |  (1, 2), (1, 2), (2, 3), (3, 4),
            |  (CAST(null AS INT), CAST(null AS INT)),
            |  (CAST(null AS INT), CAST(null AS INT)) AS tab2(k, v)""".stripMargin)

      val df = sql("""SELECT v FROM tab1 GROUP BY v
                     |INTERSECT ALL
                     |SELECT k FROM tab2 GROUP BY k""".stripMargin)
      checkSparkAnswer(df)
      assertContainsCometUnion(df)
    }
  }

  private def assertContainsCometUnion(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    val found = collectFirst(plan) { case u: CometUnionExec => u }
    assert(found.isDefined, s"Expected CometUnionExec in plan but found none:\n$plan")
  }

  test("UNION ALL with checkSparkAnswerAndOperator") {
    withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      withTempView("u1", "u2") {
        sql("""CREATE TEMPORARY VIEW u1 AS SELECT * FROM VALUES
              |  (1, 'a'), (2, 'b'), (3, 'c') AS u1(id, name)""".stripMargin)
        sql("""CREATE TEMPORARY VIEW u2 AS SELECT * FROM VALUES
              |  (4, 'd'), (5, 'e'), (6, 'f') AS u2(id, name)""".stripMargin)

        val df = sql("SELECT id, name FROM u1 UNION ALL SELECT id, name FROM u2")
        checkSparkAnswerAndOperator(df, includeClasses = Seq(classOf[CometUnionExec]))
      }
    }
  }

  test("UNION ALL with SinglePartition (coalesce(1) children)") {
    withSQLConf(
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      withTempView("s1", "s2") {
        sql("""CREATE TEMPORARY VIEW s1 AS SELECT * FROM VALUES
              |  (10), (20), (30) AS s1(x)""".stripMargin)
        sql("""CREATE TEMPORARY VIEW s2 AS SELECT * FROM VALUES
              |  (40), (50) AS s2(x)""".stripMargin)

        val df = sql("""SELECT * FROM (SELECT sum(x) as total FROM s1)
            |UNION ALL
            |SELECT * FROM (SELECT sum(x) as total FROM s2)""".stripMargin)
        checkSparkAnswer(df)
        assertContainsCometUnion(df)
      }
    }
  }
}
