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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

class CostBasedOptimizerSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val dataGen = DataGenerator.DEFAULT

  test("tbd") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_CBO_ENABLED.key -> "true",
      CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true") {
      val table = "t1"
      withTable(table, "t2") {
        sql(s"create table t1(col string, a int, b float) using parquet")
        sql(s"create table t2(col string, a int, b float) using parquet")
        val tableSchema = spark.table(table).schema
        val rows = dataGen.generateRows(
          1000,
          tableSchema,
          Some(() => dataGen.generateString("tbd:", 6)))
        val data = spark.createDataFrame(spark.sparkContext.parallelize(rows), tableSchema)
        data.write
          .mode("append")
          .insertInto(table)
        data.write
          .mode("append")
          .insertInto("t2")
        val x = checkSparkAnswer/*AndOperator*/("select t1.col as x " +
          "from t1 join t2 on cast(t1.col as timestamp) = cast(t2.col as timestamp) " +
          "order by x")

        // TODO assert that we fell back for whole plan
        println(x._1)
        println(x._2)

        assert(!x._2.toString().contains("CometSortExec"))
      }
    }
  }

}
