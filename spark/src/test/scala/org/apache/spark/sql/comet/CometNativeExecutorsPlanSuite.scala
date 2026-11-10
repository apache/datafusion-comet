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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.execution.{InputAdapter, ScalarSubquery, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometRuntimeException}
import org.apache.comet.serde.NativeExecutors.{QueryRequest, Stage}

/**
 * Plans queries for Comet Native Executors (see
 * https://github.com/apache/datafusion-comet/issues/7288) without a cluster. The tests take the
 * plan Comet would execute and turn it into the stage graph a cluster runs, so they need neither
 * a libcomet built with the `native-executors` feature nor the cluster binaries.
 * `CometNativeExecutorsSuite` runs the queries on a cluster.
 */
class CometNativeExecutorsPlanSuite extends CometTestBase {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set(CometConf.COMET_SHUFFLE_MODE.key, "native")
    conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "4")
    // Without AQE, the plan is final once it is planned.
    conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    conf
  }

  /** A Parquet table with several files, read as several partitions. */
  private def withTable(body: String => Unit): Unit = withTempPath { dir =>
    spark
      .range(0, 10000, 1, 8)
      .selectExpr("id", "id % 7 AS k", "CAST(id AS DOUBLE) * 1.5 AS v")
      .write
      .parquet(dir.getCanonicalPath)
    withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> "16384") {
      body(dir.getCanonicalPath)
    }
  }

  /**
   * The plan Comet would execute, as the planner sees it before Spark adds whole-stage codegen.
   */
  private def cometPlan(df: DataFrame): SparkPlan = df.queryExecution.executedPlan match {
    case codegen: WholeStageCodegenExec =>
      codegen.child.transform { case adapter: InputAdapter => adapter.child }
    case plan => plan
  }

  private def stagesOf(df: DataFrame): Seq[Stage] =
    CometNativeExecutors.buildQuery(cometPlan(df)).getStagesList.asScala.toSeq

  private def refusal(df: DataFrame): String =
    intercept[CometRuntimeException](CometNativeExecutors.validate(cometPlan(df))).getMessage

  test("an aggregate is a scan stage and a stage that reads its hash exchange") {
    withTable { path =>
      val stages = stagesOf(spark.read.parquet(path).groupBy("k").agg(sum("v")))
      assert(stages.size == 2)
      val Seq(scan, aggregate) = stages
      // The scan stage runs one copy of its plan for each partition, naming that partition's files.
      assert(scan.getInputsCount == 0)
      assert(scan.getPartitionPlansCount > 1)
      assert(scan.getNumPartitions == scan.getPartitionPlansCount)
      assert(aggregate.getInputsCount == 1)
      val input = aggregate.getInputs(0)
      assert(input.getProducer == 0)
      assert(input.getPartitioning.hasHashPartition)
      assert(input.getPartitioning.getHashPartition.getNumPartitions == 4)
    }
  }

  test("a global aggregate reads a single-partition exchange") {
    withTable { path =>
      val stages = stagesOf(spark.read.parquet(path).agg(sum("id")))
      assert(stages.size == 2)
      assert(stages.last.getInputs(0).getPartitioning.hasSinglePartition)
    }
  }

  test("a broadcast join reads the build side whole") {
    withTable { path =>
      val left = spark.read.parquet(path).select("id", "k")
      val right = spark.read.parquet(path).where("id < 10").select(col("k").as("k2"), col("v"))
      val stages = stagesOf(left.join(right, left("k") === right("k2")).select("id", "v"))
      assert(stages.flatMap(_.getInputsList.asScala).exists(_.getBroadcast), stages)
    }
  }

  test("a shuffled join reads both sides' hash exchanges") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true") {
      withTable { path =>
        val left = spark.read.parquet(path).select("id", "k")
        val right = spark.read.parquet(path).select(col("k").as("k2"), col("v"))
        val stages = stagesOf(left.join(right, left("k") === right("k2")).select("id", "v"))
        val join = stages.last
        assert(join.getInputsCount == 2)
        assert(join.getInputsList.asScala.forall(_.getPartitioning.hasHashPartition))
      }
    }
  }

  test("a scan that lists no files gets one empty partition") {
    withTempPath { dir =>
      spark
        .range(0, 100)
        .selectExpr("id", "id % 7 AS k")
        .write
        .partitionBy("k")
        .parquet(dir.getCanonicalPath)
      val df = spark.read.parquet(dir.getCanonicalPath).where("k = 99").groupBy().count()
      val scan = stagesOf(df).head
      assert(scan.getPartitionPlansCount == 1)
      assert(scan.getNumPartitions == 1)
    }
  }

  test("a scalar subquery's result replaces it in the plan") {
    withTable { path =>
      spark.read.parquet(path).createOrReplaceTempView("t")
      val df = sql("select id from t where v > (select avg(v) from t where k = 3)")
      // Spark runs the subquery before it executes the plan.
      df.collect()
      val plan = cometPlan(df)
      val subqueries = CometNativeExecutors.scalarSubqueries(plan).collect {
        case sub: ScalarSubquery => sub
      }
      assert(subqueries.size == 1)
      def plans(query: Array[Byte]): String =
        QueryRequest.parseFrom(query).getStagesList.asScala.map(_.getPlan.toString).mkString
      val query = CometNativeExecutors.buildQuery(plan).toByteArray
      assert(plans(query).contains("subquery"))
      val rewritten = CometNativeExecutors.withSubqueryResults(query, subqueries)
      assert(!plans(rewritten).contains("subquery"))
    }
  }

  test("a query Comet does not execute runs in Spark") {
    withSQLConf(
      CometConf.COMET_NATIVE_EXECUTORS_ENABLED.key -> "true",
      CometConf.COMET_ENABLED.key -> "false") {
      val df = spark.range(10).selectExpr("id + 1 AS i")
      assert(df.collect().map(_.getLong(0)).toSeq == (1L to 10L))
      assert(df.queryExecution.executedPlan.collect { case e: CometNativeExecutorsExec =>
        e
      }.isEmpty)
    }
  }

  test("a sort that stays in Spark is refused, naming the operator") {
    withTable { path =>
      withSQLConf(CometConf.COMET_EXEC_SORT_ENABLED.key -> "false") {
        val message = refusal(spark.read.parquet(path).orderBy("v"))
        assert(message.contains("Comet Native Executors cannot run this query"), message)
        assert(message.contains("Sort"), message)
      }
    }
  }

  test("a hash exchange on an expression is refused, naming the exchange") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true") {
      withTable { path =>
        val left = spark.read.parquet(path).select("id", "k")
        val right = spark.read.parquet(path).select(col("k").as("k2"), col("v"))
        val message = refusal(left.join(right, left("k") + 1 === right("k2")))
        assert(message.contains("Exchange"), message)
        assert(message.contains("is not a column"), message)
      }
    }
  }
}
