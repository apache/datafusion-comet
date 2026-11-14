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
import org.apache.spark.sql.DataFrame
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
class CometNativeExecutorsPlanSuite extends CometNativeExecutorsTestBase {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set(CometConf.COMET_SHUFFLE_MODE.key, "native")
    conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "4")
    // Without AQE, the plan is final once it is planned.
    conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    conf
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
      def plans(query: QueryRequest): String =
        query.getStagesList.asScala.map(_.getPlan.toString).mkString
      val query = CometNativeExecutors.buildQuery(plan)
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

  test("a scan that stays on the JVM is refused, naming the operator that reads from it") {
    withTable { path =>
      // The scan stays in Spark, and Comet converts its columnar batches to Arrow batches for the
      // operators above it.
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
        val message = refusal(spark.read.parquet(path).selectExpr("id + 1 AS i"))
        assert(message.contains("CometSparkColumnarToColumnar"), message)
        assert(message.contains("do not support it yet"), message)
      }
    }
  }

  test("a scan with dynamic partition pruning is refused, naming the scan") {
    withTempDir { dir =>
      val factPath = s"${dir.getAbsolutePath}/fact.parquet"
      val dimPath = s"${dir.getAbsolutePath}/dim.parquet"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        spark
          .range(0, 100)
          .selectExpr("id AS fact_id", "date_add(date'2020-01-01', cast(id % 10 AS int)) AS d")
          .write
          .partitionBy("d")
          .parquet(factPath)
        spark
          .range(0, 10)
          .selectExpr("id AS dim_id", "date_add(date'2020-01-01', cast(id AS int)) AS dim_d")
          .write
          .parquet(dimPath)
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        spark.read.parquet(factPath).createOrReplaceTempView("dpp_fact")
        spark.read.parquet(dimPath).createOrReplaceTempView("dpp_dim")
        val message =
          refusal(sql("select * from dpp_fact join dpp_dim on d = dim_d where dim_id > 7"))
        assert(message.contains("CometNativeScan"), message)
        assert(message.contains("dynamic partition pruning"), message)
      }
    }
  }

  test("explain shows the plan the cluster runs under the leaf") {
    withTable { path =>
      val plan = cometPlan(spark.read.parquet(path).groupBy("k").agg(sum("v")))
      val exec = CometNativeExecutorsExec(plan.output, plan, "http://localhost:1")
      val explained = exec.treeString
      assert(explained.contains("CometNativeExecutors"), explained)
      assert(explained.contains("CometHashAggregate"), explained)
      assert(explained.contains("CometNativeScan"), explained)
    }
  }
}
