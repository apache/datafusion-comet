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

import java.sql.Date
import java.time.{Duration, Period}

import scala.util.Random

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStatistics, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression, ExpressionInfo, Hex, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateMode, BloomFilterAggregate}
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.reuse.ReuseExchangeAndSubquery
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.SESSION_LOCAL_TIMEZONE
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.{CometConf, CometExecIterator, ExtendedExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark40Plus, isSpark41Plus, isSpark42Plus}
import org.apache.comet.serde.Config.ConfigMap
import org.apache.comet.testing.{DataGenOptions, ParquetGenerator, SchemaGenOptions}

class CometExecSuite extends CometTestBase {

  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_AUTO) {
        testFun
      }
    }
  }

  test("SQLConf serde") {

    def roundtrip = {
      val protobuf = CometExecIterator.serializeCometSQLConfs()
      ConfigMap.parseFrom(protobuf)
    }

    // test not setting the config
    val deserialized: ConfigMap = roundtrip
    assert(null == deserialized.getEntriesMap.get(CometConf.COMET_EXPLAIN_NATIVE_ENABLED.key))

    // test explicitly setting the config
    for (value <- Seq("true", "false")) {
      withSQLConf(CometConf.COMET_EXPLAIN_NATIVE_ENABLED.key -> value) {
        val deserialized: ConfigMap = roundtrip
        assert(
          value == deserialized.getEntriesMap.get(CometConf.COMET_EXPLAIN_NATIVE_ENABLED.key))
      }
    }
  }

  test("TopK operator should return correct results on dictionary column with nulls") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTable("test_data") {
        val data = (0 to 8000)
          .flatMap(_ => Seq((1, null, "A"), (2, "BBB", "B"), (3, "BBB", "B"), (4, "BBB", "B")))
        val tableDF = spark.sparkContext
          .parallelize(data, 3)
          .toDF("c1", "c2", "c3")
        tableDF
          .coalesce(1)
          .sortWithinPartitions("c1")
          .writeTo("test_data")
          .using("parquet")
          .create()

        val df = sql("SELECT * FROM test_data ORDER BY c1 LIMIT 3")
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("AQE DPP: fallback avoids inefficient Comet shuffle (#3874)") {
    withTempDir { path =>
      val factPath = s"${path.getAbsolutePath}/fact.parquet"
      val dimPath = s"${path.getAbsolutePath}/dim.parquet"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        val one_day = 24 * 60 * 60000
        val fact = Range(0, 100)
          .map(i => (i, new java.sql.Date(System.currentTimeMillis() + i * one_day), i.toString))
          .toDF("fact_id", "fact_date", "fact_str")
        fact.write.partitionBy("fact_date").parquet(factPath)
        val dim = Range(0, 10)
          .map(i => (i, new java.sql.Date(System.currentTimeMillis() + i * one_day), i.toString))
          .toDF("dim_id", "dim_date", "dim_str")
        dim.write.parquet(dimPath)
      }

      // Force sort-merge join to get a shuffle exchange above the DPP scan
      Seq("parquet").foreach { v1List =>
        withSQLConf(
          SQLConf.USE_V1_SOURCE_LIST.key -> v1List,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          spark.read.parquet(factPath).createOrReplaceTempView("dpp_fact2")
          spark.read.parquet(dimPath).createOrReplaceTempView("dpp_dim2")
          val df =
            spark.sql(
              "select * from dpp_fact2 join dpp_dim2 on fact_date = dim_date where dim_id > 7")
          val (_, cometPlan) = checkSparkAnswer(df)

          // Verify no CometShuffleExchangeExec wraps the DPP stage
          assert(
            !cometPlan.toString().contains("CometColumnarShuffle"),
            "Should not use Comet columnar shuffle for stages with DPP scans")
        }
      }
    }
  }

  test("non-AQE DPP: BHJ works with CometNativeScanExec") {
    withTempDir { path =>
      val factPath = s"${path.getAbsolutePath}/fact.parquet"
      val dimPath = s"${path.getAbsolutePath}/dim.parquet"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        val one_day = 24 * 60 * 60000
        val fact = Range(0, 100)
          .map(i => (i, new java.sql.Date(System.currentTimeMillis() + (i % 10) * one_day)))
          .toDF("fact_id", "fact_date")
        fact.write.partitionBy("fact_date").parquet(factPath)
        val dim = Range(0, 10)
          .map(i => (i, new java.sql.Date(System.currentTimeMillis() + i * one_day)))
          .toDF("dim_id", "dim_date")
        dim.write.parquet(dimPath)
      }

      // AQE off ensures PlanDynamicPruningFilters (non-AQE) creates the DPP filters
      // with SubqueryBroadcastExec, not SubqueryAdaptiveBroadcastExec
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        spark.read.parquet(factPath).createOrReplaceTempView("dpp_fact_bhj")
        spark.read.parquet(dimPath).createOrReplaceTempView("dpp_dim_bhj")
        val df = spark.sql(
          "select * from dpp_fact_bhj join dpp_dim_bhj on fact_date = dim_date where dim_id > 7")
        // Exclude ReusedExchangeExec — it appears inside the DPP subquery after exchange reuse
        val (_, cometPlan) = checkSparkAnswerAndOperator(df, classOf[ReusedExchangeExec])

        val nativeScans = cometPlan.collect { case s: CometNativeScanExec => s }
        assert(nativeScans.nonEmpty, "Expected CometNativeScanExec in plan")

        val dppScans =
          nativeScans.filter(_.partitionFilters.exists(_.isInstanceOf[DynamicPruningExpression]))
        assert(
          dppScans.nonEmpty,
          "Expected at least one CometNativeScanExec with DynamicPruningExpression")

        val infos = new ExtendedExplainInfo().generateExtendedInfo(cometPlan)
        assert(
          !infos.contains("AQE Dynamic Partition Pruning"),
          s"Should not fall back for non-AQE DPP:\n$infos")
      }
    }
  }

  test("non-AQE DPP: SMJ works with CometNativeScanExec") {
    withTempDir { path =>
      val factPath = s"${path.getAbsolutePath}/fact.parquet"
      val dimPath = s"${path.getAbsolutePath}/dim.parquet"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        val one_day = 24 * 60 * 60000
        val fact = Range(0, 100)
          .map(i => (i, new java.sql.Date(System.currentTimeMillis() + (i % 10) * one_day)))
          .toDF("fact_id", "fact_date")
        fact.write.partitionBy("fact_date").parquet(factPath)
        val dim = Range(0, 10)
          .map(i => (i, new java.sql.Date(System.currentTimeMillis() + i * one_day)))
          .toDF("dim_id", "dim_date")
        dim.write.parquet(dimPath)
      }

      // AQE off + broadcast disabled -> SMJ is used. PlanDynamicPruningFilters can't reuse
      // broadcast, so DPP uses SubqueryExec (aggregate) or Literal.TrueLiteral (if
      // onlyInBroadcast). Either way, non-AQE DPP should work natively.
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        spark.read.parquet(factPath).createOrReplaceTempView("dpp_fact_smj")
        spark.read.parquet(dimPath).createOrReplaceTempView("dpp_dim_smj")
        val df = spark.sql(
          "select * from dpp_fact_smj join dpp_dim_smj on fact_date = dim_date where dim_id > 7")
        val (_, cometPlan) = checkSparkAnswerAndOperator(df)

        val nativeScans = cometPlan.collect { case s: CometNativeScanExec => s }
        assert(nativeScans.nonEmpty, "Expected CometNativeScanExec in plan")

        val infos = new ExtendedExplainInfo().generateExtendedInfo(cometPlan)
        assert(
          !infos.contains("AQE Dynamic Partition Pruning"),
          s"Should not fall back for non-AQE DPP:\n$infos")
      }
    }
  }

  test("non-AQE DPP: BHJ reuses broadcast exchange") {
    withTempDir { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        spark
          .range(100)
          .selectExpr(
            "id % 10 as store_id",
            "cast(id * 2 as int) as date_id",
            "cast(id * 3 as int) as product_id",
            "cast(id as int) as units_sold")
          .write
          .partitionBy("store_id")
          .parquet(s"$path/fact")
        spark
          .range(10)
          .selectExpr("cast(id as int) as store_id", "cast(id as string) as country")
          .write
          .parquet(s"$path/dim")
      }

      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
        spark.read.parquet(s"$path/fact").createOrReplaceTempView("fact_reuse")
        spark.read.parquet(s"$path/dim").createOrReplaceTempView("dim_reuse")

        val df = spark.sql("""SELECT f.date_id, f.store_id
            |FROM fact_reuse f JOIN dim_reuse d
            |ON f.store_id = d.store_id
            |WHERE d.country = 'DE'""".stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)

        // DPP subquery should use CometSubqueryBroadcastExec (not SubqueryBroadcastExec)
        val cometSubqueries = collectWithSubqueries(cometPlan) {
          case s: CometSubqueryBroadcastExec => s
        }
        assert(
          cometSubqueries.nonEmpty,
          "Expected CometSubqueryBroadcastExec in plan for exchange reuse")

        // Broadcast exchange should be reused — only one CometBroadcastExchangeExec,
        // the other replaced by ReusedExchangeExec
        val reused = collectWithSubqueries(cometPlan) { case e: ReusedExchangeExec =>
          e
        }
        assert(
          reused.nonEmpty,
          s"Expected ReusedExchangeExec for broadcast exchange reuse:\n${cometPlan.treeString}")

        val broadcasts = collectWithSubqueries(cometPlan) { case e: CometBroadcastExchangeExec =>
          e
        }
        assert(
          broadcasts.size == 1,
          s"Expected exactly 1 CometBroadcastExchangeExec (other reused):\n${cometPlan.treeString}")

        // Verify canonical forms match — this is what ReuseExchangeAndSubquery uses to
        // determine reuse eligibility
        if (reused.nonEmpty && broadcasts.nonEmpty) {
          val reusedChild = reused.head.child
          assert(
            reusedChild.canonicalized == broadcasts.head.canonicalized,
            "ReusedExchangeExec child and CometBroadcastExchangeExec should have same " +
              "canonical form for reuse")
        }
      }
    }
  }

  test("non-AQE DPP: non-atomic type (struct/array) join key") {
    withTempDir { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        spark
          .range(100)
          .selectExpr(
            "cast(id % 10 as int) as store_id",
            "cast(id as int) as date_id",
            "cast(id * 2 as int) as units_sold")
          .write
          .partitionBy("store_id")
          .parquet(s"$path/fact")
        spark
          .range(10)
          .selectExpr("cast(id as int) as store_id", "cast(id as string) as country")
          .write
          .parquet(s"$path/dim")
      }

      Seq("struct", "array").foreach { dataType =>
        withSQLConf(
          SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          spark.read.parquet(s"$path/fact").createOrReplaceTempView("fact_nonatomic")
          spark.read.parquet(s"$path/dim").createOrReplaceTempView("dim_nonatomic")
          val df = spark.sql(s"""SELECT f.date_id, f.store_id FROM fact_nonatomic f
               |JOIN dim_nonatomic d
               |ON $dataType(f.store_id) = $dataType(d.store_id)
               |WHERE d.country = 'DE'""".stripMargin)
          checkSparkAnswer(df)
        }
      }
    }
  }

  // Regression tests for DPP exchange/subquery reuse (from DynamicPartitionPruningSuite)

  /**
   * Asserts common AQE DPP plan-shape expectations. Pass `None` to skip a check. Counts cover the
   * whole plan including subqueries (uses collectWithSubqueries).
   *
   *   - `expectedSABs`: leftover `CometSubqueryAdaptiveBroadcastExec` nodes. After
   *     `CometPlanAdaptiveDynamicPruningFilters` runs, this should always be 0.
   *   - `expectedCometSubqueryBroadcasts`: `CometSubqueryBroadcastExec` count. Non-zero means
   *     broadcast reuse was wired up for native DPP.
   *   - `expectedReusedExchanges`: `ReusedExchangeExec` count. Confirms AQE stageCache matched
   *     the SAB's broadcast to an existing broadcast.
   */
  private def assertAqeDppShape(
      plan: SparkPlan,
      expectedSABs: Int = 0,
      expectedCometSubqueryBroadcasts: Option[Int] = None,
      expectedReusedExchanges: Option[Int] = None): Unit = {
    val remainingSABs = collectWithSubqueries(plan) {
      case s: CometSubqueryAdaptiveBroadcastExec => s
    }
    assert(
      remainingSABs.size == expectedSABs,
      s"Expected $expectedSABs unconverted CometSubqueryAdaptiveBroadcastExec, " +
        s"found ${remainingSABs.size}:\n${plan.treeString}")
    expectedCometSubqueryBroadcasts.foreach { n =>
      val subqueries = collectWithSubqueries(plan) { case s: CometSubqueryBroadcastExec =>
        s
      }
      assert(
        subqueries.size == n,
        s"Expected $n CometSubqueryBroadcastExec, found ${subqueries.size}:" +
          s"\n${plan.treeString}")
    }
    expectedReusedExchanges.foreach { n =>
      val reused = collectWithSubqueries(plan) { case e: ReusedExchangeExec => e }
      assert(
        reused.size == n,
        s"Expected $n ReusedExchangeExec, found ${reused.size}:\n${plan.treeString}")
    }
  }

  private def withDppTables(f: => Unit): Unit = {
    val factData = Seq(
      (1000, 1, 1, 10),
      (1010, 2, 1, 10),
      (1020, 2, 1, 10),
      (1030, 3, 2, 10),
      (1040, 3, 2, 50),
      (1050, 3, 2, 50),
      (1060, 3, 2, 50),
      (1070, 4, 2, 10),
      (1080, 4, 3, 20),
      (1090, 4, 3, 10),
      (1100, 4, 3, 10),
      (1110, 5, 3, 10),
      (1120, 6, 4, 10),
      (1130, 7, 4, 50),
      (1140, 8, 4, 50),
      (1150, 9, 1, 20),
      (1160, 10, 1, 20),
      (1170, 11, 1, 30),
      (1180, 12, 2, 20),
      (1190, 13, 2, 20),
      (1200, 14, 3, 40),
      (1200, 15, 3, 70),
      (1210, 16, 4, 10),
      (1220, 17, 4, 20),
      (1230, 18, 4, 20),
      (1240, 19, 5, 40),
      (1250, 20, 5, 40),
      (1260, 21, 5, 40),
      (1270, 22, 5, 50),
      (1280, 23, 1, 50),
      (1290, 24, 1, 50),
      (1300, 25, 1, 50))

    val storeData = Seq(
      (1, "North-Holland", "NL"),
      (2, "South-Holland", "NL"),
      (3, "Bavaria", "DE"),
      (4, "California", "US"),
      (5, "Texas", "US"),
      (6, "Texas", "US"))

    val storeCode = Seq((1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60))

    import testImplicits._

    withTable("fact_np", "fact_sk", "fact_stats", "dim_stats", "dim_store", "code_stats") {
      factData
        .toDF("date_id", "store_id", "product_id", "units_sold")
        .write
        .format("parquet")
        .saveAsTable("fact_np")
      factData
        .toDF("date_id", "store_id", "product_id", "units_sold")
        .write
        .partitionBy("store_id")
        .format("parquet")
        .saveAsTable("fact_sk")
      factData
        .toDF("date_id", "store_id", "product_id", "units_sold")
        .write
        .partitionBy("store_id")
        .format("parquet")
        .saveAsTable("fact_stats")
      storeData
        .toDF("store_id", "state_province", "country")
        .write
        .format("parquet")
        .saveAsTable("dim_store")
      storeData
        .toDF("store_id", "state_province", "country")
        .write
        .format("parquet")
        .saveAsTable("dim_stats")
      storeCode
        .toDF("store_id", "code")
        .write
        .partitionBy("store_id")
        .format("parquet")
        .saveAsTable("code_stats")
      sql("ANALYZE TABLE fact_stats COMPUTE STATISTICS FOR COLUMNS store_id")
      sql("ANALYZE TABLE dim_stats COMPUTE STATISTICS FOR COLUMNS store_id")
      sql("ANALYZE TABLE dim_store COMPUTE STATISTICS FOR COLUMNS store_id")
      sql("ANALYZE TABLE code_stats COMPUTE STATISTICS FOR COLUMNS store_id")

      f
    }
  }

  test("non-AQE DPP: broadcast exchange reuse") {
    withDppTables {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {

        val df = sql("""SELECT /*+ BROADCAST(f)*/
            |f.date_id, f.store_id, f.product_id, f.units_sold FROM fact_np f
            |JOIN code_stats s
            |ON f.store_id = s.store_id WHERE f.date_id <= 1030""".stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)

        val reusedExchanges = collectWithSubqueries(cometPlan) { case e: ReusedExchangeExec =>
          e
        }
        assert(
          reusedExchanges.nonEmpty,
          s"Expected ReusedExchangeExec for broadcast exchange reuse:\n${cometPlan.treeString}")
      }
    }
  }

  test("non-AQE DPP: subquery reuse with uncorrelated scalar subquery") {
    withDppTables {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {

        val df = sql("""SELECT d.store_id, SUM(f.units_sold),
            |       (SELECT SUM(f.units_sold)
            |        FROM fact_stats f JOIN dim_stats d ON d.store_id = f.store_id
            |        WHERE d.country = 'US') AS total_prod
            |FROM fact_stats f JOIN dim_stats d ON d.store_id = f.store_id
            |WHERE d.country = 'US'
            |GROUP BY 1""".stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)

        val countSubqueryBroadcasts = collectWithSubqueries(cometPlan)({
          case _: SubqueryBroadcastExec => 1
          case _: CometSubqueryBroadcastExec => 1
        }).sum
        val countReusedSubqueryBroadcasts = collectWithSubqueries(cometPlan)({
          case ReusedSubqueryExec(_: SubqueryBroadcastExec) => 1
          case ReusedSubqueryExec(_: CometSubqueryBroadcastExec) => 1
        }).sum

        assert(
          countSubqueryBroadcasts == 1,
          s"Expected 1 subquery broadcast but got $countSubqueryBroadcasts:\n" +
            cometPlan.treeString)
        assert(
          countReusedSubqueryBroadcasts == 1,
          s"Expected 1 reused subquery broadcast but got $countReusedSubqueryBroadcasts:\n" +
            cometPlan.treeString)
      }
    }
  }

  test("non-AQE DPP: non-atomic type (struct/array) join key with withDppTables") {
    withDppTables {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {

        Seq("struct", "array").foreach { dataType =>
          val df =
            sql(s"""SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM fact_stats f
                 |JOIN dim_stats s
                 |ON $dataType(f.store_id) = $dataType(s.store_id) WHERE s.country = 'DE'
               """.stripMargin)
          checkSparkAnswer(df)
        }
      }
    }
  }

  test("non-AQE DPP: non-atomic type uses CometSubqueryBroadcastExec") {
    withDppTables {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {

        Seq("struct", "array").foreach { dataType =>
          val df =
            sql(s"""SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM fact_stats f
                 |JOIN dim_stats s
                 |ON $dataType(f.store_id) = $dataType(s.store_id) WHERE s.country = 'DE'
               """.stripMargin)
          val (_, cometPlan) = checkSparkAnswer(df)

          val cometSubqueries = collectWithSubqueries(cometPlan) {
            case s: CometSubqueryBroadcastExec => s
          }
          assert(
            cometSubqueries.nonEmpty,
            s"Expected DPP with CometSubqueryBroadcastExec for $dataType key:\n" +
              cometPlan.treeString)
        }
      }
    }
  }

  // Reproduces CI failure from DynamicPartitionPruningSuiteV1AEOn SPARK-37995.
  // DynamicPartitionPruningSuiteBase.checkPartitionPruningPredicate (line 233-240) asserts
  // that all non-broadcast subquery plans contain AdaptiveSparkPlanExec when AQE is on.
  test("SPARK-37995: DPP with scalar subquery does not break subquery assertions") {
    withDppTables {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
        val df = sql("""SELECT f.date_id, f.store_id FROM fact_sk f
            |JOIN dim_store s ON f.store_id = s.store_id AND s.country = 'NL'
            |WHERE s.state_province != (SELECT max(state_province) FROM dim_stats)
            |""".stripMargin)
        val (_, plan) = checkSparkAnswer(df)

        val isMainQueryAdaptive = plan.isInstanceOf[AdaptiveSparkPlanExec]
        val dpExprs = flatMap(plan) {
          case s: FileSourceScanExec =>
            s.partitionFilters.collect { case d: DynamicPruningExpression => d.child }
          case s: CometScanExec =>
            s.partitionFilters.collect { case d: DynamicPruningExpression => d.child }
          case s: CometNativeScanExec =>
            s.partitionFilters.collect { case d: DynamicPruningExpression => d.child }
          case _ => Nil
        }
        val subqueryBroadcast = dpExprs.collect {
          case InSubqueryExec(_, b: SubqueryBroadcastExec, _, _, _, _) => b
          case InSubqueryExec(_, b: CometSubqueryBroadcastExec, _, _, _, _) => b
        }
        subqueriesAll(plan).filterNot(subqueryBroadcast.contains).foreach { s =>
          val subquery = s match {
            case r: ReusedSubqueryExec => r.child
            case o => o
          }
          assert(
            subquery.exists(_.isInstanceOf[AdaptiveSparkPlanExec]) == isMainQueryAdaptive,
            s"Subquery ${subquery.getClass.getSimpleName} adaptive mismatch:\n" +
              s"${subquery.treeString}")
        }
      }
    }
  }

  test("non-AQE DPP: two separate broadcast joins") {
    withTempDir { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        spark
          .range(100)
          .selectExpr(
            "cast(id % 5 as int) as store_id",
            "cast(id % 3 as int) as region_id",
            "cast(id as int) as amount")
          .write
          .partitionBy("store_id", "region_id")
          .parquet(s"$path/fact")
        spark
          .range(5)
          .selectExpr("cast(id as int) as store_id", "cast(id as string) as store_name")
          .write
          .parquet(s"$path/store_dim")
        spark
          .range(3)
          .selectExpr("cast(id as int) as region_id", "cast(id as string) as region_name")
          .write
          .parquet(s"$path/region_dim")
      }

      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        spark.read.parquet(s"$path/fact").createOrReplaceTempView("fact_two_joins")
        spark.read.parquet(s"$path/store_dim").createOrReplaceTempView("store_dim")
        spark.read.parquet(s"$path/region_dim").createOrReplaceTempView("region_dim")

        val df = spark.sql("""SELECT f.amount, s.store_name, r.region_name
            |FROM fact_two_joins f
            |JOIN store_dim s ON f.store_id = s.store_id
            |JOIN region_dim r ON f.region_id = r.region_id
            |WHERE s.store_name = '1' AND r.region_name = '2'""".stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)

        val nativeScans = cometPlan.collect { case s: CometNativeScanExec => s }
        assert(nativeScans.nonEmpty, "Expected CometNativeScanExec in plan")

        val dppScans =
          nativeScans.filter(_.partitionFilters.exists(_.isInstanceOf[DynamicPruningExpression]))
        assert(
          dppScans.nonEmpty,
          "Expected at least one CometNativeScanExec with DynamicPruningExpression")
      }
    }
  }

  test("non-AQE DPP: fallback when broadcast exchange is not Comet") {
    withTempDir { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        spark
          .range(100)
          .selectExpr("cast(id % 10 as int) as store_id", "cast(id as int) as amount")
          .write
          .partitionBy("store_id")
          .parquet(s"$path/fact")
        spark
          .range(10)
          .selectExpr("cast(id as int) as store_id", "cast(id as string) as country")
          .write
          .parquet(s"$path/dim")
      }

      // Disable Comet broadcast exchange so SubqueryBroadcastExec wraps a Spark
      // BroadcastExchangeExec. convertSubqueryBroadcasts should skip it (child isn't
      // CometNativeExec). Query should still produce correct results via Spark's standard path.
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_EXEC_BROADCAST_EXCHANGE_ENABLED.key -> "false",
        CometConf.COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED.key -> "false") {
        spark.read.parquet(s"$path/fact").createOrReplaceTempView("fact_fallback")
        spark.read.parquet(s"$path/dim").createOrReplaceTempView("dim_fallback")

        val df = spark.sql("""SELECT f.amount, f.store_id
            |FROM fact_fallback f JOIN dim_fallback d
            |ON f.store_id = d.store_id
            |WHERE d.country = 'DE'""".stripMargin)
        checkSparkAnswer(df)
      }
    }
  }

  test("non-AQE DPP: empty broadcast result") {
    withTempDir { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        spark
          .range(100)
          .selectExpr("cast(id % 10 as int) as store_id", "cast(id as int) as amount")
          .write
          .partitionBy("store_id")
          .parquet(s"$path/fact")
        spark
          .range(10)
          .selectExpr("cast(id as int) as store_id", "cast(id as string) as country")
          .write
          .parquet(s"$path/dim")
      }

      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        spark.read.parquet(s"$path/fact").createOrReplaceTempView("fact_empty")
        spark.read.parquet(s"$path/dim").createOrReplaceTempView("dim_empty")

        // Filter on dim that matches nothing -- DPP prunes all partitions
        val df = spark.sql("""SELECT f.amount, f.store_id
            |FROM fact_empty f JOIN dim_empty d
            |ON f.store_id = d.store_id
            |WHERE d.country = 'NONEXISTENT'""".stripMargin)
        val result = df.collect()
        assert(result.isEmpty, s"Expected empty result but got ${result.length} rows")
        checkSparkAnswer(df)
      }
    }
  }

  test("non-AQE DPP: resolves both outer and inner partition filters") {
    // CometNativeScanExec.partitionFilters and CometScanExec.partitionFilters contain
    // different InSubqueryExec instances. Both must be resolved for partition selection
    // to work correctly. This test verifies correct results, which requires both sets
    // of filters to be resolved.
    withTempDir { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        spark
          .range(100)
          .selectExpr(
            "cast(id % 10 as int) as store_id",
            "cast(id as int) as date_id",
            "cast(id as int) as amount")
          .write
          .partitionBy("store_id")
          .parquet(s"$path/fact")
        spark
          .range(10)
          .selectExpr("cast(id as int) as store_id", "cast(id as string) as country")
          .write
          .parquet(s"$path/dim")
      }

      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
        spark.read.parquet(s"$path/fact").createOrReplaceTempView("fact_dual")
        spark.read.parquet(s"$path/dim").createOrReplaceTempView("dim_dual")

        val df = spark.sql("""SELECT f.date_id, f.store_id
            |FROM fact_dual f JOIN dim_dual d
            |ON f.store_id = d.store_id
            |WHERE d.country = 'DE'""".stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)

        // Verify native scan is used
        val nativeScans = cometPlan.collect { case s: CometNativeScanExec => s }
        assert(nativeScans.nonEmpty, "Expected CometNativeScanExec in plan")

        // Verify DPP is present
        val dppScans =
          nativeScans.filter(_.partitionFilters.exists(_.isInstanceOf[DynamicPruningExpression]))
        assert(dppScans.nonEmpty, "Expected DPP filter on native scan")
      }
    }
  }

  // On 3.5+, CometPlanAdaptiveDynamicPruningFilters converts SABs to
  // CometSubqueryBroadcastExec with broadcast reuse. On 3.4, AQE DPP falls back to Spark.
  test("AQE DPP: BHJ works with CometNativeScanExec") {
    withTempDir { path =>
      val factPath = s"${path.getAbsolutePath}/fact.parquet"
      val dimPath = s"${path.getAbsolutePath}/dim.parquet"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        val one_day = 24 * 60 * 60000
        val fact = Range(0, 100)
          .map(i => (i, new java.sql.Date(System.currentTimeMillis() + (i % 10) * one_day)))
          .toDF("fact_id", "fact_date")
        fact.write.partitionBy("fact_date").parquet(factPath)
        val dim = Range(0, 10)
          .map(i => (i, new java.sql.Date(System.currentTimeMillis() + i * one_day)))
          .toDF("dim_id", "dim_date")
        dim.write.parquet(dimPath)
      }

      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
        spark.read.parquet(factPath).createOrReplaceTempView("aqe_dpp_fact")
        spark.read.parquet(dimPath).createOrReplaceTempView("aqe_dpp_dim")
        val df = spark.sql(
          "select * from aqe_dpp_fact join aqe_dpp_dim on fact_date = dim_date where dim_id > 7")
        val (_, cometPlan) = checkSparkAnswer(df)
        val infos = new ExtendedExplainInfo().generateExtendedInfo(cometPlan)

        if (isSpark35Plus) {
          // Verify native scan with DPP
          val nativeScans = collect(cometPlan) { case s: CometNativeScanExec => s }
          assert(nativeScans.nonEmpty, "Expected CometNativeScanExec in plan")
          val dppScans = nativeScans.filter(
            _.partitionFilters.exists(_.isInstanceOf[DynamicPruningExpression]))
          assert(
            dppScans.nonEmpty,
            "Expected at least one CometNativeScanExec with DynamicPruningExpression")

          // Verify CometSubqueryBroadcastExec with AdaptiveSparkPlanExec child
          // (matches Spark's SubqueryBroadcastExec wrapping an ASPE that goes
          // through AQE stageCache for broadcast reuse via ReusedExchangeExec)
          val cometSubqueries = collectWithSubqueries(cometPlan) {
            case s: CometSubqueryBroadcastExec => s
          }
          assert(
            cometSubqueries.nonEmpty,
            "Expected CometSubqueryBroadcastExec for broadcast reuse")
          cometSubqueries.foreach { csb =>
            assert(
              csb.child.isInstanceOf[AdaptiveSparkPlanExec],
              "Expected AdaptiveSparkPlanExec child but got " +
                s"${csb.child.getClass.getSimpleName}")
          }

          // Verify broadcast reuse: the subquery's ASPE final plan should contain
          // ReusedExchangeExec (AQE stageCache matched the join's broadcast)
          import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
          cometSubqueries.foreach { csb =>
            val aspe = csb.child.asInstanceOf[AdaptiveSparkPlanExec]
            val hasReusedExchange = collect(aspe) { case r: ReusedExchangeExec =>
              r
            }.nonEmpty || collect(aspe) { case b: BroadcastQueryStageExec =>
              b
            }.nonEmpty
            assert(
              hasReusedExchange,
              "DPP subquery's ASPE should contain ReusedExchangeExec or " +
                "BroadcastQueryStageExec for broadcast reuse")
          }

          // Verify no unconverted SABs remain
          assertAqeDppShape(cometPlan)

          // Verify no fallback
          assert(
            !infos.contains("AQE Dynamic Partition Pruning"),
            s"Should not fall back for AQE DPP:\n$infos")
        } else {
          // 3.4: scan falls back to Spark so Spark handles DPP natively
          assert(
            infos.contains("AQE Dynamic Partition Pruning requires Spark 3.5+"),
            s"Expected 3.4 AQE DPP fallback message but got:\n$infos")
        }
      }
    }
  }

  // With Comet BHJ disabled, the join stays as BroadcastHashJoinExec. Our rule finds
  // it and creates SubqueryBroadcastExec (not CometSubqueryBroadcastExec).
  test("AQE DPP: fallback when broadcast exchange is not Comet") {
    withTempDir { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        spark
          .range(100)
          .selectExpr("cast(id % 10 as int) as store_id", "cast(id as int) as amount")
          .write
          .partitionBy("store_id")
          .parquet(s"$path/fact")
        spark
          .range(10)
          .selectExpr("cast(id as int) as store_id", "cast(id as string) as country")
          .write
          .parquet(s"$path/dim")
      }

      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        CometConf.COMET_EXEC_BROADCAST_EXCHANGE_ENABLED.key -> "false",
        CometConf.COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED.key -> "false") {
        spark.read.parquet(s"$path/fact").createOrReplaceTempView("aqe_fact_fallback")
        spark.read.parquet(s"$path/dim").createOrReplaceTempView("aqe_dim_fallback")

        val df = spark.sql("""SELECT f.amount, f.store_id
            |FROM aqe_fact_fallback f JOIN aqe_dim_fallback d
            |ON f.store_id = d.store_id
            |WHERE d.country = 'DE'""".stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)

        // Verify no CometSubqueryBroadcastExec — Spark handles DPP with its own
        // SubqueryBroadcastExec since the join is Spark's BroadcastHashJoinExec
        if (isSpark35Plus) {
          val cometSubqueries = collectWithSubqueries(cometPlan) {
            case s: CometSubqueryBroadcastExec => s
          }
          assert(
            cometSubqueries.isEmpty,
            "Should not have CometSubqueryBroadcastExec when Comet BHJ is disabled")
        }
      }
    }
  }

  // No broadcast to reuse, so DPP falls back to Literal.TrueLiteral.
  test("AQE DPP: SMJ disables DPP gracefully") {
    withTempDir { path =>
      val factPath = s"${path.getAbsolutePath}/fact.parquet"
      val dimPath = s"${path.getAbsolutePath}/dim.parquet"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        val one_day = 24 * 60 * 60000
        val fact = Range(0, 100)
          .map(i => (i, new java.sql.Date(System.currentTimeMillis() + (i % 10) * one_day)))
          .toDF("fact_id", "fact_date")
        fact.write.partitionBy("fact_date").parquet(factPath)
        val dim = Range(0, 10)
          .map(i => (i, new java.sql.Date(System.currentTimeMillis() + i * one_day)))
          .toDF("dim_id", "dim_date")
        dim.write.parquet(dimPath)
      }

      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        spark.read.parquet(factPath).createOrReplaceTempView("aqe_dpp_fact_smj")
        spark.read.parquet(dimPath).createOrReplaceTempView("aqe_dpp_dim_smj")
        val df = spark.sql(
          "select * from aqe_dpp_fact_smj join aqe_dpp_dim_smj " +
            "on fact_date = dim_date where dim_id > 7")
        val (_, cometPlan) = checkSparkAnswer(df)

        if (isSpark35Plus) {
          // Verify native scan is used (DPP disabled via TrueLiteral, but scan still native)
          val nativeScans = collect(cometPlan) { case s: CometNativeScanExec => s }
          assert(nativeScans.nonEmpty, "Expected CometNativeScanExec in plan")

          // No CometSubqueryBroadcastExec (DPP was disabled), no unconverted SABs
          assertAqeDppShape(cometPlan, expectedCometSubqueryBroadcasts = Some(0))

          // Case 2 of CometPlanAdaptiveDynamicPruningFilters: SMJ with REUSE_BROADCAST_ONLY
          // (Spark's default) sets onlyInBroadcast=true on the SAB. With no reusable
          // broadcast, the rule replaces the DPP filter with DynamicPruningExpression(
          // Literal.TrueLiteral). This distinguishes Case 2 from Case 3 (aggregate
          // SubqueryExec), which would appear as a nested SubqueryExec in the filter.
          val trueLiteralFilters = nativeScans.flatMap(_.partitionFilters).collect {
            case DynamicPruningExpression(Literal.TrueLiteral) => true
          }
          assert(
            trueLiteralFilters.nonEmpty,
            "Expected DynamicPruningExpression(TrueLiteral) for onlyInBroadcast=true SMJ, " +
              s"got partitionFilters: ${nativeScans.map(_.partitionFilters).mkString("; ")}")
        }
      }
    }
  }

  // Each DPP filter should match the correct broadcast join by buildKeys exprId.
  test("AQE DPP: two separate broadcast joins") {
    withTempDir { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        spark
          .range(100)
          .selectExpr(
            "cast(id % 5 as int) as store_id",
            "cast(id % 3 as int) as region_id",
            "cast(id as int) as amount")
          .write
          .partitionBy("store_id", "region_id")
          .parquet(s"$path/fact")
        spark
          .range(5)
          .selectExpr("cast(id as int) as store_id", "cast(id as string) as store_name")
          .write
          .parquet(s"$path/store_dim")
        spark
          .range(3)
          .selectExpr("cast(id as int) as region_id", "cast(id as string) as region_name")
          .write
          .parquet(s"$path/region_dim")
      }

      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
        spark.read.parquet(s"$path/fact").createOrReplaceTempView("aqe_fact_two_joins")
        spark.read.parquet(s"$path/store_dim").createOrReplaceTempView("aqe_store_dim")
        spark.read.parquet(s"$path/region_dim").createOrReplaceTempView("aqe_region_dim")

        val df = spark.sql("""SELECT f.amount, s.store_name, r.region_name
            |FROM aqe_fact_two_joins f
            |JOIN aqe_store_dim s ON f.store_id = s.store_id
            |JOIN aqe_region_dim r ON f.region_id = r.region_id
            |WHERE s.store_name = '1' AND r.region_name = '2'""".stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)

        if (isSpark35Plus) {
          val nativeScans = collect(cometPlan) { case s: CometNativeScanExec => s }
          assert(nativeScans.nonEmpty, "Expected CometNativeScanExec in plan")

          val dppScans = nativeScans.filter(
            _.partitionFilters.exists(_.isInstanceOf[DynamicPruningExpression]))
          assert(dppScans.nonEmpty, "Expected DPP filters on native scan")

          // Verify no unconverted SABs
          assertAqeDppShape(cometPlan)
        }
      }
    }
  }

  test("AQE DPP: empty broadcast result") {
    withTempDir { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        spark
          .range(100)
          .selectExpr("cast(id % 10 as int) as store_id", "cast(id as int) as amount")
          .write
          .partitionBy("store_id")
          .parquet(s"$path/fact")
        spark
          .range(10)
          .selectExpr("cast(id as int) as store_id", "cast(id as string) as country")
          .write
          .parquet(s"$path/dim")
      }

      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
        spark.read.parquet(s"$path/fact").createOrReplaceTempView("aqe_fact_empty")
        spark.read.parquet(s"$path/dim").createOrReplaceTempView("aqe_dim_empty")

        val df = spark.sql("""SELECT f.amount, f.store_id
            |FROM aqe_fact_empty f JOIN aqe_dim_empty d
            |ON f.store_id = d.store_id
            |WHERE d.country = 'NONEXISTENT'""".stripMargin)
        val result = df.collect()
        assert(result.isEmpty, s"Expected empty result but got ${result.length} rows")
        checkSparkAnswer(df)
      }
    }
  }

  // Both outer (CometNativeScanExec) and inner (CometScanExec) partition filters must
  // be resolved. Correct results prove both filter sets were converted.
  test("AQE DPP: resolves both outer and inner partition filters") {
    withTempDir { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        spark
          .range(100)
          .selectExpr(
            "cast(id % 10 as int) as store_id",
            "cast(id as int) as date_id",
            "cast(id as int) as amount")
          .write
          .partitionBy("store_id")
          .parquet(s"$path/fact")
        spark
          .range(10)
          .selectExpr("cast(id as int) as store_id", "cast(id as string) as country")
          .write
          .parquet(s"$path/dim")
      }

      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
        spark.read.parquet(s"$path/fact").createOrReplaceTempView("aqe_fact_dual")
        spark.read.parquet(s"$path/dim").createOrReplaceTempView("aqe_dim_dual")

        val df = spark.sql("""SELECT f.date_id, f.store_id
            |FROM aqe_fact_dual f JOIN aqe_dim_dual d
            |ON f.store_id = d.store_id
            |WHERE d.country = '3'""".stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)

        if (isSpark35Plus) {
          val nativeScans = collect(cometPlan) { case s: CometNativeScanExec => s }
          assert(nativeScans.nonEmpty, "Expected CometNativeScanExec in plan")

          val dppScans = nativeScans.filter(
            _.partitionFilters.exists(_.isInstanceOf[DynamicPruningExpression]))
          assert(dppScans.nonEmpty, "Expected DPP filter on native scan")

          // Verify CometSubqueryBroadcastExec is present (not TrueLiteral fallback)
          val cometSubqueries = collectWithSubqueries(cometPlan) {
            case s: CometSubqueryBroadcastExec => s
          }
          assert(
            cometSubqueries.nonEmpty,
            "Expected CometSubqueryBroadcastExec (DPP should be active, not TrueLiteral)")
        }
      }
    }
  }

  // DPP subquery reuses the join's broadcast via AQE stageCache.
  test("AQE DPP: broadcast exchange reuse") {
    withDppTables {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {

        val df = sql("""SELECT /*+ BROADCAST(f)*/
            |f.date_id, f.store_id, f.product_id, f.units_sold FROM fact_np f
            |JOIN code_stats s
            |ON f.store_id = s.store_id WHERE f.date_id <= 1030""".stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)

        if (isSpark35Plus) {
          // Verify no unconverted SABs remain
          assertAqeDppShape(cometPlan)

          // If DPP subqueries are present, verify they use CometSubqueryBroadcastExec
          // with AdaptiveSparkPlanExec children (ASPE wrapping broadcast for stageCache reuse)
          val cometSubqueries = collectWithSubqueries(cometPlan) {
            case s: CometSubqueryBroadcastExec => s
          }
          cometSubqueries.foreach { csb =>
            assert(
              csb.child.isInstanceOf[AdaptiveSparkPlanExec],
              "CometSubqueryBroadcastExec child should be AdaptiveSparkPlanExec, " +
                s"got ${csb.child.getClass.getSimpleName}")
          }
        }
      }
    }
  }

  test("AQE DPP: non-atomic type (struct/array) join key") {
    withTempDir { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        spark
          .range(100)
          .selectExpr(
            "cast(id % 10 as int) as store_id",
            "cast(id as int) as date_id",
            "cast(id * 2 as int) as units_sold")
          .write
          .partitionBy("store_id")
          .parquet(s"$path/fact")
        spark
          .range(10)
          .selectExpr("cast(id as int) as store_id", "cast(id as string) as country")
          .write
          .parquet(s"$path/dim")
      }

      Seq("struct", "array").foreach { dataType =>
        withSQLConf(
          SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
          spark.read.parquet(s"$path/fact").createOrReplaceTempView("aqe_fact_nonatomic")
          spark.read.parquet(s"$path/dim").createOrReplaceTempView("aqe_dim_nonatomic")
          val df = spark.sql(s"""SELECT f.date_id, f.store_id FROM aqe_fact_nonatomic f
               |JOIN aqe_dim_nonatomic d
               |ON $dataType(f.store_id) = $dataType(d.store_id)
               |WHERE d.country = '3'""".stripMargin)
          checkSparkAnswer(df)
        }
      }
    }
  }

  test("AQE DPP: non-atomic type uses CometSubqueryBroadcastExec") {
    withDppTables {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {

        Seq("struct", "array").foreach { dataType =>
          val df =
            sql(s"""SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM fact_stats f
                 |JOIN dim_stats s
                 |ON $dataType(f.store_id) = $dataType(s.store_id) WHERE s.country = 'DE'
               """.stripMargin)
          val (_, cometPlan) = checkSparkAnswer(df)

          if (isSpark35Plus) {
            val cometSubqueries = collectWithSubqueries(cometPlan) {
              case s: CometSubqueryBroadcastExec => s
            }
            assert(
              cometSubqueries.nonEmpty,
              s"Expected DPP with CometSubqueryBroadcastExec for $dataType key:\n" +
                cometPlan.treeString)

            assertAqeDppShape(cometPlan)
          }
        }
      }
    }
  }

  // With onlyInBroadcast=false and exchange reuse disabled, our rule falls through to
  // Case 3 (aggregate SubqueryExec) instead of broadcast reuse or TrueLiteral.
  // Reproduces DynamicPartitionPruningSuite "simple inner join triggers DPP with mock-up tables".
  test("AQE DPP: inner join with broadcast reuse disabled") {
    withDppTables {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
        val df = sql("""SELECT f.date_id, f.store_id FROM fact_sk f
            |JOIN dim_store s ON f.store_id = s.store_id AND s.country = 'NL'""".stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)

        if (isSpark35Plus) {
          val nativeScans = collect(cometPlan) { case s: CometNativeScanExec => s }
          assert(nativeScans.nonEmpty, "Expected CometNativeScanExec in plan")

          assertAqeDppShape(cometPlan)
        }
      }
    }
  }

  // Scan is in a shuffle stage separated from the broadcast join. Cross-stage
  // broadcast search (via context.qe.executedPlan) must find the join.
  test("AQE DPP: avoid reordering broadcast join keys") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("large", "dimTwo", "dimThree") {
        spark
          .range(100)
          .select($"id", ($"id" + 1).as("A"), ($"id" + 2).as("B"))
          .write
          .partitionBy("A")
          .format("parquet")
          .mode("overwrite")
          .saveAsTable("large")

        spark
          .range(10)
          .select($"id", ($"id" + 1).as("C"), ($"id" + 2).as("D"))
          .write
          .format("parquet")
          .mode("overwrite")
          .saveAsTable("dimTwo")

        spark
          .range(10)
          .select($"id", ($"id" + 1).as("E"), ($"id" + 2).as("F"), ($"id" + 3).as("G"))
          .write
          .format("parquet")
          .mode("overwrite")
          .saveAsTable("dimThree")

        val fact = sql("SELECT * from large")
        val dim = sql("SELECT * from dimTwo")
        val prod = sql("SELECT * from dimThree")

        val df = fact
          .join(dim, fact.col("A") === dim.col("C") && fact.col("B") === dim.col("D"), "LEFT")
          .join(
            broadcast(prod),
            fact.col("B") === prod.col("F") && fact.col("A") === prod.col("E"))
          .where(prod.col("G") > 5)

        val (_, cometPlan) = checkSparkAnswer(df)

        if (isSpark35Plus) {
          val dpExprs = flatMap(cometPlan) {
            case s: CometNativeScanExec =>
              s.partitionFilters.collect { case d: DynamicPruningExpression => d.child }
            case _ => Nil
          }
          val hasSubquery = dpExprs.exists {
            case InSubqueryExec(_, _: SubqueryExec, _, _, _, _) => true
            case _ => false
          }
          val hasBroadcast = dpExprs.exists {
            case InSubqueryExec(_, _: SubqueryBroadcastExec, _, _, _, _) => true
            case InSubqueryExec(_, _: CometSubqueryBroadcastExec, _, _, _, _) => true
            case _ => false
          }
          assert(!hasSubquery, "Should not have SubqueryExec DPP")
          assert(hasBroadcast, "Should have broadcast DPP")
        }
      }
    }
  }

  // Cross-plan subquery deduplication via the shared AdaptiveExecutionContext.subqueryCache.
  test("AQE DPP: uncorrelated scalar subquery with broadcast reuse") {
    withDppTables {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
        val df = sql("""
            |SELECT d.store_id,
            |       SUM(f.units_sold),
            |       (SELECT SUM(f.units_sold)
            |        FROM fact_stats f JOIN dim_stats d ON d.store_id = f.store_id
            |        WHERE d.country = 'US') AS total_prod
            |FROM fact_stats f JOIN dim_stats d ON d.store_id = f.store_id
            |WHERE d.country = 'US'
            |GROUP BY 1
          """.stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)

        if (isSpark35Plus) {
          val countBroadcasts = collectWithSubqueries(cometPlan) {
            case _: SubqueryBroadcastExec => 1
            case _: CometSubqueryBroadcastExec => 1
          }.sum
          val countReused = collectWithSubqueries(cometPlan) {
            case ReusedSubqueryExec(_: SubqueryBroadcastExec) => 1
            case ReusedSubqueryExec(_: CometSubqueryBroadcastExec) => 1
          }.sum

          assert(countBroadcasts == 1, s"Expected 1 SubqueryBroadcast, got $countBroadcasts")
          assert(countReused == 1, s"Expected 1 ReusedSubquery, got $countReused")
        }
      }
    }
  }

  // From RemoveRedundantProjectsSuite "join with ordering requirement".
  // DPP subquery uses ReusedExchangeExec so collectWithSubqueries doesn't
  // double-count project nodes.
  test("AQE DPP: join with ordering requirement project count") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTable("testViewTable") {
        withTempView("testView") {
          spark
            .range(0, 100, 1)
            .selectExpr(
              "id as key",
              "id as a",
              "id as b",
              "cast(id as string) as c",
              "cast(id as string) as d")
            .write
            .format("parquet")
            .mode("overwrite")
            .partitionBy("key")
            .saveAsTable("testViewTable")
          sql("CREATE OR REPLACE TEMP VIEW testView AS SELECT * FROM testViewTable")

          val query = "select * from (select key, a, c, b from testView) as t1 join " +
            "(select key, a, b, c from testView) as t2 on t1.key = t2.key where t2.a > 50"

          val (sparkPlan, cometPlan) = checkSparkAnswer(sql(query))

          if (isSpark35Plus) {
            val sparkProjects = collectWithSubqueries(sparkPlan) { case p: ProjectExec => p }
            val cometProjects = collectWithSubqueries(cometPlan) {
              case p: ProjectExec => p
              case p: CometProjectExec => p
            }
            assert(
              cometProjects.size == sparkProjects.size,
              s"Comet project count (${cometProjects.size}) should match " +
                s"Spark (${sparkProjects.size})")
          }
        }
      }
    }
  }

  // SPARK-39447: SHUFFLE_MERGE hint forces SMJ, empty CTE means the DPP subquery's
  // ASPE re-optimizes to LocalTableScan. CometBroadcastExchangeExec must handle the
  // resulting non-Comet child gracefully.
  test("AQE DPP: SPARK-39447 avoid assertion in doExecuteBroadcast") {
    withDppTables {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
        val df = sql("""
            |WITH empty_result AS (
            |  SELECT * FROM fact_stats WHERE product_id < 0
            |)
            |SELECT *
            |FROM   (SELECT /*+ SHUFFLE_MERGE(fact_sk) */ empty_result.store_id
            |        FROM   fact_sk
            |               JOIN empty_result
            |                 ON fact_sk.product_id = empty_result.product_id) t2
            |       JOIN empty_result
            |         ON t2.store_id = empty_result.store_id
          """.stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)
        checkAnswer(df, Nil)

        if (isSpark35Plus) {
          assertAqeDppShape(cometPlan)
        }
      }
    }
  }

  // SPARK-32509: previously IgnoreComet(#4045). Unused DPP filter with
  // AUTO_BROADCASTJOIN_THRESHOLD=-1 (no broadcast). Should not affect exchange reuse.
  test("AQE DPP: unused DPP filter and exchange reuse (SPARK-32509)") {
    withDppTables {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        val df = sql(""" WITH view1 as (
            |   SELECT f.store_id FROM fact_stats f WHERE f.units_sold = 70
            | )
            | SELECT * FROM view1 v1 join view1 v2 WHERE v1.store_id = v2.store_id
          """.stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)
        checkAnswer(df, Row(15, 15) :: Nil)

        import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
        val reusedExchanges = collect(cometPlan) { case r: ReusedExchangeExec => r }
        assert(
          reusedExchanges.size == 1,
          s"Expected 1 ReusedExchangeExec, got ${reusedExchanges.size}.\n" +
            s"Plan:\n${cometPlan.treeString}")
      }
    }
  }

  // SPARK-34637: previously IgnoreComet(#4045). DPP side broadcast query stage
  // should be created before the main join's broadcast stage.
  test("AQE DPP: broadcast query stage creation order (SPARK-34637)") {
    withDppTables {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
        val df = sql(""" WITH v as (
            |   SELECT f.store_id FROM fact_stats f WHERE f.units_sold = 70 group by f.store_id
            | )
            | SELECT * FROM v v1 join v v2 WHERE v1.store_id = v2.store_id
          """.stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)
        checkAnswer(df, Row(15, 15) :: Nil)

        if (isSpark35Plus) {
          val cometSubqueries = collectWithSubqueries(cometPlan) {
            case s: CometSubqueryBroadcastExec => s
          }
          assert(
            cometSubqueries.nonEmpty,
            s"Expected CometSubqueryBroadcastExec for DPP.\nPlan:\n${cometPlan.treeString}")
        } else {
          // On 3.4, fall back to Spark-native DPP: expect a SubqueryBroadcastExec
          // (not Comet) indicating Spark's PlanAdaptiveDynamicPruningFilters ran.
          val sparkSubqueries = collectWithSubqueries(cometPlan) {
            case s: SubqueryBroadcastExec => s
          }
          assert(
            sparkSubqueries.nonEmpty,
            "Expected Spark SubqueryBroadcastExec for DPP on 3.4 fallback. " +
              "If empty, Spark's rule killed DPP (likely because Comet BHJ was " +
              s"not falling back).\nPlan:\n${cometPlan.treeString}")
        }
      }
    }
  }

  // Reproduces DynamicPartitionPruningSuiteV2: SPARK-34637 with V2 BatchScan.
  // Uses InMemoryTableCatalog so Spark creates BatchScanExec (not FileSourceScanExec).
  // Comet replaces BroadcastHashJoinExec with CometBroadcastHashJoinExec, which
  // breaks Spark's PlanAdaptiveDynamicPruningFilters pattern match for non-Comet scans.
  test("AQE DPP: V2 BatchScan broadcast query stage creation order (SPARK-34637)") {
    // On Spark 4.1+, the shuffle between partial/final aggregates is elided for this
    // plan, which removes the only Comet entry point (CometColumnarShuffle over a Spark
    // shuffle) that would let the cascade reach CometBroadcastHashJoinExec. Without a
    // Comet BHJ, CometPlanAdaptiveDynamicPruningFilters falls into its Spark-native
    // branch and produces SubqueryBroadcastExec instead of CometSubqueryBroadcastExec.
    // DPP is still correct and broadcast reuse still fires, so we branch the
    // assertion by version rather than skipping the whole test.
    //
    // Enabling CometSparkToColumnar (COMET_SPARK_TO_ARROW_ENABLED + adding "BatchScan" to
    // COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST) would give Comet a scan-level entry
    // point, but it also exposes a separate bug in CometExecRule.transform
    // (https://github.com/apache/datafusion-comet/issues/4145): SAB/SubqueryBroadcastExec
    // wrapping runs only on the post-convertNode tree, so when convertNode wraps a scan in
    // CometSparkToColumnarExec the wrapped scan's runtimeFilters/partitionFilters are
    // hidden from the SAB-wrapping pass. Any V2 scan routed through CometSparkToColumnarExec
    // with DPP filters skips SAB wrapping. That bug is independent of this test.
    val factData = Seq(
      (1000, 1, 1, 10),
      (1010, 2, 1, 10),
      (1020, 2, 1, 10),
      (1030, 3, 2, 10),
      (1040, 3, 2, 50),
      (1050, 3, 2, 50),
      (1060, 3, 2, 50),
      (1070, 4, 2, 10),
      (1080, 4, 3, 20),
      (1090, 4, 3, 10),
      (1100, 4, 3, 10),
      (1110, 5, 3, 10),
      (1120, 6, 4, 10),
      (1130, 7, 4, 50),
      (1140, 8, 4, 50),
      (1150, 9, 1, 20),
      (1160, 10, 1, 20),
      (1170, 11, 1, 30),
      (1180, 12, 2, 20),
      (1190, 13, 2, 20),
      (1200, 14, 3, 40),
      (1200, 15, 3, 70),
      (1210, 16, 4, 10),
      (1220, 17, 4, 20),
      (1230, 18, 4, 20),
      (1240, 19, 5, 40),
      (1250, 20, 5, 40),
      (1260, 21, 5, 40),
      (1270, 22, 5, 50),
      (1280, 23, 1, 50),
      (1290, 24, 1, 50),
      (1300, 25, 1, 50))

    import testImplicits._
    withSQLConf(
      "spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName,
      "spark.sql.defaultCatalog" -> "testcat",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {

      factData
        .toDF("date_id", "store_id", "product_id", "units_sold")
        .write
        .partitionBy("store_id")
        .saveAsTable("fact_stats_v2")

      try {
        val df = sql(""" WITH v as (
            |   SELECT f.store_id FROM fact_stats_v2 f
            |   WHERE f.units_sold = 70 GROUP BY f.store_id
            | )
            | SELECT * FROM v v1 JOIN v v2 WHERE v1.store_id = v2.store_id
          """.stripMargin)

        val (_, cometPlan) = checkSparkAnswer(df)
        checkAnswer(df, Row(15, 15) :: Nil)

        // SABs should have been unwrapped by CometPlanAdaptiveDynamicPruningFilters on 3.5+.
        if (isSpark35Plus) {
          assertAqeDppShape(cometPlan)
        }

        if (isSpark35Plus && !isSpark41Plus) {
          // 3.5 - 4.0: CometPlanAdaptiveDynamicPruningFilters rewrites the SAB into
          // CometSubqueryBroadcastExec with the join's CometBroadcastExchange for native
          // broadcast reuse.
          val cometSubqueries = collectWithSubqueries(cometPlan) {
            case s: CometSubqueryBroadcastExec => s
          }
          assert(
            cometSubqueries.nonEmpty,
            s"V2 scan should have CometSubqueryBroadcastExec for DPP:\n$cometPlan")
        } else {
          // 3.4 and 4.1+: DPP runs as Spark-native SubqueryBroadcastExec.
          //   - 3.4: CometSpark34AqeDppFallbackRule keeps the BHJ build broadcast Spark-native
          //     so Spark's PlanAdaptiveDynamicPruningFilters can create SubqueryBroadcastExec
          //     and AQE stageCache can dedupe with the DPP subquery's broadcast.
          //   - 4.1+: Partial/final aggregate shuffle is elided, which removes Comet's entry
          //     point for this query, so CometPlanAdaptiveDynamicPruningFilters falls into
          //     its Spark-native branch.
          val sparkSubqueries = collectWithSubqueries(cometPlan) {
            case s: SubqueryBroadcastExec => s
          }
          assert(
            sparkSubqueries.nonEmpty,
            s"V2 scan should have SubqueryBroadcastExec for DPP:\n$cometPlan")

          // Broadcast reuse: the DPP subquery's BroadcastExchange must be reused in the main
          // plan as a ReusedExchangeExec (or appear directly). Mirrors Spark's
          // DynamicPartitionPruningSuiteBase.checkPartitionPruningPredicate hasReuse check
          // (DynamicPartitionPruningSuite.scala:207-231). Without this, the main BHJ's build
          // side would run a second broadcast with the same data.
          sparkSubqueries.foreach { s =>
            val dppBroadcast = s.child match {
              case aspe: AdaptiveSparkPlanExec =>
                val bqs = collectFirst(aspe) { case b: BroadcastQueryStageExec => b }
                assert(
                  bqs.isDefined,
                  s"Expected BroadcastQueryStageExec under DPP subquery's ASPE:\n$cometPlan")
                bqs.get.broadcast
              case other =>
                fail(s"Unexpected SubqueryBroadcastExec child: ${other.getClass.getSimpleName}")
            }
            val hasReuse = find(cometPlan) {
              case ReusedExchangeExec(_, e) => e eq dppBroadcast
              case b: BroadcastExchangeLike => b eq dppBroadcast
              case _ => false
            }.isDefined
            assert(hasReuse, s"DPP broadcast should be reused in main plan:\n$cometPlan")
          }
        }
      } finally {
        sql("DROP TABLE IF EXISTS testcat.fact_stats_v2")
      }
    }
  }

  // Regression for the TPC-DS q5/q14a/q14b/q54 failure: two fact scans inside a single
  // UNION ALL that joins a dimension once produce DPP subqueries that share their
  // logical build plan (since the join pushes DPP down to both scans via one subquery).
  // Spark's ReuseAdaptiveSubquery (which runs before our rule) collapses them into
  // ReusedSubqueryExec(CometSubqueryAdaptiveBroadcastExec). Our rule's extractSABData
  // must unwrap ReusedSubqueryExec before inspecting the inner plan; otherwise the
  // wrapped CSAB survives to runtime and doExecute() throws.
  test("AQE DPP: ReuseAdaptiveSubquery wraps CSAB in ReusedSubqueryExec") {
    withTempDir { path =>
      val fact1Path = s"${path.getAbsolutePath}/fact1.parquet"
      val fact2Path = s"${path.getAbsolutePath}/fact2.parquet"
      val dimPath = s"${path.getAbsolutePath}/dim.parquet"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        val one_day = 24 * 60 * 60000
        val fact1 = Range(0, 100)
          .map(i => (i, new java.sql.Date(System.currentTimeMillis() + (i % 10) * one_day)))
          .toDF("fact_id", "fact_date")
        fact1.write.partitionBy("fact_date").parquet(fact1Path)
        val fact2 = Range(100, 200)
          .map(i => (i, new java.sql.Date(System.currentTimeMillis() + (i % 10) * one_day)))
          .toDF("fact_id", "fact_date")
        fact2.write.partitionBy("fact_date").parquet(fact2Path)
        val dim = Range(0, 10)
          .map(i => (i, new java.sql.Date(System.currentTimeMillis() + i * one_day)))
          .toDF("dim_id", "dim_date")
        dim.write.parquet(dimPath)
      }

      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
        spark.read.parquet(fact1Path).createOrReplaceTempView("aqe_dpp_reuse_fact1")
        spark.read.parquet(fact2Path).createOrReplaceTempView("aqe_dpp_reuse_fact2")
        spark.read.parquet(dimPath).createOrReplaceTempView("aqe_dpp_reuse_dim")

        // Mirror TPC-DS q54: UNION ALL of two fact tables inside a single join to
        // the dimension. DPP is pushed through the UNION to both fact scans from a
        // single logical DynamicPruningSubquery, so both SABs share their buildPlan
        // and canonicalize identically. ReuseAdaptiveSubquery then wraps one in a
        // ReusedSubqueryExec, exercising the bug path.
        val df = spark.sql("""
            |SELECT f.fact_id, f.fact_date
            |FROM (
            |  SELECT fact_id, fact_date FROM aqe_dpp_reuse_fact1
            |  UNION ALL
            |  SELECT fact_id, fact_date FROM aqe_dpp_reuse_fact2
            |) f
            |JOIN aqe_dpp_reuse_dim d ON f.fact_date = d.dim_date
            |WHERE d.dim_id > 7
          """.stripMargin)
        val (_, cometPlan) = checkSparkAnswer(df)

        if (isSpark35Plus) {
          // Regression check: without the ReusedSubqueryExec unwrap in extractSABData,
          // one CSAB survives the rule and trips CometSubqueryAdaptiveBroadcastExec.doExecute
          // at runtime. assertAqeDppShape verifies no CSABs remain in the final plan.
          assertAqeDppShape(cometPlan)

          // Subquery reuse: exactly one canonical CometSubqueryBroadcastExec, plus at
          // least one ReusedSubqueryExec(CometSubqueryBroadcastExec) pointer for the
          // second fact scan. Without this dedup, both fact scans would evaluate the
          // DPP subquery independently.
          val cometSubqueries = collectWithSubqueries(cometPlan) {
            case s: CometSubqueryBroadcastExec => s
          }
          assert(
            cometSubqueries.size == 1,
            "Expected exactly 1 CometSubqueryBroadcastExec (shared between fact scans), " +
              s"got ${cometSubqueries.size}:\n${cometPlan.treeString}")
          val reusedCsbs = collectWithSubqueries(cometPlan) {
            case r @ ReusedSubqueryExec(_: CometSubqueryBroadcastExec) => r
          }
          assert(
            reusedCsbs.nonEmpty,
            "Expected at least one ReusedSubqueryExec(CometSubqueryBroadcastExec) " +
              s"for the second fact scan's DPP filter:\n${cometPlan.treeString}")

          // Broadcast reuse via AQE stageCache: the DPP subquery's ASPE and the main
          // BHJ should share the same underlying CometBroadcastExchange. Without this,
          // we'd build two identical broadcasts of the dim.
          val dppBroadcast = cometSubqueries.head.child match {
            case aspe: AdaptiveSparkPlanExec =>
              val bqs = collectFirst(aspe) { case b: BroadcastQueryStageExec => b }
              assert(
                bqs.isDefined,
                "Expected BroadcastQueryStageExec inside DPP subquery's ASPE:\n" +
                  cometPlan.treeString)
              bqs.get.broadcast
            case other =>
              fail(
                s"Unexpected CometSubqueryBroadcastExec child: ${other.getClass.getSimpleName}")
          }
          val hasReuse = find(cometPlan) {
            case ReusedExchangeExec(_, e) => e eq dppBroadcast
            case b: BroadcastExchangeLike => b eq dppBroadcast
            case _ => false
          }.isDefined
          assert(
            hasReuse,
            "DPP subquery's broadcast should be reused by the main BHJ " +
              s"(via AQE stageCache):\n${cometPlan.treeString}")
        } else {
          // Spark 3.4: injectQueryStageOptimizerRule is unavailable, so
          // CometPlanAdaptiveDynamicPruningFilters can't run. V1 fact scans are rejected
          // to Spark by CometScanRule.transformV1Scan, and CometSpark34AqeDppFallbackRule
          // tags the BHJ's build-side BroadcastExchange so Spark's own
          // PlanAdaptiveDynamicPruningFilters handles DPP natively. Expected shape
          // mirrors the 3.5+ assertions but with Spark-native node types.
          val sparkSubqueries = collectWithSubqueries(cometPlan) {
            case s: SubqueryBroadcastExec => s
          }
          assert(
            sparkSubqueries.size == 1,
            "Expected exactly 1 SubqueryBroadcastExec on 3.4 (Spark-native DPP, " +
              s"shared between fact scans), got ${sparkSubqueries.size}. If 0, " +
              "CometSpark34AqeDppFallbackRule didn't keep the BHJ Spark-native and " +
              s"Spark's rule killed DPP:\n${cometPlan.treeString}")
          val reusedSparkSubqueries = collectWithSubqueries(cometPlan) {
            case r @ ReusedSubqueryExec(_: SubqueryBroadcastExec) => r
          }
          assert(
            reusedSparkSubqueries.nonEmpty,
            "Expected at least one ReusedSubqueryExec(SubqueryBroadcastExec) on 3.4 " +
              s"for the second fact scan's DPP filter:\n${cometPlan.treeString}")
          val dppBroadcast = sparkSubqueries.head.child match {
            case aspe: AdaptiveSparkPlanExec =>
              val bqs = collectFirst(aspe) { case b: BroadcastQueryStageExec => b }
              assert(
                bqs.isDefined,
                "Expected BroadcastQueryStageExec inside DPP subquery's ASPE:\n" +
                  cometPlan.treeString)
              bqs.get.broadcast
            case other =>
              fail(s"Unexpected SubqueryBroadcastExec child: ${other.getClass.getSimpleName}")
          }
          val hasReuse = find(cometPlan) {
            case ReusedExchangeExec(_, e) => e eq dppBroadcast
            case b: BroadcastExchangeLike => b eq dppBroadcast
            case _ => false
          }.isDefined
          assert(
            hasReuse,
            "DPP subquery's broadcast should be reused by the main BHJ on 3.4:\n" +
              cometPlan.treeString)
        }
      }
    }
  }

  test("ShuffleQueryStageExec could be direct child node of CometBroadcastExchangeExec") {
    withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      val table = "src"
      withTable(table) {
        withView("lv_noalias") {
          sql(s"CREATE TABLE $table (key INT, value STRING) USING PARQUET")
          sql(s"INSERT INTO $table VALUES(238, 'val_238')")

          sql(
            "CREATE VIEW lv_noalias AS SELECT myTab.* FROM src " +
              "LATERAL VIEW explode(map('key1', 100, 'key2', 200)) myTab LIMIT 2")
          val df = sql("SELECT * FROM lv_noalias a JOIN lv_noalias b ON a.key=b.key");
          checkSparkAnswer(df)
        }
      }
    }
  }

  // repro for https://github.com/apache/datafusion-comet/issues/1251
  test("subquery/exists-subquery/exists-orderby-limit.sql") {
    withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      val table = "src"
      withTable(table) {
        sql(s"CREATE TABLE $table (key INT, value STRING) USING PARQUET")
        sql(s"INSERT INTO $table VALUES(238, 'val_238')")

        // the subquery returns the distinct group by values
        checkSparkAnswerAndOperator(s"""SELECT * FROM $table
             |WHERE EXISTS (SELECT MAX(key)
             |FROM $table
             |GROUP BY value
             |LIMIT 1
             |OFFSET 2)""".stripMargin)

        checkSparkAnswerAndOperator(s"""SELECT * FROM $table
             |WHERE NOT EXISTS (SELECT MAX(key)
             |FROM $table
             |GROUP BY value
             |LIMIT 1
             |OFFSET 2)""".stripMargin)
      }
    }
  }

  test("Sort on single struct should fallback to Spark") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      val data1 =
        Seq(Tuple1(null), Tuple1((1, "a")), Tuple1((2, null)), Tuple1((3, "b")), Tuple1(null))

      withParquetFile(data1) { file =>
        readParquetFile(file) { df =>
          val sort = df.sort("_1")
          checkSparkAnswer(sort)
        }
      }

      val data2 =
        Seq(
          Tuple2(null, 1),
          Tuple2((1, "a"), 2),
          Tuple2((2, null), 3),
          Tuple2((3, "b"), 5),
          Tuple2(null, 6))

      withParquetFile(data2) { file =>
        readParquetFile(file) { df =>
          val sort = df.sort("_1")
          checkSparkAnswer(sort)
        }
      }
    }
  }

  test("Sort on array of boolean") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {

      sql("""
          |CREATE OR REPLACE TEMPORARY VIEW test_list AS SELECT * FROM VALUES
          | (array(true)),
          | (array(false)),
          | (array(false)),
          | (array(false)) AS test(arr)
          |""".stripMargin)

      val df = sql("""
          SELECT * FROM test_list ORDER BY arr
          |""".stripMargin)
      val sort = stripAQEPlan(df.queryExecution.executedPlan).collect { case s: CometSortExec =>
        s
      }.headOption
      assert(sort.isDefined)
    }
  }

  test("Sort on TimestampNTZType") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {

      sql("""
          |CREATE OR REPLACE TEMPORARY VIEW test_list AS SELECT * FROM VALUES
          | (TIMESTAMP_NTZ'2025-08-29 00:00:00'),
          | (TIMESTAMP_NTZ'2023-07-07 00:00:00'),
          | (convert_timezone('Asia/Kathmandu', 'UTC', TIMESTAMP_NTZ'2023-07-07 00:00:00')),
          | (convert_timezone('America/Los_Angeles', 'UTC', TIMESTAMP_NTZ'2023-07-07 00:00:00')),
          | (TIMESTAMP_NTZ'1969-12-31 00:00:00') AS test(ts_ntz)
          |""".stripMargin)

      val df = sql("""
          SELECT * FROM test_list ORDER BY ts_ntz
          |""".stripMargin)
      checkSparkAnswer(df)
      val sort = stripAQEPlan(df.queryExecution.executedPlan).collect { case s: CometSortExec =>
        s
      }.headOption
      assert(sort.isDefined)
    }
  }

  test("Sort on map w/ TimestampNTZType values") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {

      sql("""
          |CREATE OR REPLACE TEMPORARY VIEW test_map AS SELECT * FROM VALUES
          | (map('a', TIMESTAMP_NTZ'2025-08-29 00:00:00')),
          | (map('b', TIMESTAMP_NTZ'2023-07-07 00:00:00')),
          | (map('c', convert_timezone('Asia/Kathmandu', 'UTC', TIMESTAMP_NTZ'2023-07-07 00:00:00'))),
          | (map('d', convert_timezone('America/Los_Angeles', 'UTC', TIMESTAMP_NTZ'2023-07-07 00:00:00'))) AS test(map)
          |""".stripMargin)

      val df = sql("""
          SELECT * FROM test_map ORDER BY map_values(map) DESC
          |""".stripMargin)
      checkSparkAnswer(df)
      val sort = stripAQEPlan(df.queryExecution.executedPlan).collect { case s: CometSortExec =>
        s
      }.headOption
      assert(sort.isDefined)
    }
  }

  test("Sort on map w/ boolean values") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SORT_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {

      sql("""
          |CREATE OR REPLACE TEMPORARY VIEW test_map AS SELECT * FROM VALUES
          | (map('a', true)),
          | (map('b', true)),
          | (map('c', false)),
          | (map('d', true)) AS test(map)
          |""".stripMargin)

      val df = sql("""
          SELECT * FROM test_map ORDER BY map_values(map) DESC
          |""".stripMargin)
      val sort = stripAQEPlan(df.queryExecution.executedPlan).collect { case s: CometSortExec =>
        s
      }.headOption
      assert(sort.isDefined)
    }
  }

  test("subquery execution under CometTakeOrderedAndProjectExec should not fail") {
    assume(isSpark35Plus, "SPARK-45584 is fixed in Spark 3.5+")

    withTable("t1") {
      sql("""
          |CREATE TABLE t1 USING PARQUET
          |AS SELECT * FROM VALUES
          |(1, "a"),
          |(2, "a"),
          |(3, "a") t(id, value)
          |""".stripMargin)
      val df = sql("""
          |WITH t2 AS (
          |  SELECT * FROM t1 ORDER BY id
          |)
          |SELECT *, (SELECT COUNT(*) FROM t2) FROM t2 LIMIT 10
          |""".stripMargin)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("fix CometNativeExec.doCanonicalize for ReusedExchangeExec") {
    withSQLConf(
      CometConf.COMET_EXEC_BROADCAST_FORCE_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable("td") {
        testData
          .withColumn("bucket", $"key" % 3)
          .write
          .mode(SaveMode.Overwrite)
          .bucketBy(2, "bucket")
          .format("parquet")
          .saveAsTable("td")
        val df = sql("""
            |SELECT t1.key, t2.key, t3.key
            |FROM td AS t1
            |JOIN td AS t2 ON t2.key = t1.key
            |JOIN td AS t3 ON t3.key = t2.key
            |WHERE t1.bucket = 1 AND t2.bucket = 1 AND t3.bucket = 1
            |""".stripMargin)
        val reusedPlan = ReuseExchangeAndSubquery.apply(df.queryExecution.executedPlan)
        val reusedExchanges = collect(reusedPlan) { case r: ReusedExchangeExec =>
          r
        }
        assert(reusedExchanges.size == 1)
        assert(reusedExchanges.head.child.isInstanceOf[CometBroadcastExchangeExec])
      }
    }
  }

  test("CometShuffleExchangeExec logical link should be correct") {
    withTempView("v") {
      spark.sparkContext
        .parallelize((1 to 4).map(i => TestData(i, i.toString)), 2)
        .toDF("c1", "c2")
        .createOrReplaceTempView("v")

      Seq("native", "jvm").foreach { columnarShuffleMode =>
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          CometConf.COMET_SHUFFLE_MODE.key -> columnarShuffleMode) {
          val df = sql("SELECT * FROM v where c1 = 1 order by c1, c2")
          val shuffle = find(df.queryExecution.executedPlan) {
            case _: CometShuffleExchangeExec if columnarShuffleMode.equalsIgnoreCase("jvm") =>
              true
            case _: ShuffleExchangeExec if !columnarShuffleMode.equalsIgnoreCase("jvm") => true
            case _ => false
          }.get
          assert(shuffle.logicalLink.isEmpty)
        }
      }
    }
  }

  test("Ensure that the correct outputPartitioning of CometSort") {
    withTable("test_data") {
      val tableDF = spark.sparkContext
        .parallelize(
          (1 to 10).map { i =>
            (if (i > 4) 5 else i, i.toString, Date.valueOf(s"${2020 + i}-$i-$i"))
          },
          3)
        .toDF("id", "data", "day")
      tableDF.write.saveAsTable("test_data")

      val df = sql("SELECT * FROM test_data")
        .repartition($"data")
        .sortWithinPartitions($"id", $"data", $"day")
      df.collect()
      val sort = stripAQEPlan(df.queryExecution.executedPlan).collect { case s: CometSortExec =>
        s
      }.head
      assert(sort.outputPartitioning == sort.child.outputPartitioning)
    }
  }

  test("try_sum should return null if overflow happens before merging") {
    val longDf = Seq(Long.MaxValue, Long.MaxValue, 2).toDF("v")
    val yearMonthDf = Seq(Int.MaxValue, Int.MaxValue, 2)
      .map(Period.ofMonths)
      .toDF("v")
    val dayTimeDf = Seq(106751991L, 106751991L, 2L)
      .map(Duration.ofDays)
      .toDF("v")
    Seq(longDf, yearMonthDf, dayTimeDf).foreach { df =>
      checkSparkAnswer(df.repartitionByRange(2, col("v")).selectExpr("try_sum(v)"))
    }
  }

  test("Fix corrupted AggregateMode when transforming plan parameters") {
    withParquetTable((0 until 5).map(i => (i, i + 1)), "table") {
      val df = sql("SELECT * FROM table").groupBy($"_1").agg(sum("_2"))
      val agg = stripAQEPlan(df.queryExecution.executedPlan).collectFirst {
        case s: CometHashAggregateExec => s
      }.get

      assert(agg.mode.isDefined && agg.mode.get.isInstanceOf[AggregateMode])
      val newAgg = agg.cleanBlock().asInstanceOf[CometHashAggregateExec]
      assert(newAgg.mode.isDefined && newAgg.mode.get.isInstanceOf[AggregateMode])
    }
  }

  test("CometBroadcastExchangeExec") {
    withSQLConf(CometConf.COMET_EXEC_BROADCAST_FORCE_ENABLED.key -> "true") {
      withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl_a") {
        withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl_b") {
          val df = sql(
            "SELECT tbl_a._1, tbl_b._2 FROM tbl_a JOIN tbl_b " +
              "WHERE tbl_a._1 > tbl_a._2 LIMIT 2")

          val nativeBroadcast = find(df.queryExecution.executedPlan) {
            case _: CometBroadcastExchangeExec => true
            case _ => false
          }.get.asInstanceOf[CometBroadcastExchangeExec]

          val numParts = nativeBroadcast.executeColumnar().getNumPartitions

          val rows = nativeBroadcast.executeCollect().toSeq.sortBy(row => row.getInt(0))
          val rowContents = rows.map(row => row.getInt(0))
          val expected = (0 until numParts).flatMap(_ => (0 until 5).map(i => i + 1)).sorted

          assert(rowContents === expected)

          val metrics = nativeBroadcast.metrics
          assert(metrics("numCoalescedBatches").value == 5L)
          assert(metrics("numCoalescedRows").value == 5L)
        }
      }
    }
  }

  test("CometBroadcastExchangeExec: empty broadcast") {
    withSQLConf(CometConf.COMET_EXEC_BROADCAST_FORCE_ENABLED.key -> "true") {
      withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl_a") {
        withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl_b") {
          val df = sql(
            "SELECT /*+ BROADCAST(a) */ *" +
              " FROM (SELECT * FROM tbl_a WHERE _1 < 0) a JOIN tbl_b b" +
              " ON a._1 = b._1")
          val nativeBroadcast = find(df.queryExecution.executedPlan) {
            case _: CometBroadcastExchangeExec => true
            case _ => false
          }.get.asInstanceOf[CometBroadcastExchangeExec]
          val rows = nativeBroadcast.executeCollect()
          assert(rows.isEmpty)

          val metrics = nativeBroadcast.metrics
          assert(metrics("numCoalescedBatches").value == 0L)
          assert(metrics("numCoalescedRows").value == 0L)
        }
      }
    }
  }

  test("scalar subquery") {
    val dataTypes =
      Seq(
        "BOOLEAN",
        "BYTE",
        "SHORT",
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        // "DATE": TODO: needs to address issue #1364 first
        // "TIMESTAMP", TODO: needs to address issue #1364 first
        "STRING",
        "BINARY",
        "DECIMAL(38, 10)")
    dataTypes.map { subqueryType =>
      withSQLConf(
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
        withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
          var column1 = s"CAST(max(_1) AS $subqueryType)"
          if (subqueryType == "BINARY") {
            // arrow-rs doesn't support casting integer to binary yet.
            // We added it to upstream but it's not released yet.
            column1 = "CAST(CAST(max(_1) AS STRING) AS BINARY)"
          }

          val df1 = sql(s"SELECT (SELECT $column1 FROM tbl) AS a, _1, _2 FROM tbl")
          checkSparkAnswerAndOperator(df1)

          var column2 = s"CAST(_1 AS $subqueryType)"
          if (subqueryType == "BINARY") {
            // arrow-rs doesn't support casting integer to binary yet.
            // We added it to upstream but it's not released yet.
            column2 = "CAST(CAST(_1 AS STRING) AS BINARY)"
          }

          val df2 = sql(s"SELECT _1, _2 FROM tbl WHERE $column2 > (SELECT $column1 FROM tbl)")
          checkSparkAnswerAndOperator(df2)

          // Non-correlated exists subquery will be rewritten to scalar subquery
          val df3 = sql(
            "SELECT * FROM tbl WHERE EXISTS " +
              s"(SELECT $column2 FROM tbl WHERE _1 > 1)")
          checkSparkAnswerAndOperator(df3)

          // Null value
          column1 = s"CAST(NULL AS $subqueryType)"
          if (subqueryType == "BINARY") {
            column1 = "CAST(CAST(NULL AS STRING) AS BINARY)"
          }

          val df4 = sql(s"SELECT (SELECT $column1 FROM tbl LIMIT 1) AS a, _1, _2 FROM tbl")
          checkSparkAnswerAndOperator(df4)
        }
      }
    }
  }

  // Regression test for https://github.com/apache/datafusion-comet/issues/4042
  // SPARK-43402 (Spark 4.0+) pushes scalar subqueries into FileSourceScanExec.dataFilters.
  // CometReuseSubquery re-applies subquery deduplication after Comet node conversions, and
  // the resolved literal is pushed to the native Parquet reader at execution time (same
  // approach as FileSourceScanLike.pushedDownFilters in DataSourceScanExec.scala).
  test("scalar subquery in data filters does not break subquery reuse") {
    assume(isSpark40Plus, "SPARK-43402 scalar subquery pushdown is Spark 4.0+ only")

    Seq(true, false).foreach { aqeEnabled =>
      withTable("t1", "t2") {
        withSQLConf(
          SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled.toString) {
          Seq(1, 2, 3).toDF("c1").write.format("parquet").saveAsTable("t1")
          Seq(4, 5, 6).toDF("c2").write.format("parquet").saveAsTable("t2")

          checkSparkAnswer(sql("SELECT * FROM t1 WHERE c1 > (SELECT min(c2) FROM t2)"))

          val df = sql("SELECT * FROM t1 WHERE c1 < (SELECT min(c2) FROM t2)")
          val (_, cometPlan) = checkSparkAnswer(df)

          val nativeScans = collectWithSubqueries(cometPlan) { case n: CometNativeScanExec =>
            n
          }
          val t1Scan =
            nativeScans.find(_.dataFilters.exists(_.exists(_.isInstanceOf[ScalarSubquery])))
          assert(t1Scan.isDefined, "Expected CometNativeScanExec with ScalarSubquery")
          val scalarSubqueries = t1Scan.get.dataFilters.flatMap(_.collect {
            case s: ScalarSubquery => s
          })
          assert(scalarSubqueries.length === 1)
          assert(t1Scan.get.metrics("numFiles").value === 1)

          // Exactly one copy should be ReusedSubqueryExec. AQE (top-down traversal via
          // CometReuseSubquery) puts it on the scan; non-AQE (bottom-up via Spark's
          // ReuseExchangeAndSubquery) puts it on the filter. Either way the subquery
          // executes once.
          val allReused = collectWithSubqueries(cometPlan) { case p: SparkPlan =>
            p.expressions.flatMap(_.collect {
              case s: ScalarSubquery if s.plan.isInstanceOf[ReusedSubqueryExec] => s
            })
          }.flatten
          assert(
            allReused.nonEmpty,
            s"Expected at least one ReusedSubqueryExec in plan (AQE=$aqeEnabled)")

          if (aqeEnabled) {
            assert(
              scalarSubqueries.head.plan.isInstanceOf[ReusedSubqueryExec],
              "Expected ReusedSubqueryExec on scan's ScalarSubquery (AQE=true) " +
                s"but got ${scalarSubqueries.head.plan.getClass.getSimpleName}")
          }
        }
      }
    }
  }

  test("Comet native metrics: scan") {
    Seq(CometConf.SCAN_NATIVE_DATAFUSION, CometConf.SCAN_NATIVE_ICEBERG_COMPAT).foreach {
      scanMode =>
        withSQLConf(
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> scanMode) {
          withTempDir { dir =>
            val path = new Path(dir.toURI.toString, "native-scan.parquet")
            makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = true, 10000)
            withParquetTable(path.toString, "tbl") {
              val df = sql("SELECT * FROM tbl")
              df.collect()

              val scan = find(df.queryExecution.executedPlan)(s =>
                s.isInstanceOf[CometScanExec] || s.isInstanceOf[CometNativeScanExec])
              assert(scan.isDefined, s"Expected to find a Comet scan node for $scanMode")
              val metrics = scan.get.metrics

              assert(
                metrics.contains("time_elapsed_scanning_total"),
                s"[$scanMode] Missing time_elapsed_scanning_total. Available: ${metrics.keys}")
              assert(metrics.contains("bytes_scanned"))
              assert(metrics.contains("output_rows"))
              assert(metrics.contains("time_elapsed_opening"))
              assert(metrics.contains("time_elapsed_processing"))
              assert(metrics.contains("time_elapsed_scanning_until_data"))
              assert(
                metrics("time_elapsed_scanning_total").value > 0,
                s"[$scanMode] time_elapsed_scanning_total should be > 0")
              assert(
                metrics("bytes_scanned").value > 0,
                s"[$scanMode] bytes_scanned should be > 0")
              assert(metrics("output_rows").value > 0, s"[$scanMode] output_rows should be > 0")
              assert(
                metrics("time_elapsed_opening").value > 0,
                s"[$scanMode] time_elapsed_opening should be > 0")
              assert(
                metrics("time_elapsed_processing").value > 0,
                s"[$scanMode] time_elapsed_processing should be > 0")
              assert(
                metrics("time_elapsed_scanning_until_data").value > 0,
                s"[$scanMode] time_elapsed_scanning_until_data should be > 0")
            }
          }
        }
    }
  }

  test("Comet native metrics: project and filter") {
    withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "true") {
      withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
        val df = sql("SELECT _1 + 1, _2 + 2 FROM tbl WHERE _1 > 3")
        df.collect()

        var metrics = find(df.queryExecution.executedPlan) {
          case _: CometProjectExec => true
          case _ => false
        }.map(_.metrics).get

        assert(metrics.contains("output_rows"))
        assert(metrics("output_rows").value == 1L)

        metrics = find(df.queryExecution.executedPlan) {
          case _: CometFilterExec => true
          case _ => false
        }.map(_.metrics).get

        assert(metrics.contains("output_rows"))
        assert(metrics("output_rows").value == 1L)
      }
    }
  }

  test("Comet native metrics: SortMergeJoin") {
    withSQLConf(
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "-1",
      "spark.sql.autoBroadcastJoinThreshold" -> "-1",
      "spark.sql.join.preferSortMergeJoin" -> "true") {
      withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl1") {
        withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl2") {
          val df = sql("SELECT * FROM tbl1 INNER JOIN tbl2 ON tbl1._1 = tbl2._1")
          df.collect()

          val metrics = find(df.queryExecution.executedPlan) {
            case _: CometSortMergeJoinExec => true
            case _ => false
          }.map(_.metrics).get

          assert(metrics.contains("input_batches"))
          assert(metrics("input_batches").value == 2L)
          assert(metrics.contains("input_rows"))
          assert(metrics("input_rows").value == 10L)
          assert(metrics.contains("output_batches"))
          assert(metrics("output_batches").value == 1L)
          assert(metrics.contains("output_rows"))
          assert(metrics("output_rows").value == 5L)
          assert(metrics.contains("peak_mem_used"))
          assert(metrics("peak_mem_used").value > 1L)
          assert(metrics.contains("join_time"))
          assert(metrics("join_time").value > 1L)
          assert(metrics.contains("spill_count"))
          assert(metrics("spill_count").value == 0)
        }
      }
    }
  }

  test("Comet native metrics: HashJoin") {
    withParquetTable((0 until 5).map(i => (i, i + 1)), "t1") {
      withParquetTable((0 until 5).map(i => (i, i + 1)), "t2") {
        val df = sql("SELECT /*+ SHUFFLE_HASH(t1) */ * FROM t1 INNER JOIN t2 ON t1._1 = t2._1")
        df.collect()

        val metrics = find(df.queryExecution.executedPlan) {
          case _: CometHashJoinExec => true
          case _ => false
        }.map(_.metrics).get

        assert(metrics.contains("build_time"))
        assert(metrics("build_time").value > 1L)
        assert(metrics.contains("build_input_batches"))
        assert(metrics("build_input_batches").value == 5L)
        assert(metrics.contains("build_mem_used"))
        assert(metrics("build_mem_used").value > 1L)
        assert(metrics.contains("build_input_rows"))
        assert(metrics("build_input_rows").value == 5L)
        assert(metrics.contains("input_batches"))
        assert(metrics("input_batches").value == 5L)
        assert(metrics.contains("input_rows"))
        assert(metrics("input_rows").value == 5L)
        assert(metrics.contains("output_batches"))
        assert(metrics("output_batches").value == 1L)
        assert(metrics.contains("output_rows"))
        assert(metrics("output_rows").value == 5L)
        assert(metrics.contains("join_time"))
        assert(metrics("join_time").value > 1L)
      }
    }
  }

  test("Comet native metrics: BroadcastHashJoin") {
    withParquetTable((0 until 5).map(i => (i, i + 1)), "t1") {
      withParquetTable((0 until 5).map(i => (i, i + 1)), "t2") {
        val df = sql("SELECT /*+ BROADCAST(t1) */ * FROM t1 INNER JOIN t2 ON t1._1 = t2._1")
        df.collect()

        val metrics = find(df.queryExecution.executedPlan) {
          case _: CometBroadcastHashJoinExec => true
          case _ => false
        }.map(_.metrics).get

        assert(metrics.contains("build_time"))
        assert(metrics("build_time").value > 1L)
        assert(metrics.contains("build_input_batches"))
        assert(metrics("build_input_batches").value == 5L)
        assert(metrics.contains("build_mem_used"))
        assert(metrics("build_mem_used").value > 1L)
        assert(metrics.contains("build_input_rows"))
        assert(metrics("build_input_rows").value == 25L)
        assert(metrics.contains("input_batches"))
        assert(metrics("input_batches").value == 5L)
        assert(metrics.contains("input_rows"))
        assert(metrics("input_rows").value == 5L)
        assert(metrics.contains("output_batches"))
        assert(metrics("output_batches").value == 5L)
        assert(metrics.contains("output_rows"))
        assert(metrics("output_rows").value == 5L)
        assert(metrics.contains("join_time"))
        assert(metrics("join_time").value > 1L)
      }
    }
  }

  test(
    "fix: ReusedExchangeExec + CometShuffleExchangeExec under QueryStageExec " +
      "should be CometRoot") {
    val tableName = "table1"
    val dim = "dim"

    withSQLConf(
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      withTable(tableName, dim) {

        sql(
          s"CREATE TABLE $tableName (id BIGINT, price FLOAT, date DATE, ts TIMESTAMP) USING parquet " +
            "PARTITIONED BY (id)")
        sql(s"CREATE TABLE $dim (id BIGINT, date DATE) USING parquet")

        spark
          .range(1, 100)
          .withColumn("date", date_add(expr("DATE '1970-01-01'"), expr("CAST(id % 4 AS INT)")))
          .withColumn("ts", expr("TO_TIMESTAMP(date)"))
          .withColumn("price", expr("CAST(id AS FLOAT)"))
          .select("id", "price", "date", "ts")
          .coalesce(1)
          .write
          .mode(SaveMode.Append)
          .partitionBy("id")
          .saveAsTable(tableName)

        spark
          .range(1, 10)
          .withColumn("date", expr("DATE '1970-01-02'"))
          .select("id", "date")
          .coalesce(1)
          .write
          .mode(SaveMode.Append)
          .saveAsTable(dim)

        val query =
          s"""
             |SELECT $tableName.id, sum(price) as sum_price
             |FROM $tableName, $dim
             |WHERE $tableName.id = $dim.id AND $tableName.date = $dim.date
             |GROUP BY $tableName.id HAVING sum(price) > (
             |  SELECT sum(price) * 0.0001 FROM $tableName, $dim WHERE $tableName.id = $dim.id AND $tableName.date = $dim.date
             |  )
             |ORDER BY sum_price
             |""".stripMargin

        val df = sql(query)
        checkSparkAnswerAndOperator(df)
        val exchanges = stripAQEPlan(df.queryExecution.executedPlan).collect {
          case s: CometShuffleExchangeExec if s.shuffleType == CometColumnarShuffle =>
            s
        }
        assert(exchanges.length == 4)
      }
    }
  }

  test("Comet Shuffled Join should be optimized to CometBroadcastHashJoin by AQE") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      withParquetTable((0 until 100).map(i => (i, i + 1)), "tbl_a") {
        withParquetTable((0 until 100).map(i => (i, i + 2)), "tbl_b") {
          withParquetTable((0 until 100).map(i => (i, i + 3)), "tbl_c") {
            val df = sql("""SELECT /*+ BROADCAST(c) */ a1, sum_b2, c._2 FROM (
                |  SELECT a._1 a1, SUM(b._2) sum_b2 FROM tbl_a a
                |  JOIN tbl_b b ON a._1 = b._1
                |  GROUP BY a._1) t
                |JOIN tbl_c c ON t.a1 = c._1
                |""".stripMargin)
            checkSparkAnswerAndOperator(df)

            // Before AQE: 1 broadcast join
            var broadcastHashJoinExec = stripAQEPlan(df.queryExecution.executedPlan).collect {
              case s: CometBroadcastHashJoinExec => s
            }
            assert(broadcastHashJoinExec.length == 1)

            // After AQE: shuffled join optimized to broadcast join
            df.collect()
            broadcastHashJoinExec = stripAQEPlan(df.queryExecution.executedPlan).collect {
              case s: CometBroadcastHashJoinExec => s
            }
            assert(broadcastHashJoinExec.length == 2)
          }
        }
      }
    }
  }

  test("CometBroadcastExchange could be converted to rows using CometColumnarToRow") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "auto") {
      withParquetTable((0 until 100).map(i => (i, i + 1)), "tbl_a") {
        withParquetTable((0 until 100).map(i => (i, i + 2)), "tbl_b") {
          withParquetTable((0 until 100).map(i => (i, i + 3)), "tbl_c") {
            val df = sql("""SELECT /*+ BROADCAST(c) */ a1, sum_b2, c._2 FROM (
                |  SELECT a._1 a1, SUM(b._2) sum_b2 FROM tbl_a a
                |  JOIN tbl_b b ON a._1 = b._1
                |  GROUP BY a._1) t
                |JOIN tbl_c c ON t.a1 = c._1
                |""".stripMargin)
            checkSparkAnswerAndOperator(df)

            // Before AQE: one CometBroadcastExchange, no CometColumnarToRow
            var columnarToRowExec: Seq[SparkPlan] =
              stripAQEPlan(df.queryExecution.executedPlan).collect {
                case s: CometColumnarToRowExec => s
                case s: CometNativeColumnarToRowExec => s
              }
            assert(columnarToRowExec.isEmpty)

            // Disable CometExecRule after the initial plan is generated. The CometSortMergeJoin and
            // CometBroadcastHashJoin nodes in the initial plan will be converted to Spark BroadcastHashJoin
            // during AQE. This will make CometBroadcastExchangeExec being converted to rows to be used by
            // Spark BroadcastHashJoin.
            withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
              df.collect()
            }

            // After AQE: CometBroadcastExchange has to be converted to rows to conform to Spark
            // BroadcastHashJoin.
            val plan = stripAQEPlan(df.queryExecution.executedPlan)
            columnarToRowExec = plan.collect {
              case s: CometColumnarToRowExec => s
              case s: CometNativeColumnarToRowExec => s
            }
            assert(columnarToRowExec.length == 1)

            // This ColumnarToRowExec should be a descendant of BroadcastHashJoinExec (possibly
            // wrapped by InputAdapter for codegen).
            val broadcastJoins = plan.collect { case b: BroadcastHashJoinExec => b }
            assert(broadcastJoins.nonEmpty, s"Expected BroadcastHashJoinExec in plan:\n$plan")
            val hasC2RDescendant = broadcastJoins.exists { join =>
              join.find {
                case _: CometColumnarToRowExec | _: CometNativeColumnarToRowExec => true
                case _ => false
              }.isDefined
            }
            assert(
              hasC2RDescendant,
              "BroadcastHashJoinExec should have a columnar-to-row descendant")

            // There should be a CometBroadcastExchangeExec under CometColumnarToRowExec
            val broadcastQueryStage =
              columnarToRowExec.head.find(_.isInstanceOf[BroadcastQueryStageExec])
            assert(broadcastQueryStage.isDefined)
            assert(
              broadcastQueryStage.get
                .asInstanceOf[BroadcastQueryStageExec]
                .broadcast
                .isInstanceOf[CometBroadcastExchangeExec])
          }
        }
      }
    }
  }

  test("expand operator") {
    val data1 = (0 until 1000)
      .map(_ % 5) // reduce value space to trigger dictionary encoding
      .map(i => (i, i + 100, i + 10))
    val data2 = (0 until 5).map(i => (i, i + 1, i * 1000))

    Seq(data1, data2).foreach { tableData =>
      withParquetTable(tableData, "tbl") {
        val df = sql("SELECT _1, _2, SUM(_3) FROM tbl GROUP BY _1, _2 GROUPING SETS ((_1), (_2))")
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("multiple distinct multiple columns sets") {
    withTable("agg2") {
      val data2 = Seq[(Integer, Integer, Integer)](
        (1, 10, -10),
        (null, -60, 60),
        (1, 30, -30),
        (1, 30, 30),
        (2, 1, 1),
        (null, -10, 10),
        (2, -1, null),
        (2, 1, 1),
        (2, null, 1),
        (null, 100, -10),
        (3, null, 3),
        (null, null, null),
        (3, null, null)).toDF("key", "value1", "value2")
      data2.write.saveAsTable("agg2")

      val df = spark.sql("""
          |SELECT
          |  key,
          |  count(distinct value1),
          |  sum(distinct value1),
          |  count(distinct value2),
          |  sum(distinct value2),
          |  count(distinct value1, value2),
          |  count(value1),
          |  sum(value1),
          |  count(value2),
          |  sum(value2),
          |  count(*),
          |  count(1)
          |FROM agg2
          |GROUP BY key
              """.stripMargin)

      // The above query uses SUM(DISTINCT) and count(distinct value1, value2)
      // which is not yet supported
      checkSparkAnswer(df)
      val subPlan = stripAQEPlan(df.queryExecution.executedPlan).collectFirst {
        case s: CometHashAggregateExec => s
      }
      assert(subPlan.isDefined)
      checkCometOperators(subPlan.get)
    }
  }

  test("explain native plan") {
    withSQLConf(
      CometConf.COMET_EXPLAIN_NATIVE_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
        val df = sql("select * FROM tbl a join tbl b on a._1 = b._2").select("a._1")
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("transformed cometPlan") {
    withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
      val df = sql("select * FROM tbl where _1 >= 2").select("_1")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("project") {
    withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
      val df = sql("SELECT _1 + 1, _2 + 2, _1 - 1, _2 * 2, _2 / 2 FROM tbl")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("project + filter on arrays") {
    withParquetTable((0 until 5).map(i => (i, i)), "tbl") {
      val df = sql("SELECT _1 FROM tbl WHERE _1 == _2")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("project + filter") {
    withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
      val df = sql("SELECT _1 + 1, _2 + 2 FROM tbl WHERE _1 > 3")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("empty projection") {
    withParquetDataFrame((0 until 5).map(i => (i, i + 1))) { df =>
      assert(df.where("_1 IS NOT NULL").count() == 5)
      checkSparkAnswerAndOperator(df)
      assert(df.select().limit(2).count() === 2)
    }
  }

  test("filter on string") {
    withParquetTable((0 until 5).map(i => (i, i.toString)), "tbl") {
      val df = sql("SELECT _1 + 1, _2 FROM tbl WHERE _2 = '3'")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("filter on dictionary string") {
    val data = (0 until 1000)
      .map(_ % 5) // reduce value space to trigger dictionary encoding
      .map(i => (i.toString, (i + 100).toString))

    withParquetTable(data, "tbl") {
      val df = sql("SELECT _1, _2 FROM tbl WHERE _1 = '3'")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("TakeOrderedAndProjectExec with positive offset") {
    withParquetTable((0 until 50).map(i => (i, i + 42)), "tbl") {
      val regularDfWithOffset = sql("SELECT _1, _2 FROM tbl order by _1 LIMIT 5 OFFSET 7")
      checkSparkAnswerAndOperator(regularDfWithOffset)
      val dfWithOffsetOnly = sql("SELECT _1, _2 FROM tbl order by _1 OFFSET 12")
      checkSparkAnswerAndOperator(dfWithOffsetOnly)
      val incompleteDf = sql("SELECT _1, _2 FROM tbl order by _1 LIMIT 5 OFFSET 47")
      checkSparkAnswerAndOperator(incompleteDf)
      val emptyDf = sql("SELECT _1, _2 FROM tbl order by _1 LIMIT 50 OFFSET 1000")
      checkSparkAnswerAndOperator(emptyDf)
    }
  }

  test("CollectLimitExec with positive offset") {
    withParquetTable((0 until 50).map(i => (i, i + 12)), "tbl") {
      // disable top-k sort to switch from TakeProjectExec to CollectLimitExec in the execution plan
      withSQLConf(SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD.key -> "0") {
        val regularDfWithOffset = sql("SELECT _1, _2 FROM tbl order by _2 LIMIT 4 OFFSET 4")
        checkSparkAnswerAndOperator(regularDfWithOffset)
        val dfWithOffsetOnly = sql("SELECT _1, _2 FROM tbl order by _2 OFFSET 15")
        checkSparkAnswerAndOperator(dfWithOffsetOnly)
        val incompleteDf = sql("SELECT _1, _2 FROM tbl order by _1 LIMIT 25 OFFSET 40")
        checkSparkAnswerAndOperator(incompleteDf)
        val emptyDf = sql("SELECT _1, _2 FROM tbl order by _1 LIMIT 50 OFFSET 1000")
        checkSparkAnswerAndOperator(emptyDf)
      }
    }
  }

  test("GlobalLimit with positive offset") {
    withParquetTable((0 until 50).map(i => (i, i + 13)), "tbl") {
      val regularDfWithOffset =
        sql("SELECT _1, _2 FROM tbl order by _2 LIMIT 4 OFFSET 1").groupBy("_1").agg(sum("_2"))
      checkSparkAnswerAndOperator(regularDfWithOffset)
      val dfWithOffsetOnly = sql("SELECT _1, _2 FROM tbl order by _2 OFFSET 15").agg(sum("_2"))
      checkSparkAnswerAndOperator(dfWithOffsetOnly)
    }
  }

  test("explicit zero limit and offset") {
    withParquetTable((0 until 50).map(i => (i, i + 8)), "tbl") {
      withSQLConf(
        "spark.sql.optimizer.excludedRules" -> "org.apache.spark.sql.catalyst.optimizer.EliminateLimits") {
        val dfWithZeroLimitAndOffsetOrdered =
          sql("SELECT _1, _2 FROM tbl order by _2 LIMIT 0 OFFSET 0")
        checkSparkAnswerAndOperator(dfWithZeroLimitAndOffsetOrdered)
        val dfWithZeroLimitAndOffsetUnordered =
          sql("SELECT _1, _2 FROM tbl LIMIT 0 OFFSET 0")
        checkSparkAnswerAndOperator(dfWithZeroLimitAndOffsetUnordered)
      }
    }
  }

  test("sort with dictionary") {
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> 8192.toString) {
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test")
        spark
          .createDataFrame((0 until 1000).map(i => (i % 5, (i % 7).toLong)))
          .write
          .option("compression", "none")
          .parquet(path.toString)

        spark
          .createDataFrame((0 until 1000).map(i => (i % 3 + 7, (i % 13 + 10).toLong)))
          .write
          .option("compression", "none")
          .mode(SaveMode.Append)
          .parquet(path.toString)

        val df = spark.read
          .format("parquet")
          .load(path.toString)
          .sortWithinPartitions($"_1".asc, $"_2".desc)

        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("final aggregation") {
    withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
      withParquetTable(
        (0 until 100)
          .map(_ => (Random.nextInt(), Random.nextInt() % 5)),
        "tbl") {
        val df = sql("SELECT _2, COUNT(*) FROM tbl GROUP BY _2")
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("bloom_filter_agg") {
    assume(!isSpark42Plus, "https://github.com/apache/datafusion-comet/issues/4142")
    val funcId_bloom_filter_agg = new FunctionIdentifier("bloom_filter_agg")
    spark.sessionState.functionRegistry.registerFunction(
      funcId_bloom_filter_agg,
      new ExpressionInfo(classOf[BloomFilterAggregate].getName, "bloom_filter_agg"),
      (children: Seq[Expression]) =>
        children.size match {
          case 1 => new BloomFilterAggregate(children.head)
          case 2 => new BloomFilterAggregate(children.head, children(1))
          case 3 => new BloomFilterAggregate(children.head, children(1), children(2))
        })

    withParquetTable(
      (0 until 100)
        .map(_ => (Random.nextInt(), Random.nextInt() % 5)),
      "tbl") {

      (if (isSpark35Plus) Seq("tinyint", "short", "int", "long", "string") else Seq("long"))
        .foreach { input_type =>
          val df = sql(f"SELECT bloom_filter_agg(cast(_2 as $input_type)) FROM tbl")
          checkSparkAnswerAndOperator(df)
        }
    }

    spark.sessionState.functionRegistry.dropFunction(funcId_bloom_filter_agg)
  }

  test("sort (non-global)") {
    withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
      val df = sql("SELECT * FROM tbl").sortWithinPartitions($"_1".desc)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("global sort (columnar shuffle only)") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
        val df = sql("SELECT * FROM tbl").sort($"_1".desc)
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("spill sort with (multiple) dictionaries") {
    withSQLConf(CometConf.COMET_ONHEAP_MEMORY_OVERHEAD.key -> "15MB") {
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        makeRawTimeParquetFileColumns(path, dictionaryEnabled = true, n = 1000, rowGroupSize = 10)
        readParquetFile(path.toString) { df =>
          Seq(
            $"_0".desc_nulls_first,
            $"_0".desc_nulls_last,
            $"_0".asc_nulls_first,
            $"_0".asc_nulls_last).foreach { colOrder =>
            val query = df.sortWithinPartitions(colOrder)
            checkSparkAnswerAndOperator(query)
          }
        }
      }
    }
  }

  test("spill sort with (multiple) dictionaries on mixed columns") {
    withSQLConf(CometConf.COMET_ONHEAP_MEMORY_OVERHEAD.key -> "15MB") {
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        makeRawTimeParquetFile(path, dictionaryEnabled = true, n = 1000, rowGroupSize = 10)
        readParquetFile(path.toString) { df =>
          Seq(
            $"_6".desc_nulls_first,
            $"_6".desc_nulls_last,
            $"_6".asc_nulls_first,
            $"_6".asc_nulls_last).foreach { colOrder =>
            // TODO: We should be able to sort on dictionary timestamp column
            val query = df.sortWithinPartitions(colOrder)
            checkSparkAnswerAndOperator(query)
          }
        }
      }
    }
  }

  test("limit") {
    Seq("native", "jvm").foreach { columnarShuffleMode =>
      withSQLConf(
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_MODE.key -> columnarShuffleMode) {
        withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl_a") {
          val df = sql("SELECT * FROM tbl_a")
            .repartition(10, $"_1")
            .limit(2)
            .sort($"_2".desc)
          checkSparkAnswerAndOperator(df)
        }
      }
    }
  }

  test("limit (cartesian product)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl_a") {
        withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl_b") {
          val df = sql("SELECT tbl_a._1, tbl_b._2 FROM tbl_a JOIN tbl_b LIMIT 2")
          checkSparkAnswerAndOperator(
            df,
            classOf[CollectLimitExec],
            classOf[CartesianProductExec])
        }
      }
    }
  }

  test("limit with more than one batch") {
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "1") {
      withParquetTable((0 until 50).map(i => (i, i + 1)), "tbl_a") {
        withParquetTable((0 until 50).map(i => (i, i + 1)), "tbl_b") {
          val df = sql("SELECT tbl_a._1, tbl_b._2 FROM tbl_a JOIN tbl_b LIMIT 2")
          checkSparkAnswerAndOperator(
            df,
            classOf[CollectLimitExec],
            classOf[BroadcastNestedLoopJoinExec],
            classOf[BroadcastExchangeExec])
        }
      }
    }
  }

  test("limit less than rows") {
    withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl_a") {
      withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl_b") {
        val df = sql(
          "SELECT tbl_a._1, tbl_b._2 FROM tbl_a JOIN tbl_b " +
            "WHERE tbl_a._1 > tbl_a._2 LIMIT 2")
        checkSparkAnswerAndOperator(
          df,
          classOf[CollectLimitExec],
          classOf[BroadcastNestedLoopJoinExec],
          classOf[BroadcastExchangeExec])
      }
    }
  }

  test("empty-column input (read schema is empty)") {
    withTable("t1") {
      Seq((1, true), (2, false))
        .toDF("l", "b")
        .repartition(2)
        .write
        .saveAsTable("t1")
      val query = spark.table("t1").selectExpr("IF(l > 1 AND null, 5, 1) AS out")
      checkSparkAnswerAndOperator(query)
    }
  }

  test("empty-column aggregation") {
    withTable("t1") {
      Seq((1, true), (2, false))
        .toDF("l", "b")
        .repartition(2)
        .write
        .saveAsTable("t1")
      val query = sql("SELECT count(1) FROM t1")
      checkSparkAnswerAndOperator(query)
    }
  }

  test("null handling") {
    Seq("true", "false").foreach { pushDown =>
      val table = "t1"
      withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> pushDown) {
        withTable(table) {
          sql(s"create table $table(a int, b int, c int) using parquet")
          sql(s"insert into $table values(1,0,0)")
          sql(s"insert into $table values(2,0,1)")
          sql(s"insert into $table values(3,1,0)")
          sql(s"insert into $table values(4,1,1)")
          sql(s"insert into $table values(5,null,0)")
          sql(s"insert into $table values(6,null,1)")
          sql(s"insert into $table values(7,null,null)")

          val query = sql(s"select a+120 from $table where b<10 OR c=1")
          checkSparkAnswerAndOperator(query)
        }
      }
    }
  }

  test("float4.sql") {
    val table = "t1"
    withTable(table) {
      sql(s"CREATE TABLE $table (f1  float) USING parquet")
      sql(s"INSERT INTO $table VALUES (float('    0.0'))")
      sql(s"INSERT INTO $table VALUES (float('1004.30   '))")
      sql(s"INSERT INTO $table VALUES (float('     -34.84    '))")
      sql(s"INSERT INTO $table VALUES (float('1.2345678901234e+20'))")
      sql(s"INSERT INTO $table VALUES (float('1.2345678901234e-20'))")

      val query = sql(s"SELECT '' AS four, f.* FROM $table f WHERE '1004.3' > f.f1")
      checkSparkAnswerAndOperator(query)
    }
  }

  test("NaN in predicate expression") {
    val t = "test_table"

    withTable(t) {
      Seq[(Integer, java.lang.Short, java.lang.Float)](
        (1, 100.toShort, 3.14.toFloat),
        (2, Short.MaxValue, Float.NaN),
        (3, Short.MinValue, Float.PositiveInfinity),
        (4, 0.toShort, Float.MaxValue),
        (5, null, null))
        .toDF("c1", "c2", "c3")
        .write
        .saveAsTable(t)

      val df = spark.table(t)

      var query = df.where("c3 > double('nan')").select("c1")
      checkSparkAnswer(query)
      // Empty result will be optimized to a local relation. No CometExec involved.
      // checkCometExec(query, 0, cometExecs => {})

      query = df.where("c3 >= double('nan')").select("c1")
      checkSparkAnswerAndOperator(query)
      // checkCometExec(query, 1, cometExecs => {})

      query = df.where("c3 == double('nan')").select("c1")
      checkSparkAnswerAndOperator(query)

      query = df.where("c3 <=> double('nan')").select("c1")
      checkSparkAnswerAndOperator(query)

      query = df.where("c3 != double('nan')").select("c1")
      checkSparkAnswerAndOperator(query)

      query = df.where("c3 <= double('nan')").select("c1")
      checkSparkAnswerAndOperator(query)

      query = df.where("c3 < double('nan')").select("c1")
      checkSparkAnswerAndOperator(query)
    }
  }

  test("table statistics") {
    withTempDatabase { database =>
      spark.catalog.setCurrentDatabase(database)
      withTempDir { dir =>
        withTable("t1", "t2") {
          spark.range(10).write.saveAsTable("t1")
          sql(
            s"CREATE EXTERNAL TABLE t2 USING parquet LOCATION '${dir.toURI}' " +
              "AS SELECT * FROM range(20)")

          sql(s"ANALYZE TABLES IN $database COMPUTE STATISTICS NOSCAN")
          checkTableStats("t1", hasSizeInBytes = true, expectedRowCounts = None)
          checkTableStats("t2", hasSizeInBytes = true, expectedRowCounts = None)

          sql("ANALYZE TABLES COMPUTE STATISTICS")
          checkTableStats("t1", hasSizeInBytes = true, expectedRowCounts = Some(10))
          checkTableStats("t2", hasSizeInBytes = true, expectedRowCounts = Some(20))
        }
      }
    }
  }

  test("like (LikeSimplification disabled)") {
    val table = "names"
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> "org.apache.spark.sql.catalyst.optimizer.LikeSimplification") {
      withTable(table) {
        sql(s"create table $table(id int, name varchar(20)) using parquet")
        sql(s"insert into $table values(1,'James Smith')")
        sql(s"insert into $table values(2,'Michael Rose')")
        sql(s"insert into $table values(3,'Robert Williams')")
        sql(s"insert into $table values(4,'Rames Rose')")
        sql(s"insert into $table values(5,'Rames rose')")

        // Filter column having values 'Rames _ose', where any character matches for '_'
        val query = sql(s"select id from $table where name like 'Rames _ose'")
        checkSparkAnswerAndOperator(query)

        // Filter rows that contains 'rose' in 'name' column
        val queryContains = sql(s"select id from $table where name like '%rose%'")
        checkSparkAnswerAndOperator(queryContains)

        // Filter rows that starts with 'R' following by any characters
        val queryStartsWith = sql(s"select id from $table where name like 'R%'")
        checkSparkAnswerAndOperator(queryStartsWith)

        // Filter rows that ends with 's' following by any characters
        val queryEndsWith = sql(s"select id from $table where name like '%s'")
        checkSparkAnswerAndOperator(queryEndsWith)
      }
    }
  }

  test("sum overflow (ANSI disable)") {
    Seq("true", "false").foreach { dictionary =>
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "false",
        "parquet.enable.dictionary" -> dictionary) {
        withParquetTable(Seq((Long.MaxValue, 1), (Long.MaxValue, 2)), "tbl") {
          val df = sql("SELECT sum(_1) FROM tbl")
          checkSparkAnswerAndOperator(df)
        }
      }
    }
  }

  test("partition col") {
    withSQLConf(SESSION_LOCAL_TIMEZONE.key -> "Asia/Kathmandu") {
      withTable("t1") {
        sql("""
            | CREATE TABLE t1(name STRING, part1 TIMESTAMP)
            | USING PARQUET PARTITIONED BY (part1)
       """.stripMargin)

        sql("""
            | INSERT OVERWRITE t1 PARTITION(
            | part1 = timestamp'2019-01-01 11:11:11'
            | ) VALUES('a')
      """.stripMargin)
        checkSparkAnswerAndOperator(sql("""
            | SELECT
            |   name,
            |   CAST(part1 AS STRING)
            | FROM t1
      """.stripMargin))
      }
    }
  }

  test("SPARK-33474: Support typed literals as partition spec values") {
    withSQLConf(SESSION_LOCAL_TIMEZONE.key -> "Asia/Kathmandu") {
      withTable("t1") {
        val binaryStr = "Spark SQL"
        val binaryHexStr = Hex.hex(UTF8String.fromString(binaryStr).getBytes).toString
        sql("""
            | CREATE TABLE t1(name STRING, part1 DATE, part2 TIMESTAMP, part3 BINARY,
            |  part4 STRING, part5 STRING, part6 STRING, part7 STRING)
            | USING PARQUET PARTITIONED BY (part1, part2, part3, part4, part5, part6, part7)
         """.stripMargin)

        sql(s"""
             | INSERT OVERWRITE t1 PARTITION(
             | part1 = date'2019-01-01',
             | part2 = timestamp'2019-01-01 11:11:11',
             | part3 = X'$binaryHexStr',
             | part4 = 'p1',
             | part5 = date'2019-01-01',
             | part6 = timestamp'2019-01-01 11:11:11',
             | part7 = X'$binaryHexStr'
             | ) VALUES('a')
        """.stripMargin)
        checkSparkAnswerAndOperator(sql("""
            | SELECT
            |   name,
            |   CAST(part1 AS STRING),
            |   CAST(part2 as STRING),
            |   CAST(part3 as STRING),
            |   part4,
            |   part5,
            |   part6,
            |   part7
            | FROM t1
        """.stripMargin))

        val e = intercept[AnalysisException] {
          sql("CREATE TABLE t2(name STRING, part INTERVAL) USING PARQUET PARTITIONED BY (part)")
        }.getMessage
        if (isSpark40Plus) {
          assert(e.contains(" Cannot use \"INTERVAL\""))
        } else {
          assert(e.contains("Cannot use interval"))
        }
      }
    }
  }

  def getCatalogTable(tableName: String): CatalogTable = {
    spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
  }

  def checkTableStats(
      tableName: String,
      hasSizeInBytes: Boolean,
      expectedRowCounts: Option[Int]): Option[CatalogStatistics] = {
    val stats = getCatalogTable(tableName).stats
    if (hasSizeInBytes || expectedRowCounts.nonEmpty) {
      assert(stats.isDefined)
      assert(stats.get.sizeInBytes >= 0)
      assert(stats.get.rowCount === expectedRowCounts)
    } else {
      assert(stats.isEmpty)
    }

    stats
  }

  def joinCondition(joinCols: Seq[String])(left: DataFrame, right: DataFrame): Column = {
    joinCols.map(col => left(col) === right(col)).reduce(_ && _)
  }

  def testBucketing(
      bucketedTableTestSpecLeft: BucketedTableTestSpec,
      bucketedTableTestSpecRight: BucketedTableTestSpec,
      joinType: String = "inner",
      joinCondition: (DataFrame, DataFrame) => Column): Unit = {
    val df1 =
      (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k").as("df1")
    val df2 =
      (0 until 50).map(i => (i % 7, i % 11, i.toString)).toDF("i", "j", "k").as("df2")

    val BucketedTableTestSpec(
      bucketSpecLeft,
      numPartitionsLeft,
      shuffleLeft,
      sortLeft,
      numOutputPartitionsLeft) = bucketedTableTestSpecLeft

    val BucketedTableTestSpec(
      bucketSpecRight,
      numPartitionsRight,
      shuffleRight,
      sortRight,
      numOutputPartitionsRight) = bucketedTableTestSpecRight

    withTable("bucketed_table1", "bucketed_table2") {
      withBucket(df1.repartition(numPartitionsLeft).write.format("parquet"), bucketSpecLeft)
        .saveAsTable("bucketed_table1")
      withBucket(df2.repartition(numPartitionsRight).write.format("parquet"), bucketSpecRight)
        .saveAsTable("bucketed_table2")

      withSQLConf(
        CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        val t1 = spark.table("bucketed_table1")
        val t2 = spark.table("bucketed_table2")
        val joined = t1.join(t2, joinCondition(t1, t2), joinType)

        val df = joined.sort("bucketed_table1.k", "bucketed_table2.k")
        checkSparkAnswer(df)

        // The sub-plan contains should contain all native operators except a SMJ
        val subPlan = stripAQEPlan(df.queryExecution.executedPlan).collectFirst {
          case s: SortMergeJoinExec => s
        }
        assert(subPlan.isDefined)
        checkCometOperators(subPlan.get, classOf[SortMergeJoinExec])
      }
    }
  }

  test("bucketed table") {
    val bucketSpec = Some(BucketSpec(8, Seq("i", "j"), Nil))
    val bucketedTableTestSpecLeft = BucketedTableTestSpec(bucketSpec, expectedShuffle = false)
    val bucketedTableTestSpecRight = BucketedTableTestSpec(bucketSpec, expectedShuffle = false)

    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight,
      joinCondition = joinCondition(Seq("i", "j")))
  }

  test("bucketed table with bucket pruning") {
    withSQLConf(
      SQLConf.BUCKETING_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
      val df1 = (0 until 100).map(i => (i, i % 13, s"left_$i")).toDF("id", "bucket_col", "value")
      val df2 = (0 until 100).map(i => (i, i % 13, s"right_$i")).toDF("id", "bucket_col", "value")
      val bucketSpec = Some(BucketSpec(8, Seq("bucket_col"), Nil))

      withTable("bucketed_table1", "bucketed_table2") {
        withBucket(df1.write.format("parquet"), bucketSpec).saveAsTable("bucketed_table1")
        withBucket(df2.write.format("parquet"), bucketSpec).saveAsTable("bucketed_table2")

        // Join two bucketed tables, but filter one side to trigger bucket pruning
        // Only buckets where hash(bucket_col) % 8 matches hash(1) % 8 will be read from left side
        val left = spark.table("bucketed_table1").filter("bucket_col = 1")
        val right = spark.table("bucketed_table2")

        val result = left.join(right, Seq("bucket_col"), "inner")

        checkSparkAnswerAndOperator(result)
      }
    }
  }

  def withBucket(
      writer: DataFrameWriter[Row],
      bucketSpec: Option[BucketSpec]): DataFrameWriter[Row] = {
    bucketSpec
      .map { spec =>
        writer.bucketBy(
          spec.numBuckets,
          spec.bucketColumnNames.head,
          spec.bucketColumnNames.tail: _*)

        if (spec.sortColumnNames.nonEmpty) {
          writer.sortBy(spec.sortColumnNames.head, spec.sortColumnNames.tail: _*)
        } else {
          writer
        }
      }
      .getOrElse(writer)
  }

  test("union") {
    withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
      val df1 = sql("select * FROM tbl where _1 >= 2").select("_1")
      val df2 = sql("select * FROM tbl where _1 >= 2").select("_2")
      val df3 = sql("select * FROM tbl where _1 >= 3").select("_2")

      val unionDf1 = df1.union(df2)
      checkSparkAnswerAndOperator(unionDf1)

      // Test union with different number of rows from inputs
      val unionDf2 = df1.union(df3)
      checkSparkAnswerAndOperator(unionDf2)

      val unionDf3 = df1.union(df2).union(df3)
      checkSparkAnswerAndOperator(unionDf3)
    }
  }

  test("native execution after union") {
    withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
      val df1 = sql("select * FROM tbl where _1 >= 2").select("_1")
      val df2 = sql("select * FROM tbl where _1 >= 2").select("_2")
      val df3 = sql("select * FROM tbl where _1 >= 3").select("_2")

      val unionDf1 = df1.union(df2).select($"_1" + 1).sortWithinPartitions($"_1")
      checkSparkAnswerAndOperator(unionDf1)

      // Test union with different number of rows from inputs
      val unionDf2 = df1.union(df3).select($"_1" + 1).sortWithinPartitions($"_1")
      checkSparkAnswerAndOperator(unionDf2)

      val unionDf3 = df1.union(df2).union(df3).select($"_1" + 1).sortWithinPartitions($"_1")
      checkSparkAnswerAndOperator(unionDf3)
    }
  }

  test("native execution after coalesce") {
    withTable("t1") {
      (0 until 5)
        .map(i => (i, (i + 1).toLong))
        .toDF("l", "b")
        .write
        .saveAsTable("t1")

      val df = sql("SELECT * FROM t1")
        .sortWithinPartitions($"l".desc)
        .repartition(10, $"l")

      val rdd = df.rdd
      assert(rdd.partitions.length == 10)

      val coalesced = df.coalesce(2).select($"l" + 1).sortWithinPartitions($"l")
      checkSparkAnswerAndOperator(coalesced)
    }
  }

  test("disabled/unsupported exec with multiple children should not disappear") {
    withSQLConf(
      CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "true",
      CometConf.COMET_EXEC_UNION_ENABLED.key -> "false") {
      withParquetDataFrame((0 until 5).map(Tuple1(_))) { df =>
        val projected = df.selectExpr("_1 as x")
        val unioned = projected.union(df)
        val p = unioned.queryExecution.executedPlan.find(_.isInstanceOf[UnionExec])
        assert(
          p.get
            .collectLeaves()
            .forall(o => o.isInstanceOf[CometScanExec] || o.isInstanceOf[CometNativeScanExec]))
      }
    }
  }

  test("coalesce") {
    withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
      withTable("t1") {
        (0 until 5)
          .map(i => (i, (i + 1).toLong))
          .toDF("l", "b")
          .write
          .saveAsTable("t1")

        val df = sql("SELECT * FROM t1")
          .sortWithinPartitions($"l".desc)
          .repartition(10, $"l")

        val rdd = df.rdd
        assert(rdd.getNumPartitions == 10)

        val coalesced = df.coalesce(2)
        checkSparkAnswerAndOperator(coalesced)

        val coalescedRdd = coalesced.rdd
        assert(coalescedRdd.getNumPartitions == 2)
      }
    }
  }

  test("TakeOrderedAndProjectExec") {
    Seq("true", "false").foreach(aqeEnabled =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled,
        CometConf.COMET_EXEC_WINDOW_ENABLED.key -> "true") {
        withTable("t1") {
          val numRows = 10
          spark
            .range(numRows)
            .selectExpr("if (id % 2 = 0, null, id) AS a", s"$numRows - id AS b")
            .repartition(3) // Move data across multiple partitions
            .write
            .saveAsTable("t1")

          val df1 = spark.sql("""
              |SELECT a, b, ROW_NUMBER() OVER(ORDER BY a, b) AS rn
              |FROM t1 LIMIT 3
              |""".stripMargin)

          assert(df1.rdd.getNumPartitions == 1)
          checkSparkAnswerAndOperator(df1, classOf[WindowExec])

          val df2 = spark.sql("""
              |SELECT b, RANK() OVER(ORDER BY a, b) AS rk, DENSE_RANK(b) OVER(ORDER BY a, b) AS s
              |FROM t1 LIMIT 2
              |""".stripMargin)
          assert(df2.rdd.getNumPartitions == 1)
          checkSparkAnswerAndOperator(df2, classOf[WindowExec], classOf[ProjectExec])

          // Other Comet native operator can take input from `CometTakeOrderedAndProjectExec`.
          val df3 = sql("SELECT * FROM t1 ORDER BY a, b LIMIT 3").groupBy($"a").sum("b")
          checkSparkAnswerAndOperator(df3)
        }
      })
  }

  test("TakeOrderedAndProjectExec without sorting") {
    Seq("true", "false").foreach(aqeEnabled =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.EliminateSorts") {
        withTable("t1") {
          val numRows = 10
          spark
            .range(numRows)
            .selectExpr("if (id % 2 = 0, null, id) AS a", s"$numRows - id AS b")
            .repartition(3) // Force repartition to test data will come to single partition
            .write
            .saveAsTable("t1")

          val df = spark
            .table("t1")
            .select("a", "b")
            .sortWithinPartitions("b", "a")
            .orderBy("b")
            .select($"b" + 1, $"a")
            .limit(3)

          val takeOrdered = stripAQEPlan(df.queryExecution.executedPlan).collect {
            case b: CometTakeOrderedAndProjectExec => b
          }
          assert(takeOrdered.length == 1)
          assert(takeOrdered.head.orderingSatisfies)

          checkSparkAnswerAndOperator(df)
        }
      })
  }

  test("TakeOrderedAndProjectExec with offset") {
    Seq("true", "false").foreach(aqeEnabled =>
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled) {
        withTable("t1") {
          val numRows = 10
          spark
            .range(numRows)
            .selectExpr("if (id % 2 = 0, null, id) AS a", s"$numRows - id AS b")
            .repartition(3) // Force repartition to test data will come to single partition
            .write
            .saveAsTable("t1")
          val df = sql("SELECT * FROM t1 ORDER BY a, b LIMIT 3 OFFSET 1").groupBy($"a").sum("b")
          checkSparkAnswerAndOperator(df)
        }
      })
  }

  test("collect limit") {
    Seq("true", "false").foreach(aqe => {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
        withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
          val df = sql("SELECT _1 as id, _2 as value FROM tbl limit 2")
          assert(df.queryExecution.executedPlan.execute().getNumPartitions === 1)
          checkSparkAnswerAndOperator(df, Seq(classOf[CometCollectLimitExec]))
          assert(df.collect().length === 2)

          val qe = df.queryExecution
          // make sure the root node is CometCollectLimitExec
          assert(qe.executedPlan.isInstanceOf[CometCollectLimitExec])
          // executes CometCollectExec directly to check doExecuteColumnar implementation
          SQLExecution.withNewExecutionId(qe, Some("count")) {
            qe.executedPlan.resetMetrics()
            assert(qe.executedPlan.execute().count() === 2)
          }

          assert(df.isEmpty === false)

          // follow up native operation is possible
          val df3 = df.groupBy("id").sum("value")
          checkSparkAnswerAndOperator(df3)
        }
      }
    })
  }

  test("SparkToColumnar over RangeExec") {
    Seq("true", "false").foreach(aqe => {
      Seq(500, 900).foreach { batchSize =>
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
          SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> batchSize.toString) {
          val df = spark.range(1000).selectExpr("id", "id % 8 as k").groupBy("k").sum("id")
          checkSparkAnswerAndOperator(df)
          // empty record batch should also be handled
          val df2 = spark.range(0).selectExpr("id", "id % 8 as k").groupBy("k").sum("id")
          checkSparkAnswerAndOperator(
            df2,
            includeClasses = Seq(classOf[CometSparkToColumnarExec]))
        }
      }
    })
  }

  test("SparkToColumnar over RangeExec directly is eliminated for row output") {
    Seq("true", "false").foreach(aqe => {
      Seq(500, 900).foreach { batchSize =>
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
          SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> batchSize.toString) {
          val df = spark.range(1000)
          val qe = df.queryExecution
          qe.executedPlan.collectFirst({ case r: CometSparkToColumnarExec => r }) match {
            case Some(_) => fail("CometSparkToColumnarExec should be eliminated")
            case _ =>
          }
        }
      }
    })
  }

  test("SparkToColumnar over BatchScan (Spark Parquet reader)") {
    Seq("", "parquet").foreach { v1List =>
      Seq(true, false).foreach { parquetVectorized =>
        Seq(
          "cast(id as tinyint)",
          "cast(id as smallint)",
          "cast(id as integer)",
          "cast(id as bigint)",
          "cast(id as float)",
          "cast(id as double)",
          "cast(id as decimal)",
          "cast(id as timestamp)",
          "cast(id as string)",
          "cast(id as binary)",
          "struct(id)").foreach { valueType =>
          {
            withSQLConf(
              SQLConf.USE_V1_SOURCE_LIST.key -> v1List,
              CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
              CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true",
              SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> parquetVectorized.toString) {
              withTempPath { dir =>
                var df = spark
                  .range(10000)
                  .selectExpr("id as key", s"$valueType as value")
                  .toDF("key", "value")

                df.write.parquet(dir.toString)

                df = spark.read.parquet(dir.toString)
                checkSparkAnswerAndOperator(
                  df.select("*").groupBy("key", "value").count(),
                  includeClasses = Seq(classOf[CometSparkToColumnarExec]))

                // Verify that the BatchScanExec nodes supported columnar output when requested for Spark 3.4+.
                // Earlier versions support columnar output for fewer type.
                val leaves = df.queryExecution.executedPlan.collectLeaves()
                if (parquetVectorized) {
                  assert(leaves.forall(_.supportsColumnar))
                } else {
                  assert(!leaves.forall(_.supportsColumnar))
                }
              }
            }
          }
        }
      }
    }
  }

  test("SparkToColumnar over InMemoryTableScanExec") {
    Seq("true", "false").foreach(cacheVectorized => {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
        SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> cacheVectorized) {
        spark
          .range(1000)
          .selectExpr("id as key", "id % 8 as value")
          .toDF("key", "value")
          .selectExpr("key", "value", "key+1")
          .createOrReplaceTempView("abc")
        spark.catalog.cacheTable("abc")
        val df = spark.sql("SELECT * FROM abc").groupBy("key").count()
        checkSparkAnswerAndOperator(df, includeClasses = Seq(classOf[CometSparkToColumnarExec]))
        df.collect() // Without this collect we don't get an aggregation of the metrics.

        val metrics = find(df.queryExecution.executedPlan) {
          case _: CometSparkToColumnarExec => true
          case _ => false
        }.map(_.metrics).get

        assert(metrics.contains("conversionTime"))
        assert(metrics("conversionTime").value > 0)

      }
    })
  }

  test("SparkToColumnar eliminate redundant in AQE") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      val df = spark
        .range(1000)
        .selectExpr("id as key", "id % 8 as value")
        .toDF("key", "value")
        .groupBy("key")
        .count()
      df.collect()

      val planAfter = df.queryExecution.executedPlan
      assert(planAfter.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
      val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan
      // Use AdaptiveSparkPlanHelper.collect so traversal descends through QueryStageExec.plan;
      // Spark 4 wraps the final plan in ResultQueryStageExec (a LeafExecNode) that
      // SparkPlan.collect would otherwise stop at.
      val numOperators = collect(adaptivePlan) { case c: CometSparkToColumnarExec =>
        c
      }
      assert(numOperators.length == 1)
    }
  }

  test("SparkToColumnar read all types") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val schemaGenOptions =
          SchemaGenOptions(generateArray = true, generateStruct = true, generateMap = true)
        val dataGenOptions = DataGenOptions(allowNull = true, generateNegativeZero = true)
        ParquetGenerator.makeParquetFile(
          random,
          spark,
          filename,
          100,
          schemaGenOptions,
          dataGenOptions)
      }
      withSQLConf(
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
        val table = spark.read.parquet(filename)
        table.createOrReplaceTempView("t1")
        checkSparkAnswer(sql("SELECT * FROM t1"))
      }
    }
  }

  test("read CSV file") {
    Seq("", "csv").foreach { v1List =>
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> v1List,
        CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true",
        CometConf.COMET_CONVERT_FROM_CSV_ENABLED.key -> "true") {
        spark.read
          .csv("src/test/resources/test-data/csv-test-1.csv")
          .createOrReplaceTempView("tbl")
        // use a projection with an expression otherwise we end up with
        // just the file scan
        checkSparkAnswerAndOperator("SELECT cast(_c0 as int), _c1, _c2 FROM tbl")
      }
    }
  }

  test("read JSON file") {
    Seq("", "json").foreach { v1List =>
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> v1List,
        CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true",
        CometConf.COMET_CONVERT_FROM_JSON_ENABLED.key -> "true") {
        spark.read
          .json("src/test/resources/test-data/json-test-1.ndjson")
          .createOrReplaceTempView("tbl")
        checkSparkAnswerAndOperator("SELECT a, b.c, b.d FROM tbl")
      }
    }
  }

  test("Supported file formats for CometScanExec") {
    assert(CometScanExec.isFileFormatSupported(new ParquetFileFormat()))

    class CustomParquetFileFormat extends ParquetFileFormat {}

    assert(!CometScanExec.isFileFormatSupported(new CustomParquetFileFormat()))
  }

  test("SparkToColumnar override node name for row input") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      val df = spark
        .range(1000)
        .selectExpr("id as key", "id % 8 as value")
        .toDF("key", "value")
        .groupBy("key")
        .count()
      df.collect()

      val planAfter = df.queryExecution.executedPlan
      assert(planAfter.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
      val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan
      // See comment in the "eliminate redundant in AQE" test about AdaptiveSparkPlanHelper.collect.
      val nodeNames = collect(adaptivePlan) { case c: CometSparkToColumnarExec =>
        c.nodeName
      }
      assert(nodeNames.length == 1)
      assert(nodeNames.head == "CometSparkRowToColumnar")
    }
  }

  test("ReusedExchange broadcast with incompatible partitions number does not fail") {
    withTable("tbl1", "tbl2", "tbl3") {
      // enforce different number of partitions for future broadcasts/exchanges
      spark
        .range(50)
        .withColumnRenamed("id", "x")
        .repartition(2)
        .writeTo("tbl1")
        .using("parquet")
        .create()
      spark
        .range(50)
        .withColumnRenamed("id", "y")
        .repartition(3)
        .writeTo("tbl2")
        .using("parquet")
        .create()
      spark
        .range(50)
        .withColumnRenamed("id", "z")
        .repartition(4)
        .writeTo("tbl3")
        .using("parquet")
        .create()
      val df1 = spark.table("tbl1")
      val df2 = spark.table("tbl2")
      val df3 = spark.table("tbl3")
      Seq("true", "false").foreach(aqeEnabled =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled) {
          val dfWithReusedExchange = df1
            .join(df3.hint("broadcast").join(df1, $"x" === $"z"), "x", "right")
            .join(
              df3
                .hint("broadcast")
                .join(df2, $"y" === $"z", "right")
                .withColumnRenamed("z", "z1"),
              $"x" === $"y")
          checkSparkAnswerAndOperator(dfWithReusedExchange, classOf[ReusedExchangeExec])
        })
    }
  }

  test("SparkToColumnar override node name for columnar input") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
      withTempDir { dir =>
        var df = spark
          .range(10000)
          .selectExpr("id as key", "id % 8 as value")
          .toDF("key", "value")

        df.write.mode("overwrite").parquet(dir.toString)
        df = spark.read.parquet(dir.toString)
        df = df.groupBy("key", "value").count()
        df.collect()

        val planAfter = df.queryExecution.executedPlan
        val nodeNames = planAfter.collect { case c: CometSparkToColumnarExec =>
          c.nodeName
        }
        assert(nodeNames.length == 1)
        assert(nodeNames.head == "CometSparkColumnarToColumnar")
      }
    }
  }

  test("LocalTableScanExec spark fallback") {
    withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "false") {
      val df = Seq.range(0, 10).toDF("id")
      checkSparkAnswerAndFallbackReason(
        df,
        "Native support for operator LocalTableScanExec is disabled")
    }
  }

  test("LocalTableScanExec with filter") {
    withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      val df = Seq.range(0, 10).toDF("id").filter(col("id") > 5)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("LocalTableScanExec with groupBy") {
    withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      val df = (Seq.range(0, 10) ++ Seq.range(0, 20))
        .toDF("id")
        .groupBy(col("id"))
        .agg(count("*"))
      checkSparkAnswerAndOperator(df)
    }
  }

  test("LocalTableScanExec with timestamps in non-UTC timezone") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      val df = Seq(
        (1, java.sql.Timestamp.valueOf("2024-01-15 10:30:00")),
        (2, java.sql.Timestamp.valueOf("2024-06-15 14:00:00")),
        (3, java.sql.Timestamp.valueOf("2024-12-25 08:00:00")))
        .toDF("id", "ts")
        .orderBy("ts")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("SparkToColumnar with timestamps in non-UTC timezone") {
    withTempDir { dir =>
      val path = new java.io.File(dir, "data").getAbsolutePath
      Seq(
        (1, java.sql.Timestamp.valueOf("2024-01-15 10:30:00")),
        (2, java.sql.Timestamp.valueOf("2024-06-15 14:00:00")),
        (3, java.sql.Timestamp.valueOf("2024-12-25 08:00:00")))
        .toDF("id", "ts")
        .write
        .parquet(path)
      withSQLConf(
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true",
        SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
        val df = spark.read.parquet(path).orderBy("ts")
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("sort on timestamps with non-UTC timezone via LocalTableScan") {
    // When session timezone is non-UTC, CometLocalTableScanExec and
    // CometSparkToColumnarExec must use UTC for the Arrow schema timezone
    // to match the native side's expectations. Without this, the native
    // ScanExec sees a timezone mismatch and performs an unnecessary cast.
    // The cast is currently a no-op (Arrow timestamps with timezone are
    // always UTC microseconds), but using UTC avoids the overhead and
    // keeps schemas consistent throughout the native plan.
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      val df = Seq(
        (1, java.sql.Timestamp.valueOf("2024-01-15 10:30:00")),
        (2, java.sql.Timestamp.valueOf("2024-06-15 14:00:00")),
        (3, java.sql.Timestamp.valueOf("2024-12-25 08:00:00")))
        .toDF("id", "ts")
        .repartition(2)
        .orderBy("ts")
      checkSparkAnswer(df)
    }
  }

  test("Native_datafusion reports correct files and bytes scanned") {
    val inputFiles = 2

    withTempDir { dir =>
      val path = new java.io.File(dir, "test_metrics").getAbsolutePath
      spark.range(100).repartition(inputFiles).write.mode("overwrite").parquet(path)

      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> "native_datafusion") {
        val df = spark.read.parquet(path)

        // Trigger two different actions to ensure metrics are not duplicated
        df.count()
        df.collect()

        val scanNode = stripAQEPlan(df.queryExecution.executedPlan)
          .collectFirst {
            case n: org.apache.spark.sql.comet.CometNativeScanExec => n
            case n: org.apache.spark.sql.comet.CometScanExec => n
          }
          .getOrElse {
            fail(
              s"Comet scan node not found in the physical plan. Plan: \n${df.queryExecution.executedPlan}")
          }

        val numFiles = scanNode.metrics("numFiles").value
        assert(
          numFiles == inputFiles,
          s"Expected exactly $inputFiles files to be scanned, but got metrics reporting $numFiles")
      }
    }
  }

}

case class BucketedTableTestSpec(
    bucketSpec: Option[BucketSpec],
    numPartitions: Int = 10,
    expectedShuffle: Boolean = true,
    expectedSort: Boolean = true,
    expectedNumOutputPartitions: Option[Int] = None)

case class TestData(key: Int, value: String)
