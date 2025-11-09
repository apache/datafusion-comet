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
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Hex}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateMode, BloomFilterAggregate}
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{CollectLimitExec, ProjectExec, SQLExecution, UnionExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.reuse.ReuseExchangeAndSubquery
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.SESSION_LOCAL_TIMEZONE
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark40Plus}
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

  test("DPP fallback") {
    withTempDir { path =>
      // create test data
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

      // note that this test does not trigger DPP with v2 data source
      Seq("parquet").foreach { v1List =>
        withSQLConf(
          SQLConf.USE_V1_SOURCE_LIST.key -> v1List,
          CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "true") {
          spark.read.parquet(factPath).createOrReplaceTempView("dpp_fact")
          spark.read.parquet(dimPath).createOrReplaceTempView("dpp_dim")
          val df =
            spark.sql(
              "select * from dpp_fact join dpp_dim on fact_date = dim_date where dim_id > 7")
          val (_, cometPlan) = checkSparkAnswer(df)
          val infos = new ExtendedExplainInfo().generateExtendedInfo(cometPlan)
          assert(infos.contains("Dynamic Partition Pruning is not supported"))

          assert(infos.contains("Comet accelerated"))
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

  test("ReusedExchangeExec should work on CometBroadcastExchangeExec") {
    withSQLConf(
      CometConf.COMET_EXEC_BROADCAST_FORCE_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempPath { path =>
        spark
          .range(5)
          .withColumn("p", $"id" % 2)
          .write
          .mode("overwrite")
          .partitionBy("p")
          .parquet(path.toString)
        withTempView("t") {
          spark.read.parquet(path.toString).createOrReplaceTempView("t")
          val df = sql("""
              |SELECT t1.id, t2.id, t3.id
              |FROM t AS t1
              |JOIN t AS t2 ON t2.id = t1.id
              |JOIN t AS t3 ON t3.id = t2.id
              |WHERE t1.p = 1 AND t2.p = 1 AND t3.p = 1
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
        CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
        CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
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

  test("Comet native metrics: scan") {
    withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "true") {
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "native-scan.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = true, 10000)
        withParquetTable(path.toString, "tbl") {
          val df = sql("SELECT * FROM tbl WHERE _2 > _3")
          df.collect()

          find(df.queryExecution.executedPlan)(s =>
            s.isInstanceOf[CometScanExec] || s.isInstanceOf[CometNativeScanExec])
            .foreach(scan => {
              val metrics = scan.metrics

              assert(metrics.contains("time_elapsed_scanning_total"))
              assert(metrics.contains("bytes_scanned"))
              assert(metrics.contains("output_rows"))
              assert(metrics.contains("time_elapsed_opening"))
              assert(metrics.contains("time_elapsed_processing"))
              assert(metrics.contains("time_elapsed_scanning_until_data"))
              assert(metrics("time_elapsed_scanning_total").value > 0)
              assert(metrics("bytes_scanned").value > 0)
              assert(metrics("output_rows").value > 0)
              assert(metrics("time_elapsed_opening").value > 0)
              assert(metrics("time_elapsed_processing").value > 0)
              assert(metrics("time_elapsed_scanning_until_data").value > 0)
            })

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
        assert(metrics("output_batches").value == 5L)
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
        assert(metrics("build_input_batches").value == 25L)
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
            var columnarToRowExec = stripAQEPlan(df.queryExecution.executedPlan).collect {
              case s: CometColumnarToRowExec => s
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
            columnarToRowExec = plan.collect { case s: CometColumnarToRowExec =>
              s
            }
            assert(columnarToRowExec.length == 1)

            // This ColumnarToRowExec should be the immediate child of BroadcastHashJoinExec
            val parent = plan.find(_.children.contains(columnarToRowExec.head))
            assert(parent.get.isInstanceOf[BroadcastHashJoinExec])

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
    withSQLConf(
      SESSION_LOCAL_TIMEZONE.key -> "Asia/Kathmandu",
      CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
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
    // native_datafusion actually passes this test, but in the case where buckets are pruned it fails, so we're
    // falling back for bucketed scans entirely as a workaround.
    // https://github.com/apache/datafusion-comet/issues/1719
    assume(CometConf.COMET_NATIVE_SCAN_IMPL.get() != CometConf.SCAN_NATIVE_DATAFUSION)
    val bucketSpec = Some(BucketSpec(8, Seq("i", "j"), Nil))
    val bucketedTableTestSpecLeft = BucketedTableTestSpec(bucketSpec, expectedShuffle = false)
    val bucketedTableTestSpecRight = BucketedTableTestSpec(bucketSpec, expectedShuffle = false)

    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight,
      joinCondition = joinCondition(Seq("i", "j")))
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
    // TODO fix for Spark 4.0.0
    assume(!isSpark40Plus)
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
      val numOperators = adaptivePlan.collect { case c: CometSparkToColumnarExec =>
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
    // TODO fix for Spark 4.0.0
    assume(!isSpark40Plus)
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
      val nodeNames = adaptivePlan.collect { case c: CometSparkToColumnarExec =>
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

}

case class BucketedTableTestSpec(
    bucketSpec: Option[BucketSpec],
    numPartitions: Int = 10,
    expectedShuffle: Boolean = true,
    expectedSort: Boolean = true,
    expectedNumOutputPartitions: Option[Int] = None)

case class TestData(key: Int, value: String)
