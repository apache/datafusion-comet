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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.{Column, CometTestBase, DataFrame, DataFrameWriter, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStatistics, CatalogTable}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometExecSuite extends CometTestBase {

  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
        testFun
      }
    }
  }

  test("empty projection") {
    withParquetDataFrame((0 until 5).map(i => (i, i + 1))) { df =>
      df.where("_1 IS NOT NULL").count()
    // df.select().limit(2).count()
    }
  }

  /*
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
   */

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
        "spark.comet.exec.sortMergeJoin.disabled" -> "true",
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
}

case class BucketedTableTestSpec(
    bucketSpec: Option[BucketSpec],
    numPartitions: Int = 10,
    expectedShuffle: Boolean = true,
    expectedSort: Boolean = true,
    expectedNumOutputPartitions: Option[Int] = None)

case class TestData(key: Int, value: String)
