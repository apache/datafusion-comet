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

import scala.util.Random

import org.apache.spark.sql.Column
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._

import org.apache.comet.DataTypeSupport.isComplexType
import org.apache.comet.serde.OperatorOuterClass

class CometFuzzIcebergSuite extends CometFuzzIcebergBase {

  test("select *") {
    val sql = s"SELECT * FROM $icebergTableName"
    val (_, cometPlan) = checkSparkAnswer(sql)
    assert(1 == collectIcebergNativeScans(cometPlan).length)
  }

  test("select * with limit") {
    val sql = s"SELECT * FROM $icebergTableName LIMIT 500"
    val (_, cometPlan) = checkSparkAnswer(sql)
    assert(1 == collectIcebergNativeScans(cometPlan).length)
  }

  test("order by single column") {
    val df = spark.table(icebergTableName)
    for (col <- df.columns) {
      val sql = s"SELECT $col FROM $icebergTableName ORDER BY $col"
      // cannot run fully natively due to range partitioning and sort
      val (_, cometPlan) = checkSparkAnswer(sql)
      assert(1 == collectIcebergNativeScans(cometPlan).length)
    }
  }

  test("order by multiple columns") {
    val df = spark.table(icebergTableName)
    val allCols = df.columns.mkString(",")
    val sql = s"SELECT $allCols FROM $icebergTableName ORDER BY $allCols"
    // cannot run fully natively due to range partitioning and sort
    val (_, cometPlan) = checkSparkAnswer(sql)
    assert(1 == collectIcebergNativeScans(cometPlan).length)
  }

  test("order by random columns") {
    val df = spark.table(icebergTableName)

    for (_ <- 1 to 10) {
      // We only do order by permutations of primitive types to exercise native shuffle's
      // RangePartitioning which only supports those types.
      val primitiveColumns =
        df.schema.fields.filterNot(f => isComplexType(f.dataType)).map(_.name)
      val shuffledPrimitiveCols = Random.shuffle(primitiveColumns.toList)
      val randomSize = Random.nextInt(shuffledPrimitiveCols.length) + 1
      val randomColsSubset = shuffledPrimitiveCols.take(randomSize).toArray.mkString(",")
      val sql = s"SELECT $randomColsSubset FROM $icebergTableName ORDER BY $randomColsSubset"
      checkSparkAnswerAndOperator(sql)
    }
  }

  test("distribute by single column (complex types)") {
    val df = spark.table(icebergTableName)
    val columns = df.schema.fields.filter(f => isComplexType(f.dataType)).map(_.name)
    for (col <- columns) {
      // DISTRIBUTE BY is equivalent to df.repartition($col) and uses
      val sql = s"SELECT $col FROM $icebergTableName DISTRIBUTE BY $col"
      val resultDf = spark.sql(sql)
      resultDf.collect()
      // check for Comet shuffle
      val plan =
        resultDf.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec].executedPlan
      val cometShuffleExchanges = collectCometShuffleExchanges(plan)
      // Iceberg native scan supports complex types
      assert(cometShuffleExchanges.length == 1)
    }
  }

  test("shuffle supports all types") {
    val df = spark.table(icebergTableName)
    val df2 = df.repartition(8, df.col("c0")).sort("c1")
    df2.collect()
    val cometShuffles = collectCometShuffleExchanges(df2.queryExecution.executedPlan)
    // Iceberg native scan supports complex types
    assert(cometShuffles.length == 2)
  }

  test("join") {
    val df = spark.table(icebergTableName)
    df.createOrReplaceTempView("t1")
    df.createOrReplaceTempView("t2")
    // Filter out complex types - iceberg-rust can't create predicates for struct/array/map equality
    val primitiveColumns = df.schema.fields.filterNot(f => isComplexType(f.dataType)).map(_.name)
    for (col <- primitiveColumns) {
      // cannot run fully native due to HashAggregate
      val sql = s"SELECT count(*) FROM t1 JOIN t2 ON t1.$col = t2.$col"
      val (_, cometPlan) = checkSparkAnswer(sql)
      assert(2 == collectIcebergNativeScans(cometPlan).length)
    }
  }

  test("filter pushdown - fuzz predicates over primitive columns") {
    val df = spark.table(icebergTableName)
    val primitiveColumns =
      df.schema.fields.filterNot(f => isComplexType(f.dataType)).map(_.name)
    assert(primitiveColumns.nonEmpty, "expected at least one primitive column in the fuzz schema")

    for (name <- primitiveColumns) {
      // Sample real values of the column's own type so predicates are always well-typed, avoiding
      // per-type literal generation and SQL formatting. Skip NaN, whose comparison differs.
      val values = df
        .select(col(name))
        .where(col(name).isNotNull)
        .distinct()
        .limit(3)
        .collect()
        .map(_.get(0))
        .filterNot {
          case d: Double => d.isNaN
          case f: Float => f.isNaN
          case _ => false
        }
        .toSeq

      val c = col(name)
      val predicates = scala.collection.mutable.ArrayBuffer[Column](c.isNull, c.isNotNull)
      values.headOption.foreach { v =>
        val l = lit(v)
        // Comparisons, not-equal and explicit NOT (exercises the native rewrite_not path), and
        // AND/OR combinations.
        predicates ++= Seq(
          c === l,
          c =!= l,
          c > l,
          c >= l,
          c < l,
          c <= l,
          !(c === l),
          c.isNotNull && (c === l),
          (c === l) || c.isNull)
      }
      if (values.nonEmpty) {
        predicates += c.isin(values: _*)
        predicates += !c.isin(values: _*)
      }

      for (pred <- predicates) {
        val (_, cometPlan) = checkSparkAnswer(df.where(pred))
        assert(
          collectIcebergNativeScans(cometPlan).nonEmpty,
          s"expected a native Iceberg scan for predicate on '$name': $pred")
      }
    }
  }

  // Asserts that predicate pushdown actually happens for the types that support it. For each
  // primitive column it runs an equality filter and inspects the serialized IcebergScanCommon: the
  // residual pool must be non-empty exactly when the column's type is one Comet maps to an Iceberg
  // literal (pushable), and empty for a gated type (binary). This works because the fuzz table is
  // unpartitioned, so Iceberg's per-file residual is the full filter rather than a partition-pruned
  // remnant. It exists because the correctness test above cannot see pushdown at all -- results
  // match and the scan stays native whether or not a predicate pushes, since CometFilter enforces
  // it either way -- so only a residual-pool check catches a regression like byte/short mapping to
  // None or binary escaping the page-index gate.
  test("filter pushdown - residual pool reflects whether the column type is pushable") {
    // Reading commonData forces Iceberg planning (ParallelIterable), which on older Iceberg leaves
    // a prefetched manifest stream open that Spark's DebugFilesystem flags at teardown.
    assume(
      !isIcebergVersionLessThan("1.8.0"),
      "ParallelIterable leaks manifest streams on older Iceberg")

    // Types predicateLiteralToProto maps to an IcebergLiteral. Everything else present in the fuzz
    // table (binary) is gated or unmapped and must not push.
    def isPushable(dt: DataType): Boolean = dt match {
      case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType |
          _: FloatType | _: DoubleType | _: DateType | _: StringType | _: TimestampType |
          _: TimestampNTZType =>
        true
      case _ => false
    }

    val df = spark.table(icebergTableName)
    val primitiveColumns = df.schema.fields.filterNot(f => isComplexType(f.dataType))
    assert(primitiveColumns.nonEmpty, "expected at least one primitive column in the fuzz schema")

    for (field <- primitiveColumns) {
      val name = field.name
      val sample = df
        .select(col(name))
        .where(col(name).isNotNull)
        .limit(1)
        .collect()
        .headOption
        .map(_.get(0))
        // Skip NaN, whose equality semantics differ, so the sampled literal is well-behaved.
        .filterNot {
          case d: Double => d.isNaN
          case f: Float => f.isNaN
          case _ => false
        }

      sample.foreach { v =>
        val (_, cometPlan) = checkSparkAnswer(df.where(col(name) === lit(v)))
        val scans = collectIcebergNativeScans(cometPlan)
        assert(scans.length == 1, s"expected one native scan for '$name'\n$cometPlan")
        val common = OperatorOuterClass.IcebergScanCommon.parseFrom(scans.head.commonData)
        val pushed = common.getResidualPoolCount > 0
        if (isPushable(field.dataType)) {
          assert(
            pushed,
            s"expected a pushed residual for pushable column '$name' (${field.dataType})")
        } else {
          assert(
            !pushed,
            s"expected no residual pushdown for gated column '$name' (${field.dataType})")
        }
      }
    }
  }

  def collectCometShuffleExchanges(plan: org.apache.spark.sql.execution.SparkPlan)
      : Seq[org.apache.spark.sql.execution.SparkPlan] = {
    collect(plan) {
      case exchange: org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec =>
        exchange
    }
  }
}
