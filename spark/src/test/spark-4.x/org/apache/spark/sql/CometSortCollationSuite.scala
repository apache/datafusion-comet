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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.comet.{CometSortExec, CometTakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.{LocalTableScanExec, SortExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.{CometConf, CometExplainInfo}
import org.apache.comet.serde.QueryPlanSerde

class CometSortCollationSuite extends CometTestBase {

  private val sortCollationReason = "Sort does not support non-default string collation"

  private def assertSparkSort(plan: SparkPlan): Unit = {
    assert(collect(plan) { case sort: SortExec => sort }.nonEmpty, plan.toString)
    assert(collect(plan) { case sort: CometSortExec => sort }.isEmpty, plan.toString)
  }

  private def sortPlan(dataTypes: Seq[DataType]): SortExec = {
    val keys = dataTypes.zipWithIndex.map { case (dataType, index) =>
      AttributeReference(s"key$index", dataType)()
    }
    SortExec(
      keys.map(key => SortOrder(key, Ascending)),
      global = false,
      child = LocalTableScanExec(keys, Nil, None))
  }

  test("window sort with UTF8_LCASE and multiple keys falls back to Spark (issue #6158)") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withParquetTable(Seq((1, "b"), (2, "A"), (3, "a"), (4, "B")), "tbl") {
        // Window ordering uses SinglePartition, so the hash/range shuffle collation guards
        // cannot hide a missing sort guard. The row numbers expose incorrect byte ordering.
        val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
          "SELECT _1, c, row_number() OVER (ORDER BY c, _1) AS rn " +
            "FROM (SELECT _1, _2 COLLATE UTF8_LCASE AS c FROM tbl)",
          sortCollationReason)
        assertSparkSort(cometPlan)
      }
    }
  }

  test("sort type check rejects non-default collation in every key and nested type") {
    val collated = StringType("UTF8_LCASE")
    val collatedTypes = Seq(
      collated,
      StructType(Seq(StructField("text", collated))),
      ArrayType(collated),
      MapType(collated, IntegerType),
      MapType(IntegerType, collated),
      ArrayType(StructType(Seq(StructField("nested", ArrayType(collated))))),
      StructType(Seq(StructField("nested", MapType(IntegerType, ArrayType(collated))))))

    for {
      dataType <- collatedTypes
      keyTypes <- Seq(Seq(dataType), Seq(dataType, IntegerType), Seq(IntegerType, dataType))
    } {
      withClue(s"Sort key types: $keyTypes: ") {
        // Maps cannot be ordered in SQL, but the recursive type guard must inspect both
        // their keys and values if it receives such a physical sort expression.
        val plan = sortPlan(keyTypes)
        assert(!QueryPlanSerde.supportedSortType(plan, plan.sortOrder))
        assert(
          plan
            .getTagValue(CometExplainInfo.FALLBACK_REASONS)
            .exists(_.contains(sortCollationReason)))
      }
    }
  }

  test("sort type check preserves default collation and existing single-key restrictions") {
    val defaultTypes = Seq(
      StringType,
      ArrayType(StringType),
      MapType(StringType, IntegerType),
      MapType(IntegerType, StringType))
    for {
      dataType <- defaultTypes
      keyTypes <- Seq(Seq(dataType), Seq(dataType, IntegerType), Seq(IntegerType, dataType))
    } {
      withClue(s"Sort key types: $keyTypes: ") {
        val plan = sortPlan(keyTypes)
        assert(QueryPlanSerde.supportedSortType(plan, plan.sortOrder))
      }
    }

    // The collation guard must not extend the existing single-key restrictions to multi-key
    // sorting, which uses a different representation and already supports structs.
    val structType = StructType(Seq(StructField("text", StringType)))
    val singleKey = sortPlan(Seq(structType))
    assert(!QueryPlanSerde.supportedSortType(singleKey, singleKey.sortOrder))
    val multipleKeys = sortPlan(Seq(structType, IntegerType))
    assert(QueryPlanSerde.supportedSortType(multipleKeys, multipleKeys.sortOrder))
  }

  test("window sort rejects collated keys after another key and inside nested types") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      // All values of the first key tie, so ordering must actually compare the collated key.
      withParquetTable(Seq((0, 1, "b"), (0, 2, "A"), (0, 3, "a"), (0, 4, "B")), "tbl") {
        for (sortKeys <- Seq("_1, c, _2", "struct(array(c)), _2")) {
          withClue(s"Sort keys: $sortKeys: ") {
            val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
              s"SELECT _2, c, row_number() OVER (ORDER BY $sortKeys) AS rn " +
                "FROM (SELECT _1, _2, _3 COLLATE UTF8_LCASE AS c FROM tbl)",
              sortCollationReason)
            assertSparkSort(cometPlan)
          }
        }
      }
    }
  }

  test("local sort after round-robin shuffle rejects non-default collation") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      val data = (1 to 8).flatMap { n =>
        Seq((n * 4, "b"), (n * 4 + 1, "A"), (n * 4 + 2, "a"), (n * 4 + 3, "B"))
      }
      withParquetTable(data, "tbl") {
        // checkSparkAnswer preserves collected order when the logical plan contains Sort,
        // including sortWithinPartitions; this checks ordering, not only row membership.
        val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
          sql("SELECT _1, _2 COLLATE UTF8_LCASE AS c FROM tbl")
            .repartition(2)
            .sortWithinPartitions("c", "_1"),
          sortCollationReason)
        assertSparkSort(cometPlan)
        assert(
          collect(cometPlan) {
            case plan if plan.outputPartitioning.isInstanceOf[RoundRobinPartitioning] => plan
          }.nonEmpty,
          cometPlan.toString)
      }
    }
  }

  test("top-k with multiple keys rejects non-default collation") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withParquetTable(Seq((1, "b"), (2, "A"), (3, "a"), (4, "B")), "tbl") {
        // LIMIT 2 changes row membership under byte ordering: A,B instead of A,a.
        // Keep COLLATE below the exchange so TopK projects attributes and reaches the sort
        // guard instead of falling back while serializing a fused COLLATE projection.
        val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
          sql("SELECT _1, _2 COLLATE UTF8_LCASE AS c FROM tbl")
            .repartition(1)
            .orderBy("c", "_1")
            .limit(2),
          sortCollationReason)
        assert(
          collect(cometPlan) { case topK: TakeOrderedAndProjectExec => topK }.nonEmpty,
          cometPlan.toString)
        assert(
          collect(cometPlan) { case topK: CometTakeOrderedAndProjectExec => topK }.isEmpty,
          cometPlan.toString)
      }
    }
  }

  test("default UTF8_BINARY string and multi-key struct sorting remain native") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withParquetTable(Seq((1, "b"), (2, "A"), (3, "a"), (4, "B")), "tbl") {
        for (sortKeys <- Seq("_2, _1", "struct(_2), _1")) {
          withClue(s"Sort keys: $sortKeys: ") {
            val (_, cometPlan) = checkSparkAnswerAndOperator(
              s"SELECT _1, _2, row_number() OVER (ORDER BY $sortKeys) AS rn FROM tbl")
            assert(
              collect(cometPlan) { case sort: CometSortExec => sort }.nonEmpty,
              cometPlan.toString)
          }
        }
        val (_, topKPlan) =
          checkSparkAnswerAndOperator("SELECT _1, _2 FROM tbl ORDER BY _2, _1 LIMIT 2")
        assert(
          collect(topKPlan) { case topK: CometTakeOrderedAndProjectExec => topK }.nonEmpty,
          topKPlan.toString)
      }
    }
  }
}
