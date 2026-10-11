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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{CometListenerBusUtils, SparkConf}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows
import org.apache.spark.sql.comet.CometMergeRowsExec
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryRowLevelOperationTableCatalog, InMemoryTable}
import org.apache.spark.sql.connector.write.MergeSummary
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.{InsertOnlyMergeExec, MergeRowsExec}
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.{CometConf, CometExplainInfo}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.Unsupported
import org.apache.comet.serde.operator.CometMergeRows
import org.apache.comet.shims.ShimCometMergeRows

/** Spark 4.2 coverage for the dedicated InsertOnlyMergeExec rewrite. */
class CometInsertOnlyMergeSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val catalog = "insert_only_merge"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(s"spark.sql.catalog.$catalog", classOf[InMemoryRowLevelOperationTableCatalog].getName)
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.shuffle.partitions", "4")
  }

  private case class MergeResult(plans: Seq[SparkPlan], rows: Seq[String], summary: MergeSummary)

  private def resetTables(target: String, source: String, sourceRows: String): Unit = {
    sql(s"DROP TABLE IF EXISTS $catalog.default.$target")
    sql(s"DROP TABLE IF EXISTS $catalog.default.$source")
    sql(s"CREATE TABLE $catalog.default.$target (id INT, amount INT) USING parquet")
    sql(s"CREATE TABLE $catalog.default.$source (id INT, amount INT) USING parquet")
    sql(s"INSERT INTO $catalog.default.$target VALUES (1, 10), (2, 20)")
    sql(s"INSERT INTO $catalog.default.$source VALUES $sourceRows")
  }

  private def lastMergeSummary(table: String): MergeSummary = {
    val cat = spark.sessionState.catalogManager
      .catalog(catalog)
      .asInstanceOf[InMemoryRowLevelOperationTableCatalog]
    cat
      .loadTable(Identifier.of(Array("default"), table))
      .asInstanceOf[InMemoryTable]
      .commits
      .last
      .writeSummary
      .get
      .asInstanceOf[MergeSummary]
  }

  private def runMerge(
      target: String,
      mergeSql: String,
      cometEnabled: Boolean,
      adaptiveEnabled: Boolean): MergeResult = {
    val captured = ArrayBuffer[QueryExecution]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        captured += qe
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        ()
    }

    spark.listenerManager.register(listener)
    try {
      val configs =
        Seq(
          CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> "true",
          "spark.sql.adaptive.enabled" -> adaptiveEnabled.toString,
          "spark.sql.ansi.enabled" -> "true") ++
          (if (cometEnabled) Seq.empty[(String, String)]
           else Seq(CometConf.COMET_ENABLED.key -> "false"))
      withSQLConf(configs: _*) {
        sql(mergeSql)
      }
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    } finally {
      spark.listenerManager.unregister(listener)
    }

    val rows = sql(s"SELECT id, amount FROM $catalog.default.$target ORDER BY id, amount")
      .collect()
      .map(_.toString)
      .toSeq
    MergeResult(captured.map(_.executedPlan).toSeq, rows, lastMergeSummary(target))
  }

  private def hasInsertOnlyMerge(plans: Seq[SparkPlan]): Boolean =
    plans.exists(plan =>
      find(plan) { case _: InsertOnlyMergeExec => true; case _ => false }.nonEmpty)

  private def hasCometMergeRows(plans: Seq[SparkPlan]): Boolean =
    plans.exists(plan =>
      find(plan) { case _: CometMergeRowsExec => true; case _ => false }.nonEmpty)

  private def hasSparkMergeRows(plans: Seq[SparkPlan]): Boolean =
    plans.exists(plan => find(plan) { case _: MergeRowsExec => true; case _ => false }.nonEmpty)

  private def assertInsertOnlySummary(summary: MergeSummary, inserted: Long): Unit = {
    assert(summary.numTargetRowsInserted() == inserted)
    assert(summary.numTargetRowsCopied() == 0)
    assert(summary.numTargetRowsUpdated() == 0)
    assert(summary.numTargetRowsDeleted() == 0)
    assert(summary.numTargetRowsMatchedUpdated() == 0)
    assert(summary.numTargetRowsMatchedDeleted() == 0)
    assert(summary.numTargetRowsNotMatchedBySourceUpdated() == 0)
    assert(summary.numTargetRowsNotMatchedBySourceDeleted() == 0)
  }

  test("multiple NOT MATCHED clauses preserve parity with AQE on and off") {
    val sourceRows = "(2, 200), (3, 300), (3, 301), (4, 400)"

    Seq(false, true).foreach { adaptiveEnabled =>
      val suffix = if (adaptiveEnabled) "aqe_on" else "aqe_off"
      val target = s"multi_target_$suffix"
      val source = s"multi_source_$suffix"
      val mergeSql =
        s"""MERGE INTO $catalog.default.$target t
           |USING $catalog.default.$source s
           |ON t.id = s.id
           |WHEN NOT MATCHED AND s.amount < 350 THEN
           |  INSERT (id, amount) VALUES (s.id, s.amount)
           |WHEN NOT MATCHED AND s.amount >= 350 THEN
           |  INSERT (id, amount) VALUES (s.id, s.amount)
           |""".stripMargin

      resetTables(target, source, sourceRows)
      val comet =
        runMerge(target, mergeSql, cometEnabled = true, adaptiveEnabled = adaptiveEnabled)

      assert(hasInsertOnlyMerge(comet.plans), "expected Spark 4.2 InsertOnlyMergeExec")
      assert(
        hasCometMergeRows(comet.plans),
        "insert-only MergeRows child did not execute natively")
      assert(
        !hasSparkMergeRows(comet.plans),
        "native insert-only path retained Spark MergeRowsExec")
      assertInsertOnlySummary(comet.summary, inserted = 3)

      resetTables(target, source, sourceRows)
      val sparkOnly =
        runMerge(target, mergeSql, cometEnabled = false, adaptiveEnabled = adaptiveEnabled)

      assert(comet.rows == sparkOnly.rows)
      assert(
        comet.rows == Seq("[1,10]", "[2,20]", "[3,300]", "[3,301]", "[4,400]"),
        s"unexpected insert-only MERGE result: ${comet.rows.mkString(", ")}")
      assertInsertOnlySummary(sparkOnly.summary, inserted = 3)
    }
  }

  test("single NOT MATCHED clause keeps InsertOnlyMergeExec summary without MergeRows") {
    val target = "single_target"
    val source = "single_source"
    val sourceRows = "(2, 200), (3, 300)"
    val mergeSql =
      s"""MERGE INTO $catalog.default.$target t
         |USING $catalog.default.$source s
         |ON t.id = s.id
         |WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)
         |""".stripMargin

    resetTables(target, source, sourceRows)
    val comet = runMerge(target, mergeSql, cometEnabled = true, adaptiveEnabled = true)

    assert(hasInsertOnlyMerge(comet.plans), "expected Spark 4.2 InsertOnlyMergeExec")
    assert(!hasCometMergeRows(comet.plans), "single-clause rewrite should not contain MergeRows")
    assert(!hasSparkMergeRows(comet.plans), "single-clause rewrite should not contain MergeRows")
    assert(comet.rows == Seq("[1,10]", "[2,20]", "[3,300]"))
    assertInsertOnlySummary(comet.summary, inserted = 1)

    resetTables(target, source, sourceRows)
    val sparkOnly = runMerge(target, mergeSql, cometEnabled = false, adaptiveEnabled = true)
    assert(comet.rows == sparkOnly.rows)
    assertInsertOnlySummary(sparkOnly.summary, inserted = 1)
  }

  test("first matching NOT MATCHED clause does not evaluate later predicates") {
    val target = "short_circuit_target"
    val source = "short_circuit_source"
    val sourceRows = "(3, 0), (4, 2)"
    val mergeSql =
      s"""MERGE INTO $catalog.default.$target t
         |USING $catalog.default.$source s
         |ON t.id = s.id
         |WHEN NOT MATCHED AND s.amount = 0 THEN
         |  INSERT (id, amount) VALUES (s.id, 111)
         |WHEN NOT MATCHED AND 2 / s.amount > 0 THEN
         |  INSERT (id, amount) VALUES (s.id, 222)
         |""".stripMargin

    resetTables(target, source, sourceRows)
    val comet = runMerge(target, mergeSql, cometEnabled = true, adaptiveEnabled = true)
    assert(hasCometMergeRows(comet.plans))
    assert(comet.rows == Seq("[1,10]", "[2,20]", "[3,111]", "[4,222]"))
    assertInsertOnlySummary(comet.summary, inserted = 2)

    resetTables(target, source, sourceRows)
    val sparkOnly = runMerge(target, mergeSql, cometEnabled = false, adaptiveEnabled = true)
    assert(comet.rows == sparkOnly.rows)
    assertInsertOnlySummary(sparkOnly.summary, inserted = 2)
  }

  test("scalar subquery in insert assignment remains discoverable") {
    val target = "subquery_target"
    val source = "subquery_source"
    val sourceRows = "(2, 200), (3, 300), (4, 400)"
    val sourceTable = s"$catalog.default.$source"
    val mergeSql =
      s"""MERGE INTO $catalog.default.$target t
         |USING $sourceTable s
         |ON t.id = s.id
         |WHEN NOT MATCHED AND s.id = 3 THEN
         |  INSERT (id, amount) VALUES (s.id, (SELECT max(amount) FROM $sourceTable))
         |WHEN NOT MATCHED THEN
         |  INSERT (id, amount) VALUES (s.id, s.amount)
         |""".stripMargin

    resetTables(target, source, sourceRows)
    val comet = runMerge(target, mergeSql, cometEnabled = true, adaptiveEnabled = true)
    assert(hasCometMergeRows(comet.plans))
    assert(comet.rows == Seq("[1,10]", "[2,20]", "[3,400]", "[4,400]"))
    assertInsertOnlySummary(comet.summary, inserted = 2)

    resetTables(target, source, sourceRows)
    val sparkOnly = runMerge(target, mergeSql, cometEnabled = false, adaptiveEnabled = true)
    assert(comet.rows == sparkOnly.rows)
    assertInsertOnlySummary(sparkOnly.summary, inserted = 2)
  }

  test("general MERGE remains on Spark in 4.2") {
    val target = "general_target"
    val source = "general_source"
    val sourceRows = "(2, 200), (3, 300)"
    val mergeSql =
      s"""MERGE INTO $catalog.default.$target t
         |USING $catalog.default.$source s
         |ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET amount = s.amount
         |WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)
         |""".stripMargin

    resetTables(target, source, sourceRows)
    val comet = runMerge(target, mergeSql, cometEnabled = true, adaptiveEnabled = true)

    assert(!hasInsertOnlyMerge(comet.plans), "general MERGE must not use InsertOnlyMergeExec")
    assert(!hasCometMergeRows(comet.plans), "general Spark 4.2 MergeRows must remain on the JVM")
    assert(hasSparkMergeRows(comet.plans), "expected Spark MergeRowsExec fallback")
    assert(comet.rows == Seq("[1,10]", "[2,200]", "[3,300]"))

    resetTables(target, source, sourceRows)
    val sparkOnly = runMerge(target, mergeSql, cometEnabled = false, adaptiveEnabled = true)
    assert(comet.rows == sparkOnly.rows)
  }

  test("near-miss insert-only shapes retain Spark MergeRowsExec") {
    withTempPath { path =>
      spark.range(3).write.parquet(path.getCanonicalPath)
      val input = spark.read.parquet(path.getCanonicalPath).queryExecution.analyzed
      val id = input.output.head
      val instructions = Seq(
        MergeRows.Keep(MergeRows.Insert, EqualTo(id, Literal(1L)), Seq(id)),
        MergeRows.Keep(MergeRows.Insert, Literal(true), Seq(id)))
      val plan = MergeRows(
        Literal(true),
        Literal(false),
        Seq.empty,
        instructions,
        Seq.empty,
        checkCardinality = false,
        output = Seq(id),
        child = input)
      val nearMisses = Seq(
        "matched instructions" -> plan.copy(matchedInstructions = instructions.take(1)),
        "not matched by source instructions" ->
          plan.copy(notMatchedBySourceInstructions = instructions.take(1)),
        "Copy Keep" -> plan.copy(notMatchedInstructions =
          Seq(MergeRows.Keep(MergeRows.Copy, Literal(true), Seq(id)), instructions.last)),
        "Update Keep" -> plan.copy(notMatchedInstructions =
          Seq(MergeRows.Keep(MergeRows.Update, Literal(true), Seq(id)), instructions.last)),
        "Delete Keep" -> plan.copy(notMatchedInstructions =
          Seq(MergeRows.Keep(MergeRows.Delete, Literal(true), Seq(id)), instructions.last)),
        "Discard" -> plan.copy(notMatchedInstructions =
          Seq(MergeRows.Discard(Literal(true)), instructions.last)),
        "Split" -> plan.copy(notMatchedInstructions =
          Seq(MergeRows.Split(Literal(true), Seq(id), Seq(id)), instructions.last)),
        "source absent" -> plan.copy(isSourceRowPresent = Literal(false)),
        "target present" -> plan.copy(isTargetRowPresent = Literal(true)),
        "nonliteral source presence" ->
          plan.copy(isSourceRowPresent = EqualTo(id, Literal(1L))),
        "nonliteral target presence" ->
          plan.copy(isTargetRowPresent = EqualTo(id, Literal(1L))),
        "single instruction" -> plan.copy(notMatchedInstructions = instructions.take(1)),
        "no instructions" -> plan.copy(notMatchedInstructions = Seq.empty))

      withSQLConf(
        CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> "true",
        "spark.sql.adaptive.enabled" -> "false") {
        nearMisses.foreach { case (name, nearMiss) =>
          withClue(s"$name: ") {
            val baseline = withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
              datasetOfRows(spark, nearMiss.clone()).collect().map(_.toString).sorted.toSeq
            }
            val df = datasetOfRows(spark, nearMiss.clone())
            assert(df.collect().map(_.toString).sorted.toSeq == baseline)
            val executed = df.queryExecution.executedPlan
            assert(find(executed)(_.isInstanceOf[CometMergeRowsExec]).isEmpty)
            val merge = find(executed)(_.isInstanceOf[MergeRowsExec])
              .getOrElse(fail(s"expected Spark MergeRowsExec: $executed"))
              .asInstanceOf[MergeRowsExec]
            assert(!ShimCometMergeRows.canRunWithoutMergeSummary(merge))
            assert(CometMergeRows.getSupportLevel(merge).isInstanceOf[Unsupported])
            assert(
              CometMergeRows
                .convert(merge, Operator.newBuilder(), Operator.newBuilder().build())
                .isEmpty)
          }
        }
      }
    }
  }

  test("insert-only fallback preserves schema and missing-child reasons") {
    val child = spark.range(3).queryExecution.sparkPlan
    val id = child.output.head
    val instructions = Seq(
      MergeRows.Keep(MergeRows.Insert, EqualTo(id, Literal(1L)), Seq(id)),
      MergeRows.Keep(MergeRows.Insert, Literal(true), Seq(id)))
    def plan = MergeRowsExec(
      Literal(true),
      Literal(false),
      Seq.empty,
      instructions,
      Seq.empty,
      checkCardinality = false,
      output = Seq(id),
      child = child)

    val missingChild = plan
    assert(CometMergeRows.convert(missingChild, Operator.newBuilder()).isEmpty)
    assert(
      missingChild
        .getTagValue(CometExplainInfo.FALLBACK_REASONS)
        .get
        .contains("No child operator"))

    Seq(Seq(Literal(1)), Seq(id, id), Seq.empty).foreach { output =>
      val malformed = plan.copy(notMatchedInstructions =
        Seq(MergeRows.Keep(MergeRows.Insert, Literal(true), output), instructions.last))
      assert(ShimCometMergeRows.canRunWithoutMergeSummary(malformed))
      val reason = "MERGE instruction must be Discard, Keep, or Split with output rows " +
        "compatible with the plan schema"
      assert(CometMergeRows.getSupportLevel(malformed) == Unsupported(Some(reason)))
      assert(
        CometMergeRows
          .convert(malformed, Operator.newBuilder(), Operator.newBuilder().build())
          .isEmpty)
      assert(malformed.getTagValue(CometExplainInfo.FALLBACK_REASONS).get.contains(reason))
    }
    assert(!ShimCometMergeRows.canRunWithoutMergeSummary(plan.copy(checkCardinality = true)))
  }

  Seq("predicate", "assignment").foreach { expressionLocation =>
    test(s"unsupported insert-only $expressionLocation falls back to Spark with its reason") {
      val target = s"unsupported_${expressionLocation}_target"
      val source = s"unsupported_${expressionLocation}_source"
      val udfName = s"insert_only_$expressionLocation"
      spark.udf.register(udfName, (value: Int) => value)
      val predicate = if (expressionLocation == "predicate") s"$udfName(s.id)" else "s.id"
      val assignment =
        if (expressionLocation == "assignment") s"$udfName(s.amount)" else "s.amount"
      val mergeSql =
        s"""MERGE INTO $catalog.default.$target t
           |USING $catalog.default.$source s
           |ON t.id = s.id
           |WHEN NOT MATCHED AND $predicate = 3 THEN
           |  INSERT (id, amount) VALUES (s.id, $assignment)
           |WHEN NOT MATCHED THEN
           |  INSERT (id, amount) VALUES (s.id, s.amount)
           |""".stripMargin

      withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
        val sourceRows = "(2, 200), (3, 300), (4, 400)"
        resetTables(target, source, sourceRows)
        val comet = runMerge(target, mergeSql, cometEnabled = true, adaptiveEnabled = false)
        assert(hasInsertOnlyMerge(comet.plans))
        assert(!hasCometMergeRows(comet.plans))
        assert(hasSparkMergeRows(comet.plans))
        val merge = comet.plans
          .flatMap(plan => collectWithSubqueries(plan) { case m: MergeRowsExec => m })
          .head
        val reasons = merge.getTagValue(CometExplainInfo.FALLBACK_REASONS).get
        assert(reasons.contains("Unsupported expression in MERGE instructions"))
        assert(!reasons.exists(_.contains("requires Spark MergeRowsExec for MergeSummary")))
        assertInsertOnlySummary(comet.summary, inserted = 2)

        resetTables(target, source, sourceRows)
        val sparkOnly = runMerge(target, mergeSql, cometEnabled = false, adaptiveEnabled = false)
        assert(comet.rows == sparkOnly.rows)
        assertInsertOnlySummary(sparkOnly.summary, inserted = 2)
      }
    }
  }
}
