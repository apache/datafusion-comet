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

import java.io.File

import scala.collection.mutable

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.{CommandExecutionMode, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, OverwriteByExpressionExec, OverwritePartitionsDynamicExec, V2ExistingTableWriteExec}

import org.apache.comet.serde.operator.CometIcebergNativeWrite

/**
 * Detection-only suite for the Iceberg native write rule. Runs writes against a Hadoop-catalog
 * Iceberg table, captures the planned (not executed) plan, and asserts the rule:
 *
 *   - recognises the three "external rewrite" V2 write operators (`AppendDataExec`,
 *     `OverwriteByExpressionExec`, `OverwritePartitionsDynamicExec`) and emits a fall-back
 *     reason. `ReplaceDataExec` (CoW MERGE/UPDATE/DELETE) shares the same
 *     `V2ExistingTableWriteExec` supertype as those three and is dispatched identically; see the
 *     inline note further down for why its detection test is deferred to the end-to-end suite.
 *   - falls back with a property-specific reason whenever a table property is set to a value
 *     iceberg-rust cannot match
 *
 * No native write conversion happens in this commit -- `CometIcebergNativeWrite.convert` returns
 * `None`, so the actual write still runs through Spark/Iceberg's JVM writer.
 */
class CometIcebergWriteDetectionSuite extends CometTestBase with CometIcebergTestBase {

  private val catalog = "test_cat"

  // -- Positive detection -----------------------------------------------------

  test("AppendDataExec is matched by CometIcebergNativeWrite") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withTempIcebergDir { warehouseDir =>
      withIcebergComet(warehouseDir) {
        val table = s"$catalog.db.append_detect"
        try {
          spark.sql(s"CREATE TABLE $table (id INT, value DOUBLE) USING iceberg")
          val plan = compileExecutedPlan(s"INSERT INTO $table VALUES (1, 1.0), (2, 2.0)")
          assertOperator[AppendDataExec](plan)
          assertFallbackReason(plan, "Iceberg native write conversion is not yet implemented")
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS $table")
        }
      }
    }
  }

  test("OverwriteByExpressionExec is matched by CometIcebergNativeWrite") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withTempIcebergDir { warehouseDir =>
      withIcebergComet(warehouseDir) {
        val table = s"$catalog.db.overwrite_by_expr_detect"
        try {
          spark.sql(s"CREATE TABLE $table (id INT, value DOUBLE) USING iceberg")
          spark.sql(s"INSERT INTO $table VALUES (1, 1.0)")
          val plan = compileExecutedPlan(s"INSERT OVERWRITE $table VALUES (2, 2.0), (3, 3.0)")
          assertOperator[OverwriteByExpressionExec](plan)
          assertFallbackReason(plan, "Iceberg native write conversion is not yet implemented")
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS $table")
        }
      }
    }
  }

  test("OverwritePartitionsDynamicExec is matched by CometIcebergNativeWrite") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withTempIcebergDir { warehouseDir =>
      withIcebergComet(warehouseDir) {
        // Default partitionOverwriteMode is STATIC, which Spark turns into
        // OverwriteByExpressionExec. We need DYNAMIC for OverwritePartitionsDynamicExec.
        withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "DYNAMIC") {
          val table = s"$catalog.db.overwrite_dynamic_detect"
          try {
            spark.sql(s"""CREATE TABLE $table (id INT, value DOUBLE, dt STRING)
                 |USING iceberg PARTITIONED BY (dt)""".stripMargin)
            spark.sql(s"INSERT INTO $table VALUES (1, 1.0, '2025-01-01')")
            val plan = compileExecutedPlan(s"""INSERT OVERWRITE TABLE $table
                 |SELECT 2 AS id, 2.0 AS value, '2025-01-02' AS dt""".stripMargin)
            assertOperator[OverwritePartitionsDynamicExec](plan)
            assertFallbackReason(plan, "Iceberg native write conversion is not yet implemented")
          } finally {
            spark.sql(s"DROP TABLE IF EXISTS $table")
          }
        }
      }
    }
  }

  // Note: ReplaceDataExec coverage (CoW MERGE/UPDATE/DELETE) is deferred to the end-to-end
  // suite that lands with the native write path. ReplaceDataExec extends V2ExistingTableWriteExec
  // so the CometExecRule case here catches it just like the three operators above; the
  // detection-only test is awkward to write because:
  //   - With CommandExecutionMode.SKIP the row-level rewrite that produces ReplaceData never
  //     fires (it is wired through Iceberg's command-execution path, not the analyzer), so the
  //     planned tree stays as DeleteFromTable/DeleteFromTableExec.
  //   - With ALL mode (eager command execution) the SparkPlan tree is wrapped in
  //     CommandResult by the time the test reads queryExecution.executedPlan, so the original
  //     ReplaceDataExec instance (and any withInfo tags on it) is no longer reachable.
  // The integration suite in the follow-up commit asserts on actual on-disk outputs and does
  // not need to round-trip the SparkPlan instance.

  // -- Fall-back triggers ----------------------------------------------------

  test("table property values that disqualify a write each emit a distinct fall-back reason") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    import CometIcebergNativeWrite.PropertyKeys._

    val triggers: Seq[FallBackCase] = Seq(
      FallBackCase(
        name = "non-parquet format",
        tableProps = Map(DefaultFileFormat -> "orc"),
        reasonContains = DefaultFileFormat),
      FallBackCase(
        name = "object-storage enabled",
        tableProps = Map(ObjectStoreEnabled -> "true"),
        reasonContains = ObjectStoreEnabled),
      FallBackCase(
        name = "custom location provider",
        tableProps = Map(WriteLocationProviderImpl -> "com.example.MyLocationProvider"),
        reasonContains = WriteLocationProviderImpl),
      FallBackCase(
        name = "encryption property set",
        tableProps = Map("encryption.kms-impl" -> "com.example.MyKms"),
        reasonContains = "encryption."),
      FallBackCase(
        name = "metrics default is counts",
        tableProps = Map(DefaultWriteMetricsMode -> "counts"),
        reasonContains = DefaultWriteMetricsMode),
      FallBackCase(
        name = "per-column counts metrics mode",
        tableProps = Map(s"${MetricsModeColumnPrefix}value" -> "counts"),
        reasonContains = s"${MetricsModeColumnPrefix}value"),
      FallBackCase(
        name = "parquet bloom-filter-max-bytes set",
        tableProps = Map(ParquetBloomFilterMaxBytes -> "1048576"),
        reasonContains = ParquetBloomFilterMaxBytes),
      FallBackCase(
        name = "parquet row-group-check min set",
        tableProps = Map(ParquetRowGroupCheckMinRecordCount -> "100"),
        reasonContains = ParquetRowGroupCheckMinRecordCount),
      FallBackCase(
        name = "parquet row-group-check max set",
        tableProps = Map(ParquetRowGroupCheckMaxRecordCount -> "10000"),
        reasonContains = ParquetRowGroupCheckMaxRecordCount),
      FallBackCase(
        name = "any column has bloom filter enabled",
        tableProps = Map(s"${BloomFilterColumnEnabledPrefix}id" -> "true"),
        reasonContains = s"${BloomFilterColumnEnabledPrefix}id=true"))

    triggers.foreach(runFallBackCase)
  }

  test("schema with too many projected field IDs falls back") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withTempIcebergDir { warehouseDir =>
      withIcebergComet(warehouseDir) {
        val table = s"$catalog.db.too_many_columns"
        try {
          // 5 columns is enough to verify the gate when we cap max-inferred at 4.
          spark.sql(s"""CREATE TABLE $table (
               |  c1 INT, c2 INT, c3 INT, c4 INT, c5 INT
               |) USING iceberg
               |TBLPROPERTIES ('write.metadata.metrics.max-inferred-column-defaults' = '4')
               |""".stripMargin)
          val plan = compileExecutedPlan(s"INSERT INTO $table VALUES (1, 2, 3, 4, 5)")
          assertFallbackReason(plan, "exceeds")
          assertFallbackReason(plan, "max-inferred-column-defaults")
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS $table")
        }
      }
    }
  }

  // -- Helpers ---------------------------------------------------------------

  private case class FallBackCase(
      name: String,
      tableProps: Map[String, String],
      reasonContains: String)

  private def runFallBackCase(tc: FallBackCase): Unit = {
    withTempIcebergDir { warehouseDir =>
      withIcebergComet(warehouseDir) {
        val table = s"$catalog.db.fallback_${slug(tc.name)}"
        try {
          val propsClause =
            if (tc.tableProps.isEmpty) ""
            else
              tc.tableProps
                .map { case (k, v) => s"'$k' = '$v'" }
                .mkString("TBLPROPERTIES (", ", ", ")")
          spark.sql(s"""CREATE TABLE $table (id INT, value DOUBLE) USING iceberg $propsClause""")
          val plan = compileExecutedPlan(s"INSERT INTO $table VALUES (1, 1.0)")
          assertFallbackReason(plan, tc.reasonContains, failureContext = s"trigger: ${tc.name}")
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS $table")
        }
      }
    }
  }

  private def slug(s: String): String = s.toLowerCase.replaceAll("[^a-z0-9]+", "_")

  /**
   * Compiles a SQL string to its executed `SparkPlan` without running the write. V2 write
   * operators are command-typed in Spark, so the default `executePlan` mode eagerly executes them
   * when `executedPlan` is touched. Passing `CommandExecutionMode.SKIP` keeps the plan
   * post-Comet-rules but unexecuted, which is what the detection assertions need.
   */
  private def compileExecutedPlan(sql: String): SparkPlan = {
    val logical = spark.sessionState.sqlParser.parsePlan(sql)
    spark.sessionState.executePlan(logical, CommandExecutionMode.SKIP).executedPlan
  }

  private def assertOperator[T <: V2ExistingTableWriteExec: scala.reflect.ClassTag](
      plan: SparkPlan): Unit = {
    val clazz = implicitly[scala.reflect.ClassTag[T]].runtimeClass
    val matches = plan.collect { case op if clazz.isInstance(op) => op }
    assert(
      matches.nonEmpty,
      s"Expected plan to contain ${clazz.getSimpleName} but found:\n${plan.toString()}")
  }

  private def assertFallbackReason(
      plan: SparkPlan,
      substring: String,
      failureContext: String = ""): Unit = {
    val reasons = collectFallbackReasons(plan)
    val ctx = if (failureContext.isEmpty) "" else s" ($failureContext)"
    assert(
      reasons.exists(_.contains(substring)),
      s"Expected a fall-back reason containing '$substring'$ctx; got:\n  " +
        reasons.mkString("\n  ") +
        s"\nPlan:\n${plan.toString()}")
  }

  private def collectFallbackReasons(plan: SparkPlan): Set[String] = {
    val out = mutable.Set[String]()
    def walk(node: TreeNode[_]): Unit = {
      node.getTagValue(CometExplainInfo.EXTENSION_INFO).foreach(out ++= _)
      node.innerChildren.foreach { case c: TreeNode[_] => walk(c); case _ => }
      node.children.foreach { case c: TreeNode[_] => walk(c); case _ => }
    }
    walk(plan)
    out.toSet
  }

  private def withIcebergComet(warehouseDir: File)(body: => Unit): Unit =
    withSQLConf(
      s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$catalog.type" -> "hadoop",
      s"spark.sql.catalog.$catalog.warehouse" -> warehouseDir.getAbsolutePath,
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
      CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key -> "true")(body)
}
