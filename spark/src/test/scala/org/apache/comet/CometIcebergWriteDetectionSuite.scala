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

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.IcebergWriteExec

import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus
import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.serde.{Compatible, SupportLevel, Unsupported}
import org.apache.comet.serde.operator.CometIcebergNativeWrite

/**
 * Pins every documented fall-back trigger in [[CometIcebergNativeWrite.getSupportLevel]] without
 * paying the cost of an actual write per case. Tests build an [[IcebergWriteExec]] via Spark's
 * SQL planning (we only inspect `queryExecution.sparkPlan`; the command itself never fires) and
 * call `getSupportLevel` directly.
 *
 * Companion to [[CometIcebergWriteActionSuite]]: the action suite checks end-to-end behaviour
 * with a real catalog and physical writes; this suite checks the detection gate in isolation.
 */
class CometIcebergWriteDetectionSuite extends CometTestBase with CometIcebergTestBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key, "true")
      .set(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key, "true")
  }

  // --- Positive baseline -----------------------------------------------------------------------

  test("clean parquet V2 table planned as AppendData yields Compatible") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(dir, "ok", partitionSpec = "")
      assertSupportLevelIs[Compatible]("ok")
    }
  }

  // --- SparkWrite reflection coverage ----------------------------------------------------------

  // Pins that every private-field / private-method accessor the native-write gate and proto
  // builder depend on still resolves against a real `SparkWrite` from the Iceberg runtime on the
  // build's classpath. These accessors fail closed (Option -> JVM fall-back) by design, so a field
  // rename (e.g. `useFanoutWriter` <-> `partitionedFanoutEnabled` between Iceberg 1.5.2 and 1.8.1)
  // would silently lose native acceleration with no other test failing. This is the explicit
  // compatibility matrix the comments in IcebergReflection only describe.
  test("SparkWrite reflection helpers all resolve on the current Iceberg runtime") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(dir, "refl_probe", partitionSpec = "")
      val sparkWrite = IcebergReflection
        .getOuterSparkWrite(insertWriteExec("refl_probe").batchWrite)
        .getOrElse(fail("could not unwrap outer SparkWrite from BatchWrite"))
      val table = IcebergReflection
        .getTableFromSparkWrite(sparkWrite)
        .getOrElse(fail("SparkWrite.table reflection returned None"))

      assert(IcebergReflection.getOperationIdFromSparkWrite(sparkWrite).isDefined, "queryId")
      assert(
        IcebergReflection.getTargetFileSizeFromSparkWrite(sparkWrite).isDefined,
        "targetFileSize")
      assert(
        IcebergReflection.getUseFanoutWriterFromSparkWrite(sparkWrite).isDefined,
        "useFanoutWriter")
      assert(
        IcebergReflection.getOutputSpecIdFromSparkWrite(sparkWrite).isDefined,
        "outputSpecId")
      assert(IcebergReflection.getWriteSchemaFromSparkWrite(sparkWrite).isDefined, "writeSchema")
      assert(IcebergReflection.getFormatFromSparkWrite(sparkWrite).isDefined, "format")
      assert(
        IcebergReflection.getWritePropertiesFromSparkWrite(sparkWrite).isDefined,
        "writeProperties")
      // `SparkWrite.writeConf.outputSortOrderId(...)` is intentionally NOT pinned here: it does not
      // exist on any Iceberg version Comet targets (1.5.2 / 1.8.1 / 1.10.0), and the proto builder
      // deliberately defaults `sort_order_id` to 0 to match Iceberg-Java (whose Spark WriterFactory
      // never stamps the table sort order on appended files). Sort-order parity is covered by the
      // action suite, not by reflection resolvability.

      assert(IcebergReflection.getMetadataLocation(table).isDefined, "metadataLocation")
      assert(IcebergReflection.getDataLocation(table).isDefined, "dataLocation")
    }
  }

  // --- write.format.default --------------------------------------------------------------------

  test("fall-back: write.format.default=orc") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "fmt_orc",
        partitionSpec = "",
        properties = Some("'write.format.default'='orc'"))
      assertUnsupportedContains("fmt_orc", "format=orc", "only parquet")
    }
  }

  // Mirrors the table-property gate above but exercises the option-override branch of
  // `SparkWriteConf.dataFileFormat()`: a per-write `write-format` option must win over a
  // parquet-default table. The DataFrame-writer driver routes the option into `SparkWriteConf`
  // the same way Iceberg-Java does at runtime, so the gate reads it from `SparkWrite.format`.
  test("fall-back: per-write write-format option overrides parquet default") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(dir, "fmt_orc_opt", partitionSpec = "")
      val writeExec = dfWriteExec("fmt_orc_opt", "write-format" -> "orc")
      val support = CometIcebergNativeWrite.getSupportLevel(writeExec)
      support match {
        case Unsupported(Some(reason)) =>
          assert(reason.contains("format=orc"), s"reason '$reason' missing 'format=orc'")
          assert(reason.contains("only parquet"), s"reason '$reason' missing 'only parquet'")
        case other => fail(s"expected Unsupported, got $other")
      }
    }
  }

  // --- write.object-storage.enabled ------------------------------------------------------------

  test("fall-back: write.object-storage.enabled=true") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "obj_store",
        partitionSpec = "",
        properties = Some("'write.object-storage.enabled'='true'"))
      assertUnsupportedContains("obj_store", "write.object-storage.enabled")
    }
  }

  // --- write.location-provider.impl ------------------------------------------------------------

  test("fall-back: write.location-provider.impl set") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      // The trigger only checks whether the property exists, not its value -- any string trips
      // it. We use a class name that isn't actually loadable: the captured `executedPlan` still
      // contains the `IcebergWriteExec` we want to inspect; the writer-side load failure
      // is caught and ignored by our `QueryExecutionListener`-based capture.
      createTable(
        dir,
        "loc_provider",
        partitionSpec = "",
        properties = Some("'write.location-provider.impl'='com.example.MyProvider'"))
      assertUnsupportedContains("loc_provider", "write.location-provider.impl")
    }
  }

  // --- format-version >= 3 ---------------------------------------------------------------------

  test("fall-back: format-version=3") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    // Iceberg 1.5.2 (Spark 3.4 profile) cannot create V3 tables -- the writer rejects with
    // "Cannot upgrade table to unsupported format version: 3". Gate on 3.5+ which ships with
    // Iceberg >= 1.8 (V3 supported on the writer side).
    assume(isSpark35Plus, "V3 tables require Iceberg 1.8.1+ (Spark 3.5 profile)")
    withDetectionCatalog { dir =>
      createTable(dir, "v3", partitionSpec = "", properties = Some("'format-version'='3'"))
      assertUnsupportedContains("v3", "format-version=3")
    }
  }

  // --- encryption.* ----------------------------------------------------------------------------

  test("fall-back: encryption.kms-client-impl set") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "enc",
        partitionSpec = "",
        properties = Some("'encryption.kms-client-impl'='com.example.MyKms'"))
      assertUnsupportedContains("enc", "encryption")
    }
  }

  // --- write.metadata.metrics.default contains 'counts' ----------------------------------------

  test("fall-back: write.metadata.metrics.default=counts") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "metrics_counts",
        partitionSpec = "",
        properties = Some("'write.metadata.metrics.default'='counts'"))
      assertUnsupportedContains("metrics_counts", "write.metadata.metrics.default", "counts")
    }
  }

  // --- write.metadata.metrics.column.<c>=counts ------------------------------------------------

  test("fall-back: per-column metrics mode=counts") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "metrics_col_counts",
        partitionSpec = "",
        properties = Some("'write.metadata.metrics.column.id'='counts'"))
      assertUnsupportedContains(
        "metrics_col_counts",
        "write.metadata.metrics.column.id",
        "counts")
    }
  }

  // --- write.parquet.bloom-filter-max-bytes ----------------------------------------------------

  test("fall-back: write.parquet.bloom-filter-max-bytes set") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "bloom_max",
        partitionSpec = "",
        properties = Some("'write.parquet.bloom-filter-max-bytes'='524288'"))
      assertUnsupportedContains("bloom_max", "write.parquet.bloom-filter-max-bytes")
    }
  }

  // --- write.parquet.bloom-filter-enabled.column.<c>=true --------------------------------------

  test("fall-back: per-column bloom filter enabled") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "bloom_col",
        partitionSpec = "",
        properties = Some("'write.parquet.bloom-filter-enabled.column.id'='true'"))
      assertUnsupportedContains(
        "bloom_col",
        "write.parquet.bloom-filter-enabled.column.id",
        "true")
    }
  }

  // --- schema field count > max-inferred-column-defaults ---------------------------------------

  test("fall-back: schema exceeds write.metadata.metrics.max-inferred-column-defaults") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { _ =>
      // Lower the cap to 2 so our 3-column fixture table trips it without making the schema huge.
      val sql = s"""
        CREATE TABLE $catalog.$ns.too_many_cols (
          id INT,
          region STRING,
          amount DOUBLE
        ) USING iceberg
        TBLPROPERTIES ('write.metadata.metrics.max-inferred-column-defaults'='2')
      """
      spark.sql(sql)
      assertUnsupportedContains(
        "too_many_cols",
        "projected field IDs which exceeds",
        "write.metadata.metrics.max-inferred-column-defaults=2")
    }
  }

  // Iceberg only applies the inferred-column none-truncation when NO explicit
  // `write.metadata.metrics.default` is set; with a default present every column uses that mode
  // regardless of count, so the gate must not fire even when the schema exceeds the cap.
  test("Compatible when over max-inferred cap but an explicit metrics.default is set") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { _ =>
      val sql = s"""
        CREATE TABLE $catalog.$ns.cap_with_default (
          id INT,
          region STRING,
          amount DOUBLE
        ) USING iceberg
        TBLPROPERTIES (
          'write.metadata.metrics.max-inferred-column-defaults'='2',
          'write.metadata.metrics.default'='full'
        )
      """
      spark.sql(sql)
      assertSupportLevelIs[Compatible]("cap_with_default")
    }
  }

  // --- write.parquet.row-group-check-min-record-count != default -------------------------------

  test("fall-back: row-group-check-min-record-count non-default") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "rg_min",
        partitionSpec = "",
        properties = Some("'write.parquet.row-group-check-min-record-count'='500'"))
      assertUnsupportedContains("rg_min", "write.parquet.row-group-check-min-record-count=500")
    }
  }

  test("Compatible when row-group-check-min-record-count is at default (100)") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      // Explicitly set to the Iceberg default -- the gate must not trigger on equal-to-default
      // values, only on divergent ones.
      createTable(
        dir,
        "rg_min_default",
        partitionSpec = "",
        properties = Some("'write.parquet.row-group-check-min-record-count'='100'"))
      assertSupportLevelIs[Compatible]("rg_min_default")
    }
  }

  // --- write.parquet.row-group-check-max-record-count != default -------------------------------

  test("fall-back: row-group-check-max-record-count non-default") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "rg_max",
        partitionSpec = "",
        properties = Some("'write.parquet.row-group-check-max-record-count'='50000'"))
      assertUnsupportedContains("rg_max", "write.parquet.row-group-check-max-record-count=50000")
    }
  }

  // --- write.metadata.metrics.default=none -----------------------------------------------------

  test("fall-back: write.metadata.metrics.default=none") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "metrics_none",
        partitionSpec = "",
        properties = Some("'write.metadata.metrics.default'='none'"))
      assertUnsupportedContains("metrics_none", "write.metadata.metrics.default", "none")
    }
  }

  // --- per-column metrics mode=none ------------------------------------------------------------

  test("fall-back: per-column metrics mode=none") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "col_metrics_none",
        partitionSpec = "",
        properties = Some("'write.metadata.metrics.column.region'='none'"))
      assertUnsupportedContains(
        "col_metrics_none",
        "write.metadata.metrics.column.region",
        "none")
    }
  }

  // (No `write.parquet.page-version` gate: that property does not exist in any Iceberg version
  // Comet targets -- the parquet writer is hardwired to PARQUET_1_0 -- so setting it is a no-op in
  // Iceberg-Java just as in the native path. Nothing to gate.)

  // --- parquet.enable.dictionary set -----------------------------------------------------------

  test("fall-back: parquet.enable.dictionary set") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "enable_dict",
        partitionSpec = "",
        properties = Some("'parquet.enable.dictionary'='false'"))
      assertUnsupportedContains("enable_dict", "parquet.enable.dictionary")
    }
  }

  // --- per-column write.parquet.stats-enabled.column.<c> ---------------------------------------

  // `write.parquet.stats-enabled.column.*` only exists (and is honoured) on Iceberg 1.10.0+
  // (`TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX`). On 1.5.2 / 1.8.1 Iceberg-Java ignores
  // it, so the native write matches Java and must NOT fall back. Branch the expectation on whether
  // the constant exists, so the test asserts the right thing on every profile.
  test("per-column write.parquet.stats-enabled.<col>: gated only where Iceberg honours it") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "col_stats",
        partitionSpec = "",
        properties = Some("'write.parquet.stats-enabled.column.region'='false'"))
      val honoured =
        IcebergReflection
          .tablePropertyConstantOpt("PARQUET_COLUMN_STATS_ENABLED_PREFIX")
          .isDefined
      if (honoured) {
        assertUnsupportedContains("col_stats", "write.parquet.stats-enabled.column.region")
      } else {
        assertSupportLevelIs[Compatible]("col_stats")
      }
    }
  }

  // --- io-impl set -----------------------------------------------------------------------------

  test("fall-back: io-impl set") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "io_impl",
        partitionSpec = "",
        properties = Some("'io-impl'='com.example.MyFileIO'"))
      assertUnsupportedContains("io_impl", "io-impl")
    }
  }

  // --- unsupported data location URI scheme ----------------------------------------------------

  test("fall-back: data location URI scheme not supported by the native writer") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    // Override the table's data location via `write.data.path` so the location provider returns
    // a URI whose scheme `iceberg_common::storage_factory_for` does not resolve. The actual
    // path is never opened -- detection runs at planning time before any write attempt.
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "bad_scheme",
        partitionSpec = "",
        properties = Some("'write.data.path'='hdfs://nonexistent.invalid/iceberg/db/bad_scheme'"))
      assertUnsupportedContains("bad_scheme", "scheme 'hdfs'", "not supported")
    }
  }

  // --- Helpers -----------------------------------------------------------------------------------

  private val catalog = "cat"
  private val ns = "db"

  /**
   * Lighter analogue of `CometIcebergWriteActionSuite.withIcebergCatalog`: same catalog wiring,
   * no Comet read/exec acceleration (we're only inspecting planning, not executing).
   */
  private def withDetectionCatalog(f: File => Unit): Unit = withTempIcebergDir { warehouseDir =>
    withSQLConf(
      s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$catalog.type" -> "hadoop",
      s"spark.sql.catalog.$catalog.warehouse" -> warehouseDir.getAbsolutePath) {
      f(warehouseDir)
    }
  }

  private def createTable(
      warehouseDir: File,
      tableName: String,
      partitionSpec: String,
      properties: Option[String] = None): Unit = {
    val props = properties.map(s => s" TBLPROPERTIES ($s)").getOrElse("")
    spark.sql(s"""
      CREATE TABLE $catalog.$ns.$tableName (
        id INT,
        region STRING,
        amount DOUBLE
      ) USING iceberg
      $partitionSpec
      $props
    """)
  }

  /**
   * Trigger an `INSERT INTO` and pluck the `IcebergWriteExec` out of the captured `executedPlan`.
   */
  private def insertWriteExec(tableName: String): IcebergWriteExec =
    captureWriteExec(tableName) {
      spark.sql(s"INSERT INTO $catalog.$ns.$tableName VALUES (1, 'us', 1.0)")
    }

  /**
   * Sibling of [[insertWriteExec]] that uses the DataFrameWriterV2 API so per-write `option(...)`
   * pairs flow into `SparkWriteConf`. Pins gates whose effective value resolves through a
   * `SparkWrite` field (e.g. the resolved `format` overlaying `write.format.default` with the
   * per-write `write-format` option).
   */
  private def dfWriteExec(tableName: String, options: (String, String)*): IcebergWriteExec =
    captureWriteExec(tableName) {
      val df = spark
        .createDataFrame(Seq((1, "us", 1.0)))
        .toDF("id", "region", "amount")
      val writer = options.foldLeft(df.writeTo(s"$catalog.$ns.$tableName")) { case (w, (k, v)) =>
        w.option(k, v)
      }
      writer.append()
    }

  /**
   * Spark 3.5+ `QueryExecution.sparkPlan` accesses `commandExecuted`, which eagerly executes V2
   * commands. So we can't inspect a planned-but-unexecuted tree -- the write fires whether we
   * want it to or not. A `QueryExecutionListener` lets us capture the executedPlan whether the
   * write succeeds or throws (some negative fixtures, e.g. encryption, deliberately set values
   * that crash the JVM writer at task time).
   */
  private def captureWriteExec(tableName: String)(trigger: => Unit): IcebergWriteExec = {
    val captured =
      new java.util.concurrent.atomic.AtomicReference[org.apache.spark.sql.execution.SparkPlan]()
    val listener = new org.apache.spark.sql.util.QueryExecutionListener {
      override def onSuccess(
          funcName: String,
          qe: org.apache.spark.sql.execution.QueryExecution,
          durationNs: Long): Unit =
        captured.compareAndSet(null, qe.executedPlan)
      override def onFailure(
          funcName: String,
          qe: org.apache.spark.sql.execution.QueryExecution,
          exception: Exception): Unit =
        captured.compareAndSet(null, qe.executedPlan)
    }
    // Drain any pending `QueryExecutionListener` events from the prior `createTable` SQL --
    // Spark's `ExecutionListenerBus` delivers events asynchronously, so a CREATE TABLE event
    // queued before we register can still arrive after, and our `compareAndSet(null, ...)`
    // would capture the wrong plan.
    try org.apache.spark.CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    catch { case _: java.util.concurrent.TimeoutException => () }
    spark.listenerManager.register(listener)
    try {
      try trigger
      catch { case _: Throwable => () }
      try org.apache.spark.CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
      catch { case _: java.util.concurrent.TimeoutException => () }
    } finally {
      spark.listenerManager.unregister(listener)
    }
    val plan = Option(captured.get())
      .getOrElse(fail(s"No QueryExecution captured for $tableName"))
    findWriteExecOrFail(plan)
  }

  private def findWriteExecOrFail(
      plan: org.apache.spark.sql.execution.SparkPlan): IcebergWriteExec =
    findWriteExec(plan).getOrElse(fail(s"no IcebergWriteExec found in:\n$plan"))

  private def findWriteExec(
      plan: org.apache.spark.sql.execution.SparkPlan): Option[IcebergWriteExec] =
    plan match {
      case e: IcebergWriteExec => Some(e)
      case other =>
        val descend = other.children.iterator ++ wrappedChildren(other).iterator
        descend.flatMap(findWriteExec).toSeq.headOption
    }

  /**
   * Some Spark execs hide their physical tree behind accessors that aren't reported in
   * `children`:
   *   - `CommandResultExec` stores the command tree on `commandPhysicalPlan` (the result is
   *     already materialised, so `children` returns Nil).
   *   - `AdaptiveSparkPlanExec` stores its inner plan on `executedPlan` (the wrapped plan re-
   *     plans on stage materialisation, so `children` is empty by design). Both accessors are
   *     stable across Spark 3.4-4.0; we pluck them reflectively so the test stays
   *     version-independent.
   */
  private def wrappedChildren(plan: org.apache.spark.sql.execution.SparkPlan)
      : Iterable[org.apache.spark.sql.execution.SparkPlan] = {
    def viaAccessor(method: String): Option[org.apache.spark.sql.execution.SparkPlan] =
      scala.util
        .Try {
          plan.getClass
            .getMethod(method)
            .invoke(plan)
            .asInstanceOf[org.apache.spark.sql.execution.SparkPlan]
        }
        .toOption
        .filter(_ ne plan)
    // `plan` covers Spark 4.0's `ResultQueryStage` (and other `QueryStageExec` subclasses); the
    // wrapped plan is held on a field that doesn't make it into `children`.
    Seq("commandPhysicalPlan", "executedPlan", "plan").flatMap(viaAccessor)
  }

  private def assertSupportLevelIs[T <: SupportLevel: scala.reflect.ClassTag](
      tableName: String): Unit = {
    val support = CometIcebergNativeWrite.getSupportLevel(insertWriteExec(tableName))
    val expected = scala.reflect.classTag[T].runtimeClass
    assert(
      expected.isInstance(support),
      s"expected ${expected.getSimpleName} for $tableName, got $support")
  }

  /**
   * Pin both that the gate returns `Unsupported` and that the reason string contains every
   * fragment in `fragments`. Substring matching keeps tests resilient to small phrasing changes
   * while still catching wrong-trigger drift (a different gate firing would surface a different
   * fragment set).
   */
  private def assertUnsupportedContains(tableName: String, fragments: String*): Unit = {
    val support = CometIcebergNativeWrite.getSupportLevel(insertWriteExec(tableName))
    support match {
      case Unsupported(Some(reason)) =>
        fragments.foreach(f =>
          assert(reason.contains(f), s"reason '$reason' missing fragment '$f'"))
      case Unsupported(None) =>
        fail("Unsupported without a reason string")
      case other =>
        fail(s"expected Unsupported, got $other")
    }
  }
}
