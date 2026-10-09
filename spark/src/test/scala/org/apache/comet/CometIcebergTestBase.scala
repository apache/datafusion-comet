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
import java.nio.file.Files

import scala.collection.mutable

import org.apache.spark.CometListenerBusUtils
import org.apache.spark.sql.{CometTestBase, SparkSession}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometSparkSessionExtensions.isSpark42Plus
import org.apache.comet.iceberg.IcebergReflection

/**
 * Shared fixtures for Iceberg-backed test suites: classpath probe, per-test temp directory, a
 * Hadoop catalog, and a table of pre-1970 timestamps. Mix in alongside `CometTestBase`.
 */
trait CometIcebergTestBase { this: CometTestBase =>

  // No Iceberg spark-runtime is published for Spark 4.2 yet, so the build reuses the 4.0 runtime.
  // That jar is binary-incompatible with Spark 4.2, whose `connector.catalog.View` is a class
  // rather than an interface, so loading `SparkView` throws IncompatibleClassChangeError. Report
  // Iceberg as unavailable on 4.2 so these suites skip until a compatible runtime exists.
  protected def icebergAvailable: Boolean =
    !isSpark42Plus &&
      (try {
        IcebergReflection.loadClass("org.apache.iceberg.catalog.Catalog")
        true
      } catch {
        case _: ClassNotFoundException => false
      })

  /**
   * Whether the Iceberg library on the classpath is at least the given (major, minor) version.
   * Returns false if the version cannot be determined, so version-gated tests skip rather than
   * risk running on an unsupported version.
   */
  protected def icebergVersionAtLeast(major: Int, minor: Int): Boolean =
    try {
      val version = IcebergReflection
        .loadClass("org.apache.iceberg.IcebergBuild")
        .getMethod("version")
        .invoke(null)
        .toString
      version.split("[.-]", 3) match {
        case Array(maj, min, _*) =>
          val m = maj.toInt
          val n = min.toInt
          m > major || (m == major && n >= minor)
        case _ => false
      }
    } catch {
      case _: Exception => false
    }

  /**
   * Loads the Iceberg `Table` behind a Spark catalog table (via `SparkTable.table()`), going
   * through the session's own catalog instance so it works regardless of where the catalog's
   * warehouse actually resolved.
   */
  protected def loadIcebergTable(
      spark: SparkSession,
      catalogName: String,
      namespace: String,
      tableName: String): AnyRef = {
    val sparkTable = spark.sessionState.catalogManager
      .catalog(catalogName)
      .asInstanceOf[TableCatalog]
      .loadTable(Identifier.of(Array(namespace), tableName))
    sparkTable.getClass.getMethod("table").invoke(sparkTable)
  }

  /**
   * Adds a column of an Iceberg type Spark DDL cannot declare (e.g. `uuid`, `fixed(N)`) by
   * committing an `UpdateSchema` against the Iceberg table directly. Callers must `REFRESH TABLE`
   * afterwards so Spark's catalog cache picks up the new schema.
   */
  protected def addIcebergColumn(
      icebergTable: AnyRef,
      columnName: String,
      icebergType: AnyRef): Unit = {
    val update = IcebergReflection
      .loadClass("org.apache.iceberg.Table")
      .getMethod("updateSchema")
      .invoke(icebergTable)
    val updateSchemaClass = IcebergReflection.loadClass("org.apache.iceberg.UpdateSchema")
    updateSchemaClass
      .getMethod(
        "addColumn",
        classOf[String],
        IcebergReflection.loadClass("org.apache.iceberg.types.Type"))
      .invoke(update, columnName, icebergType)
    updateSchemaClass.getMethod("commit").invoke(update)
  }

  protected def icebergUuidType(): AnyRef =
    IcebergReflection
      .loadClass("org.apache.iceberg.types.Types$UUIDType")
      .getMethod("get")
      .invoke(null)

  /**
   * Adds an `int` column whose initial and write defaults are both `defaultValue`, through
   * `UpdateSchema.addColumn(name, type, Literal)`. Requires Iceberg 1.10+ and a v3 table. Callers
   * must `REFRESH TABLE` afterwards.
   */
  protected def addIcebergIntColumnWithDefault(
      icebergTable: AnyRef,
      columnName: String,
      defaultValue: Int): Unit = {
    val intType = IcebergReflection
      .loadClass("org.apache.iceberg.types.Types$IntegerType")
      .getMethod("get")
      .invoke(null)
    val literal = IcebergReflection
      .loadClass("org.apache.iceberg.expressions.Expressions")
      .getMethod("lit", classOf[Object])
      .invoke(null, Integer.valueOf(defaultValue))
    val update = IcebergReflection
      .loadClass("org.apache.iceberg.Table")
      .getMethod("updateSchema")
      .invoke(icebergTable)
    val updateSchemaClass = IcebergReflection.loadClass("org.apache.iceberg.UpdateSchema")
    updateSchemaClass
      .getMethod(
        "addColumn",
        classOf[String],
        IcebergReflection.loadClass("org.apache.iceberg.types.Type"),
        IcebergReflection.loadClass("org.apache.iceberg.expressions.Literal"))
      .invoke(update, columnName, intType, literal)
    updateSchemaClass.getMethod("commit").invoke(update)
  }

  /** Iceberg's v3 `unknown` type. Requires Iceberg 1.10+. */
  protected def icebergUnknownType(): AnyRef =
    IcebergReflection
      .loadClass("org.apache.iceberg.types.Types$UnknownType")
      .getMethod("get")
      .invoke(null)

  protected def icebergFixedType(length: Int): AnyRef =
    IcebergReflection
      .loadClass("org.apache.iceberg.types.Types$FixedType")
      .getMethod("ofLength", classOf[Int])
      .invoke(null, Integer.valueOf(length))

  protected def withTempIcebergDir(f: File => Unit): Unit = {
    val dir = Files.createTempDirectory("comet-iceberg-test").toFile
    try f(dir)
    finally deleteRecursively(dir)
  }

  protected def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) file.listFiles().foreach(deleteRecursively)
    file.delete()
  }

  /** Runs `f` with an Iceberg `hadoop` catalog registered as `catalog`, in a temp warehouse. */
  protected def withHadoopCatalog(catalog: String)(f: => Unit): Unit =
    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$catalog.type" -> "hadoop",
        s"spark.sql.catalog.$catalog.warehouse" -> warehouseDir.getAbsolutePath)(f)
    }

  /**
   * Timestamps just after pre-1970 unit boundaries, where Iceberg does not floor. Its
   * `DateTimeUtil` places a pre-1970 timestamp whose microsecond of second is 999999 by the
   * second before it, so right after a boundary it gets the unit before: 1969-01-01
   * 00:00:00.999999 is in year -2, month -13, day 1968-12-31, and hour -8761, where a floor gives
   * -1, -12, 1969-01-01, and -8760. `sql-tests/iceberg/temporal_functions_pre_epoch.sql` runs the
   * system functions over the same timestamps in projections and filters.
   */
  protected val preEpochTimestamps: Seq[String] = Seq(
    "1969-01-01 00:00:00.999999", // a year, month, day, and hour boundary
    "1969-12-01 00:00:00.999999", // a month, day, and hour boundary
    "1969-12-31 00:00:00.999999", // a day and hour boundary
    "1969-12-31 23:00:00.999999", // an hour boundary
    "1969-12-31 22:30:00",
    "1968-12-31 12:00:00",
    // After the epoch, where Iceberg floors.
    "1970-01-01 01:00:00.999999")

  /**
   * Runs `f` with a parquet table `pre_epoch (id, ts)` holding `preEpochTimestamps`, numbered
   * from 1. A parquet table, so that no scan absorbs a filter on `ts` and Comet evaluates it. The
   * session timezone is UTC, so that the values sit on the unit boundaries.
   */
  protected def withPreEpochTable(f: => Unit): Unit = withSQLConf(
    SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
    SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS") {
    withTable("pre_epoch") {
      sql("CREATE TABLE pre_epoch (id INT, ts TIMESTAMP) USING parquet")
      val rows = preEpochTimestamps.zipWithIndex.map { case (timestamp, i) =>
        s"(${i + 1}, TIMESTAMP '$timestamp')"
      }
      sql(s"INSERT INTO pre_epoch VALUES ${rows.mkString(", ")}")
      f
    }
  }

  /**
   * The executed plan of every query that ran while `action` ran. Queries that failed are
   * included only when `includeFailures` is set, which is what an action expected to abort needs.
   */
  protected def capturePlans(spark: SparkSession, includeFailures: Boolean = false)(
      action: => Unit): Seq[SparkPlan] = {
    val captured = mutable.Buffer.empty[SparkPlan]
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        captured += qe.executedPlan
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        if (includeFailures) captured += qe.executedPlan
    }
    // Events from earlier queries may still be queued; drain them so they do not reach the
    // listener.
    CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    spark.listenerManager.register(listener)
    try {
      action
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    } finally {
      spark.listenerManager.unregister(listener)
    }
    captured.toSeq
  }

  /**
   * The executed plan of every query that fails while `action` runs, and the failure `action`
   * itself raised (`None` if it unexpectedly succeeded).
   */
  protected def captureFailedPlans(spark: SparkSession)(
      action: => Unit): (Seq[SparkPlan], Option[Exception]) = {
    val captured = mutable.Buffer.empty[SparkPlan]
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = ()
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        captured += qe.executedPlan
    }
    CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    spark.listenerManager.register(listener)
    try {
      val error =
        try {
          action
          None
        } catch { case e: Exception => Some(e) }
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
      (captured.toSeq, error)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }
}
