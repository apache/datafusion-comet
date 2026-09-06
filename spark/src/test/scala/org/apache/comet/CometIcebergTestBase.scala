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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometSparkSessionExtensions.isSpark42Plus
import org.apache.comet.iceberg.IcebergReflection

/**
 * Shared fixtures for Iceberg-backed test suites: classpath probe and per-test temp directory.
 */
trait CometIcebergTestBase {

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

  /** The executed plan of every query that completes successfully while `action` runs. */
  protected def capturePlans(spark: SparkSession)(action: => Unit): Seq[SparkPlan] = {
    val captured = mutable.Buffer.empty[SparkPlan]
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        captured += qe.executedPlan
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        ()
    }
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
