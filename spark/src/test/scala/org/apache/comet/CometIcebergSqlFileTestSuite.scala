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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

/**
 * SQL file test suite for Iceberg tables.
 *
 * Tests are placed in sql-tests/iceberg/ and use a pre-configured Iceberg catalog. Tables should
 * use the `iceberg_cat` catalog prefix (e.g., `iceberg_cat.db.table_name`).
 */
class CometIcebergSqlFileTestSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private def icebergAvailable: Boolean = {
    try {
      Class.forName("org.apache.iceberg.catalog.Catalog")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      assume(icebergAvailable, "Iceberg not available in classpath")
      testFun
    }
  }

  private val testResourceDir = {
    val url = getClass.getClassLoader.getResource("sql-tests/iceberg")
    if (url == null) null else new File(url.toURI)
  }

  private def discoverTestFiles(dir: File): Seq[File] = {
    if (dir == null || !dir.exists()) return Seq.empty
    val files = dir.listFiles().toSeq
    val sqlFiles = files.filter(f => f.isFile && f.getName.endsWith(".sql"))
    val subDirFiles = files.filter(_.isDirectory).flatMap(discoverTestFiles)
    sqlFiles ++ subDirFiles
  }

  private val constantFoldingExcluded = Seq(
    "spark.sql.optimizer.excludedRules" ->
      "org.apache.spark.sql.catalyst.optimizer.ConstantFolding")

  private def runTestFile(file: SqlTestFile, warehouseDir: File): Unit = {
    val icebergConfigs = Seq(
      "spark.sql.catalog.iceberg_cat" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.iceberg_cat.type" -> "hadoop",
      "spark.sql.catalog.iceberg_cat.warehouse" -> warehouseDir.getAbsolutePath,
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true")

    val allConfigs = icebergConfigs ++ file.configs ++ constantFoldingExcluded
    withSQLConf(allConfigs: _*) {
      file.records.foreach {
        case SqlStatement(sql, _) =>
          spark.sql(sql)
        case SqlQuery(sql, mode, _) =>
          mode match {
            case CheckCoverageAndAnswer =>
              checkSparkAnswerAndOperator(sql)
            case SparkAnswerOnly =>
              checkSparkAnswer(sql)
            case WithTolerance(tol) =>
              checkSparkAnswerWithTolerance(sql, tol)
            case ExpectFallback(reason) =>
              checkSparkAnswerAndFallbackReason(sql, reason)
            case Ignore(reason) =>
              logInfo(s"IGNORED query (${reason}): $sql")
          }
      }
    }
  }

  if (testResourceDir != null) {
    discoverTestFiles(testResourceDir).foreach { file =>
      val relativePath =
        new File(testResourceDir.getParentFile, "iceberg").toURI.relativize(file.toURI).getPath
      val parsed = SqlFileTestParser.parse(file)

      test(s"iceberg-sql: $relativePath") {
        val warehouseDir = Files.createTempDirectory("iceberg-sql-test").toFile
        try {
          runTestFile(parsed, warehouseDir)
        } finally {
          deleteRecursively(warehouseDir)
        }
      }
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}
