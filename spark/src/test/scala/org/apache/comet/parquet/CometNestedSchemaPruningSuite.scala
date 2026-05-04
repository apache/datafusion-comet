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

package org.apache.comet.parquet

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.comet.{CometNativeScanExec, CometScanExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import org.apache.comet.CometConf

/**
 * Verifies that Comet honors `spark.sql.optimizer.nestedSchemaPruning.enabled` end-to-end: the
 * executed plan's required schema is pruned when the flag is `true`, the full schema is read when
 * the flag is `false`, and results match Spark in both cases.
 *
 * Each test runs once per Comet scan implementation (`SCAN_NATIVE_DATAFUSION`,
 * `SCAN_NATIVE_ICEBERG_COMPAT`). The V1 datasource path is pinned because plain Parquet V2 is not
 * Comet-accelerated (only CSV and Iceberg V2 scans are).
 */
class CometNestedSchemaPruningSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val scanImpls =
    Seq(CometConf.SCAN_NATIVE_DATAFUSION, CometConf.SCAN_NATIVE_ICEBERG_COMPAT)

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    scanImpls.foreach { scan =>
      super.test(s"$testName - $scan", testTags: _*) {
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "false",
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> scan,
          SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
          testFun
        }
      }
    }
  }

  /**
   * Walks the executed plan, collects the required schema from any Comet scan exec, and asserts
   * it matches `expected` (a catalyst-style schema string). Field nullability is ignored.
   */
  private def assertScanSchema(df: DataFrame, expected: String): Unit = {
    val scanSchemas = collect(df.queryExecution.executedPlan) {
      case scan: CometScanExec => scan.requiredSchema
      case scan: CometNativeScanExec => scan.requiredSchema
    }
    assert(
      scanSchemas.size == 1,
      s"Expected exactly one Comet scan in plan, found ${scanSchemas.size}:\n" +
        df.queryExecution.executedPlan.toString)
    val expectedSchema = CatalystSqlParser.parseDataType(expected).asInstanceOf[StructType]
    // Compare via catalogString which omits nullability flags so the assertions stay readable.
    assert(
      scanSchemas.head.catalogString == expectedSchema.catalogString,
      s"Pruned schema mismatch.\n  expected: ${expectedSchema.catalogString}\n" +
        s"  actual:   ${scanSchemas.head.catalogString}")
  }

  /**
   * Writes a small `Contact` parquet dataset to a temp path and runs `body` with the path.
   * Mirrors the case-class shape used by Spark's `SchemaPruningSuite` so the audit
   * cross-references easily.
   */
  private def withContactsParquet(body: String => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      import testImplicits._
      val df = Seq(
        Contact(
          id = 0,
          name = FullName("Jane", "X.", "Doe"),
          address = "123 Main Street",
          pets = 1,
          friends = Array(FullName("Susan", "Z.", "Smith")),
          relatives = Map("brother" -> FullName("John", "Y.", "Doe")),
          employer = Employer(0, Company("abc", "123 Business Street"))),
        Contact(
          id = 1,
          name = FullName("John", "Y.", "Doe"),
          address = "321 Wall Street",
          pets = 3,
          friends = Array(FullName("Alice", "A.", "Jones")),
          relatives = Map("sister" -> FullName("Jane", "X.", "Doe")),
          employer = null)).toDF()
      df.write.parquet(path)
      body(path)
    }
  }

  // Top-level field of a struct -- pruned schema retains only the projected leaf.
  test("prune top-level struct field") {
    withContactsParquet { path =>
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
        val df = spark.read.parquet(path).selectExpr("name.first")
        assertScanSchema(df, "struct<name:struct<first:string>>")
        checkSparkAnswer(df)
      }
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "false") {
        val df = spark.read.parquet(path).selectExpr("name.first")
        assertScanSchema(df, "struct<name:struct<first:string,middle:string,last:string>>")
        checkSparkAnswer(df)
      }
    }
  }

  // Field inside an array of struct.
  test("prune field inside array of struct") {
    withContactsParquet { path =>
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
        val df = spark.read.parquet(path).selectExpr("friends.first")
        assertScanSchema(df, "struct<friends:array<struct<first:string>>>")
        checkSparkAnswer(df)
      }
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "false") {
        val df = spark.read.parquet(path).selectExpr("friends.first")
        assertScanSchema(
          df,
          "struct<friends:array<struct<first:string,middle:string,last:string>>>")
        checkSparkAnswer(df)
      }
    }
  }

  // Field inside a map value.
  test("prune field inside map value") {
    withContactsParquet { path =>
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
        val df = spark.read.parquet(path).selectExpr("relatives['brother'].first")
        assertScanSchema(df, "struct<relatives:map<string,struct<first:string>>>")
        checkSparkAnswer(df)
      }
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "false") {
        val df = spark.read.parquet(path).selectExpr("relatives['brother'].first")
        assertScanSchema(
          df,
          "struct<relatives:map<string,struct<first:string,middle:string,last:string>>>")
        checkSparkAnswer(df)
      }
    }
  }

  // Doubly-nested struct: only the deep leaf is required.
  test("prune doubly-nested struct field") {
    withContactsParquet { path =>
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
        val df = spark.read.parquet(path).selectExpr("employer.company.name")
        assertScanSchema(df, "struct<employer:struct<company:struct<name:string>>>")
        checkSparkAnswer(df)
      }
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "false") {
        val df = spark.read.parquet(path).selectExpr("employer.company.name")
        assertScanSchema(
          df,
          "struct<employer:struct<id:int,company:struct<name:string,address:string>>>")
        checkSparkAnswer(df)
      }
    }
  }

  // Filter on a nested field plus a separate top-level projection. The required schema must
  // include both the filtered leaf and the projected top-level column.
  test("prune with filter on nested field") {
    withContactsParquet { path =>
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
        val df = spark.read
          .parquet(path)
          .where("name.first = 'Jane'")
          .selectExpr("id")
        assertScanSchema(df, "struct<id:int,name:struct<first:string>>")
        checkSparkAnswer(df)
      }
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "false") {
        val df = spark.read
          .parquet(path)
          .where("name.first = 'Jane'")
          .selectExpr("id")
        assertScanSchema(df, "struct<id:int,name:struct<first:string,middle:string,last:string>>")
        checkSparkAnswer(df)
      }
    }
  }

  // Pruning correctly returns null when the intermediate struct is null in the row.
  // The second contact has employer = null; the projected leaf must round-trip as null and match
  // Spark's behavior.
  test("prune with null at intermediate struct level") {
    withContactsParquet { path =>
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
        val df = spark.read.parquet(path).selectExpr("employer.company.name")
        assertScanSchema(df, "struct<employer:struct<company:struct<name:string>>>")
        checkSparkAnswer(df)
      }
    }
  }
}

private case class FullName(first: String, middle: String, last: String)
private case class Company(name: String, address: String)
private case class Employer(id: Int, company: Company)
private case class Contact(
    id: Int,
    name: FullName,
    address: String,
    pets: Int,
    friends: Array[FullName] = Array.empty,
    relatives: Map[String, FullName] = Map.empty,
    employer: Employer = null)
