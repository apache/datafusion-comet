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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Tests for `spark.comet.exceptionOnDatetimeRebase`.
 *
 * Comet's native scan does not rebase dates/timestamps written in the legacy hybrid (Julian +
 * Gregorian) calendar, so it silently returns shifted values for dates before 1582-10-15 and
 * timestamps before 1900-01-01T00:00:00Z. Implementing rebasing is tracked by
 * https://github.com/apache/datafusion-comet/issues/5010; this config is the interim safety
 * valve, so a user can choose to fail rather than get wrong answers.
 *
 * See https://github.com/apache/datafusion-comet/issues/5195
 */
class ParquetDatetimeRebaseSuite extends CometTestBase {

  /**
   * A date whose Julian and Proleptic Gregorian representations differ, and one where they agree.
   */
  private val ancientDate = "1000-01-01"
  private val modernDate = "1990-01-01"

  /** What a legacy-written `ancientDate` reads back as when nobody rebases it. */
  private val ancientDateUnrebased = "1000-01-06"

  private def writeLegacyDates(path: String): Unit = {
    withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> "LEGACY") {
      spark
        .sql(s"SELECT cast(s as date) AS d, s AS label " +
          s"FROM VALUES ('$ancientDate'), ('$modernDate') AS v(s)")
        .write
        .mode("overwrite")
        .parquet(path)
    }
  }

  private def writeCorrectedDates(path: String): Unit = {
    withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> "CORRECTED") {
      spark
        .sql(s"SELECT cast(s as date) AS d, s AS label " +
          s"FROM VALUES ('$ancientDate'), ('$modernDate') AS v(s)")
        .write
        .mode("overwrite")
        .parquet(path)
    }
  }

  private def assertNativeScan(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(
      plan.collectLeaves().exists(_.isInstanceOf[CometNativeScanExec]),
      s"expected a CometNativeScanExec, got:\n$plan")
  }

  /** The messages in a throwable's cause chain, for substring assertions. */
  private def causeChain(t: Throwable): List[String] =
    Iterator
      .iterate(t)(_.getCause)
      .takeWhile(_ != null)
      .map(e => s"${e.getClass.getName}: ${e.getMessage}")
      .toList

  private def assertRaisesOnRebase(df: => DataFrame): Unit = {
    val chain = causeChain(intercept[Throwable](df.collect()))
    assert(
      chain.exists(_.contains("legacy hybrid (Julian + Gregorian) calendar")),
      s"expected the legacy-calendar rejection in the cause chain, got:\n  ${chain.mkString("\n  ")}")
    assert(
      chain.exists(_.contains(CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.key)),
      s"expected the message to name the config that raised, got:\n  ${chain.mkString("\n  ")}")
  }

  test("legacy-calendar dates raise when exceptionOnDatetimeRebase is enabled") {
    withSQLConf(CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        writeLegacyDates(path)
        assertRaisesOnRebase(spark.read.parquet(path))
        // A projection that only reads the date column still raises.
        assertRaisesOnRebase(spark.read.parquet(path).select("d"))
        // So does one that only filters on it: pushed-down filter columns are part of the
        // required schema, so they are covered too.
        assertRaisesOnRebase(
          spark.read.parquet(path).where(s"d > date'$modernDate'").select("label"))
      }
    }
  }

  test("legacy-calendar dates are read unrebased when exceptionOnDatetimeRebase is disabled") {
    // The default. This is the wrong-answer behavior of #5010, asserted here so that wiring the
    // config up is provably a no-op unless the user opts in.
    withSQLConf(
      CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.key -> "false",
      // Return java.time.LocalDate rather than java.sql.Date, so `toString` renders the Proleptic
      // Gregorian value and does not depend on JDK hybrid-calendar conversion.
      SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        writeLegacyDates(path)
        val df = spark.read.parquet(path).select("d")
        assertNativeScan(df)
        assert(
          df.collect().map(_.get(0).toString).sorted ===
            Array(ancientDateUnrebased, modernDate).sorted)
      }
    }
  }

  test("a projection with no date or timestamp column does not raise") {
    // Nothing else in a Parquet file is calendar-sensitive, and Spark only raises when it
    // decodes an affected value, so a scan that reads no date/timestamp must not be failed.
    withSQLConf(CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        writeLegacyDates(path)
        val df = spark.read.parquet(path).select("label")
        assertNativeScan(df)
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("proleptic-Gregorian files do not raise when exceptionOnDatetimeRebase is enabled") {
    // The overwhelmingly common case: written by Spark 3.0+ with the default CORRECTED mode, so
    // the footer carries no legacy marker and enabling the config must not cost anything.
    withSQLConf(CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        writeCorrectedDates(path)
        val df = spark.read.parquet(path)
        assertNativeScan(df)
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("legacy-calendar INT96 timestamps raise when exceptionOnDatetimeRebase is enabled") {
    withSQLConf(CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        withSQLConf(
          SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "INT96",
          SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> "LEGACY") {
          spark
            .sql("SELECT cast('1800-01-01 12:00:00' as timestamp) AS ts")
            .write
            .mode("overwrite")
            .parquet(path)
        }
        assertRaisesOnRebase(spark.read.parquet(path))
      }
    }
  }
}
