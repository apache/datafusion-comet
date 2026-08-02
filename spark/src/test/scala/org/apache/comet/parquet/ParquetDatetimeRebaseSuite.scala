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
import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Tests for `spark.comet.exceptionOnDatetimeRebase`.
 *
 * Comet's native scan does not rebase dates/timestamps written in the legacy hybrid (Julian +
 * Gregorian) calendar, so reading them unrebased returns values shifted by up to ten days.
 * Implementing rebasing is tracked by https://github.com/apache/datafusion-comet/issues/5010;
 * until then Comet fails such a read by default rather than returning wrong answers.
 *
 * The interesting cases here are the ones that must NOT fail: Spark stamps the legacy-calendar
 * marker on a whole file whenever the write mode was LEGACY, but dates from 1582-10-15 onward
 * rebase to themselves, so a marked file whose values are all modern is read normally.
 *
 * See https://github.com/apache/datafusion-comet/issues/5195
 */
class ParquetDatetimeRebaseSuite extends CometTestBase {

  /** A date before the 1582-10-15 switch, so its Julian and Gregorian forms differ. */
  private val ancientDate = "1000-01-01"

  /** Dates from the switch onward rebase to themselves. */
  private val modernDate = "1990-01-01"

  /** What a legacy-written `ancientDate` reads back as when nobody rebases it. */
  private val ancientDateUnrebased = "1000-01-06"

  /** Writes the rows `select` produces to `path`, under the given SQL confs. */
  private def writeParquet(path: String, select: String, confs: (String, String)*): Unit =
    withSQLConf(confs: _*) {
      spark.sql(select).write.mode("overwrite").parquet(path)
    }

  /** Writes `dates` (plus each date's text as a `label` column) in the given rebase mode. */
  private def writeDates(path: String, mode: String, dates: String*): Unit = {
    val values = dates.map(d => s"('$d')").mkString(", ")
    writeParquet(
      path,
      s"SELECT cast(s as date) AS d, s AS label FROM VALUES $values AS v(s)",
      SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> mode)
  }

  private def writeLegacyDates(path: String): Unit =
    writeDates(path, "LEGACY", ancientDate, modernDate)

  /** `checkSparkAnswerAndOperator`, additionally requiring Comet's native Parquet scan. */
  private def checkNativeScanAnswer(df: => DataFrame): Unit =
    checkSparkAnswerAndOperator(df, includeClasses = Seq(classOf[CometNativeScanExec]))

  /**
   * Asserts a native scan without comparing to Spark, for the tests that deliberately expect
   * Comet to differ from Spark. `stripAQEPlan` mirrors what `checkSparkAnswerAndOperator` does.
   */
  private def assertNativeScan(df: DataFrame): Unit = {
    val plan = stripAQEPlan(df.queryExecution.executedPlan)
    assert(
      plan.collectLeaves().exists(_.isInstanceOf[CometNativeScanExec]),
      s"expected a CometNativeScanExec, got:\n$plan")
  }

  /** A throwable and everything in its cause chain. */
  private def causeChain(t: Throwable): List[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).toList

  private def render(chain: List[Throwable]): String =
    chain.map(e => s"${e.getClass.getName}: ${e.getMessage}").mkString("\n  ")

  private def assertRaisesOnRebase(df: => DataFrame): Unit = {
    val chain = causeChain(intercept[Throwable](df.collect()))
    val messages = chain.map(_.getMessage).filter(_ != null)
    assert(
      messages.exists(_.contains("legacy hybrid (Julian + Gregorian) calendar")),
      s"expected the legacy-calendar rejection in the cause chain, got:\n  ${render(chain)}")
    assert(
      messages.exists(_.contains(CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.key)),
      s"expected the message to name the config that raised, got:\n  ${render(chain)}")
  }

  test("the native rebase thresholds match Spark's own") {
    // The native guard hardcodes these rather than receiving them from the JVM, because touching
    // RebaseDateTime forces a static initializer that parses ~590 KB of bundled JSON and retains
    // several MB -- a cost every driver would pay on its first native scan. This test is what keeps
    // the copies honest: if Spark ever moves a switch point, it fails here rather than silently
    // shifting the threshold. Keep in sync with LAST_SWITCH_JULIAN_DAY / LAST_SWITCH_JULIAN_MICROS
    // in native/core/src/parquet/legacy_datetime.rs.
    assert(RebaseDateTime.lastSwitchJulianDay == -141427) // 1582-10-15
    assert(RebaseDateTime.lastSwitchJulianTs == -2208988800000000L) // 1900-01-01T00:00:00Z
  }

  test("ancient legacy-calendar dates raise by default") {
    // No config set: the guard is on out of the box, which is the point of the default. Comet
    // must not silently return shifted values.
    assert(CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.defaultValue.contains(true))
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writeLegacyDates(path)
      assertRaisesOnRebase(spark.read.parquet(path))
      // A projection that only reads the date column still raises.
      assertRaisesOnRebase(spark.read.parquet(path).select("d"))
      // So does one that only filters on it: pushed-down filter columns are part of the required
      // schema, so they are covered too.
      assertRaisesOnRebase(
        spark.read.parquet(path).where(s"d > date'$modernDate'").select("label"))
    }
  }

  test("legacy-calendar dates are read unrebased when exceptionOnDatetimeRebase is disabled") {
    // The opt-out. Pins the pre-guard behaviour of #5010 so it stays reachable for anyone who
    // needs it, and documents exactly how wrong it is.
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
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writeLegacyDates(path)
      checkNativeScanAnswer(spark.read.parquet(path).select("label"))
    }
  }

  test("proleptic-Gregorian files do not raise") {
    // The overwhelmingly common case: written by Spark 3.0+ with the default CORRECTED mode, so
    // the footer carries no legacy marker and the guard must cost nothing.
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writeDates(path, "CORRECTED", ancientDate, modernDate)
      checkNativeScanAnswer(spark.read.parquet(path))
    }
  }

  test("legacy-calendar INT96 timestamps raise even when their values look modern") {
    // The footer marks this file legacy, and INT96 has no meaningful byte ordering, so writers
    // produce no usable min/max and nothing can narrow the refusal to the affected rows. A file
    // that declares itself legacy gets refused whole. (A file that declares no writer at all is
    // read instead -- see the version-less INT96 fixtures below.)
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writeParquet(
        path,
        "SELECT cast('2020-06-30 12:00:00' as timestamp) AS ts",
        SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "INT96",
        SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> "LEGACY")
      assertRaisesOnRebase(spark.read.parquet(path))
    }
  }

  test("a legacy-marked file holding only modern dates is read normally") {
    // The case that makes a default-on guard tolerable. Spark stamps the legacy marker on the
    // whole file whenever the write mode was LEGACY, but 1990-01-01 rebases to itself, so the
    // values Comet reads are correct and the scan must not be failed. Row-group statistics are
    // what let Comet tell this file apart from one holding ancient values.
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writeDates(path, "LEGACY", modernDate, "2020-06-30")
      checkNativeScanAnswer(spark.read.parquet(path))
    }
  }

  test("a legacy-marked file holding only modern timestamps is read normally") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writeParquet(
        path,
        "SELECT cast('2020-06-30 12:00:00' as timestamp) AS ts",
        SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS",
        SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> "LEGACY")
      checkNativeScanAnswer(spark.read.parquet(path))
    }
  }

  test("a legacy-marked file holding an ancient timestamp raises") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writeParquet(
        path,
        "SELECT cast('1800-01-01 12:00:00' as timestamp) AS ts",
        SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS",
        SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> "LEGACY")
      assertRaisesOnRebase(spark.read.parquet(path))
    }
  }

  test("a legacy-marked TIMESTAMP_NTZ column is read even when its values are ancient") {
    // Spark stamps the legacy marker from the write-mode conf alone, without regard to whether the
    // schema holds a column the mode could apply to -- and it never rebases NTZ, in either
    // direction: "TIMESTAMP_NTZ is a new data type and has no legacy files that need to do rebase".
    // So these values read back exactly as written and Comet must agree with Spark, marker or not.
    // Mirrors Spark's own "SPARK-46466: write and read TimestampNTZ with legacy rebase mode".
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writeParquet(
        path,
        s"SELECT cast('$ancientDate 01:10:10' as timestamp_ntz) AS ts",
        SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> "LEGACY")
      checkNativeScanAnswer(spark.read.parquet(path))
    }
  }

  test("the rejection surfaces as SparkUpgradeException, not FAILED_READ_FILE") {
    // Spark raises SparkUpgradeException for this data and its FileScanRDD deliberately rethrows
    // that type rather than wrapping it in FAILED_READ_FILE, because the file is not corrupt.
    // Comet classifies it the same way.
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      writeLegacyDates(path)
      val chain = causeChain(intercept[Throwable](spark.read.parquet(path).collect()))
      val rendered = render(chain)
      // Matched by name: SparkUpgradeException is private[spark], so this package cannot name
      // the type.
      assert(
        chain.exists(_.getClass.getName == "org.apache.spark.SparkUpgradeException"),
        s"expected a SparkUpgradeException in the cause chain, got:\n  $rendered")
      assert(
        !rendered.contains("Encountered error while reading file"),
        s"the rejection must not be relabelled FAILED_READ_FILE, got:\n  $rendered")
    }
  }

  test("only the requested columns arm the guard") {
    // Two date columns, only one ancient. Reading the modern one must not fail even though the
    // file as a whole does hold an affected value.
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> "LEGACY") {
        spark
          .sql(
            s"SELECT cast('$ancientDate' as date) AS old_d, cast('$modernDate' as date) AS new_d")
          .write
          .mode("overwrite")
          .parquet(path)
      }
      checkNativeScanAnswer(spark.read.parquet(path).select("new_d"))

      assertRaisesOnRebase(spark.read.parquet(path).select("old_d"))
    }
  }

  /** Sets both `*RebaseModeInRead` settings, which Spark applies only to version-less files. */
  private def withReadMode(mode: String)(f: => Unit): Unit =
    withSQLConf(
      SQLConf.PARQUET_REBASE_MODE_IN_READ.key -> mode,
      SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ.key -> mode)(f)

  private def fixture(name: String): String =
    getResourceParquetFilePath(s"test-data/$name.snappy.parquet")

  /**
   * Checked-in files holding pre-1582 dates / pre-1900 timestamps, in every physical encoding a
   * calendar-sensitive column can use.
   *
   * Spark 2.4.5 stamped no `org.apache.spark.version`, so those files record no provenance and
   * Spark resolves them through the `*RebaseModeInRead` settings. Spark 2.4.6 added the version
   * key, and the v3_2_0 files were written with LEGACY rebase mode, so both of those are
   * identified from the footer alone and the read modes do not apply to them.
   *
   * For a version-less file the guard refuses only what row-group statistics positively expose,
   * which splits these two ways.
   */
  private val versionlessProvableFixtures = Seq(
    "before_1582_date_v2_4_5",
    "before_1582_timestamp_micros_v2_4_5",
    "before_1582_timestamp_millis_v2_4_5")

  /**
   * The version-less INT96 fixtures, where nothing can be proven: the footer names no writer, and
   * the Parquet spec gives INT96's 12 bytes no meaningful ordering, so there is no usable
   * min/max. The guard reads these rather than refusing them -- see the test below for why.
   */
  private val versionlessInt96Fixtures =
    Seq("before_1582_timestamp_int96_plain_v2_4_5", "before_1582_timestamp_int96_dict_v2_4_5")

  private val versionlessFixtures = versionlessProvableFixtures ++ versionlessInt96Fixtures

  private val markedFixtures = Seq(
    "before_1582_date_v2_4_6",
    "before_1582_date_v3_2_0",
    "before_1582_timestamp_micros_v2_4_6",
    "before_1582_timestamp_micros_v3_2_0",
    "before_1582_timestamp_millis_v2_4_6",
    "before_1582_timestamp_millis_v3_2_0",
    "before_1582_timestamp_int96_plain_v2_4_6",
    "before_1582_timestamp_int96_plain_v3_2_0",
    "before_1582_timestamp_int96_dict_v2_4_6",
    "before_1582_timestamp_int96_dict_v3_2_0")

  private val ancientFixtures = versionlessFixtures ++ markedFixtures

  (versionlessProvableFixtures ++ markedFixtures).foreach { name =>
    test(s"$name raises under EXCEPTION read mode") {
      // EXCEPTION is Spark's own default. For a version-less file Spark raises here too; for a
      // footer-marked one Spark rebases and returns correct values, which Comet cannot do. Either
      // way Comet must not return the shifted values.
      withReadMode("EXCEPTION") {
        assertRaisesOnRebase(spark.read.parquet(fixture(name)))
      }
    }
  }

  versionlessInt96Fixtures.foreach { name =>
    test(s"$name is read unrebased under EXCEPTION read mode") {
      // The guard's one blind spot, and a deliberate trade rather than an oversight. Spark decides
      // per decoded value, so it raises for these; Comet decides per column, and for a file that
      // names no writer it refuses only what statistics expose. INT96 has none.
      //
      // Assuming the worst instead would refuse every INT96 column in every file no Spark wrote --
      // which is how Hive writes TIMESTAMP, and Hive, Impala, Trino and plain parquet-mr all leave
      // the version key unset. Those reads are overwhelmingly of modern values that Spark returns
      // without complaint, so refusing them all to catch this fixture is the worse trade. Closing
      // the gap properly needs a per-value check in the decoder, or the rebasing itself (#5010).
      withReadMode("EXCEPTION") {
        val df = spark.read.parquet(fixture(name))
        assertNativeScan(df)
        assert(df.collect().length == 8)
      }
    }
  }

  versionlessFixtures.foreach { name =>
    test(s"$name honors a per-read datetimeRebaseMode option") {
      // Spark resolves the rebase mode through `ParquetOptions`, so `.option(...)` on the reader
      // overrides the session conf. Comet reads it from the same place; previously it looked at
      // the session conf only and ignored the option. Spotted by @peterxcli in #5048.
      withReadMode("EXCEPTION") {
        val df = spark.read
          .option("datetimeRebaseMode", "CORRECTED")
          .option("int96RebaseMode", "CORRECTED")
          .parquet(fixture(name))
        checkNativeScanAnswer(df)
      }
    }
  }

  versionlessFixtures.foreach { name =>
    test(s"$name matches Spark under CORRECTED read mode") {
      // CORRECTED asserts the values are already Proleptic Gregorian, so Spark reads them as-is
      // and no rebasing applies. Comet honors that and must agree with Spark exactly.
      withReadMode("CORRECTED") {
        checkNativeScanAnswer(spark.read.parquet(fixture(name)))
      }
    }
  }

  markedFixtures.foreach { name =>
    test(s"$name still raises under CORRECTED read mode") {
      // The footer records these files' provenance, so Spark ignores the read mode for them and
      // rebases. Comet cannot, so it keeps refusing regardless of the setting.
      withReadMode("CORRECTED") {
        assertRaisesOnRebase(spark.read.parquet(fixture(name)))
      }
    }
  }

  ancientFixtures.foreach { name =>
    test(s"$name is readable with exceptionOnDatetimeRebase disabled") {
      // The opt-out has to keep working for every encoding, not just the ones the guard was
      // easiest to write for.
      withSQLConf(CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.key -> "false") {
        withReadMode("EXCEPTION") {
          val df = spark.read.parquet(fixture(name))
          assertNativeScan(df)
          assert(df.collect().length == 8)
        }
      }
    }
  }
}
