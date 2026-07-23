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

package org.apache.comet.rules

import java.io.File
import java.nio.file.Files
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, SaveMode}
import org.apache.spark.sql.comet.CometScanExec
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}

import org.apache.comet.CometConf
import org.apache.comet.hadoop.fs.{FakeHDFSFileSystem, FakeHdfsSchemeFileSystem}

/**
 * Comet's native readers go through object_store, which only understands a fixed set of URL
 * schemes. A custom Hadoop FileSystem scheme that object_store can't parse (here `fake://`) must
 * NOT be claimed by the native scan -- it would fail at execution with "Unable to recognise URL".
 * `CometScanRule` must decline it so Spark's Hadoop-FS-aware reader handles the scan.
 *
 * Unlike `ParquetReadFromFakeHadoopFsSuite`, this suite does NOT route the `fake` scheme through
 * libhdfs (`spark.hadoop.fs.comet.libhdfs.schemes`), so it exercises the decline path. The test
 * applies the rule directly to the physical plan and asserts fallback -- no query execution, so
 * it doesn't depend on the native reader actually attempting (and failing on) the scheme.
 */
class CometScanSchemeFallbackSuite extends CometTestBase {

  private var fakeRootDir: File = _

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.hadoop.fs.fake.impl", "org.apache.comet.hadoop.fs.FakeHDFSFileSystem")
    // Back the `hdfs` scheme with a local FS so we can exercise an `hdfs://` path without a live
    // cluster. `hdfs` is natively readable by default, so this scan must be CLAIMED, not declined.
    conf.set("spark.hadoop.fs.hdfs.impl", "org.apache.comet.hadoop.fs.FakeHdfsSchemeFileSystem")
    conf.set("spark.hadoop.fs.defaultFS", FakeHDFSFileSystem.PREFIX)
    // Intentionally NOT setting CometConf.COMET_LIBHDFS_SCHEMES -- `fake` is not natively readable,
    // and `hdfs` must still be claimed by default (mirrors the native `is_hdfs_scheme` default).
    conf
  }

  override def beforeAll(): Unit = {
    fakeRootDir = Files.createTempDirectory(s"comet_scheme_${UUID.randomUUID().toString}").toFile
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    if (fakeRootDir != null) FileUtils.deleteDirectory(fakeRootDir)
    super.afterAll()
  }

  test("native scan declines a filesystem scheme object_store can't read (fake://)") {
    val path = s"${FakeHDFSFileSystem.PREFIX}${fakeRootDir.getAbsolutePath}/data"
    spark.range(0, 10).toDF("id").write.format("parquet").mode(SaveMode.Overwrite).save(path)

    // Obtain a clean Spark physical plan (Comet disabled) with the FileSourceScanExec, then apply
    // CometScanRule directly. No execution -- we only check whether the rule claims the scan.
    // Capture via a var inside the block: `SQLTestUtils.withSQLConf` returns Unit on Spark 3.5
    // but a value on Spark 4.x, so we can't return the plan out of it cross-version.
    var sparkPlan: SparkPlan = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      sparkPlan = spark.read.parquet(path).queryExecution.executedPlan
    }

    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      val transformed = CometScanRule(spark).apply(stripAQEPlan(sparkPlan))

      val cometScans = transformed.collect { case s: CometScanExec => s }
      val sparkScans = transformed.collect { case s: FileSourceScanExec => s }
      assert(
        cometScans.isEmpty,
        "`fake://` is not object_store-readable; the native scan must fall back to Spark, " +
          s"but Comet claimed it:\n$transformed")
      assert(
        sparkScans.size == 1,
        s"expected the scan to remain a Spark FileSourceScanExec:\n$transformed")
    }
  }

  test("native scan claims hdfs:// when libhdfs.schemes is unset (native-default lockstep)") {
    // Native's `is_hdfs_scheme` treats `hdfs` as readable when `fs.comet.libhdfs.schemes` is unset,
    // and `create_hdfs_object_store` is in the default build. The JVM gate must agree: with the
    // config unset, an `hdfs://` scan must be CLAIMED by Comet, not declined to Spark. Guards the
    // `case None => Set("hdfs")` default in CometScanRule against the silent-fallback regression
    // Andy flagged in #4525.
    val path = s"${FakeHdfsSchemeFileSystem.PREFIX}${fakeRootDir.getAbsolutePath}/hdfs-data"
    spark.range(0, 10).toDF("id").write.format("parquet").mode(SaveMode.Overwrite).save(path)

    var sparkPlan: SparkPlan = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      sparkPlan = spark.read.parquet(path).queryExecution.executedPlan
    }

    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      val transformed = CometScanRule(spark).apply(stripAQEPlan(sparkPlan))

      val cometScans = transformed.collect { case s: CometScanExec => s }
      val sparkScans = transformed.collect { case s: FileSourceScanExec => s }
      assert(
        cometScans.size == 1,
        "`hdfs://` is natively readable by default; Comet must claim the scan, " +
          s"but it fell back to Spark:\n$transformed")
      assert(sparkScans.isEmpty, s"expected no leftover Spark FileSourceScanExec:\n$transformed")
    }
  }
}
