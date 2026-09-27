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

import java.io.File
import java.nio.file.Files
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame, SaveMode}
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, sum}

import org.apache.comet.CometConf
import org.apache.comet.hadoop.fs.FakeHDFSFileSystem

/**
 * End-to-end coverage of a native Parquet scan routed through libhdfs, using a fake Hadoop
 * FileSystem instead of a live namenode.
 *
 * '''This suite is excluded from CI and must be run manually''' (it is in the ignore list in
 * `dev/ci/check-suites.py`, so it is not listed in either `pr_build_*.yml` workflow):
 *
 * {{{
 * ./mvnw test -Dtest=none \
 *   -Dsuites="org.apache.comet.parquet.ParquetReadFromFakeHadoopFsSuite"
 * }}}
 *
 * It is the only suite that actually loads libhdfs, and libhdfs registers a pthread thread-local
 * destructor (`hdfsThreadDestructor`) that detaches the current thread from the JVM regardless of
 * who attached it. Comet attaches its own Tokio workers, so once one of them has touched libhdfs
 * the destructor dereferences a `JNIEnv` that Comet has already freed and the JVM dies with
 * `SIGSEGV at pc=0x0`. That is [[https://issues.apache.org/jira/browse/HDFS-16021 HDFS-16021]],
 * still open upstream. The crash lands on whichever suite happens to be running when the worker
 * exits -- usually minutes later, in a different suite -- so it reads as a random `[scans]` flake
 * rather than an HDFS failure. See
 * [[https://github.com/apache/datafusion-comet/issues/5023 #5023]].
 *
 * Comet's HDFS support is experimental (see the
 * [[https://datafusion.apache.org/comet/user-guide/latest/datasources.html#hdfs data sources guide]]),
 * and working around the upstream bug would mean carrying a patched copy of libhdfs in this repo,
 * so we run this suite by hand instead of paying for the flake on every pull request. The
 * planner-side half of the coverage -- that a `hdfs://` scan is still claimed natively rather
 * than silently falling back -- does run in CI, in `CometScanSchemeFallbackSuite`, because it
 * never executes the scan and so never loads libhdfs.
 */
class ParquetReadFromFakeHadoopFsSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private var fake_root_dir: File = _

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.hadoop.fs.fake.impl", "org.apache.comet.hadoop.fs.FakeHDFSFileSystem")
    conf.set("spark.hadoop.fs.defaultFS", FakeHDFSFileSystem.PREFIX)
    conf.set(CometConf.COMET_LIBHDFS_SCHEMES.key, "fake,hdfs")
  }

  override def beforeAll(): Unit = {
    // Initialize fake root dir
    fake_root_dir = Files.createTempDirectory(s"comet_fake_${UUID.randomUUID().toString}").toFile
    // Initialize Spark session
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    if (fake_root_dir != null) FileUtils.deleteDirectory(fake_root_dir)
    super.afterAll()
  }

  private def writeTestParquetFile(filePath: String): Unit = {
    val df = spark.range(0, 1000)
    df.write.format("parquet").mode(SaveMode.Overwrite).save(filePath)
  }

  private def assertCometNativeScanOnFakeFs(df: DataFrame): Unit = {
    val scans = collect(df.queryExecution.executedPlan) { case p: CometNativeScanExec =>
      p
    }
    assert(scans.size == 1)
    // File partitions are now accessed from the scan field, not from the protobuf
    val filePartitions = scans.head.scan.getFilePartitions()
    assert(filePartitions.nonEmpty)
    assert(
      filePartitions.head.files.head.filePath.toString
        .startsWith(FakeHDFSFileSystem.PREFIX))
  }

  test("native scan on fake fs") {
    // Skip test if HDFS feature is not enabled in native library
    assume(isFeatureEnabled("hdfs-opendal"))
    val testFilePath =
      s"${FakeHDFSFileSystem.PREFIX}${fake_root_dir.getAbsolutePath}/data/test-file.parquet"
    writeTestParquetFile(testFilePath)
    val df = spark.read.format("parquet").load(testFilePath).agg(sum(col("id")))
    assertCometNativeScanOnFakeFs(df)
    assert(df.first().getLong(0) == 499500)
  }
}
