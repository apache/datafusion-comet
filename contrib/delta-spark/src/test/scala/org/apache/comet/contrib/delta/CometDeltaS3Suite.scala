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

package org.apache.comet.contrib.delta

import scala.util.{Failure, Success, Try}

import org.testcontainers.DockerClientFactory

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor

import org.apache.comet.CometS3TestBase

/**
 * MinIO-backed integration coverage for the multi-bucket Delta shapes behind review findings 2
 * and 4 (design doc "Shared MinIO test infra"): a real two-bucket shallow clone, which a
 * single-bucket `withTempPath` table can never produce, because it is `DeltaTable`'s CLONE
 * machinery -- not test fixturing -- that leaves some `AddFile` entries pointing at the source
 * table's absolute location while new files land under the clone's own root.
 *
 * Manual/opt-in, same as [[org.apache.comet.parquet.ParquetReadFromS3Suite]] in the spark module
 * -- but gated differently out of necessity. That suite is invisible to every PR workflow simply
 * because `.github/workflows/pr_build_linux.yml` / `pr_build_macos.yml` enumerate test classes by
 * name and never name it (`dev/ci/check-suites.py` exempts it via `ignore_list` instead of
 * requiring it be listed). The contrib module has no such allowlist: `delta_contrib_test.yml`
 * runs `mvn ... test -pl contrib/delta-spark`, which discovers and runs every suite on the
 * module's test classpath, and `check-suites.py` does not enforce anything under `contrib/` at
 * all (see its `path.parts[0] == "contrib"` skip), so there is no file to omit this suite from.
 * Every test therefore starts with `assume(dockerAvailable, ...)`: when no Docker daemon is
 * reachable, ScalaTest reports the test CANCELED rather than failed or run, which
 * `scalatest-maven-plugin` does not treat as a build failure -- the practical equivalent of
 * `ParquetReadFromS3Suite`'s blanket omission, reached by a runtime check instead of never being
 * named. `beforeAll` mirrors this: it probes Docker BEFORE calling `CometS3TestBase#beforeAll`,
 * because that trait's `sparkConf` dereferences `minioContainer` unconditionally, and starting
 * the Spark session (let alone a container) is exactly what a Docker-less run must not do.
 */
class CometDeltaS3Suite extends CometDeltaTestBase with CometS3TestBase with Logging {

  override protected val testBucketName = "comet-delta-a"

  /**
   * The clone's destination bucket: distinct from [[testBucketName]] on purpose -- these tests
   * exist to put a table's data (or its deletion vectors) across two object-store authorities.
   */
  private val cloneBucketName = "comet-delta-b"

  private var dockerAvailable = false

  override def beforeAll(): Unit = {
    dockerAvailable = DockerClientFactory.instance().isDockerAvailable
    if (dockerAvailable) {
      // Fail soft: this suite runs unconditionally in CI (no allowlist to omit it from, see the
      // class doc above), and testcontainers networking inside a CI job container is unverified
      // -- MinIO is a sibling container there, so `getS3URL` may resolve to an address that is
      // wrong from inside the job container. If startup or bucket creation blows up, log the
      // resolved URL (the signal needed to diagnose a first bad CI run), flip `dockerAvailable`
      // back off so every test cancels via `assume` instead of aborting the whole suite, and
      // best-effort stop whatever container did come up.
      Try {
        super.beforeAll() // CometS3TestBase starts MinIO, then CometTestBase starts the session.
        createBucketIfNotExists(cloneBucketName)
      } match {
        case Success(_) =>
          logInfo(s"CometDeltaS3Suite: MinIO reachable at ${minioContainer.getS3URL}")
        case Failure(e) =>
          val resolvedUrl = Try(minioContainer.getS3URL).getOrElse("<unresolvable>")
          logWarning(
            s"CometDeltaS3Suite: MinIO setup failed (resolved S3 URL: $resolvedUrl); " +
              "skipping all tests in this suite",
            e)
          dockerAvailable = false
          // Tear down here, synchronously: super.beforeAll() may have partially succeeded
          // (e.g. the Spark session started but createBucketIfNotExists(cloneBucketName)
          // failed), and this suite's own afterAll() below is gated on `dockerAvailable`,
          // which is now false -- the framework-invoked afterAll() will no-op and never get a
          // chance to stop anything. super.afterAll() stops both the Spark session
          // (CometTestBase#afterAll, tolerates a session that never started) and MinIO
          // (CometS3TestBase#afterAll, tolerates a container that never started), so this is
          // safe to call unconditionally here regardless of how far beforeAll got.
          Try(super.afterAll())
      }
    }
  }

  override def afterAll(): Unit = {
    if (dockerAvailable) {
      super.afterAll()
    }
  }

  // CometTestBase#afterEach unconditionally touches `spark` (cache-clearing, open-stream
  // assertions); with no session ever created in a Docker-less run, that NPEs and aborts the
  // whole suite -- turning a clean per-test cancellation into a module-wide build failure.
  override def afterEach(): Unit = {
    if (dockerAvailable) {
      super.afterEach()
    }
  }

  private def tablePath(bucket: String, relPath: String): String = s"s3a://$bucket/$relPath"

  test("shallow clone across buckets + append declines with the multi-store reason") {
    assume(dockerAvailable, "Docker is not available; skipping MinIO-backed Delta test")

    val sourcePath = tablePath(testBucketName, "clone-append/source")
    val clonePath = tablePath(cloneBucketName, "clone-append/clone")

    spark.range(0, 100).selectExpr("id", "id * 2 as v").write.format("delta").save(sourcePath)
    spark.sql(s"CREATE TABLE delta.`$clonePath` SHALLOW CLONE delta.`$sourcePath`")
    // The clone's own transaction log still references the SOURCE's physical files (bucket A)
    // for every row carried over by the clone. This append writes NEW physical files under the
    // clone's own root (bucket B): the clone's data files now span two object-store
    // authorities -- exactly the shape finding 2's decline gate exists for, since the shared
    // native scan builder resolves the whole scan's ObjectStoreUrl from the first selected file
    // only.
    spark
      .range(100, 150)
      .selectExpr("id", "id * 2 as v")
      .write
      .format("delta")
      .mode("append")
      .save(clonePath)

    val df = spark.read.format("delta").load(clonePath)
    checkSparkAnswerAndFallbackReason(
      df,
      "Native Delta scan does not support data files spanning multiple object stores")
  }

  test(
    "clone across buckets + DELETE on the clone reads correct rows natively " +
      "(finding 4: cold cross-bucket deletion-vector store)") {
    assume(dockerAvailable, "Docker is not available; skipping MinIO-backed Delta test")

    val sourcePath = tablePath(testBucketName, "clone-delete/source")
    val clonePath = tablePath(cloneBucketName, "clone-delete/clone")

    spark.range(0, 1000).selectExpr("id", "id * 2 as v").write.format("delta").save(sourcePath)
    spark.sql(
      s"ALTER TABLE delta.`$sourcePath` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    spark.sql(s"CREATE TABLE delta.`$clonePath` SHALLOW CLONE delta.`$sourcePath`")
    // DELETE against a deletion-vector table does not rewrite the target file; it attaches a
    // deletion-vector sidecar to the existing `AddFile` action instead. The sidecar is written
    // under the CLONE's own root (bucket B), while the `AddFile` it decorates still points at
    // the SOURCE's absolute, un-copied physical file (bucket A) -- shallow clone never
    // relocates data it did not modify. That is the bug shape finding 4 fixes: attaching the
    // access plan nested a `Handle::block_on` call that built the (previously untouched, so
    // cold) bucket-B object store for the sidecar from inside an already-running Tokio runtime,
    // which panics.
    //
    // It is also, deliberately, NOT the shape finding 2's multi-store gate catches: that gate
    // inspects only DATA-file authorities (`scanHelper.selectedPartitions...map(_.getPath)`),
    // and every data file this scan selects is still on bucket A -- only the deletion vector's
    // own authority is bucket B. Comment kept here, not just in the design doc, because it is
    // the one property that makes this test exercise finding 4 instead of re-proving finding 2.
    spark.sql(s"DELETE FROM delta.`$clonePath` WHERE id % 2 = 0")

    // Assert the cross-bucket shape STRUCTURALLY, not just end-to-end via the read below: if
    // Delta's shallow-clone or DELETE-on-a-DV-table semantics ever change (DELETE starts
    // rewriting the file instead of writing a DV, or the DV sidecar starts landing next to the
    // data it decorates instead of under the clone's own root), the test must fail loudly right
    // here -- otherwise it would silently degrade into a same-bucket read that never exercises
    // finding 4's cold-store code path at all, while `checkDeltaNativeScanAnswer` below would
    // still pass.
    val log = DeltaLog.forTable(spark, clonePath)
    val cloneTableRootPath = new Path(clonePath)
    val files = log.update().allFiles.collect()

    // At least one data file must still resolve into the SOURCE bucket: shallow clone never
    // copies files it did not modify.
    val dataAuthorities = files.map(_.absolutePath(log).toUri.getHost).distinct
    assert(
      dataAuthorities.contains(testBucketName),
      "expected at least one data file to still resolve into the SOURCE bucket " +
        s"($testBucketName, carried over unmodified by the shallow clone); resolved data-file " +
        s"authorities: ${dataAuthorities.mkString(", ")}")

    // At least one deletion-vector descriptor must resolve into the CLONE's own bucket.
    // Resolution mirrors DeltaScanSupport.selectedDvDescriptors (copyWithAbsolutePath against
    // the table root) followed by CometDeltaNativeScan.storeUris's own absolutePath call --
    // the exact path production code takes from AddFile to an object-store authority. Inline
    // or canonically-empty descriptors are excluded first (`cardinality == 0` mirrors finding
    // 7's EMPTY characterization): DeletionVectorDescriptor#absolutePath's isOnDisk precondition
    // throws for inline ones, and neither carries a resolvable external authority.
    val dvAuthorities = files
      .flatMap(f => Option(f.deletionVector))
      .filter(dv =>
        dv.storageType != DeletionVectorDescriptor.INLINE_DV_MARKER && dv.cardinality > 0)
      .map(
        _.copyWithAbsolutePath(cloneTableRootPath).absolutePath(cloneTableRootPath).toUri.getHost)
      .distinct
    assert(
      dvAuthorities.contains(cloneBucketName),
      "expected at least one deletion-vector sidecar to resolve into the CLONE's own bucket " +
        s"($cloneBucketName); resolved deletion-vector authorities: " +
        s"${dvAuthorities.mkString(", ")}")

    val df = spark.read.format("delta").load(clonePath)
    checkDeltaNativeScanAnswer(df)
    assert(df.count() == 500)
  }
}
