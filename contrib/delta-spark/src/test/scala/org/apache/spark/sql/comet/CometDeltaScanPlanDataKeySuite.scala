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

package org.apache.spark.sql.comet

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.UnionExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.contrib.delta.{CometDeltaTestBase, DeltaSparkScanEnvelope}
import org.apache.comet.serde.OperatorOuterClass

/**
 * Planning data keys of Delta scans built through the real conversion path. Lives in this package
 * because [[PlanDataInjector.findAllPlanData]] is `private[comet]`.
 */
class CometDeltaScanPlanDataKeySuite extends CometDeltaTestBase {

  private def deltaScan(df: DataFrame): CometDeltaNativeScanExec = {
    val scans = deltaNativeScans(df)
    assert(scans.size == 1, s"Expected one native Delta scan in:\n${df.queryExecution}")
    scans.head.asInstanceOf[CometDeltaNativeScanExec]
  }

  private def filePaths(perPartition: Array[Array[Byte]]): Seq[String] =
    perPartition.toSeq.flatMap { bytes =>
      OperatorOuterClass.DeltaSparkScan
        .parseFrom(bytes)
        .getFilePartition
        .getPartitionedFileList
        .asScala
        .map(_.getFile.getFilePath)
    }

  test("scans of one table with different partition filters keep their own planning data") {
    // Without AQE the executed plan is a plain tree, so the scans are taken straight from it.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        spark
          .range(0, 20)
          .selectExpr("id", "id % 2 as p")
          .write
          .format("delta")
          .partitionBy("p")
          .save(path)
        val table = spark.read.format("delta").load(path)

        val p0 = deltaScan(table.where("p = 0"))
        val p1 = deltaScan(table.where("p = 1"))
        // Same table, schema and projection with no data filters: the common halves match, so
        // only the plan id can tell the two scans apart.
        assert(
          DeltaSparkScanEnvelope.unpack(p0.nativeOp).getCommon ==
            DeltaSparkScanEnvelope.unpack(p1.nativeOp).getCommon)
        assert(p0.sourceKey != p1.sourceKey)
        Seq(p0, p1).foreach { scan =>
          assert(scan.sourceKey.endsWith(s"#${scan.nativeOp.getPlanId}"), scan.sourceKey)
        }

        // A parent holding both scans, such as a join fused into one native block.
        val (commonByKey, perPartitionByKey) =
          PlanDataInjector.findAllPlanData(UnionExec(Seq(p0, p1)))
        assert(commonByKey.keySet == Set(p0.sourceKey, p1.sourceKey))
        assert(perPartitionByKey.keySet == Set(p0.sourceKey, p1.sourceKey))
        val p0Files = filePaths(perPartitionByKey(p0.sourceKey))
        val p1Files = filePaths(perPartitionByKey(p1.sourceKey))
        assert(p0Files.nonEmpty && p0Files.forall(_.contains("p=0")), p0Files)
        assert(p1Files.nonEmpty && p1Files.forall(_.contains("p=1")), p1Files)

        // The executor reads back the key the driver stored, and rewrapping the same op keeps it.
        val injector = new DeltaPlanDataInjector
        Seq(p0, p1).foreach { scan =>
          assert(injector.getKey(scan.nativeOp).contains(scan.sourceKey))
          assert(
            CometDeltaNativeScanExec(scan.nativeOp, scan.originalPlan).sourceKey ==
              scan.sourceKey)
        }

        // A second plan of the same query is a distinct scan node with its own key, yet the key
        // stays out of canonicalization: equal-content scans still have the same result.
        val p0Again = deltaScan(table.where("p = 0"))
        assert(p0Again.sourceKey != p0.sourceKey)
        assert(p0Again.sameResult(p0))
        assert(!p1.sameResult(p0))
      }
    }
  }
}
