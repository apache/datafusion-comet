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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, input_file_name}

/**
 * Coverage for the contrib-delta CLAIM/DECLINE path: `DeltaScanRule` plants a `CometDeltaScanMarker`,
 * which -- now that the serde (`CometDeltaNativeScan`) is present in this unit -- `CometExecRule`
 * CONVERTS into a `CometDeltaNativeScanExec` (a real native read). So a CLAIMED scan is observable as
 * a `CometDeltaNativeScanExec` in the plan, and a DECLINED scan falls back to vanilla Spark (no native
 * scan). (Before the serde landed, a claimed scan left the marker in the plan executing as a vanilla
 * fallback; that earlier-unit behaviour is what changed here.)
 */
class CometDeltaMarkerSuite extends CometDeltaTestBase {

  test("DeltaScanRule claims a plain Delta read and it engages the native scan") {
    assume(deltaSparkAvailable, "io.delta.spark not on the test classpath")
    withDeltaTable("claim-native") { tablePath =>
      spark.range(0, 100).toDF("id").write.format("delta").save(tablePath)
      // The rule claims the scan (plants the marker); with the serde present, CometExecRule converts
      // the marker to a CometDeltaNativeScanExec -- so the engaged-native check is the claim signal.
      assertKernelReadEngaged(tablePath)
    }
  }

  test("a filtered/projected claimed read goes native and matches vanilla Spark") {
    assume(deltaSparkAvailable, "io.delta.spark not on the test classpath")
    withDeltaTable("claim-native-filtered") { tablePath =>
      spark.range(0, 100).selectExpr("id", "id * 2 as v").write.format("delta").save(tablePath)
      // Asserts the read engages `CometDeltaNativeScanExec` AND results match vanilla -- catches a
      // claim-path regression (no native scan) and a correctness regression in one shot.
      assertDeltaNativeMatches(tablePath, (df: DataFrame) => df.filter("id > 10").select("id", "v"))
    }
  }

  test("DeltaScanRule declines an input_file_name() projection (falls back to vanilla, no native scan)") {
    assume(deltaSparkAvailable, "io.delta.spark not on the test classpath")
    withDeltaTable("decline-input-file-name") { tablePath =>
      spark.range(0, 50).toDF("id").write.format("delta").save(tablePath)
      // `input_file_name()` forces a fall back to vanilla (per-file provenance the native scan can't
      // surface), so the rule declines, plants no marker, and no CometDeltaNativeScanExec appears.
      val query = (df: DataFrame) => df.select(col("id"), input_file_name())
      assertDeltaFallback(tablePath, query)
      assertNoMarker(query(spark.read.format("delta").load(tablePath)))
      assert(
        query(spark.read.format("delta").load(tablePath)).count() == 50L,
        "declined read must still return all rows via vanilla Spark")
    }
  }
}
