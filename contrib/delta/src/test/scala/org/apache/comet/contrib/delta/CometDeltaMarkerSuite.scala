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
 * Coverage for the contrib-delta CLAIM/DECLINE layer (`DeltaScanRule` + `CometDeltaScanMarker`)
 * that this unit introduces, independent of the native read path (the serde/exec land later).
 *
 * On this build there is no `CometDeltaNativeScan` serde, so `CometExecRule`'s `scanHandler`
 * lookup returns `None` and a planted `CometDeltaScanMarker` is left in the plan executing as a
 * vanilla Delta fallback. That makes the marker's PRESENCE the observable signal that the rule
 * claimed the scan, and its absence the signal that the rule declined -- exactly what these tests
 * assert. The native-read assertions live with the serde/exec unit.
 */
class CometDeltaMarkerSuite extends CometDeltaTestBase {

  test("DeltaScanRule plants the marker on a plain Delta read (claim path active)") {
    assume(deltaSparkAvailable, "io.delta.spark not on the test classpath")
    withDeltaTable("marker-planted") { tablePath =>
      spark.range(0, 100).toDF("id").write.format("delta").save(tablePath)
      val df = spark.read.format("delta").load(tablePath)
      // Red-green vs the A.2 build: with `DeltaScanRule$` absent (A.2 bridge only) no marker is
      // planted; this unit supplies the rule, so the marker appears (then falls back to vanilla).
      assertMarkerPlanted(df)
    }
  }

  test("marker is planted on a filtered/projected read and the fallback stays result-correct") {
    assume(deltaSparkAvailable, "io.delta.spark not on the test classpath")
    withDeltaTable("marker-fallback-correct") { tablePath =>
      spark.range(0, 100).selectExpr("id", "id * 2 as v").write.format("delta").save(tablePath)
      val query = (df: DataFrame) => df.filter("id > 10").select("id", "v")
      // Assert the rule actually CLAIMS this query shape (catches a claim-path regression, not just
      // a result mismatch -- a disengaged claim path would still match rows since both sides run
      // vanilla), AND that the marker's vanilla fallback returns identical rows.
      assertMarkerPlanted(query(spark.read.format("delta").load(tablePath)))
      assertResultsMatchVanilla(tablePath, query)
    }
  }

  test("DeltaScanRule declines an input_file_name() projection (no marker, vanilla read)") {
    assume(deltaSparkAvailable, "io.delta.spark not on the test classpath")
    withDeltaTable("decline-input-file-name") { tablePath =>
      spark.range(0, 50).toDF("id").write.format("delta").save(tablePath)
      // `input_file_name()` forces a fall back to vanilla (per-file provenance the native scan
      // can't surface), so the rule declines and plants no marker.
      val df = spark.read.format("delta").load(tablePath).select(col("id"), input_file_name())
      assertNoMarker(df)
      assert(df.count() == 50L, "declined read must still return all rows via vanilla Spark")
    }
  }
}
