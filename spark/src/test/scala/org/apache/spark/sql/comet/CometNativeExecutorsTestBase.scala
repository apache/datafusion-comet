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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.internal.SQLConf

/** What the suites of Comet Native Executors share. */
trait CometNativeExecutorsTestBase extends CometTestBase {

  /** A Parquet table with several files, read as several partitions. */
  protected def withTable(body: String => Unit): Unit = withTempPath { dir =>
    spark
      .range(0, 10000, 1, 8)
      .selectExpr("id", "id % 7 AS k", "CAST(id AS DOUBLE) * 1.5 AS v")
      .write
      .parquet(dir.getCanonicalPath)
    withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> "16384") {
      body(dir.getCanonicalPath)
    }
  }
}
