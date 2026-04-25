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

import scala.util.Try

import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.schema.MessageTypeParser
import org.apache.spark.SparkException
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Documents Comet's behavior for the Parquet read-schema/file-schema mismatch cases tracked in
 * https://github.com/apache/datafusion-comet/issues/3720.
 *
 * Each test exercises one case under one of the two Comet scan implementations
 * (`native_datafusion`, `native_iceberg_compat`). Assertions encode Comet's actual current
 * behavior. Spark's reference behavior is recorded in the per-case comments and in the matrix
 * below; assertions do not run Spark in isolation.
 *
 * Behavior matrix (Spark reference behavior; Comet behavior is asserted by each test). "OK" =
 * read succeeds. "throw" = SparkException at runtime.
 *
 * Case 3.4 3.5 4.0
 *   1. BINARY -> TIMESTAMP throw throw throw 2. INT32 -> INT64 throw throw OK (widening) 3. INT96
 *      LTZ -> TIMESTAMP_NTZ throw throw throw 4. Decimal(10,2) -> Decimal(5,0) throw throw throw
 *      5. INT32 -> INT64 with rowgroup filter throw throw OK 6. STRING -> INT throw throw throw
 *      7. TIMESTAMP_NTZ -> ARRAY<...> throw throw throw C1. INT8 -> INT32 OK OK OK C2. FLOAT ->
 *      DOUBLE OK OK OK
 *
 * If a Comet fix lands that aligns one of these cases with Spark, update the affected test(s) and
 * this matrix in the same PR.
 */
class ParquetSchemaMismatchSuite extends CometTestBase {
  import testImplicits._

  /**
   * Force a specific Comet scan implementation, force V1 datasource (both native_datafusion and
   * native_iceberg_compat are V1-only), then run the given block in a fresh temp directory. The
   * block writes Parquet under `path`, then reads it back with a mismatched schema.
   */
  private def withMismatchedSchema(scanImpl: String)(body: String => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_NATIVE_SCAN_IMPL.key -> scanImpl,
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { dir =>
        body(dir.getCanonicalPath)
      }
    }
  }

  /** Both scan implementations under test, used as a `foreach` driver. */
  private val scanImpls: Seq[String] =
    Seq(CometConf.SCAN_NATIVE_DATAFUSION, CometConf.SCAN_NATIVE_ICEBERG_COMPAT)
}
