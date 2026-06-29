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

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.serde.OperatorOuterClass

/**
 * Guards that a native Delta CDF read (`readChangeFeed`) propagates object-store credentials into
 * its scan proto (credential audit P0). The `s3a` scheme is backed by a local FS
 * ([[FakeS3aSchemeFileSystem]], registered at context creation via [[sparkConf]] -- the runtime
 * `fs.s3a.impl` override doesn't take, the real S3AFileSystem loads instead), so an `s3a://` table
 * root makes `NativeConfig.extractObjectStoreOptions` bridge the `fs.s3a.*` keys without a live S3.
 */
class CometDeltaCdfCredentialSuite extends CometDeltaTestBase {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.comet.contrib.delta.FakeS3aSchemeFileSystem")
    conf.set("spark.hadoop.fs.s3a.access.key", "TESTAK")
    conf.set("spark.hadoop.fs.s3a.secret.key", "TESTSK")
    conf.set("spark.hadoop.fs.s3a.endpoint.region", "us-west-2")
    conf
  }

  test("CDF read ships bridged S3 credentials in the scan proto (credential audit P0)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    // Inspect the planned exec directly (AQE off so executedPlan is final without collect(); we
    // never execute, so there is no real S3 contact).
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withDeltaTable("cdf_s3a_creds") { localPath =>
        // RawLocalFileSystem ignores the authority and maps the URI path to a local path, so this
        // s3a:// address resolves to the local temp dir `localPath`.
        val s3aPath = s"${FakeS3aSchemeFileSystem.PREFIX}$localPath"
        spark.sql(
          s"""CREATE OR REPLACE TABLE delta.`$s3aPath`(id INT, v STRING)
              USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
        spark.sql(s"INSERT INTO delta.`$s3aPath` VALUES (1, 'a'), (2, 'b')")
        spark.sql(s"UPDATE delta.`$s3aPath` SET v = 'A' WHERE id = 1")

        val df = spark.read
          .format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", "0")
          .load(s3aPath)
        val cdfScans = collect(df.queryExecution.executedPlan) {
          case s: org.apache.spark.sql.comet.CometDeltaCdfScanExec => s
        }
        assert(
          cdfScans.nonEmpty,
          s"expected a native CometDeltaCdfScanExec for the s3a CDF read:\n" +
            s"${df.queryExecution.executedPlan}")
        // RED before P0: convertCdf shipped an empty object_store_options map. GREEN after: the
        // bridged fs.s3a.* credentials are in the CDF scan's common proto, so a CDF read of a
        // private bucket authenticates the same way the V1 scan does.
        val common = OperatorOuterClass.DeltaScanCommon.parseFrom(cdfScans.head.commonData)
        val opts = common.getObjectStoreOptionsMap
        assert(
          opts.get("fs.s3a.access.key") == "TESTAK",
          s"CDF proto missing bridged S3 access key; object_store_options=$opts")
        assert(
          opts.get("fs.s3a.secret.key") == "TESTSK",
          s"CDF proto missing bridged S3 secret key; object_store_options=$opts")
      }
    }
  }
}
