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

package org.apache.comet.cloud.s3

import org.apache.spark.sql.CometTestBase

import org.apache.comet.NativeBase

/**
 * Builds native Iceberg `FileIO`s with [[TestCometS3CredentialProvider]] and checks that dropping
 * one releases its provider over JNI. Building a `FileIO` makes no storage request, so unlike
 * [[CometS3CredentialBridgeSuite]] this needs no S3 server.
 */
class CometS3CredentialReleaseSuite extends CometTestBase {

  private val table = "s3://release-bucket/warehouse/db/t/metadata"

  private def load(metadataFile: String, tag: String): Unit = {
    val properties = Seq(
      "s3.comet.credential.provider.class" -> classOf[TestCometS3CredentialProvider].getName,
      "test.instance-tag" -> tag)
    NativeBase.loadIcebergFileIOForTesting(
      s"$table/$metadataFile",
      "release_catalog",
      properties.map(_._1).toArray,
      properties.map(_._2).toArray)
  }

  test("a superseded or cleared Iceberg FileIO releases its provider over JNI") {
    assume(NativeBase.isLoaded, "Comet native library not loaded")
    NativeBase.clearIcebergFileIOCacheForTesting()
    TestCometS3CredentialProvider.reset()
    // Close on the releasing thread, so the close count is final when the native call returns.
    CometS3CredentialDispatcher.setCloserForTesting((r: Runnable) => r.run())
    try {
      val before = CometS3CredentialDispatcher.registeredCount()

      load("00001.metadata.json", "a")
      assert(CometS3CredentialDispatcher.registeredCount() == before + 1)
      assert(TestCometS3CredentialProvider.initCount.get() == 1)

      // A later commit of the same table with new properties evicts the first FileIO, and with
      // it the last reference to the first provider.
      load("00002.metadata.json", "b")
      assert(TestCometS3CredentialProvider.initCount.get() == 2)
      assert(CometS3CredentialDispatcher.registeredCount() == before + 1)
      assert(TestCometS3CredentialProvider.closeCount.get() == 1)

      NativeBase.clearIcebergFileIOCacheForTesting()
      assert(CometS3CredentialDispatcher.registeredCount() == before)
      assert(TestCometS3CredentialProvider.closeCount.get() == 2)
    } finally {
      CometS3CredentialDispatcher.setCloserForTesting(null)
      NativeBase.clearIcebergFileIOCacheForTesting()
    }
  }
}
