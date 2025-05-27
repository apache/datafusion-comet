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

package org.apache.comet.objectstore

import java.net.URI

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.s3a.S3AFileSystem

class NativeConfigSuite extends AnyFunSuite with Matchers {

  // Mock S3AFileSystem for testing
  class MockS3AFileSystem(val regionToReturn: String) extends S3AFileSystem {
    override def getBucketLocation(): String = regionToReturn
  }

  test("extractObjectStoreOptions - multiple cloud provider configurations") {
    val hadoopConf = new Configuration()
    // S3A configs
    hadoopConf.set("fs.s3a.access.key", "s3-access-key")
    hadoopConf.set("fs.s3a.secret.key", "s3-secret-key")
    hadoopConf.set("fs.s3a.endpoint.region", "us-east-1")
    hadoopConf.set("fs.s3a.bucket.special-bucket.access.key", "special-access-key")
    hadoopConf.set("fs.s3a.bucket.special-bucket.endpoint.region", "eu-central-1")

    // GCS configs
    hadoopConf.set("fs.gs.project.id", "gcp-project")

    // Azure configs
    hadoopConf.set("fs.azure.account.key.testaccount.blob.core.windows.net", "azure-key")

    // Should extract s3 options
    Seq("s3a://test-bucket/test-object", "s3://test-bucket/test-object").foreach { path =>
      val options = NativeConfig.extractObjectStoreOptions(hadoopConf, new URI(path))
      assert(options("fs.s3a.access.key") == "s3-access-key")
      assert(options("fs.s3a.secret.key") == "s3-secret-key")
      assert(options("fs.s3a.endpoint.region") == "us-east-1")
      assert(options("fs.s3a.bucket.special-bucket.access.key") == "special-access-key")
      assert(options("fs.s3a.bucket.special-bucket.endpoint.region") == "eu-central-1")
      assert(!options.contains("fs.gs.project.id"))
    }
    val gsOptions =
      NativeConfig.extractObjectStoreOptions(hadoopConf, new URI("gs://test-bucket/test-object"))
    assert(gsOptions("fs.gs.project.id") == "gcp-project")
    assert(!gsOptions.contains("fs.s3a.access.key"))

    val azureOptions = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("wasb://test-bucket/test-object"))
    assert(azureOptions("fs.azure.account.key.testaccount.blob.core.windows.net") == "azure-key")
    assert(!azureOptions.contains("fs.s3a.access.key"))

    // Unsupported scheme should return empty options
    val unsupportedOptions = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("unsupported://test-bucket/test-object"))
    assert(unsupportedOptions.isEmpty, "Unsupported scheme should return empty options")
  }

  test("extractObjectStoreOptions - S3 region auto-detection with mocked filesystem") {
    val testCases = Seq(
      ("us-west-2", "Should extract valid region"),
      ("eu-central-1", "Should extract European region"),
      ("ap-southeast-1", "Should extract Asia Pacific region"))

    testCases.foreach { case (expectedRegion, description) =>
      val hadoopConf = new Configuration()
      hadoopConf.set("fs.s3a.access.key", "test-key")
      hadoopConf.set("fs.s3a.secret.key", "test-secret")

      // Mock the FileSystem creation to return our mock
      val mockFs = new MockS3AFileSystem(expectedRegion)

      val uri = new URI("s3a://test-bucket/test-object")
      val options =
        NativeConfig.extractObjectStoreOptions(hadoopConf, uri, Some(mockFs))

      // Verify that region was auto-detected and added
      assert(
        options.contains("fs.s3a.bucket.test-bucket.endpoint.region"),
        s"$description - bucket-specific region should be added")
      assert(
        options("fs.s3a.bucket.test-bucket.endpoint.region") == expectedRegion,
        s"$description - region should match expected value")

      // Verify other configurations are present
      assert(options("fs.s3a.access.key") == "test-key")
      assert(options("fs.s3a.secret.key") == "test-secret")
    }
  }

  test("extractObjectStoreOptions - S3 region not added when already configured globally") {
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.s3a.access.key", "test-key")
    hadoopConf.set("fs.s3a.secret.key", "test-secret")
    hadoopConf.set("fs.s3a.endpoint.region", "us-east-1") // Global region config

    val mockFs = new MockS3AFileSystem("us-west-2") // Different region
    val uri = new URI("s3a://test-bucket/test-object")
    val options =
      NativeConfig.extractObjectStoreOptions(hadoopConf, uri, Some(mockFs))

    // Should not attempt to auto-detect region when global region is already set
    assert(options("fs.s3a.endpoint.region") == "us-east-1")
    assert(
      !options.contains("fs.s3a.bucket.test-bucket.endpoint.region"),
      "Should not add bucket-specific region when global region exists")
  }

  test("extractObjectStoreOptions - S3 region not added when bucket-specific region configured") {
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.s3a.access.key", "test-key")
    hadoopConf.set("fs.s3a.secret.key", "test-secret")
    hadoopConf.set(
      "fs.s3a.bucket.test-bucket.endpoint.region",
      "eu-west-1"
    ) // Bucket-specific region

    val mockFs = new MockS3AFileSystem("ap-southeast-1") // Different region
    val uri = new URI("s3a://test-bucket/test-object")
    val options =
      NativeConfig.extractObjectStoreOptions(hadoopConf, uri, Some(mockFs))

    // Should not attempt to auto-detect region when bucket-specific region is already set
    assert(
      options("fs.s3a.bucket.test-bucket.endpoint.region") == "eu-west-1",
      "Should keep existing bucket-specific region")
  }

  test("extractObjectStoreOptions - bucket-specific region should override global region") {
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.s3a.access.key", "test-key")
    hadoopConf.set("fs.s3a.secret.key", "test-secret")
    hadoopConf.set("fs.s3a.endpoint.region", "eu-west-1") // Bucket-specific region
    hadoopConf.set(
      "fs.s3a.bucket.test-bucket.endpoint.region",
      "us-west-2"
    ) // Bucket-specific region

    val mockFs = new MockS3AFileSystem("ap-southeast-1") // Different region
    val uri = new URI("s3a://test-bucket/test-object")
    val options =
      NativeConfig.extractObjectStoreOptions(hadoopConf, uri, Some(mockFs))

    // Should not attempt to auto-detect region when bucket-specific region is already set
    assert(
      options("fs.s3a.bucket.test-bucket.endpoint.region") == "us-west-2",
      "Should use bucket-specific region rather than global region")
  }
}
