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

class NativeConfigSuite extends AnyFunSuite with Matchers {

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
}
