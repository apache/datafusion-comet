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

package org.apache.comet

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.comet.iceberg.{CometCredentialProvider, MockCometCredentialProvider, ResolveContext}

class CometCredentialProviderSuite extends AnyFunSuite {

  private def ctx(loc: String): ResolveContext = new ResolveContext(loc)

  test("MockCometCredentialProvider initializes and resolves") {
    val provider = new MockCometCredentialProvider()
    val props = Map(
      "s3.access-key-id" -> "AKIA_TEST",
      "s3.secret-access-key" -> "secret_TEST",
      "s3.session-token" -> "token_TEST").asJava

    provider.initialize(props)

    val creds = provider.resolveCredentials(ctx("s3://my-bucket/db/my_table"))
    assert(creds.length == 4)
    assert(creds(0) == "AKIA_TEST")
    assert(creds(1) == "secret_TEST")
    assert(creds(2) == "token_TEST")
    assert(creds(3).toLong > System.currentTimeMillis())
  }

  test("resolveCredentials returns correct format") {
    val provider = new MockCometCredentialProvider()
    provider.initialize(Map.empty[String, String].asJava)

    val creds = provider.resolveCredentials(ctx("s3://bucket/table"))
    assert(creds.length == 4, "Expected [accessKeyId, secretAccessKey, sessionToken, expiresAt]")
  }

  test("resolveCredentials captures the table location passed by native via ResolveContext") {
    MockCometCredentialProvider.reset()
    val provider = new MockCometCredentialProvider()
    provider.initialize(Map.empty[String, String].asJava)

    provider.resolveCredentials(ctx("s3://bucket/db/table_a"))
    assert(MockCometCredentialProvider.getLastTableLocation == "s3://bucket/db/table_a")

    provider.resolveCredentials(ctx("s3://bucket/db/table_b"))
    assert(MockCometCredentialProvider.getLastTableLocation == "s3://bucket/db/table_b")

    assert(MockCometCredentialProvider.getResolveCount == 2)
  }

  test("ResolveContext normalizes a null tableLocation to empty string (never null)") {
    val c = new ResolveContext(null)
    assert(c.tableLocation == "")
  }
}
