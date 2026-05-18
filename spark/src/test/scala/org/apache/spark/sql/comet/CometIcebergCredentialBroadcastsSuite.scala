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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.concurrent.{Callable, Executors, TimeUnit}

import scala.jdk.CollectionConverters._

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.comet.iceberg.{CometCredentialProvider, MockCometCredentialProvider, ResolveContext}

class CometIcebergCredentialBroadcastsSuite extends AnyFunSuite with BeforeAndAfterEach {

  private var sc: SparkContext = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    val conf = new SparkConf()
      .setAppName("CometIcebergCredentialBroadcastsSuite")
      .setMaster("local[2]")
    sc = new SparkContext(conf)
    CometIcebergCredentialBroadcasts.clear()
    MockCometCredentialProvider.reset()
  }

  override def afterEach(): Unit = {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    CometIcebergCredentialBroadcasts.clear()
    super.afterEach()
  }

  private val mockClass = classOf[MockCometCredentialProvider].getName

  test("getOrCreate builds a broadcast and initializes the provider once") {
    val props = Map("tenant-id" -> "T1", "s3.access-key-id" -> "AKIA_T1")

    val bcast = CometIcebergCredentialBroadcasts.getOrCreate(sc, mockClass, "cat_a", props)

    assert(bcast != null)
    assert(MockCometCredentialProvider.getInitCount == 1)
    assert(CometIcebergCredentialBroadcasts.cacheSize() == 1)

    val provider = bcast.value.asInstanceOf[CometCredentialProvider]
    assert(provider != null)
    val creds = provider.resolveCredentials(new ResolveContext("s3://bucket/db/t"))
    assert(creds(0) == "AKIA_T1")
  }

  test("getOrCreate reuses the same broadcast for identical catalogName") {
    val props = Map("tenant-id" -> "T1", "s3.access-key-id" -> "AKIA_T1")

    val bcast1 = CometIcebergCredentialBroadcasts.getOrCreate(sc, mockClass, "cat_a", props)
    val bcast2 = CometIcebergCredentialBroadcasts.getOrCreate(sc, mockClass, "cat_a", props)

    assert(bcast1.id == bcast2.id, "Same catalog name must yield the same broadcast")
    assert(MockCometCredentialProvider.getInitCount == 1, "initialize() called only once")
    assert(CometIcebergCredentialBroadcasts.cacheSize() == 1)
  }

  test("getOrCreate returns DIFFERENT broadcasts for different catalogNames") {
    val props = Map("tenant-id" -> "T_shared")

    val bcastA = CometIcebergCredentialBroadcasts.getOrCreate(sc, mockClass, "cat_a", props)
    val bcastB = CometIcebergCredentialBroadcasts.getOrCreate(sc, mockClass, "cat_b", props)

    assert(bcastA.id != bcastB.id, "Different catalog names must yield different broadcasts")
    assert(MockCometCredentialProvider.getInitCount == 2)
    assert(CometIcebergCredentialBroadcasts.cacheSize() == 2)
  }

  test(
    "getOrCreate REUSES broadcast when per-table FileIO props differ but catalogName matches") {
    val tableA = Map("tenant-id" -> "T1", "table-refresh-id" -> "REFRESH_A")
    val tableB = Map("tenant-id" -> "T1", "table-refresh-id" -> "REFRESH_B")

    val bcastA = CometIcebergCredentialBroadcasts.getOrCreate(sc, mockClass, "rest_cat", tableA)
    val bcastB = CometIcebergCredentialBroadcasts.getOrCreate(sc, mockClass, "rest_cat", tableB)

    assert(
      bcastA.id == bcastB.id,
      "Per-table FileIO drift must not split the catalog-scoped cache")
    assert(MockCometCredentialProvider.getInitCount == 1, "initialize() must only run once")
    assert(CometIcebergCredentialBroadcasts.cacheSize() == 1)
  }
}
