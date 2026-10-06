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

package org.apache.comet.serde.operator

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.iceberg.expressions.Expressions
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}

/**
 * Unit tests for [[CometIcebergNativeScan.hadoopToIcebergS3Properties]]. The pinned iceberg-rust
 * S3 parser reads ONLY global `s3.*` keys (never `s3.bucket.*`), so the function drops per-bucket
 * keys and promotes just the TARGET bucket's keys to global `s3.*`. Pure-function assertions, so
 * a lightweight `AnyFunSuite` (no Spark session) suffices.
 */
class CometIcebergNativeScanSuite extends AnyFunSuite with Matchers {

  test("complex type null residuals are not serialized") {
    // Container predicates stay in the post-scan filter, not the native residual pool.
    for (dataType <- Seq(
        ArrayType(IntegerType),
        MapType(StringType, IntegerType),
        new StructType().add("value", IntegerType));
      predicate <- Seq(Expressions.isNull("value"), Expressions.notNull("value"))) {
      withClue(s"$dataType: $predicate") {
        CometIcebergNativeScan
          .icebergExprToProto(predicate, Seq(AttributeReference("value", dataType)()), Set.empty)
          .isEmpty shouldBe true
      }
    }
    CometIcebergNativeScan
      .icebergExprToProto(
        Expressions.notNull("value"),
        Seq(AttributeReference("value", IntegerType)()),
        Set.empty)
      .nonEmpty shouldBe true
  }

  /** The scan output the residual tests below convert against. */
  private val intColumn = Seq(AttributeReference("value", IntegerType)())

  private def serialize(residual: Any) =
    CometIcebergNativeScan.serializeResidual(residual, intColumn, Set.empty)

  private def causes(t: Throwable): Iterator[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null)

  /**
   * Shaped like an Iceberg `UnboundPredicate` as far as the residual converter is concerned -- it
   * dispatches on the class-name suffix -- but its accessor throws, the way reflection would
   * against an Iceberg whose expression API has moved. It must stay a named class: a local or
   * anonymous one gets a runtime name that no longer ends in `UnboundPredicate`.
   */
  class ThrowingUnboundPredicate {
    def op(): AnyRef = throw new IllegalStateException("residual op() blew up")
  }

  /** Same shape, but the accessor is not declared at all. */
  class AccessorlessUnboundPredicate

  // Serde keeps its two ways of not pushing a residual apart. A reflection failure fails the
  // query: serde runs after CometScanRule has committed the scan to native execution, so there
  // is no fallback left. A residual the converter declines on purpose is left to the post-scan
  // filter. These go through serializeResidual, so a swallowing catch in either it or the
  // converter underneath turns them red.
  test("a residual whose reflection fails is fatal, not a silent drop") {
    val thrown = intercept[RuntimeException](serialize(new ThrowingUnboundPredicate))
    thrown.getMessage should include("Iceberg reflection failure")
    causes(thrown).exists(_.getMessage == "residual op() blew up") shouldBe true
  }

  test("a residual whose accessor is missing is fatal, not a silent drop") {
    val thrown = intercept[RuntimeException](serialize(new AccessorlessUnboundPredicate))
    causes(thrown).exists(_.isInstanceOf[NoSuchMethodException]) shouldBe true
  }

  test("residuals the converter declines are still dropped rather than raised") {
    val declined = Seq(
      // No native predicate models NOT_IN.
      Expressions.notIn("value", Integer.valueOf(1), Integer.valueOf(2)),
      // Not a node type the converter maps.
      Expressions.alwaysTrue(),
      Expressions.alwaysFalse(),
      // Iceberg spells a nested field as a dotted path, which is never a scan output attribute.
      Expressions.equal("outer.value", Integer.valueOf(1)),
      // A conjunct that does not convert elides the whole residual.
      Expressions.and(
        Expressions.equal("value", Integer.valueOf(1)),
        Expressions.notIn("value", Integer.valueOf(2))))
    for (expr <- declined) {
      withClue(s"$expr: ") {
        serialize(expr).isEmpty shouldBe true
      }
    }
    // iceberg-rust cannot use these columns in the page index; the post-scan filter has them.
    CometIcebergNativeScan
      .serializeResidual(Expressions.equal("value", Integer.valueOf(1)), intColumn, Set("value"))
      .isEmpty shouldBe true
    // The same predicate converts when nothing declines it, so the cases above are not vacuous.
    serialize(Expressions.equal("value", Integer.valueOf(1))).nonEmpty shouldBe true
  }

  test("a transform residual is declined rather than read as its source column") {
    // UnboundTransform answers ref() with the source column, so converting the term like a bare
    // reference would push bucket(4, value) = 1 as value = 1 and drop matching rows.
    // CometScanRule declines a non-identity transform during planning, but only when the
    // residual is a bare predicate, so a nested one has to be declined here. The end-to-end
    // wrong answer is pinned in CometIcebergResidualPushdownSuite.
    val bucketed =
      Expressions.equal(Expressions.bucket[Integer]("value", 4), Integer.valueOf(1))
    serialize(bucketed).isEmpty shouldBe true
    val nested = Expressions.and(bucketed, Expressions.equal("value", Integer.valueOf(1)))
    serialize(nested).isEmpty shouldBe true
  }

  private def translate(
      props: Map[String, String],
      targetBucket: Option[String]): Map[String, String] =
    CometIcebergNativeScan.hadoopToIcebergS3Properties(props, targetBucket)

  /** Every `fs.s3a.*` suffix the translator maps, paired with its global iceberg `s3.*` key. */
  private val suffixToIcebergKey = Seq(
    "access.key" -> "s3.access-key-id",
    "secret.key" -> "s3.secret-access-key",
    "session.token" -> "s3.session-token",
    "endpoint" -> "s3.endpoint",
    "path.style.access" -> "s3.path-style-access",
    "endpoint.region" -> "s3.region")

  test("full fs.s3a.* suffix mapping to global s3.* keys") {
    // Mappings are per-key independent (no cross-key interaction), so this table-driven case also
    // covers the "several global fs.s3a.* keys at once" scenario.
    suffixToIcebergKey.foreach { case (hadoopSuffix, icebergKey) =>
      val out = translate(Map(s"fs.s3a.$hadoopSuffix" -> "v"), None)
      out should contain(icebergKey -> "v")
      // The Hadoop key itself is never passed through untranslated.
      out.keys should not contain s"fs.s3a.$hadoopSuffix"
    }
  }

  test("target bucket per-bucket keys are promoted to global s3.*") {
    val props = suffixToIcebergKey.map { case (suffix, _) =>
      s"fs.s3a.bucket.target.$suffix" -> s"value-$suffix"
    }.toMap

    val out = translate(props, Some("target"))

    suffixToIcebergKey.foreach { case (suffix, icebergKey) =>
      out(icebergKey) shouldBe s"value-$suffix"
    }
    // Per-bucket keys are never emitted in s3.bucket.* form (the pinned parser ignores those).
    out.keys.foreach(k => k should not startWith "s3.bucket.")
  }

  test("target bucket keys coexist with a different non-target bucket") {
    val props = Map(
      "fs.s3a.bucket.target.endpoint" -> "https://target.example.com",
      "fs.s3a.bucket.target.access.key" -> "AKIA-target",
      "fs.s3a.bucket.other.endpoint" -> "https://other.example.com",
      "fs.s3a.bucket.other.access.key" -> "AKIA-other")

    val out = translate(props, Some("target"))

    out("s3.endpoint") shouldBe "https://target.example.com"
    out("s3.access-key-id") shouldBe "AKIA-target"
    // The non-target bucket contributes nothing: not promoted, not leaked into the global keys.
    out.values.toSet should not contain "https://other.example.com"
    out.values.toSet should not contain "AKIA-other"
    out.keys.size shouldBe 2
  }

  test("target bucket per-bucket value overrides a conflicting global value") {
    // targetBucketGlobals is merged last, so the per-bucket endpoint wins over the global one.
    val props = Map(
      "fs.s3a.endpoint" -> "https://global.example.com",
      "fs.s3a.access.key" -> "AKIA-global",
      "fs.s3a.bucket.target.endpoint" -> "https://target.example.com")

    val out = translate(props, Some("target"))

    out("s3.endpoint") shouldBe "https://target.example.com"
    // A global key with no per-bucket override survives.
    out("s3.access-key-id") shouldBe "AKIA-global"
  }

  test("dotted target bucket names survive (prefix match, not split)") {
    val props = Map(
      "fs.s3a.bucket.my.bucket.name.endpoint" -> "https://dotted.example.com",
      "fs.s3a.bucket.my.bucket.name.access.key" -> "AKIA-dotted",
      "fs.s3a.bucket.my.bucket.name.secret.key" -> "secret-dotted")

    val out = translate(props, Some("my.bucket.name"))

    out("s3.endpoint") shouldBe "https://dotted.example.com"
    out("s3.access-key-id") shouldBe "AKIA-dotted"
    out("s3.secret-access-key") shouldBe "secret-dotted"
  }

  test("keys already in iceberg s3.* form pass through; unrelated keys are ignored") {
    val props = Map(
      "s3.endpoint" -> "https://passthrough.example.com",
      "s3.access-key-id" -> "AKIA-passthrough",
      "fs.gs.project.id" -> "gcp-project",
      "fs.azure.account.key.acct.blob.core.windows.net" -> "azure-key",
      "spark.sql.shuffle.partitions" -> "200")

    translate(props, Some("target")) shouldBe Map(
      "s3.endpoint" -> "https://passthrough.example.com",
      "s3.access-key-id" -> "AKIA-passthrough")
  }

  test("no target bucket means no per-bucket promotion") {
    // Required-parameter contract: with None, per-bucket keys drop, only global keys survive.
    val props = Map(
      "fs.s3a.bucket.some.endpoint" -> "https://some.example.com",
      "fs.s3a.endpoint" -> "https://global.example.com")

    val out = translate(props, None)

    out("s3.endpoint") shouldBe "https://global.example.com"
    out.values.toSet should not contain "https://some.example.com"
  }

  // --- hadoopToIcebergHdfsProperties -------------------------------------------------------
  //
  // These pin the declarations that make `hdfs://<nameservice>/...` reachable: the pinned
  // iceberg-rust resolves a portless authority only through `hdfs.name-node.<authority>`; see
  // that method's scaladoc.

  private def hadoopConfOf(conf: Map[String, String]): org.apache.hadoop.conf.Configuration = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration(false)
    conf.foreach { case (k, v) => hadoopConf.set(k, v) }
    hadoopConf
  }

  private def hdfsProps(location: String, conf: Map[String, String]): Map[String, String] =
    CometIcebergNativeScan.hadoopToIcebergHdfsProperties(
      new java.net.URI(location),
      hadoopConfOf(conf))

  test("HA nameservice is declared with its NameNode list, in declaration order") {
    val out = hdfsProps(
      "hdfs://nameservice1/warehouse/db/t/metadata.json",
      Map(
        "dfs.ha.namenodes.nameservice1" -> "nn1,nn2",
        "dfs.namenode.rpc-address.nameservice1.nn1" -> "host-a.example.com:8020",
        "dfs.namenode.rpc-address.nameservice1.nn2" -> "host-b.example.com:8020"))

    // The per-nameservice key: iceberg-rust reads the global `hdfs.name-node` only for an
    // authority-less path, so emitting that one would leave the nameservice undeclared.
    out shouldBe Map(
      "hdfs.name-node.nameservice1" ->
        "hdfs://host-a.example.com:8020,hdfs://host-b.example.com:8020")
  }

  test("a plain host:port authority needs no declaration") {
    hdfsProps("hdfs://nn.example.com:8020/warehouse/db/t", Map.empty) shouldBe Map.empty
    hdfsProps("hdfs://[::1]:8020/warehouse/db/t", Map.empty) shouldBe Map.empty
  }

  test(
    "a portless plain host is declared on the default NameNode RPC port, as the JVM dials it") {
    // DFSUtilClient.getNNAddress applies 8020 to a portless `hdfs://host` URI; iceberg-rust has no
    // default port and would treat the host as an undeclared nameservice.
    hdfsProps("hdfs://nn.example.com/warehouse", Map.empty) shouldBe
      Map("hdfs.name-node.nn.example.com" -> "hdfs://nn.example.com:8020")
    // A portless IPv6 literal is not declared: iceberg-rust looks it up by the url crate's
    // canonical spelling, which a raw Java authority need not match. The gate declines it.
    hdfsProps("hdfs://[::1]/warehouse", Map.empty) shouldBe Map.empty
  }

  test("a nameservice name with an underscore is read from the raw authority") {
    // `URI.getHost` is null for it, which would silently derive nothing.
    val conf = Map(
      "dfs.ha.namenodes.name_service1" -> "nn1",
      "dfs.namenode.rpc-address.name_service1.nn1" -> "h.example.com:8020")
    hdfsProps("hdfs://name_service1/w", conf) shouldBe
      Map("hdfs.name-node.name_service1" -> "hdfs://h.example.com:8020")
    hdfsReason("hdfs://name_service1/w", conf, hdfsProps("hdfs://name_service1/w", conf)) shouldBe
      None
  }

  test("DNS-resolved NameNodes are not declared, since the native client would not expand them") {
    val conf = Map(
      "dfs.nameservices" -> "ns1",
      "dfs.ha.namenodes.ns1" -> "nn",
      "dfs.namenode.rpc-address.ns1.nn" -> "nn-dns.example.com:8020",
      "dfs.client.failover.resolve-needed.ns1" -> "true")
    hdfsProps("hdfs://ns1/warehouse", conf) shouldBe Map.empty
    hdfsReason("hdfs://ns1/warehouse", conf, hdfsProps("hdfs://ns1/warehouse", conf))
      .getOrElse(fail("expected a reason")) should include(
      "dfs.client.failover.resolve-needed.ns1")
  }

  test("a partially resolved HA list yields nothing rather than a short failover list") {
    // Dropping nn2 would silently turn a failover into an outage.
    hdfsProps(
      "hdfs://nameservice1/warehouse",
      Map(
        "dfs.ha.namenodes.nameservice1" -> "nn1,nn2",
        "dfs.namenode.rpc-address.nameservice1.nn1" -> "host-a.example.com:8020")) shouldBe Map.empty
  }

  test("a portless rpc-address leaves the nameservice unresolved, as the JVM rejects it") {
    // Hadoop's HA client throws "Does not contain a valid host:port authority" for it, so Comet
    // does not invent a port the JVM would not use.
    for (address <- Seq("host-a.example.com", "hdfs://host-a.example.com", "[::1]")) {
      withClue(s"rpc-address=$address: ") {
        hdfsProps(
          "hdfs://ns/warehouse",
          Map(
            "dfs.ha.namenodes.ns" -> "nn1,nn2",
            "dfs.namenode.rpc-address.ns.nn1" -> address,
            "dfs.namenode.rpc-address.ns.nn2" -> "host-b.example.com:9000")) shouldBe Map.empty
      }
    }
  }

  test("rpc-addresses are normalized to hdfs://host:port") {
    def resolved(address: String): Map[String, String] =
      hdfsProps(
        "hdfs://ns/warehouse",
        Map("dfs.ha.namenodes.ns" -> "nn1", "dfs.namenode.rpc-address.ns.nn1" -> address))

    // Not double-prefixed; a trailing slash and surrounding spaces dropped; IPv6 kept bracketed.
    resolved("hdfs://host-a.example.com:8020") shouldBe
      Map("hdfs.name-node.ns" -> "hdfs://host-a.example.com:8020")
    resolved(" host-a.example.com:8020/ ") shouldBe
      Map("hdfs.name-node.ns" -> "hdfs://host-a.example.com:8020")
    resolved("[::1]:9000") shouldBe Map("hdfs.name-node.ns" -> "hdfs://[::1]:9000")
  }

  test("a nameservice in dfs.nameservices without dfs.ha.namenodes yields nothing") {
    // Not a plain host either, so no default port is applied to it.
    hdfsProps("hdfs://ns2/warehouse", Map("dfs.nameservices" -> "ns1,ns2")) shouldBe Map.empty
  }

  test("non-hdfs and authority-less locations are ignored") {
    // Without the scheme check a bucket would look like a plain host and get `:8020`, and a
    // resolvable "nameservice" would be declared for an S3 table.
    hdfsProps("s3://bucket/key", Map.empty) shouldBe Map.empty
    hdfsProps(
      "s3://ns/key",
      Map(
        "dfs.ha.namenodes.ns" -> "nn1",
        "dfs.namenode.rpc-address.ns.nn1" -> "h.example.com:8020")) shouldBe
      Map.empty
    hdfsProps("hdfs:///warehouse/db/t", Map.empty) shouldBe Map.empty
  }

  // --- hdfsNameNodeFallbackReason ----------------------------------------------------------
  //
  // The decision shared by the scan and the write planners: Some(reason) only when the pinned
  // iceberg-rust could not resolve the NameNode with the properties native receives.

  private def hdfsReason(
      location: String,
      conf: Map[String, String] = Map.empty,
      catalogProperties: Map[String, String] = Map.empty): Option[String] =
    CometIcebergNativeScan.hdfsNameNodeFallbackReason(
      new java.net.URI(location),
      hadoopConfOf(conf),
      catalogProperties)

  // What the planners pass: the Hadoop-derived declaration, then the catalog's properties.
  private def plannedReason(
      location: String,
      conf: Map[String, String],
      catalogProperties: Map[String, String] = Map.empty): Option[String] =
    hdfsReason(location, conf, hdfsProps(location, conf) ++ catalogProperties)

  private val haConf = Map(
    "dfs.nameservices" -> "ns1",
    "dfs.ha.namenodes.ns1" -> "nn1,nn2",
    "dfs.namenode.rpc-address.ns1.nn1" -> "host-a.example.com:8020",
    "dfs.namenode.rpc-address.ns1.nn2" -> "host-b.example.com:8020")

  test("an authority-less hdfs location is declined, whatever else is configured") {
    // iceberg-rust would open it from `hdfs.name-node`, `hdfs.host`/`hdfs.port` or `fs.defaultFS`,
    // but Comet requires the NameNode in the location itself.
    val reason = hdfsReason("hdfs:///warehouse/db/t")
    reason.getOrElse(fail("expected a reason")) should startWith("authority-less hdfs location")
    reason.get should include("hdfs:///warehouse/db/t")
    hdfsReason(
      "hdfs:///warehouse/db/t",
      haConf ++ Map("fs.defaultFS" -> "hdfs://nn.example.com:8020"),
      Map(
        "hdfs.name-node" -> "nn.example.com:8020",
        "hdfs.host" -> "nn.example.com",
        "hdfs.port" -> "8020",
        "hadoop.fs.defaultFS" -> "hdfs://nn.example.com:8020")).isDefined shouldBe true
  }

  test("a resolved HA nameservice stays native") {
    plannedReason("hdfs://ns1/warehouse", haConf) shouldBe None
  }

  test("a portless plain host stays native through its derived declaration") {
    plannedReason("hdfs://nn.example.com/warehouse", Map.empty) shouldBe None
    // Without the declaration the same location is declined, so the derivation is what matters.
    hdfsReason("hdfs://nn.example.com/warehouse").getOrElse(fail("expected a reason")) should
      include("hdfs.name-node.nn.example.com")
  }

  test("an authority with a port is dialed as is") {
    hdfsReason("hdfs://nn.example.com:8020/warehouse") shouldBe None
    hdfsReason("hdfs://[::1]:8020/warehouse") shouldBe None
    // An authority that merely differs from the configured nameservice is just another host.
    hdfsReason("hdfs://other.example.com:8020/warehouse", haConf) shouldBe None
  }

  test("a configured nameservice that did not resolve is declined") {
    val conf = haConf - "dfs.namenode.rpc-address.ns1.nn2"
    val reason = plannedReason("hdfs://ns1/warehouse", conf)
    reason.getOrElse(fail("expected a reason")) should include(
      "HDFS location authority 'ns1' has no port")
    reason.get should include("hdfs.name-node.ns1")
    reason.get should include("could not resolve it")
    reason.get should include("dfs.namenode.rpc-address.ns1.<nn>")
  }

  test("a nameservice listed only in dfs.nameservices is declined") {
    plannedReason("hdfs://ns2/warehouse", Map("dfs.nameservices" -> "ns1, ns2"))
      .getOrElse(fail("expected a reason")) should include("nameservice 'ns2'")
    // A nameservice that has only `dfs.ha.namenodes.<ns>` set is configured too.
    plannedReason("hdfs://ns3/warehouse", Map("dfs.ha.namenodes.ns3" -> "nn1"))
      .getOrElse(fail("expected a reason")) should include("nameservice 'ns3'")
  }

  test("a catalog hdfs.name-node.<nameservice> declares an unresolved nameservice") {
    val conf = haConf - "dfs.namenode.rpc-address.ns1.nn2"
    plannedReason(
      "hdfs://ns1/warehouse",
      conf,
      Map(
        "hdfs.name-node.ns1" -> "host-a.example.com:8020,host-b.example.com:8020")) shouldBe None
  }

  test("the global hdfs.name-node does not declare a nameservice") {
    // iceberg-rust consults it only for authority-less paths, so it cannot rescue `hdfs://ns1`.
    val conf = haConf - "dfs.namenode.rpc-address.ns1.nn2"
    val reason = plannedReason(
      "hdfs://ns1/warehouse",
      conf,
      Map("hdfs.name-node" -> "host-a.example.com:8020"))
    reason.getOrElse(fail("expected a reason")) should include("has no port")
    reason.get should include("hdfs.name-node.ns1")
    // A blank global value is no value at all, not an invalid one.
    hdfsReason(
      "hdfs://nn.example.com:8020/w",
      catalogProperties = Map("hdfs.name-node" -> " , ")) shouldBe
      None
  }

  test("a declaration for another nameservice does not declare this one") {
    hdfsReason(
      "hdfs://ns1/warehouse",
      haConf - "dfs.namenode.rpc-address.ns1.nn2",
      Map("hdfs.name-node.ns2" -> "host-a.example.com:8020"))
      .getOrElse(fail("expected a reason")) should include("hdfs.name-node.ns1")
  }

  test("Hadoop's own HA keys passed as hadoop.* declare a nameservice") {
    val declared = Map(
      "hadoop.dfs.ha.namenodes.ns9" -> "a,b",
      "hadoop.dfs.namenode.rpc-address.ns9.a" -> "host-a.example.com:8020",
      "hadoop.dfs.namenode.rpc-address.ns9.b" -> "hdfs://host-b.example.com:8020")
    hdfsReason("hdfs://ns9/warehouse", catalogProperties = declared) shouldBe None
    def undeclared(props: Map[String, String]): Unit = {
      val reason = hdfsReason("hdfs://ns9/warehouse", catalogProperties = props)
      reason.getOrElse(fail(s"expected a reason for $props")) should include("hdfs.name-node.ns9")
      reason.get should not include "is not host:port"
    }
    // iceberg-rust rejects a declared NameNode whose rpc-address is missing or portless.
    undeclared(declared - "hadoop.dfs.namenode.rpc-address.ns9.b")
    undeclared(declared + ("hadoop.dfs.namenode.rpc-address.ns9.b" -> "host-b.example.com"))
    // An empty id list declares nothing.
    undeclared(Map("hadoop.dfs.ha.namenodes.ns9" -> " , "))
    // The hadoop.* keys win over the sugar, so a sugar entry does not repair them.
    undeclared(
      Map(
        "hadoop.dfs.ha.namenodes.ns9" -> "a",
        "hdfs.name-node.ns9" -> "host-a.example.com:8020"))
  }

  test("a NameNode entry that is not host:port is declined, in any hdfs.name-node key") {
    // iceberg-rust parses every one of them when it builds the storage, so one bad entry fails
    // every read and write through it, even for a location that would not use it.
    for {
      key <- Seq("hdfs.name-node", "hdfs.name-node.ns1", "hdfs.name-node.other")
      entry <- Seq(
        "nn1.example.com",
        "hdfs://nn1.example.com",
        "hdfs://nn1.example.com/",
        "[::1]",
        "nn1.example.com:",
        "nn1.example.com:http",
        "nn1.example.com:0",
        "nn1.example.com:99999",
        "user@nn1.example.com:8020",
        "nn1.example.com:8020/path")
    } {
      withClue(s"$key=$entry: ") {
        val reason = hdfsReason(
          "hdfs://nn9.example.com:8020/warehouse",
          catalogProperties = Map(key -> entry))
        reason.getOrElse(fail("expected a reason")) should include(s"$key entry")
        reason.get should include("is not host:port")
      }
    }
  }

  test("one bad entry in a NameNode list is enough to decline it") {
    val reason = hdfsReason(
      "hdfs://ns1/warehouse",
      catalogProperties = Map("hdfs.name-node.ns1" -> "nn1.example.com:8020, nn2.example.com"))
    reason.getOrElse(fail("expected a reason")) should include(
      "hdfs.name-node.ns1 entry 'nn2.example.com' is not host:port")
  }

  test("a nameservice declaration must name a nameservice and list NameNodes") {
    for (key <- Seq("hdfs.name-node.", "hdfs.name-node.n s")) {
      withClue(s"$key: ") {
        hdfsReason(
          "hdfs://nn.example.com:8020/warehouse",
          catalogProperties = Map(key -> "nn1.example.com:8020"))
          .getOrElse(fail("expected a reason")) should include("does not name a nameservice")
      }
    }
    hdfsReason(
      "hdfs://nn.example.com:8020/warehouse",
      catalogProperties = Map("hdfs.name-node.ns1" -> " , "))
      .getOrElse(fail("expected a reason")) should include("lists no NameNodes")
  }

  test("well formed NameNode lists stay native") {
    // As iceberg-rust reads them: entries trimmed, a trailing '/' dropped, `hdfs://` optional,
    // a bracketed IPv6 literal allowed, and empty entries ignored.
    for (value <- Seq(
        "nn1.example.com:8020",
        "hdfs://nn1.example.com:8020",
        " hdfs://nn1.example.com:8020/ , nn2.example.com:9000 ",
        "[::1]:8020,hdfs://[2001:db8::1]:8020",
        "nn1.example.com:8020,")) {
      withClue(s"hdfs.name-node.ns1=$value: ") {
        hdfsReason("hdfs://ns1/warehouse", haConf, Map("hdfs.name-node.ns1" -> value)) shouldBe
          None
      }
    }
  }

  test("an authority iceberg-rust cannot parse, or might key differently, is declined") {
    hdfsReason("hdfs://user@nn.example.com:8020/warehouse")
      .getOrElse(fail("expected a reason")) should include("userinfo")
    for (location <- Seq(
        "hdfs://nn.example.com:0/warehouse",
        "hdfs://nn.example.com:99999/warehouse",
        // Portless IPv6 and non-ASCII names: iceberg-rust's url crate rewrites them, so a
        // declaration keyed by the raw spelling could miss.
        "hdfs://[::1]/warehouse",
        "hdfs://ns%C3%A9/warehouse")) {
      withClue(s"$location: ") {
        hdfsReason(location).getOrElse(fail("expected a reason")) should include(
          "neither host:port")
      }
    }
  }

  test("non-hdfs locations never get an HDFS reason") {
    val badProps = Map("hdfs.name-node" -> "no-port", "hdfs.name-node.ns1" -> "no-port")
    for (location <- Seq(
        "s3://ns1/key",
        "s3a://ns1/key",
        "gs://ns1/key",
        "file:///tmp/warehouse",
        "/tmp/warehouse",
        "memory://ns1/key")) {
      withClue(s"$location: ") {
        hdfsReason(location, haConf - "dfs.namenode.rpc-address.ns1.nn2", badProps) shouldBe None
      }
    }
  }

  test("the hdfs scheme is matched case-insensitively") {
    hdfsReason("HDFS:///warehouse").isDefined shouldBe true
    hdfsReason("HDFS://ns1/warehouse", Map("dfs.nameservices" -> "ns1")).isDefined shouldBe true
  }

  test("other HDFS settings iceberg-rust rejects for every path are declined") {
    // iceberg-rust parses these when it builds the storage, before resolving any path.
    for (props <- Seq(
        Map("hdfs.port" -> "80x"),
        Map("hdfs.port" -> "70000"),
        Map("hdfs.host" -> "nn.example.com:8020"),
        Map("hdfs.host" -> "hdfs://nn.example.com"),
        Map("hdfs.host" -> "nn.example.com", "hdfs.port" -> "0"),
        Map("hadoop." -> "x"))) {
      withClue(s"$props: ") {
        hdfsReason("hdfs://nn.example.com:8020/w", catalogProperties = props).isDefined shouldBe
          true
      }
    }
    // Valid ones, and blank ones, are left alone.
    for (props <- Seq(
        Map("hdfs.host" -> "nn.example.com", "hdfs.port" -> "9000"),
        Map("hdfs.host" -> "::1"),
        Map("hdfs.port" -> " "),
        Map("hdfs.user" -> "hdfs"))) {
      withClue(s"$props: ") {
        hdfsReason("hdfs://nn.example.com:8020/w", catalogProperties = props) shouldBe None
      }
    }
  }

  test("declaring a nameservice named like the native client's synthetic one is declined") {
    // opendal builds every client against a synthetic HA nameservice called `nameservice`, and a
    // forwarded declaration of a real one by that name replaces its NameNodes.
    for (props <- Seq(
        Map("hdfs.name-node.nameservice" -> "nn1.example.com:8020"),
        Map(
          "hadoop.dfs.ha.namenodes.nameservice" -> "a",
          "hadoop.dfs.namenode.rpc-address.nameservice.a" -> "nn1.example.com:8020"))) {
      withClue(s"$props: ") {
        hdfsReason(
          "hdfs://ns1/w",
          catalogProperties = props + ("hdfs.name-node.ns1" ->
            "nn2.example.com:8020")).getOrElse(fail("expected a reason")) should include(
          "synthetic nameservice")
        // The location's own nameservice by that name is consistent with itself.
        hdfsReason("hdfs://nameservice/w", catalogProperties = props) shouldBe None
      }
    }
  }

  test("NameNodes resolve from the session configuration overlaid with the FileIO's") {
    val session = hadoopConfOf(
      Map(
        "dfs.nameservices" -> "ns1",
        "dfs.ha.namenodes.ns1" -> "nn1",
        "dfs.namenode.rpc-address.ns1.nn1" -> "session.example.com:8020",
        "dfs.ha.namenodes.ns2" -> "nn1",
        "dfs.namenode.rpc-address.ns2.nn1" -> "only-session.example.com:8020",
        "fs.defaultFS" -> "hdfs://session.example.com:8020"))
    val fileIO = hadoopConfOf(
      Map(
        "dfs.namenode.rpc-address.ns1.nn1" -> "catalog.example.com:8020",
        "dfs.ha.namenodes.nsb" -> "nn1",
        "dfs.namenode.rpc-address.nsb.nn1" -> "only-catalog.example.com:8020"))
    val conf = CometIcebergNativeScan.hdfsResolutionConf(session, Some(fileIO))

    // The FileIO's catalog overrides win; what only one side knows is kept from it.
    hdfsProps("hdfs://ns1/w", confMap(conf)) shouldBe
      Map("hdfs.name-node.ns1" -> "hdfs://catalog.example.com:8020")
    hdfsProps("hdfs://ns2/w", confMap(conf)) shouldBe
      Map("hdfs.name-node.ns2" -> "hdfs://only-session.example.com:8020")
    hdfsProps("hdfs://nsb/w", confMap(conf)) shouldBe
      Map("hdfs.name-node.nsb" -> "hdfs://only-catalog.example.com:8020")
    // Only `dfs.*` settings take part.
    conf.get("fs.defaultFS") shouldBe null
    // No FileIO configuration: the session's alone.
    hdfsProps(
      "hdfs://ns2/w",
      confMap(CometIcebergNativeScan.hdfsResolutionConf(session, None))) shouldBe
      Map("hdfs.name-node.ns2" -> "hdfs://only-session.example.com:8020")
  }

  private def confMap(conf: org.apache.hadoop.conf.Configuration): Map[String, String] =
    conf.getPropsWithPrefix("").asScala.toMap
}
