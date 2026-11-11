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

import java.io.File
import java.lang.reflect.InvocationTargetException
import java.net.URI
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}
import org.apache.iceberg.{DeleteFile, FileContent}
import org.apache.iceberg.expressions.Expressions
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}

import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.tags.ExecGroup

/** Unit tests for Iceberg native-scan serde helpers that do not require a Spark session. */
@ExecGroup
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

  test("HDFS nameservices are declared from the Hadoop configuration") {
    val conf = new Configuration(false)
    conf.set("dfs.ha.namenodes.ns1", "nn2, nn1")
    conf.set("dfs.namenode.rpc-address.ns1.nn1", "host1:8020")
    conf.set("dfs.namenode.rpc-address.ns1.nn2", "host2:8020")
    // A NameNode without a port cannot be dialed, so ns2 is not declared.
    conf.set("dfs.ha.namenodes.ns2", "nn1")
    conf.set("dfs.namenode.rpc-address.ns2.nn1", "host3")
    val authorities = Set("ns1", "ns2", "ns3", "host4:8020")

    val properties = CometIcebergNativeScan.hadoopToIcebergHdfsProperties(conf, authorities)

    // NameNodes keep their declaration order, an authority with a port needs no declaration, and
    // one the configuration knows nothing of is a host on the default port.
    properties shouldBe Map(
      "hdfs.name-node.ns1" -> "host2:8020,host1:8020",
      "hdfs.name-node.ns3" -> "ns3:8020")
    CometIcebergNativeScan.undeclaredHdfsNameservice(authorities, properties) shouldBe Some("ns2")
    // A catalog's own declaration counts in either form.
    val catalog = Map("hdfs.name-node.ns2" -> "host3:8020", "hadoop.dfs.ha.namenodes.ns3" -> "a")
    CometIcebergNativeScan.undeclaredHdfsNameservice(
      authorities,
      properties ++ catalog) shouldBe None
  }

  test("HDFS authorities with a port need no declaration, IPv6 literals included") {
    val conf = new Configuration(false)
    conf.set("dfs.ha.namenodes.ns1", "nn1")
    conf.set("dfs.namenode.rpc-address.ns1.nn1", "[::1]:8020")
    val authorities = Set("[::1]:8020", "[2001:db8::1]:9000", "[::1]", "ns1")

    val properties = CometIcebergNativeScan.hadoopToIcebergHdfsProperties(conf, authorities)

    // The colons inside the brackets are not a port separator: a bracketed rpc-address keeps
    // its port, and a bracketed literal without one is a portless host, so it gets the default
    // port like any other undeclared host.
    properties shouldBe Map(
      "hdfs.name-node.ns1" -> "[::1]:8020",
      "hdfs.name-node.[::1]" -> "[::1]:8020")
    CometIcebergNativeScan.undeclaredHdfsNameservice(authorities, properties) shouldBe None
  }

  test("an undeclared portless HDFS host is declared on Hadoop's default NameNode port") {
    val conf = new Configuration(false)
    conf.set("dfs.nameservices", "ns1")
    val authorities = Set("nn.example.com", "ns1")

    val properties = CometIcebergNativeScan.hadoopToIcebergHdfsProperties(conf, authorities)

    // What DFSUtilClient.getNNAddress does for the JVM client: a host without a port is dialed on
    // 8020. A name in dfs.nameservices is logical to Hadoop too; without NameNodes it stays
    // undeclared rather than becoming a host.
    properties shouldBe Map("hdfs.name-node.nn.example.com" -> "nn.example.com:8020")
    CometIcebergNativeScan.undeclaredHdfsNameservice(authorities, properties) shouldBe Some("ns1")
  }

  test("HDFS client settings the session sets are forwarded under hadoop.") {
    val conf = new Configuration(false)
    conf.set("dfs.client.use.datanode.hostname", "true")
    conf.set("dfs.client.socket-timeout", "30000")
    // Hadoop's own default is not a setting: forwarded, it would override the executors' XML.
    conf.set("dfs.client.failover.max.attempts", "15", "hdfs-default.xml")
    // What the client does not read, or must not be given, stays behind.
    conf.set("dfs.client.read.shortcircuit", "true")
    conf.set("dfs.data.transfer.protection", "privacy")
    conf.set("dfs.client.failover.random.order.ns1", "true")

    CometIcebergNativeScan.hadoopToIcebergHdfsProperties(conf, Set("nn1:8020")) shouldBe Map(
      "hadoop.dfs.client.use.datanode.hostname" -> "true",
      "hadoop.dfs.client.socket-timeout" -> "30000")
    // A scan with no HDFS files needs none of it.
    CometIcebergNativeScan.hadoopToIcebergHdfsProperties(conf, Set.empty) shouldBe Map.empty
  }

  test("the encryption-zone probe lists each parent directory once and keeps the listing") {
    val root = Files.createTempDirectory("comet_ez_probe").toFile
    def dir(relative: String): String = {
      val dir = new File(root, relative)
      assert(dir.mkdirs())
      s"${CountingHdfsFileSystem.PREFIX}${dir.getAbsolutePath}"
    }
    val dirs = (1 to 3).map(i => dir(s"data/p=$i")).toSet
    val conf = new Configuration(false)
    conf.set("fs.hdfs.impl", classOf[CountingHdfsFileSystem].getName)
    conf.set("fs.hdfs.impl.disable.cache", "true")
    // A configured key provider makes the probe run; the local filesystem has no zones.
    conf.set("hadoop.security.key.provider.path", "kms://http@kms.example.com:9600/kms")
    CountingHdfsFileSystem.reset()

    CometIcebergNativeScan.hdfsEncryptionZoneFallback(conf, dirs) shouldBe None
    // The three partition directories share a parent: one listing, no per-directory status.
    CountingHdfsFileSystem.listStatusCalls.get() shouldBe 1
    CountingHdfsFileSystem.getFileStatusCalls.get() shouldBe 0

    CometIcebergNativeScan.hdfsEncryptionZoneFallback(conf, dirs) shouldBe None
    CountingHdfsFileSystem.listStatusCalls.get() shouldBe 1

    // A directory the kept listing does not have, a new partition, lists its parent again.
    CometIcebergNativeScan.hdfsEncryptionZoneFallback(conf, dirs + dir("data/p=4")) shouldBe None
    CountingHdfsFileSystem.listStatusCalls.get() shouldBe 2

    // An expired listing is taken again: after the TTL when one is set, after an hour otherwise.
    // A listing carries the clock it was taken with, so the second clock counts from the first.
    val later = System.currentTimeMillis() + 61 * 1000
    CometIcebergNativeScan.hdfsEncryptionZoneFallback(
      conf,
      dirs,
      ttlSeconds = 60,
      nowMillis = later) shouldBe None
    CountingHdfsFileSystem.listStatusCalls.get() shouldBe 3
    CometIcebergNativeScan.hdfsEncryptionZoneFallback(
      conf,
      dirs,
      nowMillis = later + 3601 * 1000) shouldBe None
    CountingHdfsFileSystem.listStatusCalls.get() shouldBe 4

    // Another table at the same directories, say one re-created after a purge, has its own
    // listing.
    CometIcebergNativeScan.hdfsEncryptionZoneFallback(
      conf,
      dirs,
      tableUuid = Some("re-created")) shouldBe None
    CountingHdfsFileSystem.listStatusCalls.get() shouldBe 5
    CometIcebergNativeScan.hdfsEncryptionZoneFallback(
      conf,
      dirs,
      tableUuid = Some("re-created")) shouldBe None
    CountingHdfsFileSystem.listStatusCalls.get() shouldBe 5

    // More entries than the budget: the scan is declined once that many are listed, the parents
    // listed whole are kept, and the next scan continues from them.
    val spread = (1 to 3).map(i => dir(s"spread/t=$i/p=1")).toSet
    CountingHdfsFileSystem.reset()
    val declined =
      CometIcebergNativeScan.hdfsEncryptionZoneFallback(conf, spread, maxEntries = 2)
    assert(declined.exists(_.contains("2 directory entries")), declined)
    CountingHdfsFileSystem.listStatusCalls.get() shouldBe 2
    CometIcebergNativeScan.hdfsEncryptionZoneFallback(conf, spread, maxEntries = 2) shouldBe None
    CountingHdfsFileSystem.listStatusCalls.get() shouldBe 3

    // A directory at the filesystem root has no parent to list.
    CometIcebergNativeScan.hdfsEncryptionZoneFallback(
      conf,
      Set(s"${CountingHdfsFileSystem.PREFIX}/")) shouldBe None
  }

  test("the encryption-zone probe lists a parent of many partitions once, in linear time") {
    val conf = new Configuration(false)
    conf.set("fs.hdfs.impl", classOf[CountingHdfsFileSystem].getName)
    conf.set("fs.hdfs.impl.disable.cache", "true")
    conf.set("hadoop.security.key.provider.path", "kms://http@kms.example.com:9600/kms")
    val prefix = CountingHdfsFileSystem.PREFIX
    def partitions(parent: String): Set[String] =
      (1 to 50000).map(i => s"$prefix/$parent/p=$i").toSet
    // Listings of these parents are made up, not read from disk.
    CountingHdfsFileSystem.synthetic = Map(s"$prefix/wide" -> 50000, s"$prefix/wider" -> 50000)
    CountingHdfsFileSystem.reset()
    try {
      val wide = partitions("wide")
      val keptBefore = CometIcebergNativeScan.keptEncryptionZoneEntries
      val started = System.nanoTime()
      CometIcebergNativeScan.hdfsEncryptionZoneFallback(
        conf,
        wide,
        tableUuid = Some("wide")) shouldBe None
      val elapsedMillis = (System.nanoTime() - started) / 1000000
      // One listing, no per-directory status, and nothing quadratic in the partitions: a lookup
      // that scanned the listing per directory took minutes here.
      CountingHdfsFileSystem.listStatusCalls.get() shouldBe 1
      CountingHdfsFileSystem.getFileStatusCalls.get() shouldBe 0
      assert(elapsedMillis < 10000, s"the probe took $elapsedMillis ms over 50000 partitions")
      CometIcebergNativeScan.keptEncryptionZoneEntries - keptBefore shouldBe 50000

      // The budget counts entries, not listings: a parent beyond it is declined and not kept.
      val declined = CometIcebergNativeScan.hdfsEncryptionZoneFallback(
        conf,
        wide,
        tableUuid = Some("over-budget"),
        maxEntries = 10)
      assert(declined.exists(_.contains("10 directory entries")), declined)
      CountingHdfsFileSystem.listStatusCalls.get() shouldBe 2
      CometIcebergNativeScan.keptEncryptionZoneEntries - keptBefore shouldBe 50000

      // What is kept is bounded in entries too, the least recently used parents leaving first:
      // the first table's listing goes, and its next scan lists again.
      CometIcebergNativeScan.hdfsEncryptionZoneFallback(
        conf,
        partitions("wider"),
        tableUuid = Some("wider"),
        maxKeptEntries = 60000) shouldBe None
      assert(CometIcebergNativeScan.keptEncryptionZoneEntries <= 60000)
      CometIcebergNativeScan.hdfsEncryptionZoneFallback(
        conf,
        wide,
        tableUuid = Some("wide")) shouldBe None
      CountingHdfsFileSystem.listStatusCalls.get() shouldBe 4
    } finally {
      CountingHdfsFileSystem.synthetic = Map.empty
    }
  }

  private def keyMetadataMethod(clazz: Class[_]) = clazz.getMethod("keyMetadata")

  private def serializeDeleteFile(file: AnyRef) =
    CometIcebergNativeScan.serializeDeleteFile(
      file,
      file.getClass,
      file.getClass,
      keyMetadataMethod(file.getClass),
      _ => 0)

  test("required Iceberg delete-file accessors are present") {
    Seq("content", "specId", "equalityFieldIds").foreach { accessor =>
      IcebergReflection.findMethod(classOf[DeleteFile], accessor).isDefined shouldBe true
    }
  }

  test("Iceberg delete content names match native serde literals") {
    FileContent.POSITION_DELETES.toString shouldBe IcebergReflection.ContentTypes.POSITION_DELETES
    FileContent.EQUALITY_DELETES.toString shouldBe IcebergReflection.ContentTypes.EQUALITY_DELETES
  }

  test("position-delete file with null equality ids serializes without equality ids") {
    val proto = serializeDeleteFile(new PositionDeleteFile)
    proto.getContentType shouldBe "POSITION_DELETES"
    proto.getPartitionSpecId shouldBe 7
    proto.getEqualityIdsCount shouldBe 0
    proto.getFilePathIdx shouldBe 0
  }

  test("equality-delete file serializes declared equality ids") {
    val proto = serializeDeleteFile(new EqualityDeleteFile)
    proto.getContentType shouldBe "EQUALITY_DELETES"
    proto.getEqualityIdsCount shouldBe 2
    proto.getEqualityIds(0) shouldBe 3
    proto.getEqualityIds(1) shouldBe 5
  }

  test("equality-delete file with null equality ids is fatal") {
    val ex =
      intercept[IllegalStateException](serializeDeleteFile(new EqualityDeleteFileWithNullIds))
    ex.getMessage shouldBe
      "Iceberg equality delete file 's3://bucket/eq-null-ids.parquet' has no equality field IDs"
  }

  test("equality-delete file with empty equality ids is fatal") {
    val ex =
      intercept[IllegalStateException](serializeDeleteFile(new EqualityDeleteFileWithEmptyIds))
    ex.getMessage shouldBe
      "Iceberg equality delete file 's3://bucket/eq-empty-ids.parquet' has no equality field IDs"
  }

  test("content invocation failure propagates instead of defaulting to position deletes") {
    val ex =
      intercept[InvocationTargetException](serializeDeleteFile(new ThrowingContentDeleteFile))
    ex.getCause.getMessage shouldBe "content boom"
  }

  test("spec id invocation failure propagates instead of defaulting to zero") {
    val ex =
      intercept[InvocationTargetException](serializeDeleteFile(new ThrowingSpecIdDeleteFile))
    ex.getCause.getMessage shouldBe "spec boom"
  }

  test("equality-id invocation failure propagates instead of dropping ids") {
    val ex =
      intercept[InvocationTargetException](serializeDeleteFile(new ThrowingEqualityIdsDeleteFile))
    ex.getCause.getMessage shouldBe "ids boom"
  }

  test("missing content accessor is fatal") {
    assertThrows[NoSuchMethodException](serializeDeleteFile(new NoContentAccessorDeleteFile))
  }

  test("missing equality-id accessor is fatal") {
    assertThrows[NoSuchMethodException](serializeDeleteFile(new NoEqualityIdsAccessorDeleteFile))
  }

  test("missing delete-file path accessor is fatal") {
    val ex = intercept[RuntimeException](serializeDeleteFile(new NoPathAccessorDeleteFile))
    ex.getMessage should include("Neither location() nor path() is declared")
  }

  private abstract class BaseDeleteFile {
    def location(): String
    def format(): String = "PARQUET"
    def recordCount(): java.lang.Long = java.lang.Long.valueOf(1L)
    def keyMetadata(): java.nio.ByteBuffer = null
  }

  private class PositionDeleteFile extends BaseDeleteFile {
    override def location(): String = "s3://bucket/pos-delete.parquet"
    def content(): String = "POSITION_DELETES"
    def specId(): Int = 7
    def equalityFieldIds(): java.util.List[Integer] = null
  }

  private class EqualityDeleteFile extends BaseDeleteFile {
    override def location(): String = "s3://bucket/eq-delete.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] =
      java.util.List.of(Integer.valueOf(3), Integer.valueOf(5))
  }

  private class EqualityDeleteFileWithNullIds extends BaseDeleteFile {
    override def location(): String = "s3://bucket/eq-null-ids.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = null
  }

  private class EqualityDeleteFileWithEmptyIds extends BaseDeleteFile {
    override def location(): String = "s3://bucket/eq-empty-ids.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = java.util.List.of[Integer]()
  }

  private class ThrowingContentDeleteFile extends BaseDeleteFile {
    override def location(): String = "s3://bucket/d.parquet"
    def content(): String = throw new RuntimeException("content boom")
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = null
  }

  private class ThrowingSpecIdDeleteFile extends BaseDeleteFile {
    override def location(): String = "s3://bucket/d.parquet"
    def content(): String = "POSITION_DELETES"
    def specId(): Int = throw new RuntimeException("spec boom")
    def equalityFieldIds(): java.util.List[Integer] = null
  }

  private class ThrowingEqualityIdsDeleteFile extends BaseDeleteFile {
    override def location(): String = "s3://bucket/d.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = throw new RuntimeException("ids boom")
  }

  private class NoContentAccessorDeleteFile extends BaseDeleteFile {
    override def location(): String = "s3://bucket/d.parquet"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = null
  }

  private class NoEqualityIdsAccessorDeleteFile extends BaseDeleteFile {
    override def location(): String = "s3://bucket/d.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
  }

  private class NoPathAccessorDeleteFile {
    def keyMetadata(): java.nio.ByteBuffer = null
  }
}

/**
 * A local-disk `hdfs://` filesystem that counts the NameNode calls the planner would make. The
 * local `listStatus` stats each child itself, which a NameNode listing does not, so those inner
 * calls are not counted.
 */
class CountingHdfsFileSystem extends RawLocalFileSystem {
  RawLocalFileSystem.useStatIfAvailable()
  private var listing = false
  override def getScheme: String = "hdfs"
  override def getUri: URI = URI.create(CountingHdfsFileSystem.PREFIX)
  override def listStatus(path: Path): Array[FileStatus] = {
    CountingHdfsFileSystem.listStatusCalls.incrementAndGet()
    CountingHdfsFileSystem.synthetic.get(path.toString) match {
      case Some(children) =>
        Array.tabulate(children)(i =>
          new FileStatus(0, true, 0, 0, 0, new Path(path, s"p=${i + 1}")))
      case None =>
        listing = true
        try super.listStatus(path)
        finally listing = false
    }
  }
  override def getFileStatus(path: Path): FileStatus = {
    if (!listing) CountingHdfsFileSystem.getFileStatusCalls.incrementAndGet()
    super.getFileStatus(path)
  }
}

object CountingHdfsFileSystem {
  val PREFIX = "hdfs://counting"
  val listStatusCalls = new AtomicInteger()
  val getFileStatusCalls = new AtomicInteger()

  /** Parents whose listing is made up of that many `p=<i>` directories, by path. */
  @volatile var synthetic: Map[String, Int] = Map.empty
  def reset(): Unit = {
    listStatusCalls.set(0)
    getFileStatusCalls.set(0)
  }
}
