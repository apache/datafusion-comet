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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager

import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator

class PlanDataInjectorSuite extends AnyFunSuite {

  /** Builds an un-injected IcebergScan operator: hasCommon, zero file_scan_tasks. */
  private def icebergScanOp(metadataLocation: String, scanHashCode: Int): Operator = {
    val common = OperatorOuterClass.IcebergScanCommon
      .newBuilder()
      .setMetadataLocation(metadataLocation)
      .setScanHashCode(scanHashCode)
      .build()
    val icebergScan = OperatorOuterClass.IcebergScan.newBuilder().setCommon(common).build()
    Operator.newBuilder().setIcebergScan(icebergScan).build()
  }

  /** Serialized (commonBytes, partitionBytes) a real CometIcebergNativeScanExec would produce. */
  private def icebergPlanData(
      metadataLocation: String,
      scanHashCode: Int,
      columnNames: Seq[String],
      dataFilePath: String): (Array[Byte], Array[Byte]) = {
    val commonBuilder = OperatorOuterClass.IcebergScanCommon
      .newBuilder()
      .setMetadataLocation(metadataLocation)
      .setScanHashCode(scanHashCode)
    columnNames.foreach { name =>
      commonBuilder.addRequiredSchema(
        OperatorOuterClass.SparkStructField.newBuilder().setName(name).setNullable(true).build())
    }
    val commonBytes = commonBuilder.build().toByteArray

    val partitionBytes = OperatorOuterClass.IcebergScan
      .newBuilder()
      .addFileScanTasks(
        OperatorOuterClass.IcebergFileScanTask.newBuilder().setDataFilePath(dataFilePath).build())
      .build()
      .toByteArray

    (commonBytes, partitionBytes)
  }

  test("injectPlanData leaves a non-scan operator tree unchanged") {
    // An operator with no injectable scan (here, an empty op_struct, but the same holds for
    // Filter/Projection/etc.) must pass through untouched. This exercises the O(1)
    // injectorsByKind miss path (`case _ =>`) that replaced the per-injector canInject walk.
    val child = Operator.newBuilder().setPlanId(2).build()
    val root = Operator.newBuilder().setPlanId(1).addChildren(child).build()
    val cached = parseBasePlan(root.toByteArray)

    val result = PlanDataInjector.injectPlanData(
      cached,
      Map.empty[String, Array[Byte]],
      Map.empty[String, Array[Byte]])

    assert(
      result eq cached.plan,
      "a tree with nothing to inject should be returned by reference, not rebuilt")
  }

  test("injectPlanData rebuilds only the path to the injected scan") {
    // Operators are immutable protobuf messages, so subtrees that need no injection are shared.
    val scanOp = icebergScanOp("s3://table/metadata/v1.json", scanHashCode = 111)
    val (commonBytes, partitionBytes) =
      icebergPlanData(
        "s3://table/metadata/v1.json",
        scanHashCode = 111,
        columnNames = Seq("id", "v"),
        dataFilePath = "data.parquet")
    val key = IcebergPlanDataInjector.getKey(scanOp).get

    val filter = Operator.newBuilder().setPlanId(2).addChildren(scanOp).build()
    val untouchedSibling = Operator
      .newBuilder()
      .setPlanId(3)
      .addChildren(Operator.newBuilder().setPlanId(4).build())
      .build()
    val root = Operator
      .newBuilder()
      .setPlanId(1)
      .addChildren(filter)
      .addChildren(untouchedSibling)
      .build()

    val cached = parseBasePlan(root.toByteArray)
    val result =
      PlanDataInjector.injectPlanData(cached, Map(key -> commonBytes), Map(key -> partitionBytes))

    assert(
      result.getChildren(1) eq cached.plan.getChildren(1),
      "a sibling subtree with no injectable scan should be shared, not rebuilt")
    val injectedScan = result.getChildren(0).getChildren(0)
    assert(injectedScan.getIcebergScan.getCommon.getRequiredSchemaCount == 2)
    assert(injectedScan.getIcebergScan.getFileScanTasks(0).getDataFilePath == "data.parquet")
    // Everything outside the injected scan is preserved verbatim.
    assert(result.getPlanId == 1)
    assert(result.getChildren(0).getPlanId == 2)
  }

  test("each registered injector is reachable by its opStructCase") {
    // The O(1) lookup keys injectors by opStructCase, so two injectors sharing a kind would
    // silently shadow one another in the map. Guard that every registered injector resolves back
    // to itself via its declared opStructCase (i.e. the kinds are distinct and the map is complete).
    val injectors = Seq(IcebergPlanDataInjector, NativeScanPlanDataInjector)
    val byKind = injectors.map(i => i.opStructCase -> i).toMap
    assert(byKind.size == injectors.size, "injectors must have distinct opStructCase keys")
    injectors.foreach { i =>
      assert(byKind(i.opStructCase) eq i)
    }
    assert(IcebergPlanDataInjector.opStructCase == Operator.OpStructCase.ICEBERG_SCAN)
    assert(NativeScanPlanDataInjector.opStructCase == Operator.OpStructCase.NATIVE_SCAN)
  }

  test("two Iceberg scans of the same table with different scan_hash_code get distinct keys") {
    val targetOp = icebergScanOp("s3://table/metadata/v1.json", scanHashCode = 111)
    val sourceOp = icebergScanOp("s3://table/metadata/v1.json", scanHashCode = 222)

    assert(IcebergPlanDataInjector.getKey(targetOp) != IcebergPlanDataInjector.getKey(sourceOp))
  }

  test("two Iceberg scans of the same table with equal scan_hash_code get the same key") {
    val opA = icebergScanOp("s3://table/metadata/v1.json", scanHashCode = 111)
    val opB = icebergScanOp("s3://table/metadata/v1.json", scanHashCode = 111)

    assert(IcebergPlanDataInjector.getKey(opA) == IcebergPlanDataInjector.getKey(opB))
  }

  /**
   * Builds an un-injected NativeScan operator the way the driver ships it: hasCommon, no
   * file_partition, source_key_hash embedded (see CometNativeScanExec.apply).
   */
  private def nativeScanOp(source: String, columnNames: Seq[String]): Operator = {
    val common = nativeScanCommon(source, columnNames)
    Operator
      .newBuilder()
      .setNativeScan(
        OperatorOuterClass.NativeScan
          .newBuilder()
          .setCommon(common)
          .setSourceKeyHash(NativeScanPlanDataInjector.sourceKeyHash(common)))
      .build()
  }

  private def nativeScanCommon(
      source: String,
      columnNames: Seq[String]): OperatorOuterClass.NativeScanCommon = {
    val builder = OperatorOuterClass.NativeScanCommon.newBuilder().setSource(source)
    columnNames.foreach { name =>
      builder.addRequiredSchema(
        OperatorOuterClass.SparkStructField.newBuilder().setName(name).setNullable(true).build())
    }
    builder.build()
  }

  private def nativeScanPartitionBytes(filePath: String): Array[Byte] = {
    OperatorOuterClass.NativeScan
      .newBuilder()
      .setFilePartition(
        OperatorOuterClass.SparkFilePartition
          .newBuilder()
          .addPartitionedFile(
            OperatorOuterClass.SparkPartitionedFile.newBuilder().setFilePath(filePath)))
      .build()
      .toByteArray
  }

  /** Parses the way CometExecRDD.compute does: the fingerprint comes with the plan bytes. */
  private def parseBasePlan(bytes: Array[Byte]): PlanDataInjector.CachedPlanData =
    PlanDataInjector.parseBasePlan(bytes, PlanDataInjector.planFingerprint(bytes))

  test("PlanKey hashes by the driver fingerprint and still compares by content") {
    val bytes = nativeScanOp("file:///fingerprint-tbl", Seq("a", "b")).toByteArray
    val fingerprint = PlanDataInjector.planFingerprint(bytes)
    assert(fingerprint == PlanDataInjector.planFingerprint(bytes.clone()))
    assert(fingerprint != PlanDataInjector.planFingerprint(bytes.dropRight(1)))

    val key = new PlanDataInjector.PlanKey(bytes, fingerprint)
    assert(key.hashCode == (fingerprint ^ (fingerprint >>> 32)).toInt)
    assert(key == new PlanDataInjector.PlanKey(bytes.clone(), fingerprint))
    // A fingerprint collision must still keep distinct plans apart.
    val other = nativeScanOp("file:///fingerprint-other", Seq("a", "b")).toByteArray
    assert(key != new PlanDataInjector.PlanKey(other, fingerprint))
  }

  test("parseBasePlan shares one parsed Operator across byte-identical plans") {
    val op = Operator
      .newBuilder()
      .setPlanId(10)
      .addChildren(nativeScanOp("file:///cache-hit-tbl", Seq("a", "b")))
      .build()
    // Each Spark task deserializes its own copy of the task binary, so the bytes arrive as
    // distinct arrays with identical content.
    val bytes1 = op.toByteArray
    val bytes2 = op.toByteArray
    assert(!(bytes1 eq bytes2))

    val parsed1 = parseBasePlan(bytes1)
    val parsed2 = parseBasePlan(bytes2)

    assert(parsed1 eq parsed2, "equal plan bytes should hit the cache, not re-parse")
    assert(parsed1.plan == Operator.parseFrom(bytes1))
  }

  test("parseBasePlan keeps distinct plans separate") {
    val opA = Operator
      .newBuilder()
      .setPlanId(20)
      .addChildren(nativeScanOp("file:///distinct-tbl-a", Seq("a")))
      .build()
    val opB = Operator
      .newBuilder()
      .setPlanId(21)
      .addChildren(nativeScanOp("file:///distinct-tbl-b", Seq("b")))
      .build()

    val parsedA = parseBasePlan(opA.toByteArray)
    val parsedB = parseBasePlan(opB.toByteArray)

    assert(parsedA.plan == opA)
    assert(parsedB.plan == opB)
    assert(parsedA.plan != parsedB.plan)
  }

  test("parseBasePlan re-parses correctly after eviction") {
    val first = Operator
      .newBuilder()
      .setPlanId(30)
      .addChildren(nativeScanOp("file:///evict-tbl-first", Seq("a")))
      .build()
    val firstBytes = first.toByteArray
    val firstParsed = parseBasePlan(firstBytes)

    // Push enough distinct plans through to evict the first entry.
    (0 until PlanDataInjector.maxCachedBasePlans).foreach { i =>
      val filler = Operator
        .newBuilder()
        .setPlanId(1000 + i)
        .addChildren(nativeScanOp(s"file:///evict-filler-$i", Seq("a")))
        .build()
      parseBasePlan(filler.toByteArray)
    }

    val reParsed = parseBasePlan(firstBytes)
    assert(!(reParsed eq firstParsed), "the first plan should have been evicted")
    assert(reParsed.plan == first, "a rerun after eviction must still parse correctly")
  }

  test("parseBasePlan returns each thread the plan matching its bytes under concurrency") {
    import java.util.concurrent.Executors
    import scala.concurrent.{Await, ExecutionContext, Future}
    import scala.concurrent.duration._

    val plans = (0 until 4).map { i =>
      Operator
        .newBuilder()
        .setPlanId(40 + i)
        .addChildren(nativeScanOp(s"file:///concurrent-tbl-$i", Seq("a", "b")))
        .build()
    }
    val pool = Executors.newFixedThreadPool(8)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
    try {
      val checks = Future.sequence((0 until 64).map { i =>
        val plan = plans(i % plans.size)
        Future(parseBasePlan(plan.toByteArray).plan == plan)
      })
      assert(Await.result(checks, 30.seconds).forall(identity))
    } finally {
      pool.shutdown()
    }
  }

  test("parseBasePlan gives racing threads on a cold key the same instance") {
    import java.util.concurrent.{CyclicBarrier, Executors}
    import scala.concurrent.{Await, ExecutionContext, Future}
    import scala.concurrent.duration._

    val pool = Executors.newFixedThreadPool(2)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
    try {
      (0 until 200).foreach { trial =>
        val op = Operator
          .newBuilder()
          .setPlanId(5000 + trial)
          .addChildren(nativeScanOp(s"file:///race-tbl-$trial", (0 until 64).map(i => s"c$i")))
          .build()
        val barrier = new CyclicBarrier(2)
        val results = (0 until 2)
          .map { _ =>
            Future {
              barrier.await()
              parseBasePlan(op.toByteArray)
            }
          }
          .map(Await.result(_, 30.seconds))
        // Whoever inserts first wins; the loser must adopt that instance, not its own parse,
        // or downstream reference-identity sharing silently degrades.
        assert(results(0) eq results(1), s"trial $trial: racing threads must share one instance")
      }
    } finally {
      pool.shutdown()
    }
  }

  test("NativeScan inject shares one parsed common across a stage's partitions") {
    val scanOp = nativeScanOp("file:///shared-common-tbl", Seq("id", "v"))
    val commonProto = nativeScanCommon("file:///shared-common-tbl", Seq("id", "v"))
    val key = NativeScanPlanDataInjector.getKey(scanOp).get
    // Two tasks of the same stage: each deserializes its own copy of the plan and common bytes,
    // and both resolve to the same cached base plan entry.
    val task1 = parseBasePlan(scanOp.toByteArray)
    val task2 = parseBasePlan(scanOp.toByteArray)

    val injected1 = PlanDataInjector.injectPlanData(
      task1,
      Map(key -> commonProto.toByteArray),
      Map(key -> nativeScanPartitionBytes("part-0.parquet")))
    val injected2 = PlanDataInjector.injectPlanData(
      task2,
      Map(key -> commonProto.toByteArray),
      Map(key -> nativeScanPartitionBytes("part-1.parquet")))

    assert(
      injected1.getNativeScan.getCommon eq injected2.getNativeScan.getCommon,
      "equal common bytes should be prepared once per plan entry and shared")
    assert(injected1.getNativeScan.getCommon == commonProto)
    // Each partition still gets its own file list.
    val file1 = injected1.getNativeScan.getFilePartition.getPartitionedFile(0).getFilePath
    val file2 = injected2.getNativeScan.getFilePartition.getPartitionedFile(0).getFilePath
    assert(file1 == "part-0.parquet")
    assert(file2 == "part-1.parquet")
  }

  test("prepared scan data shares the base plan's eviction unit, not a per-scan budget") {
    // A single plan with more scans than the base plan cache holds plans (17 > 16). All of the
    // plan's prepared commons must be reused across tasks together; nothing may churn because
    // the ownership unit is the plan entry, not a scan-count LRU.
    val n = PlanDataInjector.maxCachedBasePlans + 1
    val scans = (0 until n).map(i => nativeScanOp(s"file:///wide-plan-tbl-$i", Seq("a")))
    val root = {
      val builder = Operator.newBuilder().setPlanId(60)
      scans.foreach(builder.addChildren)
      builder.build()
    }
    val commonByKey = scans.map { s =>
      NativeScanPlanDataInjector.getKey(s).get -> s.getNativeScan.getCommon.toByteArray
    }.toMap
    val partByKey = scans.zipWithIndex.map { case (s, i) =>
      NativeScanPlanDataInjector.getKey(s).get -> nativeScanPartitionBytes(s"part-$i.parquet")
    }.toMap
    assert(commonByKey.size == n)

    val first =
      PlanDataInjector.injectPlanData(parseBasePlan(root.toByteArray), commonByKey, partByKey)
    val second =
      PlanDataInjector.injectPlanData(parseBasePlan(root.toByteArray), commonByKey, partByKey)

    (0 until n).foreach { i =>
      assert(
        first.getChildren(i).getNativeScan.getCommon eq
          second.getChildren(i).getNativeScan.getCommon,
        s"scan $i must reuse the prepared common held by the plan's cache entry")
    }
  }

  test("shuffle-path injection shares prepared commons across a shuffle's map tasks") {
    // The native shuffle writer's unified plan differs per task, so it cannot share a base plan
    // cache entry; prepared commons are scoped to the shuffleId instead.
    val scanOp = nativeScanOp("file:///shuffle-share-tbl", Seq("id", "v"))
    val key = NativeScanPlanDataInjector.getKey(scanOp).get
    val common = scanOp.getNativeScan.getCommon

    val task1 = PlanDataInjector.injectPlanDataForShuffle(
      1234567,
      scanOp,
      Map(key -> common.toByteArray),
      Map(key -> nativeScanPartitionBytes("map-0.parquet")))
    val task2 = PlanDataInjector.injectPlanDataForShuffle(
      1234567,
      scanOp,
      Map(key -> common.toByteArray),
      Map(key -> nativeScanPartitionBytes("map-1.parquet")))

    assert(
      task1.getNativeScan.getCommon eq task2.getNativeScan.getCommon,
      "one shuffle's map tasks must share the prepared common, not re-parse it")
    assert(task1.getNativeScan.getCommon == common)
  }

  /** Injects one scan under `shuffleId` the way a map task would, returning its scan key. */
  private def injectShuffleScan(shuffleId: Int, source: String): String = {
    val scanOp = nativeScanOp(source, Seq("id"))
    val key = NativeScanPlanDataInjector.getKey(scanOp).get
    PlanDataInjector.injectPlanDataForShuffle(
      shuffleId,
      scanOp,
      Map(key -> scanOp.getNativeScan.getCommon.toByteArray),
      Map(key -> nativeScanPartitionBytes("map-0.parquet")))
    PlanDataInjector.preparedKey(NativeScanPlanDataInjector, key)
  }

  test("the shuffle store evicts the oldest shuffle beyond maxCachedShuffles") {
    PlanDataInjector.releaseAll()
    val first = injectShuffleScan(100, "file:///evict-shuffle-first")
    (1 to PlanDataInjector.maxCachedShuffles).foreach { i =>
      injectShuffleScan(100 + i, s"file:///evict-shuffle-$i")
    }
    val snapshot = PlanDataInjector.preparedShuffleSnapshot
    assert(snapshot.size == PlanDataInjector.maxCachedShuffles)
    assert(!snapshot.contains(100), s"shuffle 100 should have been evicted, still holds $first")
  }

  private object IntInjector extends PlanDataInjector {
    override type Prepared = java.lang.Integer
    override val opStructCase: Operator.OpStructCase = Operator.OpStructCase.CONTRIB_SCAN
    override def canInject(op: Operator): Boolean = false
    override def getKey(op: Operator): Option[String] = None
    override def prepareCommon(commonBytes: Array[Byte]): Prepared = commonBytes.length
    override def inject(op: Operator, prepared: Prepared, partitionBytes: Array[Byte]): Operator =
      op
  }

  private object StringInjector extends PlanDataInjector {
    override type Prepared = String
    override val opStructCase: Operator.OpStructCase = Operator.OpStructCase.CONTRIB_SCAN
    override def canInject(op: Operator): Boolean = false
    override def getKey(op: Operator): Option[String] = None
    override def prepareCommon(commonBytes: Array[Byte]): Prepared =
      new String(commonBytes, java.nio.charset.StandardCharsets.UTF_8)
    override def inject(op: Operator, prepared: Prepared, partitionBytes: Array[Byte]): Operator =
      op
  }

  test("two injectors agreeing on a key and bytes keep separate prepared commons") {
    // Every contrib scan arrives as the same CONTRIB_SCAN envelope, so two injectors can agree on
    // a scan key and receive byte-identical commons; each must still get its own prepared object.
    import java.util.concurrent.ConcurrentHashMap
    val memo = new ConcurrentHashMap[String, PlanDataInjector.PreparedCommon]()
    val bytes = "shared".getBytes(java.nio.charset.StandardCharsets.UTF_8)

    val asInt: java.lang.Integer = PlanDataInjector.prepareShared(IntInjector, "k", bytes, memo)
    val asString: String = PlanDataInjector.prepareShared(StringInjector, "k", bytes, memo)

    assert(asInt == bytes.length)
    assert(asString == "shared")
    assert(memo.size == 2, "each injector must own its own memo slot under the shared key")
    assert(
      PlanDataInjector.prepareShared(StringInjector, "k", bytes, memo) eq asString,
      "a repeat lookup must serve the injector's own prepared object")
  }

  test("recreated context does not accumulate prepared commons under reused shuffle ids") {
    // Shuffle ids restart at zero for every SparkContext in a JVM, so a local or embedded caller
    // that stops and recreates its context reuses ids the previous context already cached under.
    // The manager's stop() is the boundary where the old context's prepared data must go.
    PlanDataInjector.releaseAll()
    val firstContext = new CometShuffleManager(new SparkConf(false))
    val a = injectShuffleScan(0, "file:///recreated-ctx-a")
    val b = injectShuffleScan(0, "file:///recreated-ctx-b")
    assert(PlanDataInjector.preparedShuffleSnapshot(0) == Set(a, b))
    parseBasePlan(nativeScanOp("file:///recreated-ctx-plan", Seq("id")).toByteArray)
    assert(PlanDataInjector.basePlanSnapshot.nonEmpty)

    firstContext.stop()

    assert(
      PlanDataInjector.basePlanSnapshot.isEmpty,
      "stopping the manager must drop the base plan cache with the shuffle store")
    val c = injectShuffleScan(0, "file:///recreated-ctx-c")
    val d = injectShuffleScan(0, "file:///recreated-ctx-d")
    assert(
      PlanDataInjector.preparedShuffleSnapshot(0) == Set(c, d),
      "shuffle 0 must hold only the new context's scans, not the stopped context's as well")
  }

  test("unregisterShuffle releases only that shuffle's prepared commons") {
    PlanDataInjector.releaseAll()
    val manager = new CometShuffleManager(new SparkConf(false))
    val gone = injectShuffleScan(7, "file:///unregister-gone")
    val kept = injectShuffleScan(8, "file:///unregister-kept")
    assert(PlanDataInjector.preparedShuffleSnapshot == Map(7 -> Set(gone), 8 -> Set(kept)))

    manager.unregisterShuffle(7)

    assert(
      PlanDataInjector.preparedShuffleSnapshot == Map(8 -> Set(kept)),
      "unregistering shuffle 7 must drop exactly its entry")
  }

  test("a changed finalized common under the same key is replaced, not served stale") {
    // Scalar-subquery data filters are appended to the finalized common after planning
    // (CometNativeScanExec.serializedPartitionData), so a byte-identical base plan can ship
    // different finalized commons under the same transported key across executions.
    val scanOp = nativeScanOp("file:///stale-common-tbl", Seq("id"))
    val key = NativeScanPlanDataInjector.getKey(scanOp).get
    val baseCommon = scanOp.getNativeScan.getCommon
    val finalizedCommon = baseCommon.toBuilder
      .addDataFilters(org.apache.comet.serde.ExprOuterClass.Expr.newBuilder())
      .build()
    assert(baseCommon != finalizedCommon)

    val cached = parseBasePlan(scanOp.toByteArray)
    val firstRun = PlanDataInjector.injectPlanData(
      cached,
      Map(key -> baseCommon.toByteArray),
      Map(key -> nativeScanPartitionBytes("run-1.parquet")))
    val secondRun = PlanDataInjector.injectPlanData(
      cached,
      Map(key -> finalizedCommon.toByteArray),
      Map(key -> nativeScanPartitionBytes("run-2.parquet")))

    assert(firstRun.getNativeScan.getCommon == baseCommon)
    assert(
      secondRun.getNativeScan.getCommon == finalizedCommon,
      "changed common bytes under the same key must be re-prepared, never served stale")
  }

  test("NativeScan inject keeps different commons separate") {
    val scanA = nativeScanOp("file:///separate-tbl-a", Seq("a"))
    val scanB = nativeScanOp("file:///separate-tbl-b", Seq("b"))
    val commonA = nativeScanCommon("file:///separate-tbl-a", Seq("a"))
    val commonB = nativeScanCommon("file:///separate-tbl-b", Seq("b"))
    val keyA = NativeScanPlanDataInjector.getKey(scanA).get
    val keyB = NativeScanPlanDataInjector.getKey(scanB).get
    assert(keyA != keyB)

    val commonByKey = Map(keyA -> commonA.toByteArray, keyB -> commonB.toByteArray)
    val partByKey = Map(
      keyA -> nativeScanPartitionBytes("a.parquet"),
      keyB -> nativeScanPartitionBytes("b.parquet"))

    val injectedA =
      PlanDataInjector.injectPlanData(parseBasePlan(scanA.toByteArray), commonByKey, partByKey)
    val injectedB =
      PlanDataInjector.injectPlanData(parseBasePlan(scanB.toByteArray), commonByKey, partByKey)

    assert(injectedA.getNativeScan.getCommon == commonA)
    assert(injectedB.getNativeScan.getCommon == commonB)
  }

  test("NativeScan getKey rebuilds the key from the source and the transported hash") {
    // Only the hash travels in the plan; the source is already in the common next to it.
    val common = nativeScanCommon("file:///transported-tbl", Seq("id", "v"))
    val op = Operator
      .newBuilder()
      .setNativeScan(
        OperatorOuterClass.NativeScan
          .newBuilder()
          .setCommon(common)
          .setSourceKeyHash(1234))
      .build()

    assert(NativeScanPlanDataInjector.getKey(op).contains("file:///transported-tbl_1234"))
    assert(
      NativeScanPlanDataInjector.sourceKey(common) ==
        s"file:///transported-tbl_${NativeScanPlanDataInjector.sourceKeyHash(common)}",
      "the driver-side key must be the same source and hash the executor rebuilds")
  }

  test("NativeScan getKey treats a zero hash as transported, not absent") {
    val common = nativeScanCommon("file:///zero-hash-tbl", Seq("id"))
    val op = Operator
      .newBuilder()
      .setNativeScan(
        OperatorOuterClass.NativeScan.newBuilder().setCommon(common).setSourceKeyHash(0))
      .build()

    assert(NativeScanPlanDataInjector.getKey(op).contains("file:///zero-hash-tbl_0"))
  }

  test("NativeScan getKey derives the key only when the plan carries none") {
    val common = nativeScanCommon("file:///fallback-tbl", Seq("id", "v", "w"))
    val op = Operator
      .newBuilder()
      .setNativeScan(OperatorOuterClass.NativeScan.newBuilder().setCommon(common))
      .build()

    val derived = NativeScanPlanDataInjector.sourceKey(common)
    assert(NativeScanPlanDataInjector.getKey(op).contains(derived))
  }

  test("direct injection without a cached base plan looks up by the transported key") {
    // CometNativeShuffleWriter injects into a per-task plan built around spec.childNativeOp
    // without going through parseBasePlan, so the lookup must ride the transported key alone.
    val common = nativeScanCommon("file:///shuffle-tbl", Seq("id"))
    val op = Operator
      .newBuilder()
      .setNativeScan(
        OperatorOuterClass.NativeScan.newBuilder().setCommon(common).setSourceKeyHash(77))
      .build()
    val key = "file:///shuffle-tbl_77"

    val injected = PlanDataInjector.injectPlanDataForShuffle(
      424242,
      op,
      Map(key -> common.toByteArray),
      Map(key -> nativeScanPartitionBytes("shuffled.parquet")))

    assert(injected.getNativeScan.getCommon == common)
    assert(
      injected.getNativeScan.getFilePartition
        .getPartitionedFile(0)
        .getFilePath == "shuffled.parquet")
  }

  test(
    "self-join: scans sharing a metadataLocation but differing scan_hash_code inject their " +
      "own data, not each other's") {
    val targetOp = icebergScanOp("s3://table/metadata/v1.json", scanHashCode = 111)
    val sourceOp = icebergScanOp("s3://table/metadata/v1.json", scanHashCode = 222)

    val (targetCommon, targetPartition) =
      icebergPlanData(
        "s3://table/metadata/v1.json",
        scanHashCode = 111,
        columnNames = Seq("id", "v", "_file", "_pos"),
        dataFilePath = "target.parquet")
    val (sourceCommon, sourcePartition) =
      icebergPlanData(
        "s3://table/metadata/v1.json",
        scanHashCode = 222,
        columnNames = Seq("id", "v"),
        dataFilePath = "source.parquet")

    val targetKey = IcebergPlanDataInjector.getKey(targetOp).get
    val sourceKey = IcebergPlanDataInjector.getKey(sourceOp).get
    val commonByKey = Map(targetKey -> targetCommon, sourceKey -> sourceCommon)
    val partitionByKey = Map(targetKey -> targetPartition, sourceKey -> sourcePartition)

    val injectedTarget = PlanDataInjector.injectPlanData(
      parseBasePlan(targetOp.toByteArray),
      commonByKey,
      partitionByKey)
    val injectedSource = PlanDataInjector.injectPlanData(
      parseBasePlan(sourceOp.toByteArray),
      commonByKey,
      partitionByKey)

    assert(
      injectedTarget.getIcebergScan.getCommon.getRequiredSchemaList
        .get(0)
        .getName == "id")
    assert(injectedTarget.getIcebergScan.getCommon.getRequiredSchemaCount == 4)
    assert(injectedTarget.getIcebergScan.getFileScanTasks(0).getDataFilePath == "target.parquet")

    assert(injectedSource.getIcebergScan.getCommon.getRequiredSchemaCount == 2)
    assert(injectedSource.getIcebergScan.getFileScanTasks(0).getDataFilePath == "source.parquet")
  }
}
