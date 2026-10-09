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

import java.io.File
import java.net.URLClassLoader
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

import org.apache.comet.CometRuntimeException
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Unit coverage for the core SPI that lets out-of-tree contrib leaf scans (e.g. the Delta
 * contrib's `CometDeltaNativeScanExec`) participate in Comet native planning without core holding
 * a compile-time reference to them:
 *
 *   - the [[CometScanWithPlanData]] trait contract and its collection through
 *     [[PlanDataInjector.findAllPlanData]], and
 *   - the `ServiceLoader`-based discovery of contrib [[PlanDataInjector]]s in
 *     [[PlanDataInjector]]'s registry.
 *
 * Deliberately does not exercise the per-op injector registry mechanics; that surface is owned by
 * `PlanDataInjectorSuite`.
 */
class CometScanWithPlanDataSuite extends AnyFunSuite {

  /**
   * Minimal `CometLeafExec with CometScanWithPlanData` (the trait's `self: CometLeafExec`
   * self-type forces the leaf base). It opts into none of the optional DPP behaviour and records
   * whether the subquery-resolution lifecycle hook was driven, so a test can confirm
   * `findAllPlanData` routes a non-`CometNativeScanExec` trait scan through it.
   */
  private case class StubScan(
      override val sourceKey: String = "stub-key",
      common: Array[Byte] = Array.emptyByteArray,
      perPart: Array[Array[Byte]] = Array.empty)
      extends CometLeafExec
      with CometScanWithPlanData {

    @transient var subqueriesResolved: Boolean = false

    // Real CometLeafExec.ensureSubqueriesResolved drives Spark's prepare/waitForSubqueries, which
    // needs a live session; for this pure unit test we record the call instead. The behaviour
    // under test is that findAllPlanData *invokes* the hook for a trait scan, not the hook itself
    // (CometLeafExec.ensureSubqueriesResolved is exercised by the operator suites end to end).
    override def ensureSubqueriesResolved(): Unit = { subqueriesResolved = true }

    // findAllPlanData only reads the trait members + the lifecycle hook; originalPlan (used by
    // CometExec for output/ordering/partitioning) is never touched on this path.
    override def output: Seq[Attribute] = Seq.empty
    override def originalPlan: SparkPlan = null
    override def nativeOp: Operator = Operator.getDefaultInstance
    override def serializedPlanOpt: SerializedPlan = SerializedPlan(None)
    override def commonData: Array[Byte] = common
    override def perPartitionData: Array[Array[Byte]] = perPart
  }

  /** A non-boundary parent, like a join, that puts two scans in one native plan. */
  private case class StubParent(left: SparkPlan, right: SparkPlan) extends BinaryExecNode {
    override def output: Seq[Attribute] = Seq.empty
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("not executed")
    override protected def withNewChildrenInternal(
        newLeft: SparkPlan,
        newRight: SparkPlan): StubParent = copy(left = newLeft, right = newRight)
  }

  test(
    "CometScanWithPlanData defaults: no DPP filters and withDynamicPruningFilters is unsupported") {
    val scan = StubScan()
    assert(scan.dynamicPruningFilters == Nil, "a scan must expose no DPP filters by default")
    // The default exists only so the DPP rule never calls it for scans that leave
    // dynamicPruningFilters empty; if a scan does expose filters it must override this.
    val ex = intercept[UnsupportedOperationException] {
      scan.withDynamicPruningFilters(Seq.empty[Expression])
    }
    assert(
      ex.getMessage.contains("withDynamicPruningFilters"),
      s"default failure must name the un-overridden method, got: ${ex.getMessage}")
  }

  test("findAllPlanData collects data from a non-CometNativeScanExec trait scan") {
    // The whole point of the SPI: a brand-new leaf scan that core has never heard of, surfacing
    // its data only through CometScanWithPlanData, is collected by findAllPlanData. This exercises
    // the generic trait arm independently of CometNativeScanExec / CometIcebergNativeScanExec.
    val common = Array[Byte](1, 2, 3)
    val perPart = Array(Array[Byte](10), Array[Byte](20))
    val scan = StubScan("contrib-key", common, perPart)

    val (commonByKey, perPartitionByKey) = PlanDataInjector.findAllPlanData(scan)

    assert(commonByKey.keySet == Set("contrib-key"))
    assert(commonByKey("contrib-key") sameElements common)
    assert(perPartitionByKey.keySet == Set("contrib-key"))
    assert(perPartitionByKey("contrib-key").map(_.toSeq).toSeq == perPart.map(_.toSeq).toSeq)
    assert(
      scan.subqueriesResolved,
      "findAllPlanData must drive the scan's subquery-resolution lifecycle before reading data")
  }

  test("findAllPlanData skips a trait scan that surfaces no data") {
    // Mirrors the Iceberg arm: an implementation that returns empty common/per-partition data
    // contributes nothing, rather than an empty-array entry that would fail downstream injection.
    val scan = StubScan("empty-key")
    val (commonByKey, perPartitionByKey) = PlanDataInjector.findAllPlanData(scan)
    assert(commonByKey.isEmpty)
    assert(perPartitionByKey.isEmpty)
  }

  test("findAllPlanData keeps one copy when two scans carry the same data under one key") {
    val left = StubScan("shared-key", Array[Byte](1, 2), Array(Array[Byte](10), Array[Byte](20)))
    val right = StubScan("shared-key", Array[Byte](1, 2), Array(Array[Byte](10), Array[Byte](20)))

    val (commonByKey, perPartitionByKey) =
      PlanDataInjector.findAllPlanData(StubParent(left, right))

    assert(commonByKey.keySet == Set("shared-key"))
    assert(commonByKey("shared-key") sameElements Array[Byte](1, 2))
    assert(
      perPartitionByKey("shared-key").map(_.toSeq).toSeq == Seq(Seq[Byte](10), Seq[Byte](20)))
  }

  test("findAllPlanData rejects two scans with different data under one key") {
    // Keeping either side would hand both scans the same files and return wrong rows.
    val base = StubScan("clash-key", Array[Byte](1), Array(Array[Byte](10), Array[Byte](20)))
    val otherFiles = base.copy(perPart = Array(Array[Byte](10), Array[Byte](30)))
    val otherCommon = base.copy(common = Array[Byte](2))

    Seq(otherFiles, otherCommon).foreach { other =>
      val e = intercept[CometRuntimeException] {
        PlanDataInjector.findAllPlanData(StubParent(base, other))
      }
      assert(e.getMessage.contains("clash-key"), s"message should name the key: ${e.getMessage}")
    }
  }

  test("PlanDataInjector registry contains only built-in injectors on a default build") {
    // No contrib ships a META-INF/services/...PlanDataInjector on the default test classpath, so
    // ServiceLoader discovery adds nothing and the registry is exactly the two built-ins.
    val injectors = PlanDataInjector.injectors
    assert(
      injectors.contains(IcebergPlanDataInjector),
      "registry must keep the built-in Iceberg injector")
    assert(
      injectors.contains(NativeScanPlanDataInjector),
      "registry must keep the built-in native-scan injector")
    assert(
      injectors.forall(i => i == IcebergPlanDataInjector || i == NativeScanPlanDataInjector),
      s"default build must carry no contrib injectors, got: ${injectors.map(_.getClass.getName)}")
  }

  test("a contrib PlanDataInjector is discovered via ServiceLoader (META-INF/services)") {
    // Proves the discovery contract a contrib relies on: dropping a service file naming a
    // PlanDataInjector implementation makes it visible to ServiceLoader -- no core change. We use
    // an isolated child classloader carrying only the service file so the global registry (asserted
    // built-ins-only above) is untouched.
    // The scalatest plugin points java.io.tmpdir at spark/target/tmp, which may not exist yet;
    // ensure it before createTempDirectory (which requires its parent to exist).
    val baseTmp = new File(System.getProperty("java.io.tmpdir"))
    baseTmp.mkdirs()
    val svcDir = Files.createTempDirectory(baseTmp.toPath, "comet-spi-test").toFile
    try {
      val servicesDir = new File(svcDir, "META-INF/services")
      assert(servicesDir.mkdirs(), s"could not create $servicesDir")
      Files.write(
        new File(servicesDir, classOf[PlanDataInjector].getName).toPath,
        (classOf[TestStubPlanDataInjector].getName + "\n").getBytes(StandardCharsets.UTF_8))

      val loader = new URLClassLoader(Array(svcDir.toURI.toURL), getClass.getClassLoader)
      val discovered =
        ServiceLoader.load(classOf[PlanDataInjector], loader).asScala.toSeq
      assert(
        discovered.exists(_.isInstanceOf[TestStubPlanDataInjector]),
        s"ServiceLoader should discover the stub, got: ${discovered.map(_.getClass.getName)}")
    } finally {
      def del(f: File): Unit = {
        Option(f.listFiles()).foreach(_.foreach(del))
        f.delete()
      }
      del(svcDir)
    }
  }
}

/**
 * Top-level public no-arg `PlanDataInjector` so `ServiceLoader` can instantiate it (Scala
 * `object`s have a private constructor and would not be instantiable). Inert: it claims no
 * operator kind.
 */
class TestStubPlanDataInjector extends PlanDataInjector {
  override def opStructCase: Operator.OpStructCase = Operator.OpStructCase.OPSTRUCT_NOT_SET
  override def canInject(op: Operator): Boolean = false
  override def getKey(op: Operator): Option[String] = None
  override type Prepared = Array[Byte]
  override def prepareCommon(commonBytes: Array[Byte]): Prepared = commonBytes
  override def inject(
      op: Operator,
      preparedCommon: Prepared,
      partitionBytes: Array[Byte]): Operator =
    op
}
