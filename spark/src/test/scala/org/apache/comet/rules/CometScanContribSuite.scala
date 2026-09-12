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

package org.apache.comet.rules

import java.io.File
import java.net.URLClassLoader
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.ServiceLoader

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.comet.util.ClassLoaders

/**
 * Unit coverage for [[CometScanContrib]] -- the core, format-agnostic hook that lets an optional
 * out-of-tree contrib (Delta, Lance, ...) claim a scan before Comet's built-in scan handling
 * runs.
 *
 * The contract under test is the registry's, not any contrib's:
 *   - a default build registers no contrib, so both hooks return `None` and cost nothing;
 *   - a contrib dropped on the classpath via `META-INF/services` is discovered and its claim is
 *     returned;
 *   - the first claim wins and later contribs are not consulted;
 *   - a contrib that throws is treated as having declined, so the next contrib still gets a look
 *     and the query is never failed by speculative contrib planning.
 *
 * Sibling to `CometScanWithPlanDataSuite`, which covers the other half of the contrib SPI (plan
 * data). Everything here runs on the default build.
 */
class CometScanContribSuite extends AnyFunSuite {

  /**
   * Dispatch reads only the hooks' return values, never the scan itself, so the arguments are
   * irrelevant to what is being tested here -- a `FileSourceScanExec`/`BatchScanExec` would have
   * to be built against a live session purely to be ignored. Passing nulls keeps the suite a pure
   * unit test and, incidentally, pins that core does not touch the scan on the way to a contrib.
   * Contrib-side handling of a real scan is covered by the contrib's own suites.
   */
  private def offerV1(
      contribs: Seq[CometScanContrib],
      detectConflicts: Boolean = false): Option[SparkPlan] =
    CometScanContrib.firstClaimFrom(contribs, detectConflicts)(
      _.tryTransformV1(
        null.asInstanceOf[SparkPlan],
        null.asInstanceOf[SparkSession],
        null.asInstanceOf[FileSourceScanExec],
        null.asInstanceOf[HadoopFsRelation]))

  private def offerV2(
      contribs: Seq[CometScanContrib],
      detectConflicts: Boolean = false): Option[SparkPlan] =
    CometScanContrib.firstClaimFrom(contribs, detectConflicts)(
      _.tryTransformV2(null.asInstanceOf[BatchScanExec]))

  test("a default build registers no contribs and both hooks return None") {
    // Assert on raw discovery first, not just on the registry. `contribs` swallows a
    // ServiceConfigurationError and yields an empty registry -- correct at runtime (a misbuilt
    // contrib jar must not take down the planner) but it makes "the registry is empty" ambiguous:
    // it holds both when nothing is registered and when something is registered but unloadable.
    // Loading directly distinguishes the two, so a stray service file on the test classpath (e.g.
    // left in target/classes by an earlier -Pcontrib-delta build) fails loudly here instead of
    // passing vacuously below.
    val discovered =
      ServiceLoader.load(classOf[CometScanContrib], getClass.getClassLoader).asScala.toSeq
    assert(
      discovered.isEmpty,
      s"default build must ship no contrib service file, got: ${discovered.map(_.getClass.getName)}")

    // No contrib ships a META-INF/services/org.apache.comet.rules.CometScanContrib on a default
    // build, so the registry is empty and there is zero contrib surface at runtime.
    assert(
      CometScanContrib.contribs.isEmpty,
      "default build must register no contribs, got: " +
        CometScanContrib.contribs.map(_.getClass.getName))

    // With an empty registry the hooks short-circuit without dereferencing the scan, which is why
    // nulls are safe here -- and is the property that makes the hook free on a default build.
    assert(CometScanContrib.tryTransformV1(null, null, null, null).isEmpty)
    assert(CometScanContrib.tryTransformV2(null).isEmpty)
  }

  test("a contrib registered via META-INF/services is discovered and its claim is returned") {
    // Proves the whole registration contract a contrib depends on: dropping a service file naming
    // an implementation makes it visible to ServiceLoader, and a Some(...) it returns is what the
    // scan rule gets back -- no core change, no compile-time reference to the contrib.
    withServiceFile(Seq(classOf[ClaimingScanContrib].getName)) { loader =>
      val discovered = CometScanContrib.loadContribs(loader)
      assert(
        discovered.exists(_.isInstanceOf[ClaimingScanContrib]),
        s"ServiceLoader should discover the stub, got: ${discovered.map(_.getClass.getName)}")

      assert(offerV1(discovered).contains(ContribStubs.ClaimedByV1))
      assert(offerV2(discovered).contains(ContribStubs.ClaimedByV2))
    }
  }

  test("an unusable provider is skipped and the usable ones still register") {
    // Two defects, one test. (1) `ServiceLoader.asScala.toSeq` is a lazy `Stream` on Scala 2.12,
    // so the provider iterator was forced OUTSIDE the discovery `try` and a bad entry threw
    // `ServiceConfigurationError` from the middle of scan planning. (2) Catching around the whole
    // traversal fixes that but discards EVERY provider when any one fails -- with Delta and Lance
    // both registered, one misbuilt jar would silently disable the other.
    //
    // `ContribServices` iterates defensively: catch per step, skip the bad provider, keep the good
    // ones. Checked in both orderings and for each realistic way an entry can be unusable, since
    // they surface at different points (`hasNext` rejects an unloadable or mistyped class, `next`
    // fails construction).
    val good = classOf[ClaimingScanContrib].getName
    val second = classOf[RecordingClaimingScanContrib].getName
    for (bad <- Seq(
        "org.apache.comet.rules.NoSuchContribClass",
        classOf[NotAContribAtAll].getName,
        classOf[ThrowingCtorScanContrib].getName,
        classOf[NoNoArgCtorScanContrib].getName)) {
      for ((label, names) <- Seq("bad last" -> Seq(good, bad), "bad first" -> Seq(bad, good))) {
        withServiceFile(names) { loader =>
          val discovered = CometScanContrib.loadContribs(loader)
          assert(
            discovered.exists(_.isInstanceOf[ClaimingScanContrib]),
            s"$bad / $label: the usable provider must survive, got: " +
              discovered.map(_.getClass.getName))
          assert(
            offerV1(discovered).contains(ContribStubs.ClaimedByV1),
            s"$bad / $label: the surviving provider must still be dispatchable")
        }
      }
      withServiceFile(Seq(good, bad, second)) { loader =>
        val discovered = CometScanContrib.loadContribs(loader)
        assert(
          discovered.size == 2,
          s"$bad / bad in the middle: both usable providers must survive, got: " +
            discovered.map(_.getClass.getName))
      }
    }
  }

  test("discovery uses the thread context ClassLoader when one is set") {
    // Comet is normally installed on the driver's extraClassPath while a contrib arrives through
    // `--jars`, which puts the contrib on Spark's user-jar loader -- a CHILD of Comet's own
    // defining loader. Discovering from the defining loader would silently miss it.
    val original = Thread.currentThread().getContextClassLoader
    try {
      withServiceFile(Seq(classOf[ClaimingScanContrib].getName)) { childLoader =>
        Thread.currentThread().setContextClassLoader(childLoader)
        assert(
          ClassLoaders.contextOrDefault(getClass.getClassLoader) eq childLoader,
          "discovery must start from the context ClassLoader")
        val discovered =
          CometScanContrib.loadContribs(ClassLoaders.contextOrDefault(getClass.getClassLoader))
        assert(
          discovered.exists(_.isInstanceOf[ClaimingScanContrib]),
          "a contrib visible only to the context ClassLoader must still be discovered")
      }
    } finally {
      Thread.currentThread().setContextClassLoader(original)
    }
  }

  test("discovery falls back to the defining ClassLoader when no context loader is set") {
    val original = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(null)
      assert(ClassLoaders.contextOrDefault(getClass.getClassLoader) eq getClass.getClassLoader)
    } finally {
      Thread.currentThread().setContextClassLoader(original)
    }
  }

  test("a contrib that declines passes the scan to the next contrib") {
    // Both hooks default to None, so DecliningScanContrib claims nothing and the claim must come
    // from the contrib behind it. This is the pass-through the ownership contract relies on.
    val contribs = Seq(new DecliningScanContrib, new ClaimingScanContrib)
    assert(offerV1(contribs).contains(ContribStubs.ClaimedByV1))
    assert(offerV2(contribs).contains(ContribStubs.ClaimedByV2))
  }

  test("the first claim wins and later contribs are not consulted") {
    // Registration order decides, and a claimed scan is never offered again -- the property that
    // makes the "MUST return None for a scan you do not own" contract load-bearing.
    val second = new RecordingClaimingScanContrib
    assert(offerV1(Seq(new ClaimingScanContrib, second)).contains(ContribStubs.ClaimedByV1))
    assert(!second.wasConsulted, "a contrib after the claiming one must not be consulted")
  }

  test("conflict detection consults every contrib but still uses the first claim") {
    // Off by default, later contribs are never asked (pinned by the test above). With detection
    // on, everyone is asked so a contract violation is visible in the log -- but the winner is
    // unchanged, so turning it on cannot alter query behaviour.
    val second = new RecordingClaimingScanContrib
    assert(
      offerV1(Seq(new ClaimingScanContrib, second), detectConflicts = true)
        .contains(ContribStubs.ClaimedByV1),
      "the first claim must still win when conflict detection is on")
    assert(second.wasConsulted, "conflict detection must consult contribs after the first claim")
  }

  test("conflict detection still declines when nothing claims") {
    val contribs = Seq(new DecliningScanContrib, new DecliningScanContrib)
    assert(offerV1(contribs, detectConflicts = true).isEmpty)
    assert(offerV2(contribs, detectConflicts = true).isEmpty)
  }

  test("a contrib that throws is declined so the next contrib still gets a look") {
    // Contrib planning is speculative and can fail for reasons outside the query (unreachable
    // object store, unknown metadata version, version-skewed reflection). None of those may turn
    // a runnable query into a failed one, and none may hide a later contrib that can read it.
    val contribs = Seq(new ThrowingScanContrib, new ClaimingScanContrib)
    assert(offerV1(contribs).contains(ContribStubs.ClaimedByV1))
    assert(offerV2(contribs).contains(ContribStubs.ClaimedByV2))
  }

  test("a throwing contrib with nothing behind it declines rather than failing the query") {
    val contribs = Seq(new ThrowingScanContrib)
    assert(offerV1(contribs).isEmpty, "the scan must fall through to Comet's built-in handling")
    assert(offerV2(contribs).isEmpty)
  }

  test("a fatal error from a contrib is not swallowed") {
    // OutOfMemoryError is neither NonFatal nor a LinkageError: it signals real JVM-level
    // exhaustion, not a version-skewed contrib jar, and must always propagate uncontained.
    val contribs = Seq(new FatalScanContrib)
    intercept[OutOfMemoryError](offerV1(contribs))
  }

  test(
    "a LinkageError from a contrib is contained, logged by name, and the next contrib still " +
      "gets a look") {
    // A version-skewed contrib jar (compiled against a Comet internal that has since moved or
    // been removed) throws NoSuchMethodError/NoClassDefFoundError -- a LinkageError, which
    // NonFatal does not match. It must be contained the same way a NonFatal decline is: logged,
    // treated as "does not claim this scan", and the next contrib still consulted.
    val contribs = Seq(new VersionSkewedScanContrib, new ClaimingScanContrib)
    val events = withCapturedLogEvents(classOf[CometScanContrib].getName) {
      assert(offerV1(contribs).contains(ContribStubs.ClaimedByV1))
      assert(offerV2(contribs).contains(ContribStubs.ClaimedByV2))
    }
    val messages = events.map(_.getMessage.getFormattedMessage)
    assert(
      messages.count(m =>
        m.contains(classOf[VersionSkewedScanContrib].getName) &&
          m.contains(classOf[NoSuchMethodError].getName)) == 2,
      "expected one warning per hook naming both the contrib class and the LinkageError " +
        s"subtype, got: $messages")
  }

  test("a LinkageError with nothing behind it declines rather than failing the query") {
    val contribs = Seq(new VersionSkewedScanContrib)
    assert(offerV1(contribs).isEmpty, "the scan must fall through to Comet's built-in handling")
    assert(offerV2(contribs).isEmpty)
  }

  /**
   * Attaches a minimal Log4j2 appender directly to the logger named `loggerName` for the duration
   * of `f`, returning every event it captured. `CometScanContrib`'s `logWarning` calls go through
   * Spark's `Logging` trait to a logger named after the emitting class, so this lets a test
   * assert a specific warning was actually emitted -- not merely that the surrounding code path
   * didn't throw. Restores the logger's prior appenders/level afterward so this cannot leak into
   * other tests in the same JVM.
   */
  private def withCapturedLogEvents(loggerName: String)(f: => Unit): Seq[LogEvent] = {
    val logger =
      LogManager.getLogger(loggerName).asInstanceOf[org.apache.logging.log4j.core.Logger]
    val appender = new CapturingAppender(s"CometScanContribSuite-${System.nanoTime()}")
    appender.start()
    val originalLevel = logger.getLevel
    logger.addAppender(appender)
    logger.setLevel(org.apache.logging.log4j.Level.WARN)
    try {
      f
      appender.events.toSeq
    } finally {
      logger.removeAppender(appender)
      logger.setLevel(originalLevel)
      appender.stop()
    }
  }

  /**
   * Writes a `META-INF/services/org.apache.comet.rules.CometScanContrib` naming `impls` into a
   * temp directory, loads it through an isolated child `URLClassLoader`, and hands the discovered
   * instances to `f`. The child loader keeps the discovery out of the JVM-wide registry that the
   * default-build test above asserts is empty.
   */
  private def withServiceFile(implNames: Seq[String])(f: ClassLoader => Unit): Unit = {
    // The scalatest plugin points java.io.tmpdir at spark/target/tmp, which may not exist yet;
    // ensure it before createTempDirectory (which requires its parent to exist).
    val baseTmp = new File(System.getProperty("java.io.tmpdir"))
    baseTmp.mkdirs()
    val svcDir = Files.createTempDirectory(baseTmp.toPath, "comet-scan-contrib-test").toFile
    try {
      val servicesDir = new File(svcDir, "META-INF/services")
      assert(servicesDir.mkdirs(), s"could not create $servicesDir")
      Files.write(
        new File(servicesDir, classOf[CometScanContrib].getName).toPath,
        implNames.mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8))

      f(new URLClassLoader(Array(svcDir.toURI.toURL), getClass.getClassLoader))
    } finally {
      def del(file: File): Unit = {
        Option(file.listFiles()).foreach(_.foreach(del))
        file.delete()
      }
      del(svcDir)
    }
  }
}

/** Inert leaf plan standing in for whatever node a real contrib would return. */
case class StubContribScanExec(tag: String) extends LeafExecNode {
  override def output: Seq[Attribute] = Seq.empty
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("stub plan is never executed")
}

/** The two sentinel claims, shared by the stub contribs and the assertions. */
object ContribStubs {
  val ClaimedByV1: StubContribScanExec = StubContribScanExec("v1")
  val ClaimedByV2: StubContribScanExec = StubContribScanExec("v2")
}

// The stubs below are top-level and public with a no-arg constructor so ServiceLoader can
// instantiate them (a Scala `object` has a private constructor and would not be instantiable).

/** Claims every scan it is offered. */
class ClaimingScanContrib extends CometScanContrib {
  override def tryTransformV1(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      relation: HadoopFsRelation): Option[SparkPlan] = Some(ContribStubs.ClaimedByV1)

  override def tryTransformV2(scanExec: BatchScanExec): Option[SparkPlan] = Some(
    ContribStubs.ClaimedByV2)
}

/** Claims every scan, and records that it was asked at all. */
class RecordingClaimingScanContrib extends ClaimingScanContrib {
  @volatile var wasConsulted: Boolean = false

  override def tryTransformV1(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      relation: HadoopFsRelation): Option[SparkPlan] = {
    wasConsulted = true
    super.tryTransformV1(plan, session, scanExec, relation)
  }

  override def tryTransformV2(scanExec: BatchScanExec): Option[SparkPlan] = {
    wasConsulted = true
    super.tryTransformV2(scanExec)
  }
}

/** Named by a service file but does not implement the service; `hasNext` rejects it. */
class NotAContribAtAll

/** Implements the service but cannot be constructed; `next` throws. */
class ThrowingCtorScanContrib extends CometScanContrib {
  throw new IllegalStateException("contrib constructor blew up")
}

/** Implements the service but has no usable no-arg constructor. */
class NoNoArgCtorScanContrib(unused: Int) extends CometScanContrib

/** Owns nothing: inherits both hooks' `None` default, as the ownership contract requires. */
class DecliningScanContrib extends CometScanContrib

/** Blows up while examining a scan; must be treated as a decline. */
class ThrowingScanContrib extends CometScanContrib {
  override def tryTransformV1(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      relation: HadoopFsRelation): Option[SparkPlan] =
    throw new IllegalStateException("contrib blew up while planning a V1 scan")

  override def tryTransformV2(scanExec: BatchScanExec): Option[SparkPlan] =
    throw new IllegalStateException("contrib blew up while planning a V2 scan")
}

/** Fails in a way that must NOT be caught: neither `NonFatal` nor a `LinkageError`. */
class FatalScanContrib extends CometScanContrib {
  override def tryTransformV1(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      relation: HadoopFsRelation): Option[SparkPlan] =
    throw new OutOfMemoryError("simulated JVM-level exhaustion, not a version-skewed contrib jar")
}

/**
 * Simulates a contrib jar built against a Comet internal (a method signature, a class) that has
 * since moved, been renamed, or been removed -- the exact failure mode a stale `--jars` contrib
 * hits against a newer Comet on the driver's classpath. Must be contained the same way a
 * `NonFatal` decline is, unlike [[FatalScanContrib]]'s genuinely fatal error.
 */
class VersionSkewedScanContrib extends CometScanContrib {
  override def tryTransformV1(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      relation: HadoopFsRelation): Option[SparkPlan] =
    throw new NoSuchMethodError(
      "org.apache.comet.rules.CometScanContribSuite$InternalApi.movedMethod()V")

  override def tryTransformV2(scanExec: BatchScanExec): Option[SparkPlan] =
    throw new NoSuchMethodError(
      "org.apache.comet.rules.CometScanContribSuite$InternalApi.movedMethod()V")
}

/**
 * Minimal Log4j2 appender that records every event it receives, verbatim, for
 * [[CometScanContribSuite.withCapturedLogEvents]] to inspect after the fact.
 */
private class CapturingAppender(name: String) extends AbstractAppender(name, null, null, false) {
  val events: ArrayBuffer[LogEvent] = ArrayBuffer.empty

  override def append(event: LogEvent): Unit = events.synchronized {
    events += event.toImmutable
  }
}
