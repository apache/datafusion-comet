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

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.Comparator

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.alias.CredentialProviderFactory

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import org.apache.comet.objectstore.AbfsAuthParityCases._
import org.apache.comet.objectstore.AbfsAuthResolver._
import org.apache.comet.objectstore.AbfsReflection.Handles

/**
 * Pins the resolver's behaviour per hadoop-azure version and cross-checks it against the real
 * `AzureBlobFileSystem`, offline.
 *
 * Every [[AbfsAuthParityCases]] case runs through [[AbfsAuthResolver.resolve]] and is compared
 * with `abfs-auth-parity/expected-<version>.json`, where the version is the one the capability
 * probes identify (never the Spark profile), and with the case's pins. Every case is then handed
 * to `FileSystem.newInstance` with `fs.azure.account.hns.enabled=true`, which keeps 3.4.2+
 * initialization off the network.
 *
 * The FileSystem cross-check compares outcome classes, not values: `Resolved` needs a successful
 * initialization, `Failed` an initialization that threw with the resolver's exception class in
 * the cause chain, `Unconfigured` a `KeyProviderException`, and `Declined` either success or a
 * failure while Hadoop loaded the provider the resolver left alone. Forwarded values are covered
 * by the pins, the live-value checks and the digests in the recorded files, not by this check.
 *
 * To regenerate the expectation files, run the suite once per hadoop-azure line with
 * `-Dcomet.abfs.parity.record=<dir>` (spark-3.5 for 3.3.4, spark-4.0 for 3.4.1, spark-4.1 for
 * 3.4.2, spark-4.2 for 3.5.0) and copy the written files into
 * `spark/src/test/resources/abfs-auth-parity/`. The pins and the FileSystem cross-check still
 * assert while recording, so a recorded file never contradicts the hadoop-azure source. The file
 * is written only when every case ran, so a filtered run cannot leave a partial file behind.
 */
class AbfsAuthParitySuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  import AbfsAuthParitySuite._

  private lazy val handles: Handles = AbfsReflection.handles(getClass.getClassLoader).get
  private lazy val version: String = versionLabel(handles)

  private val recordDir: Option[Path] = Option(System.getProperty(RECORD_PROPERTY))
    // An undefined Maven property reaches the forked JVM as "null" or as the literal
    // "${comet.abfs.parity.record}"; neither is a directory.
    .map(_.trim)
    .filter(p => p.nonEmpty && p != "null" && !p.startsWith("$"))
    .map(p => Paths.get(p))

  // The build points java.io.tmpdir under target/, which a fresh checkout may not have yet.
  private val tempDir: Path = Files.createTempDirectory(
    Files.createDirectories(Paths.get(System.getProperty("java.io.tmpdir"))),
    "abfs-auth-parity")
  private val keystore: Path = tempDir.resolve("credentials.jceks")
  private val credentialProviderPath = "jceks://file" + keystore.toUri.getPath

  private lazy val cases: Seq[ParityCase] = AbfsAuthParityCases.cases(credentialProviderPath)
  private lazy val expected: Map[String, Recorded] =
    if (recordDir.isDefined) Map.empty else loadExpected(version)
  private val recorded = mutable.Map[String, Recorded]()

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new Configuration()
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credentialProviderPath)
    val provider = CredentialProviderFactory.getProviders(conf).get(0)
    credentialEntries.foreach { case (alias, value) =>
      provider.createCredentialEntry(alias, value.toCharArray)
    }
    provider.flush()
  }

  override def afterAll(): Unit = {
    try {
      recordDir.foreach { dir =>
        require(
          recorded.size == cases.size,
          s"only ${recorded.size} of ${cases.size} cases ran; not writing a partial expectation " +
            s"file for hadoop-azure $version")
        Files.createDirectories(dir)
        val file = dir.resolve(s"expected-$version.json")
        Files.write(
          file,
          renderExpected(version, recorded.toMap).getBytes(StandardCharsets.UTF_8))
        announce(s"AbfsAuthParitySuite recorded ${recorded.size} cases to $file")
      }
    } finally {
      deleteRecursively(tempDir)
      super.afterAll()
    }
  }

  // scalastyle:off println
  private def announce(message: String): Unit = println(message)
  // scalastyle:on println

  private def hadoopConf(c: ParityCase): Configuration = {
    val conf = new Configuration()
    c.conf.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  test("case ids are unique and every calibration case is pinned on all four versions") {
    val ids = cases.map(_.id)
    ids.distinct should have size ids.size.toLong
    val unpinned =
      cases.filter(_.group == "calibration").filterNot(_.pins.keySet == VERSIONS.toSet)
    assert(
      unpinned.isEmpty,
      s"calibration cases without pins for every version: ${unpinned.map(_.id)}")
    val unknownVersions = cases.flatMap(_.pins.keySet).distinct.filterNot(VERSIONS.contains)
    assert(unknownVersions.isEmpty, s"pins for unknown versions: $unknownVersions")
    assert(!sys.env.contains(UNSET_ENV_VAR), s"$UNSET_ENV_VAR must not be set in the environment")
    info(
      s"hadoop-azure $version, ${cases.size} cases, " +
        cases
          .groupBy(_.group)
          .toSeq
          .sortBy(_._1)
          .map { case (g, cs) => s"$g=${cs.size}" }
          .mkString(", "))
  }

  test("the expectation file matches the hadoop-azure version and the case list") {
    assume(recordDir.isEmpty, "recording")
    expected.keySet shouldBe cases.map(_.id).toSet
  }

  cases.foreach { c =>
    test(s"${c.group}/${c.id}: ${c.note}") {
      val outcome = resolve(hadoopConf(c), c.uri)
      c.liveValues.foreach { case (key, value) =>
        withClue(s"live value of $key: ") {
          outcome shouldBe a[Resolved]
          outcome.asInstanceOf[Resolved].values.get(key) shouldBe Some(value)
        }
      }
      val actual = Recorded.of(outcome, c.liveValues.keySet)
      c.pins.get(version).foreach { pin =>
        withClue(s"pin for $version: ") {
          actual.outcome shouldBe pin.outcome
          actual.errorClass shouldBe pin.errorClass
        }
      }
      recorded(c.id) = actual
      if (recordDir.isEmpty) {
        withClue(s"recorded expectation for hadoop-azure $version: ") {
          actual shouldBe expected(c.id)
        }
      }
    }
  }

  test("FileSystem.newInstance agrees with the resolver on every case") {
    val timings = mutable.ArrayBuffer[(String, Long)]()
    val disagreements = cases.flatMap { c =>
      val outcome = resolve(hadoopConf(c), c.uri)
      val started = System.nanoTime()
      val fsResult = initializeFileSystem(c)
      timings += c.id -> (System.nanoTime() - started) / 1000000L
      checkAgreement(outcome, fsResult).map(reason => s"${c.id}: $reason")
    }
    info(s"${cases.size - disagreements.size} of ${cases.size} cases agree")
    // A slow initialization would point at a network attempt, which this suite must never make.
    val slowest = timings.sortBy(-_._2).take(3).map { case (id, ms) => s"$id ${ms}ms" }
    info(s"slowest FileSystem initializations: ${slowest.mkString(", ")}")
    assert(disagreements.isEmpty, disagreements.mkString("\n", "\n", ""))
  }

  /** The real initialization, offline: HNS known, caches off, closed whatever happens. */
  private def initializeFileSystem(c: ParityCase): Either[Throwable, Unit] = {
    val conf = hadoopConf(c)
    if (Option(conf.get(HNS_ENABLED)).isEmpty) conf.set(HNS_ENABLED, "true")
    conf.set("fs.abfs.impl.disable.cache", "true")
    conf.set("fs.abfss.impl.disable.cache", "true")
    var fs: Option[FileSystem] = None
    try {
      fs = Some(FileSystem.newInstance(c.uri, conf))
      Right(())
    } catch {
      // The resolver reports LinkageErrors too, so the cross-check must see them.
      case e @ (_: LinkageError | NonFatal(_)) => Left(e)
    } finally {
      fs.foreach(_.close())
    }
  }

  private def checkAgreement(outcome: Outcome, fs: Either[Throwable, Unit]): Option[String] = {
    def chain(e: Throwable): Seq[String] = causeChain(e).map(_.getClass.getName)
    (outcome, fs) match {
      case (_: Resolved, Right(())) => None
      case (_: Resolved, Left(e)) => Some(s"resolver resolved, FileSystem threw ${chain(e)}")
      case (Failed(cls, _), Right(())) =>
        Some(s"resolver failed with $cls, FileSystem initialized")
      case (Failed(cls, _), Left(e)) =>
        if (chain(e).contains(cls)) None
        else Some(s"resolver failed with $cls, FileSystem threw ${chain(e)}")
      case (Unconfigured, Right(())) => Some("resolver unconfigured, FileSystem initialized")
      case (Unconfigured, Left(e)) =>
        if (chain(e).contains(ExceptionClasses.KEY_PROVIDER)) None
        else Some(s"resolver unconfigured, FileSystem threw ${chain(e)} not KeyProviderException")
      case (_: Declined, Right(())) => None
      case (_: Declined, Left(e)) =>
        // Hadoop instantiates the provider the resolver declined to; whatever that does is fine.
        if (chain(e).exists(PROVIDER_FAILURES.contains)) None
        else Some(s"resolver declined, FileSystem threw ${chain(e)} outside provider loading")
    }
  }
}

object AbfsAuthParitySuite {
  val RECORD_PROPERTY = "comet.abfs.parity.record"
  val RESOURCE_DIR = "abfs-auth-parity"
  private val HNS_ENABLED = "fs.azure.account.hns.enabled"
  private val MAX_CAUSE_DEPTH = 8

  private val PROVIDER_FAILURES: Set[String] = Set(
    ExceptionClasses.TOKEN_ACCESS_PROVIDER,
    ExceptionClasses.SAS_TOKEN_PROVIDER,
    ExceptionClasses.ILLEGAL_ARGUMENT)

  /** The forwarded values whose recorded form is a SHA-256 digest. */
  private val SECRET_KEYS: Set[String] = Set(
    Keys.ACCOUNT_KEY,
    Keys.CLIENT_SECRET,
    Keys.USER_PASSWORD,
    Keys.REFRESH_TOKEN,
    Keys.SAS_FIXED_TOKEN)
  private val ENVIRONMENT_PLACEHOLDER = "<environment>"

  /** The hadoop-azure line on the classpath, from what it can do rather than from a version. */
  def versionLabel(handles: Handles): String =
    if (handles.hasClientAssertionProvider) "3.5.0"
    else if (handles.hasContainerScopedConfiguration) "3.4.2"
    else if (handles.hasFixedSasTokenProvider) "3.4.1"
    else "3.3.4"

  /**
   * One case's expectation; `values` holds digests for secrets and a placeholder for live values.
   */
  final case class Recorded(
      outcome: String,
      authType: Option[String],
      providerClass: Option[String],
      assertionProviderClass: Option[String],
      errorClass: Option[String],
      values: SortedMap[String, String])

  object Recorded {
    def of(outcome: Outcome, liveKeys: Set[String]): Recorded = outcome match {
      case Resolved(authType, providerClass, values) =>
        val normalized = values.map { case (k, v) =>
          val recorded =
            if (liveKeys.contains(k)) ENVIRONMENT_PLACEHOLDER
            else if (SECRET_KEYS.contains(k)) sha256(v)
            else v
          k -> recorded
        }
        Recorded(
          Outcomes.RESOLVED,
          Some(authType),
          providerClass,
          None,
          None,
          SortedMap(normalized.toSeq: _*))
      case Unconfigured =>
        Recorded(Outcomes.UNCONFIGURED, None, None, None, None, SortedMap.empty)
      case Declined(authType, providerClass, assertionProviderClass) =>
        Recorded(
          Outcomes.DECLINED,
          Some(authType),
          providerClass,
          assertionProviderClass,
          None,
          SortedMap.empty)
      case Failed(exceptionClass, _) =>
        Recorded(Outcomes.FAILED, None, None, None, Some(exceptionClass), SortedMap.empty)
    }
  }

  private def sha256(value: String): String = {
    val digest =
      MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))
    "sha256:" + digest.map(b => f"$b%02x").mkString
  }

  private def causeChain(e: Throwable): Seq[Throwable] = {
    val chain = mutable.ArrayBuffer[Throwable](e)
    var cause = e.getCause
    while (cause != null && chain.length < MAX_CAUSE_DEPTH && !chain.contains(cause)) {
      chain += cause
      cause = cause.getCause
    }
    chain.toSeq
  }

  private def loadExpected(version: String): Map[String, Recorded] = {
    val resource = s"/$RESOURCE_DIR/expected-$version.json"
    val stream: InputStream = getClass.getResourceAsStream(resource)
    require(
      stream != null,
      s"$resource is missing; record it with -D$RECORD_PROPERTY=<dir> on a classpath with " +
        s"hadoop-azure $version")
    try {
      val root = new ObjectMapper().readTree(stream)
      require(
        root.get("hadoopAzureVersion").asText == version,
        s"$resource records ${root.get("hadoopAzureVersion").asText}, classpath has $version")
      val cases = root.get("cases")
      // fieldNames() rather than fields(): the latter is deprecated on the Jackson Spark 4 ships
      // and its replacement is missing on the one Spark 3 ships.
      cases.fieldNames().asScala.map(id => id -> parseRecorded(cases.get(id))).toMap
    } finally {
      stream.close()
    }
  }

  private def parseRecorded(node: JsonNode): Recorded = {
    def optional(field: String): Option[String] = Option(node.get(field)).map(_.asText)
    val values = Option(node.get("values"))
      .map(v => v.fieldNames().asScala.map(k => k -> v.get(k).asText).toSeq)
      .getOrElse(Seq.empty)
    Recorded(
      node.get("outcome").asText,
      optional("authType"),
      optional("providerClass"),
      optional("assertionProviderClass"),
      optional("errorClass"),
      SortedMap(values: _*))
  }

  /** Sorted keys and a fixed layout, so a regeneration diff shows only behaviour changes. */
  private def renderExpected(version: String, recorded: Map[String, Recorded]): String = {
    val cases: SortedMap[String, Any] = SortedMap(recorded.toSeq: _*).map { case (id, r) =>
      val fields: Seq[(String, Any)] = Seq(
        Some("outcome" -> r.outcome),
        r.authType.map("authType" -> _),
        r.providerClass.map("providerClass" -> _),
        r.assertionProviderClass.map("assertionProviderClass" -> _),
        r.errorClass.map("errorClass" -> _),
        Some("values" -> r.values)).flatten
      id -> SortedMap[String, Any](fields: _*)
    }
    val root = SortedMap[String, Any]("cases" -> cases, "hadoopAzureVersion" -> version)
    val out = new StringBuilder
    renderJson(root, 0, out)
    out.append('\n').toString
  }

  private def renderJson(value: Any, indent: Int, out: StringBuilder): Unit = value match {
    case s: String => out.append(quote(s))
    case m: scala.collection.Map[_, _] if m.isEmpty => out.append("{}")
    case m: scala.collection.Map[_, _] =>
      val pad = "  " * (indent + 1)
      out.append("{\n")
      val entries = m.toSeq.map { case (k, v) => k.toString -> v }.sortBy(_._1)
      entries.zipWithIndex.foreach { case ((k, v), i) =>
        out.append(pad).append(quote(k)).append(": ")
        renderJson(v, indent + 1, out)
        if (i < entries.size - 1) out.append(',')
        out.append('\n')
      }
      out.append("  " * indent).append('}')
    case other => throw new IllegalArgumentException(s"unsupported JSON value: $other")
  }

  private def quote(s: String): String = {
    val out = new StringBuilder("\"")
    s.foreach {
      case '"' => out.append("\\\"")
      case '\\' => out.append("\\\\")
      case '\n' => out.append("\\n")
      case '\r' => out.append("\\r")
      case '\t' => out.append("\\t")
      case ch if ch < 0x20 || ch > 0x7e => out.append('\\').append('u').append(f"${ch.toInt}%04x")
      case ch => out.append(ch)
    }
    out.append('"').toString
  }

  private def deleteRecursively(dir: Path): Unit = {
    if (Files.exists(dir)) {
      Files.walk(dir).sorted(Comparator.reverseOrder[Path]()).forEach { (p: Path) =>
        Files.deleteIfExists(p)
        ()
      }
    }
  }
}
