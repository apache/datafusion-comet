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

import java.io.File
import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.comet.annotation.Public

/**
 * Guards the enumerated public API described in `docs/source/about/versioning_policy.md`.
 *
 * Adding or removing `@Public` is a versioning-policy decision, not a routine code change, so
 * this suite pins the exact set. If it fails, either the annotation was applied or dropped by
 * accident, or the policy is genuinely changing and both the doc and this list need updating in
 * the same PR.
 */
class CometPublicApiSuite extends AnyFunSuite {

  private val expected: Set[String] = Set(
    "org.apache.spark.CometPlugin",
    "org.apache.comet.ExtendedExplainInfo",
    "org.apache.comet.cloud.s3.CometS3AccessMode",
    "org.apache.comet.cloud.s3.CometS3CredentialContext",
    "org.apache.comet.cloud.s3.CometS3CredentialProvider",
    "org.apache.comet.cloud.s3.CometS3Credentials")

  test("the public API is exactly the set enumerated in the versioning policy") {
    val classesDir = mainClassesDir()
    val annotated = allClassNames(classesDir).filter { name =>
      // Skip anything that fails to load: Spark-version shims and optional integrations
      // (Iceberg, cloud SDKs) are not all resolvable from the test classpath, and none of
      // them are candidates for public API.
      loadClass(name).exists(_.isAnnotationPresent(classOf[Public]))
    }.toSet

    assert(
      annotated == expected,
      s"""Public API drift.
         |  unexpectedly annotated: ${(annotated -- expected).toSeq.sorted.mkString(", ")}
         |  missing @Public:        ${(expected -- annotated).toSeq.sorted.mkString(", ")}
         |Update docs/source/about/versioning_policy.md and this suite together.""".stripMargin)
  }

  private def mainClassesDir(): File = {
    // Locate target/classes via a class we know is compiled into this module.
    val marker = classOf[Public].getProtectionDomain.getCodeSource.getLocation.toURI
    val dir = new File(marker)
    assert(
      dir.isDirectory,
      s"expected compiled classes directory, got $dir; run this suite against a built module")
    dir
  }

  private def allClassNames(root: File): Seq[String] = {
    val stream = Files.walk(root.toPath)
    try {
      stream
        .iterator()
        .asScala
        .map(_.toFile)
        .filter(f => f.isFile && f.getName.endsWith(".class"))
        .map { f =>
          root.toPath
            .relativize(f.toPath)
            .toString
            .stripSuffix(".class")
            .replace(File.separatorChar, '.')
        }
        // Scala companion objects and anonymous/inner classes cannot carry the annotation
        // meaningfully and would only add noise to the diff message.
        .filterNot(n => n.endsWith("$") || n.contains("$$") || n.contains("$anon"))
        // Must be strict: on Scala 2.12 `Iterator.toSeq` is `toStream`, which would not be
        // forced until after the `finally` below closes the walk, failing with
        // IllegalStateException.
        .toList
    } finally {
      stream.close()
    }
  }

  private def loadClass(name: String): Option[Class[_]] =
    try {
      Some(Class.forName(name, false, getClass.getClassLoader))
    } catch {
      case _: Throwable => None
    }
}
