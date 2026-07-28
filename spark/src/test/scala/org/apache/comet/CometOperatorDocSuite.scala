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
import java.lang.reflect.Modifier
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.jar.JarFile

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.SparkPlan

/**
 * Guards the operator reference in `docs/source/user-guide/latest/operators.md`.
 *
 * That page claims to describe every Spark physical operator, which is only useful if it is
 * actually complete: an operator that is silently absent reads as "supported" to anyone scanning
 * the tables. This suite enumerates the concrete `SparkPlan` implementations in the spark-sql jar
 * on the test classpath and fails if any of them has no entry.
 *
 * Because the suite runs under every Spark profile, the doc must cover the union of operators
 * across all supported Spark versions. When a new Spark version introduces an operator, add a row
 * for it — including a ❌ or ➖ row saying why it is not accelerated.
 */
class CometOperatorDocSuite extends AnyFunSuite {

  test("every Spark physical operator is listed in operators.md") {
    val doc = new String(Files.readAllBytes(operatorDoc().toPath), StandardCharsets.UTF_8)
    val operators = sparkOperators()

    // Without this the suite would pass vacuously if the classpath scan ever stopped finding
    // anything. Spark 3.4 has ~125 concrete operators, so this floor has plenty of headroom.
    assert(
      operators.size > 100,
      s"only found ${operators.size} Spark operators; the classpath scan is broken")

    val undocumented = operators.filterNot(name => doc.contains(s"`$name`")).sorted

    assert(
      undocumented.isEmpty,
      s"""${undocumented.size} Spark operator(s) are missing from
         |docs/source/user-guide/latest/operators.md:
         |
         |  ${undocumented.mkString("\n  ")}
         |
         |Add a row for each one. If Comet does not accelerate it, use the ❌ status and say why
         |in the notes; if it is plan plumbing rather than an acceleration target, use ➖.""".stripMargin)
  }

  /**
   * Simple names of every concrete operator in `org.apache.spark.sql.execution`, read from the
   * spark-sql jar on the test classpath.
   */
  private def sparkOperators(): Seq[String] = {
    val jar = new JarFile(sparkSqlJar())
    try {
      jar
        .entries()
        .asScala
        .map(_.getName)
        .filter(n => n.startsWith("org/apache/spark/sql/execution/") && n.endsWith("Exec.class"))
        .map(_.stripSuffix(".class").replace('/', '.'))
        // Scala companion objects and anonymous classes are not operators.
        .filterNot(n => n.contains("$"))
        .flatMap(loadClass)
        .filter(c =>
          classOf[SparkPlan].isAssignableFrom(c) && !Modifier.isAbstract(c.getModifiers))
        .map(_.getSimpleName)
        .toList
        .distinct
    } finally {
      jar.close()
    }
  }

  private def sparkSqlJar(): File = {
    val location = classOf[SparkPlan].getProtectionDomain.getCodeSource.getLocation
    val file = new File(location.toURI)
    assert(
      file.isFile && file.getName.endsWith(".jar"),
      s"expected spark-sql to resolve to a jar, got $file")
    file
  }

  /**
   * Walk up from the working directory to find the doc, which is at a fixed repo-relative path.
   */
  private def operatorDoc(): File = {
    val relative = "docs/source/user-guide/latest/operators.md"
    var dir = new File(".").getCanonicalFile
    while (dir != null) {
      val candidate = new File(dir, relative)
      if (candidate.isFile) return candidate
      dir = dir.getParentFile
    }
    fail(s"could not locate $relative above ${new File(".").getCanonicalPath}")
  }

  private def loadClass(name: String): Option[Class[_]] =
    try {
      // Do not initialize: some operators touch optional integrations at class-init time.
      Some(Class.forName(name, false, getClass.getClassLoader))
    } catch {
      // Skip anything unresolvable from the test classpath rather than reporting it as
      // undocumented; a class we cannot even load is not something we can describe.
      case _: Throwable => None
    }
}
