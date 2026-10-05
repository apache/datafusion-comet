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
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import javax.tools.ToolProvider

/**
 * Compiles Java source at test time, for fixtures that need a class only a user jar would hold:
 * one the ClassLoader that loaded Comet cannot see. A directory works as a classpath entry, so
 * there is no need to package a jar.
 */
object TestJavaCompiler {

  /**
   * Compile `source`, saved as `fileName`, into a fresh directory that is deleted when the JVM
   * exits, and return the directory. `classpath` lists one class from each jar or directory the
   * source refers to.
   */
  def compile(fileName: String, source: String, classpath: Seq[Class[_]]): Path = {
    // createTempDirectory does not create its parent (java.io.tmpdir, pinned to target/tmp by the
    // pom), which does not exist yet on a fresh checkout, so make it up front.
    val tmpRoot = Files.createDirectories(Paths.get(System.getProperty("java.io.tmpdir")))
    val workDir = Files.createTempDirectory(tmpRoot, "comet-test-java")
    val src = workDir.resolve(fileName)
    Files.write(src, source.getBytes(UTF_8))

    val classesDir = Files.createDirectories(workDir.resolve("classes"))
    val compiler = ToolProvider.getSystemJavaCompiler
    assert(compiler != null, "test must run on a JDK (needs the javax.tools compiler)")
    // Only what the source refers to. Handing javac the whole test classpath makes it open and
    // index every jar on it, which costs more than the compile itself.
    val cp = classpath
      .map(c =>
        Option(c.getProtectionDomain.getCodeSource)
          .map(_.getLocation.getPath)
          .getOrElse(System.getProperty("java.class.path")))
      .distinct
      .mkString(File.pathSeparator)
    val rc = compiler.run(null, null, null, "-cp", cp, "-d", classesDir.toString, src.toString)
    assert(rc == 0, s"javac failed with exit code $rc")

    deleteOnExitRecursively(workDir.toFile)
    classesDir
  }

  /** Parents are registered before children, and deletion runs in reverse registration order. */
  private def deleteOnExitRecursively(file: File): Unit = {
    file.deleteOnExit()
    Option(file.listFiles()).foreach(_.foreach(deleteOnExitRecursively))
  }
}
