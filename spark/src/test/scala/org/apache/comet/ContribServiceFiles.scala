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
import java.net.URLClassLoader
import java.nio.charset.StandardCharsets
import java.nio.file.Files

/** Test support for driving real `ServiceLoader` discovery of contrib registries. */
object ContribServiceFiles {

  /**
   * Writes a `META-INF/services/<service>` file naming `implNames` into a temp directory and
   * hands `f` an isolated child `URLClassLoader` that sees it. The child loader keeps the
   * discovery out of the JVM-wide registries, which the default-build tests assert are empty.
   */
  def withServiceFile(service: Class[_], implNames: Seq[String])(f: ClassLoader => Unit): Unit = {
    // The scalatest plugin points java.io.tmpdir at spark/target/tmp, which may not exist yet;
    // ensure it before createTempDirectory (which requires its parent to exist).
    val baseTmp = new File(System.getProperty("java.io.tmpdir"))
    baseTmp.mkdirs()
    val svcDir =
      Files.createTempDirectory(baseTmp.toPath, s"comet-${service.getSimpleName}-test").toFile
    try {
      val servicesDir = new File(svcDir, "META-INF/services")
      assert(servicesDir.mkdirs(), s"could not create $servicesDir")
      Files.write(
        new File(servicesDir, service.getName).toPath,
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
