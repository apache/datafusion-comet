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
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import javax.tools.ToolProvider

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, Encoders}
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Regression coverage for UDF closures whose capturing class lives in a user jar, which used to
 * fail with:
 *
 * {{{
 * java.lang.ClassCastException: cannot assign instance of java.lang.invoke.SerializedLambda
 *   to field org.apache.spark.sql.catalyst.expressions.ScalaUDF.f of type scala.Function1
 *   at org.apache.comet.udf.codegen.CometScalaUDFCodegen.lookupOrCompile
 * }}}
 *
 * That exception is a masked `ClassNotFoundException`: when the deserializing ClassLoader cannot
 * resolve a lambda's capturing class, `ObjectInputStream` records the CNFE against the object
 * handle, therefore skips `SerializedLambda.readResolve`, and the raw `SerializedLambda` then
 * fails the field-type check in `defaultCheckFieldValues`. The classloading rationale lives on
 * `CometUdfBridge.evaluate`.
 *
 * The fixture puts the capturing class on `spark.executor.extraClassPath`, the local-mode
 * equivalent of a `--jars` submission: `LocalSchedulerBackend` feeds it into the executor's
 * `MutableURLClassLoader`, which becomes the task thread's context ClassLoader while staying
 * invisible to the ClassLoader that loaded Comet.
 */
class CometScalaUDFClassLoaderSuite extends CometTestBase {

  import CometScalaUDFClassLoaderSuite._

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key, "true")
      .set("spark.executor.extraClassPath", hiddenClassesDir.toString)

  private def withHiddenUdfTable(f: => Unit): Unit = {
    spark.udf.register("hiddenUdf", hiddenFn)
    withTable("t") {
      sql("CREATE TABLE t (s STRING) USING parquet")
      sql("INSERT INTO t VALUES ('a'), ('b'), (NULL)")
      f
    }
  }

  test("fixture: hidden class reachable from task threads, not from Comet's ClassLoader") {
    // Guards the tests below from passing vacuously. `classOf[Expression].getClassLoader` is the
    // fallback `lookupOrCompile` uses when the calling thread has no context ClassLoader.
    intercept[ClassNotFoundException] {
      Class.forName(HiddenClassName, false, classOf[Expression].getClassLoader)
    }
    val reachable = spark
      .range(4)
      .repartition(2)
      .mapPartitions(_ => Iterator(canLoadHiddenClass))(Encoders.scalaBoolean)
      .collect()
    assert(reachable.forall(identity), s"task threads could not load $HiddenClassName")
  }

  // Both leaf shapes matter. With the native scan the dispatcher runs on a Tokio worker, which has
  // no context ClassLoader of its own and depends on the propagated one; without it the dispatcher
  // runs on the Spark task thread, which already has the executor's ClassLoader installed.
  Seq(true, false).foreach { nativeScan =>
    test(s"ScalaUDF closure captured by a user-jar class, nativeScan=$nativeScan") {
      withSQLConf(CometConf.COMET_NATIVE_SCAN_ENABLED.key -> nativeScan.toString) {
        withHiddenUdfTable {
          checkSparkAnswer(sql("SELECT hiddenUdf(s) FROM t"))
        }
      }
    }
  }

  test("the thread running the UDF sees the task thread's context ClassLoader") {
    // Pins the propagation itself rather than its symptom: the UDF body reports whether the
    // ClassLoader installed on whatever thread invoked it can reach the user jar.
    spark.udf.register("loaderProbe", (_: String) => loaderReport())
    withHiddenUdfTable {
      val reports = sql("SELECT loaderProbe(s) FROM t").collect().map(_.getString(0)).distinct
      assert(
        reports.forall(_.startsWith("loaded|")),
        s"UDF thread could not load $HiddenClassName: ${reports.mkString(", ")}")
    }
  }
}

object CometScalaUDFClassLoaderSuite {

  val HiddenClassName = "hidden.HiddenUdf"

  /**
   * Directory holding the compiled capturing class. Compiled once per JVM, before the
   * SparkSession starts, because `spark.executor.extraClassPath` is read at session creation. A
   * directory works as a classpath entry, so there is no need to package a jar.
   */
  lazy val hiddenClassesDir: Path = compileHiddenClass()

  /**
   * The UDF, obtained through a ClassLoader over `hiddenClassesDir` alone. Deliberately not
   * closed: the loaded class stays live in the registered UDF for the rest of the JVM's life.
   */
  lazy val hiddenFn: String => String =
    new URLClassLoader(Array(hiddenClassesDir.toUri.toURL), getClass.getClassLoader)
      .loadClass(HiddenClassName)
      .getMethod("make")
      .invoke(null)
      .asInstanceOf[String => String]

  /** Runs on Spark threads; kept on the companion so the closures capture nothing. */
  def canLoadHiddenClass: Boolean =
    try {
      Class.forName(HiddenClassName, false, Thread.currentThread().getContextClassLoader)
      true
    } catch {
      case _: ClassNotFoundException => false
    }

  /**
   * Reports whether the current thread's context ClassLoader reaches the user jar, tagged with
   * the thread name so a failure says which thread was missing it.
   */
  def loaderReport(): String = {
    val status = if (canLoadHiddenClass) "loaded" else "MISSING"
    s"$status|${Thread.currentThread().getName}"
  }

  private def compileHiddenClass(): Path = {
    val workDir = Files.createTempDirectory("comet-hidden-udf")
    val src = workDir.resolve("HiddenUdf.java")
    // The intersection cast is what makes the lambda serializable, and therefore what makes
    // `hidden.HiddenUdf` the capturing class recorded in the SerializedLambda.
    Files.write(
      src,
      """package hidden;
        |
        |import java.io.Serializable;
        |import scala.Function1;
        |
        |public class HiddenUdf {
        |  public static Function1<Object, Object> make() {
        |    return (Function1<Object, Object> & Serializable)
        |        (Object o) -> (o == null ? null : "hidden:" + o);
        |  }
        |}
        |""".stripMargin.getBytes(UTF_8))

    val classesDir = Files.createDirectories(workDir.resolve("classes"))
    val compiler = ToolProvider.getSystemJavaCompiler
    assert(compiler != null, "test must run on a JDK (needs the javax.tools compiler)")
    // Only scala-library is needed. Handing javac the whole test classpath makes it open and index
    // every jar on it, which costs more than the compile itself.
    val classpath = Option(classOf[Function1[_, _]].getProtectionDomain.getCodeSource)
      .map(_.getLocation.getPath)
      .getOrElse(System.getProperty("java.class.path"))
    val rc =
      compiler.run(null, null, null, "-cp", classpath, "-d", classesDir.toString, src.toString)
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
