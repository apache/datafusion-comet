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

import java.io.{File, FileOutputStream}
import java.net.URLClassLoader
import java.nio.file.{Files, Path}
import java.util.jar.{JarEntry, JarOutputStream}
import javax.tools.ToolProvider

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Regression coverage for UDF closures whose capturing class lives in a user jar. Before the
 * task-thread ClassLoader was propagated to Tokio workers, these queries failed with:
 *
 * {{{
 * java.lang.ClassCastException: cannot assign instance of java.lang.invoke.SerializedLambda
 *   to field org.apache.spark.sql.catalyst.expressions.ScalaUDF.f of type scala.Function1
 *   at org.apache.comet.udf.codegen.CometScalaUDFCodegen.lookupOrCompile
 * }}}
 *
 * It took two conditions together:
 *
 *   1. The class that captured the UDF lambda lives in a user jar, so it is reachable from
 *      Spark's executor ClassLoader but not from the ClassLoader that loaded Comet and
 *      spark-catalyst. `spark.executor.extraClassPath` is the local-mode equivalent of a `--jars`
 *      submission: `LocalSchedulerBackend` feeds it into the executor's `MutableURLClassLoader`,
 *      which becomes the task thread's context ClassLoader.
 *   1. The dispatcher's `evaluate` runs on a Tokio worker rather than the Spark task thread.
 *      Tokio workers attach to the JVM through JNI and an attached thread has no context
 *      ClassLoader of its own, so `CometScalaUDFCodegen.lookupOrCompile` fell back to
 *      `classOf[Expression].getClassLoader`, which cannot see the user jar. That is the normal
 *      case when the stage's leaf is a native scan (`CometNativeScan`, `CometIcebergNativeScan`).
 *
 * The underlying `ClassNotFoundException` on the capturing class was masked: `ObjectInputStream`
 * records it against the handle, skips `SerializedLambda.readResolve`, and the raw
 * `SerializedLambda` then fails the field-type check in `defaultCheckFieldValues`.
 *
 * `CometExecIterator` now captures the task thread's context ClassLoader and `CometUdfBridge`
 * installs it on the calling thread for the duration of each UDF call.
 */
class CometScalaUDFClassLoaderSuite extends CometTestBase {

  import CometScalaUDFClassLoaderSuite._

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key, "true")
      // Visible to executor task threads, invisible to Comet's own ClassLoader.
      .set("spark.executor.extraClassPath", hiddenJar.getAbsolutePath)

  /** Registers a UDF whose lambda was captured by a class living only in the user jar. */
  private def withHiddenUdf(name: String)(f: => Unit): Unit = {
    val loader = new URLClassLoader(Array(hiddenJar.toURI.toURL), getClass.getClassLoader)
    val fn = loader
      .loadClass(HiddenClassName)
      .getMethod("make")
      .invoke(null)
      .asInstanceOf[String => String]
    spark.udf.register(name, fn)
    try f
    finally loader.close()
  }

  test("sanity: the hidden class is reachable from task threads but not from Comet's loader") {
    // `classOf[Expression].getClassLoader` is exactly the fallback `lookupOrCompile` uses when the
    // calling thread has no context ClassLoader.
    intercept[ClassNotFoundException] {
      Class.forName(HiddenClassName, false, classOf[Expression].getClassLoader)
    }
    val reachable = spark
      .range(4)
      .repartition(2)
      .mapPartitions(_ => Iterator(canLoadHiddenClassOnThisThread))(
        org.apache.spark.sql.Encoders.scalaBoolean)
      .collect()
    assert(reachable.forall(identity), s"task threads could not load $HiddenClassName")
  }

  test("ScalaUDF whose capturing class is only in the user jar, native scan leaf") {
    // Fails without ClassLoader propagation: the dispatcher deserializes the closure on a Tokio
    // worker, which sees no user jar, and the resulting ClassNotFoundException surfaces as
    // `cannot assign instance of java.lang.invoke.SerializedLambda to field ScalaUDF.f`.
    withHiddenUdf("hiddenUdf") {
      withTable("t") {
        sql("CREATE TABLE t (s STRING) USING parquet")
        sql("INSERT INTO t VALUES ('a'), ('b'), (NULL)")
        // Stage leaf is CometNativeScan, so the dispatcher runs on a Tokio worker.
        checkSparkAnswer(sql("SELECT hiddenUdf(s) FROM t"))
      }
    }
  }

  test("the thread running the UDF sees the task thread's context ClassLoader") {
    // Pins the propagation itself rather than its symptom: the UDF body reports whether the
    // ClassLoader installed on whatever thread invoked it can reach the user jar.
    spark.udf.register("loaderProbe", (_: String) => loaderReport())
    withTable("t") {
      sql("CREATE TABLE t (s STRING) USING parquet")
      sql("INSERT INTO t VALUES ('a'), ('b'), ('c'), ('d')")
      val reports = sql("SELECT loaderProbe(s) FROM t").collect().map(_.getString(0)).distinct
      assert(
        reports.forall(_.startsWith("loaded|")),
        s"UDF thread could not load $HiddenClassName: ${reports.mkString(", ")}")
    }
  }

  test("control: same UDF evaluated on the Spark task thread") {
    // With the native scan disabled the dispatcher runs on the task thread, whose context
    // ClassLoader includes the user jar, so deserialization resolves the capturing class.
    withSQLConf(CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false") {
      withHiddenUdf("hiddenUdf2") {
        withTable("t2") {
          sql("CREATE TABLE t2 (s STRING) USING parquet")
          sql("INSERT INTO t2 VALUES ('a'), ('b'), (NULL)")
          checkSparkAnswer(sql("SELECT hiddenUdf2(s) FROM t2"))
        }
      }
    }
  }
}

object CometScalaUDFClassLoaderSuite {

  val HiddenClassName = "hidden.HiddenUdf"

  /**
   * Jar holding a serializable `scala.Function1` lambda. The lambda's capturing class exists only
   * inside this jar, so it is visible only to ClassLoaders the jar is wired into. Built once,
   * before the SparkSession starts, because `spark.executor.extraClassPath` is read at session
   * creation.
   */
  lazy val hiddenJar: File = buildHiddenUdfJar(Files.createTempDirectory("comet-hidden-udf"))

  /**
   * Reports whether the current thread's context ClassLoader can reach the user jar, tagged with
   * the thread name so a failure says which thread was missing it. Kept on the companion so the
   * UDF closure captures nothing from the suite.
   */
  def loaderReport(): String = {
    val status = if (canLoadHiddenClassOnThisThread) "loaded" else "MISSING"
    s"$status|${Thread.currentThread().getName}"
  }

  /** Runs on a Spark task thread; kept on the companion so the closure captures nothing. */
  def canLoadHiddenClassOnThisThread: Boolean =
    try {
      Class.forName(HiddenClassName, false, Thread.currentThread().getContextClassLoader)
      true
    } catch {
      case _: ClassNotFoundException => false
    }

  private def buildHiddenUdfJar(workDir: Path): File = {
    val src = workDir.resolve("HiddenUdf.java")
    Files.write(
      src,
      ("""package hidden;
        |
        |import java.io.Serializable;
        |import scala.Function1;
        |
        |public class HiddenUdf {
        |  public interface SerFn extends Function1<Object, Object>, Serializable {}
        |
        |  public static Function1<Object, Object> make() {
        |    return (SerFn) (Object o) -> (o == null ? null : "hidden:" + o);
        |  }
        |}
        |""".stripMargin).getBytes("UTF-8"))

    val classesDir = Files.createDirectories(workDir.resolve("classes"))
    val compiler = ToolProvider.getSystemJavaCompiler
    assert(compiler != null, "test must run on a JDK (needs the javax.tools compiler)")
    val rc = compiler.run(
      null,
      null,
      null,
      "-cp",
      System.getProperty("java.class.path"),
      "-d",
      classesDir.toString,
      src.toString)
    assert(rc == 0, s"javac failed with exit code $rc")

    val jar = workDir.resolve("hidden-udf.jar").toFile
    val jos = new JarOutputStream(new FileOutputStream(jar))
    try {
      Files
        .walk(classesDir)
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .foreach { p =>
          jos.putNextEntry(new JarEntry(classesDir.relativize(p).toString))
          jos.write(Files.readAllBytes(p))
          jos.closeEntry()
        }
    } finally {
      jos.close()
    }
    jar.deleteOnExit()
    jar
  }
}
