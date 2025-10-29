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

package org.apache.spark.sql

import java.io.{File, FileOutputStream, OutputStream, PrintStream}

import scala.collection.mutable

import org.apache.commons.io.output.TeeOutputStream
import org.apache.spark.benchmark.Benchmarks
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.comet.CometExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.shims.ShimCometSparkSessionExtensions

trait CometTPCQueryListBase
    extends CometTPCQueryBase
    with AdaptiveSparkPlanHelper
    with SQLHelper
    with ShimCometSparkSessionExtensions {
  var output: Option[OutputStream] = None

  def main(args: Array[String]): Unit = {
    val resultFileName =
      s"${this.getClass.getSimpleName.replace("$", "")}-results.txt"
    val prefix = Benchmarks.currentProjectRoot.map(_ + "/").getOrElse("")
    val dir = new File(s"${prefix}inspections/")
    if (!dir.exists()) {
      // scalastyle:off println
      println(s"Creating ${dir.getAbsolutePath} for query inspection results.")
      // scalastyle:on println
      dir.mkdirs()
    }
    val file = new File(dir, resultFileName)
    if (!file.exists()) {
      file.createNewFile()
    }
    output = Some(new FileOutputStream(file))

    runSuite(args)

    output.foreach { o =>
      if (o != null) {
        o.close()
      }
    }
  }

  protected def runQueries(
      queryLocation: String,
      queries: Seq[String],
      nameSuffix: String = ""): Unit = {

    val out = if (output.isDefined) {
      new PrintStream(new TeeOutputStream(System.out, output.get))
    } else {
      System.out
    }

    queries.foreach { name =>
      val queryString = resourceToString(
        s"$queryLocation/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)

      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        // Lower bloom filter thresholds to allows us to simulate the plan produced at larger scale.
        "spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold" -> "1MB",
        "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold" -> "1MB") {

        val df = cometSpark.sql(queryString)
        val cometPlans = mutable.HashSet.empty[String]
        val executedPlan = df.queryExecution.executedPlan
        stripAQEPlan(executedPlan).foreach {
          case op: CometExec =>
            cometPlans += s"${op.nodeName}"
          case _ =>
        }

        if (cometPlans.nonEmpty) {
          out.println(
            s"Query: $name$nameSuffix. Comet Exec: Enabled (${cometPlans.mkString(", ")})")
        } else {
          out.println(s"Query: $name$nameSuffix. Comet Exec: Disabled")
        }
        out.println(
          s"Query: $name$nameSuffix: ExplainInfo:\n" +
            s"${new ExtendedExplainInfo().generateExtendedInfo(executedPlan)}\n")
      }
    }
  }

  protected def checkCometExec(df: DataFrame, f: SparkPlan => Unit): Unit = {
    if (CometConf.COMET_ENABLED.get() && CometConf.COMET_EXEC_ENABLED.get()) {
      f(stripAQEPlan(df.queryExecution.executedPlan))
    }
  }

  def runSuite(mainArgs: Array[String]): Unit
}
