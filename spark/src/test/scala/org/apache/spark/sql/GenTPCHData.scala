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

import java.io.{File, PrintWriter}
import java.nio.file.Files
import java.nio.file.Path

import scala.sys.process._
import scala.util.Try

import org.apache.commons.io.FileUtils
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * This class generates TPCH table data by using tpch-dbgen:
 *   - https://github.com/databricks/tpch-dbgen
 *
 * To run this:
 * {{{
 *   make benchmark-org.apache.spark.sql.GenTPCHData -- --location <path> --scaleFactor 1
 * }}}
 */
object GenTPCHData {
  val TEMP_DBGEN_DIR: Path = new File("/tmp").toPath
  val DBGEN_DIR_PREFIX = "tempTPCHGen"

  def main(args: Array[String]): Unit = {
    val config = new GenTPCHDataConfig(args)

    val spark = SparkSession
      .builder()
      .appName(getClass.getName)
      .master(config.master)
      .getOrCreate()

    setScaleConfig(spark, config.scaleFactor)

    // Number of worker nodes
    val workers = spark.sparkContext.getExecutorMemoryStatus.size

    var defaultDbgenDir: File = null

    val dbgenDir = if (config.dbgenDir == null) {
      defaultDbgenDir = Files.createTempDirectory(TEMP_DBGEN_DIR, DBGEN_DIR_PREFIX).toFile
      val baseDir = defaultDbgenDir.getAbsolutePath
      defaultDbgenDir.delete()
      // Install the data generators in all nodes
      // TODO: think a better way to install on each worker node
      //       such as https://stackoverflow.com/a/40876671
      spark.range(0, workers, 1, workers).foreach(worker => installDBGEN(baseDir)(worker))
      s"${baseDir}/dbgen"
    } else {
      config.dbgenDir
    }

    val tables = new TPCHTables(spark.sqlContext, dbgenDir, config.scaleFactor.toString)

    // Generate data
    // Since dbgen may uses stdout to output the data, tables.genData needs to run table by table
    val tableNames =
      if (config.tableFilter.trim.isEmpty) tables.tables.map(_.name) else Seq(config.tableFilter)
    tableNames.foreach { tableName =>
      tables.genData(
        location = s"${config.location}/tpch/sf${config.scaleFactor}_${config.format}",
        format = config.format,
        overwrite = config.overwrite,
        partitionTables = config.partitionTables,
        clusterByPartitionColumns = config.clusterByPartitionColumns,
        filterOutNullPartitionValues = config.filterOutNullPartitionValues,
        tableFilter = tableName,
        numPartitions = config.numPartitions)
    }

    // Clean up
    if (defaultDbgenDir != null) {
      spark.range(0, workers, 1, workers).foreach { _ =>
        val _ = FileUtils.deleteQuietly(defaultDbgenDir)
      }
    }

    spark.stop()
  }

  def setScaleConfig(spark: SparkSession, scaleFactor: Int): Unit = {
    // Avoid OOM when shuffling large scale factors and errors like 2GB shuffle limit at 10TB like:
    // org.apache.spark.shuffle.FetchFailedException: Too large frame: 9640891355
    // For 10TB 16x4core nodes were needed with the config below, 8x for 1TB and below.
    // About 24hrs. for SF 1 to 10,000.
    if (scaleFactor >= 10000) {
      spark.conf.set("spark.sql.shuffle.partitions", "20000")
      SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.1")
    } else if (scaleFactor >= 1000) {
      spark.conf.set(
        "spark.sql.shuffle.partitions",
        "2001"
      ) // one above 2000 to use HighlyCompressedMapStatus
      SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.3")
    } else {
      spark.conf.set("spark.sql.shuffle.partitions", "200") // default
      SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.5")
    }
  }

  // Install tpch-dbgen (with the stdout patch)
  def installDBGEN(
      baseDir: String,
      url: String = "https://github.com/databricks/tpch-dbgen.git",
      useStdout: Boolean = true)(i: java.lang.Long): Unit = {
    // Check if we want the revision which makes dbgen output to stdout
    val checkoutRevision: String =
      if (useStdout) "git checkout 0469309147b42abac8857fa61b4cf69a6d3128a8 -- bm_utils.c" else ""

    Seq("mkdir", "-p", baseDir).!
    val pw = new PrintWriter(s"${baseDir}/dbgen_$i.sh")
    pw.write(s"""
      |rm -rf ${baseDir}/dbgen
      |rm -rf ${baseDir}/dbgen_install_$i
      |mkdir ${baseDir}/dbgen_install_$i
      |cd ${baseDir}/dbgen_install_$i
      |git clone '$url'
      |cd tpch-dbgen
      |$checkoutRevision
      |sed -i'' -e 's/#include <malloc\\.h>/#ifndef __APPLE__\\n#include <malloc\\.h>\\n#endif/' bm_utils.c
      |sed -i'' -e 's/#include <malloc\\.h>/#if defined(__MACH__)\\n#include <stdlib\\.h>\\n#else\\n#include <malloc\\.h>\\n#endif/' varsub.c
      |make
      |ln -sf ${baseDir}/dbgen_install_$i/tpch-dbgen ${baseDir}/dbgen || echo "ln -sf failed"
      |test -e ${baseDir}/dbgen/dbgen
      |echo "OK"
      """.stripMargin)
    pw.close
    Seq("chmod", "+x", s"${baseDir}/dbgen_$i.sh").!
    Seq(s"${baseDir}/dbgen_$i.sh").!!
  }
}

class GenTPCHDataConfig(args: Array[String]) {
  var master: String = "local[*]"
  var dbgenDir: String = null
  var location: String = null
  var scaleFactor: Int = 1
  var format: String = "parquet"
  var overwrite: Boolean = false
  var partitionTables: Boolean = false
  var clusterByPartitionColumns: Boolean = false
  var filterOutNullPartitionValues: Boolean = false
  var tableFilter: String = ""
  var numPartitions: Int = 100

  parseArgs(args.toList)

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (args.nonEmpty) {
      args match {
        case "--master" :: value :: tail =>
          master = value
          args = tail

        case "--dbgenDir" :: value :: tail =>
          dbgenDir = value
          args = tail

        case "--location" :: value :: tail =>
          location = value
          args = tail

        case "--scaleFactor" :: value :: tail =>
          scaleFactor = toPositiveIntValue("Scale factor", value)
          args = tail

        case "--format" :: value :: tail =>
          format = value
          args = tail

        case "--overwrite" :: tail =>
          overwrite = true
          args = tail

        case "--partitionTables" :: tail =>
          partitionTables = true
          args = tail

        case "--clusterByPartitionColumns" :: tail =>
          clusterByPartitionColumns = true
          args = tail

        case "--filterOutNullPartitionValues" :: tail =>
          filterOutNullPartitionValues = true
          args = tail

        case "--tableFilter" :: value :: tail =>
          tableFilter = value
          args = tail

        case "--numPartitions" :: value :: tail =>
          numPartitions = toPositiveIntValue("Number of partitions", value)
          args = tail

        case "--help" :: _ =>
          printUsageAndExit(0)

        case _ =>
          // scalastyle:off println
          System.err.println("Unknown/unsupported param " + args)
          // scalastyle:on println
          printUsageAndExit(1)
      }
    }

    checkRequiredArguments()
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off
    System.err.println("""
      |make benchmark-org.apache.spark.sql.GenTPCHData -- [Options]
      |Options:
      |  --master                        the Spark master to use, default to local[*]
      |  --dbgenDir                      location of dbgen on worker nodes, if not provided, installs default dbgen
      |  --location                      root directory of location to generate data in
      |  --scaleFactor                   size of the dataset to generate (in GB)
      |  --format                        generated data format, Parquet, ORC ...
      |  --overwrite                     whether to overwrite the data that is already there
      |  --partitionTables               whether to create the partitioned fact tables
      |  --clusterByPartitionColumns     whether to shuffle to get partitions coalesced into single files
      |  --filterOutNullPartitionValues  whether to filter out the partition with NULL key value
      |  --tableFilter                   comma-separated list of table names to generate (e.g., store_sales,store_returns),
      |                                  all the tables are generated by default
      |  --numPartitions                 how many dbgen partitions to run - number of input tasks
      """.stripMargin)
    // scalastyle:on
    System.exit(exitCode)
  }

  private def toPositiveIntValue(name: String, v: String): Int = {
    if (Try(v.toInt).getOrElse(-1) <= 0) {
      // scalastyle:off println
      System.err.println(s"$name must be a positive number")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
    v.toInt
  }

  private def checkRequiredArguments(): Unit = {
    if (location == null) {
      // scalastyle:off println
      System.err.println("Must specify an output location")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
  }
}
