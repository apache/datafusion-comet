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

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Base trait for TPC related query execution
 */
trait CometTPCQueryBase extends Logging {
  protected val cometSpark: SparkSession = {
    val conf = new SparkConf()
      .setMaster(System.getProperty("spark.sql.test.master", "local[*]"))
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .set("spark.sql.parquet.compression.codec", "snappy")
      .set(
        "spark.sql.shuffle.partitions",
        System.getProperty("spark.sql.shuffle.partitions", "4"))
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "3g")
      .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)
      .set("spark.sql.crossJoin.enabled", "true")
      .setIfMissing("parquet.enable.dictionary", "true")

    val sparkSession = SparkSession.builder
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    // Set default configs. Individual cases will change them if necessary.
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")

    sparkSession
  }

  protected def setupTables(
      dataLocation: String,
      createTempView: Boolean,
      tables: Seq[String],
      tableColumns: Map[String, StructType] = Map.empty): Map[String, Long] = {
    tables.map { tableName =>
      if (createTempView) {
        cometSpark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
      } else {
        cometSpark.sql(s"DROP TABLE IF EXISTS $tableName")
        if (tableColumns.isEmpty) {
          cometSpark.catalog.createTable(tableName, s"$dataLocation/$tableName", "parquet")
        } else {
          // SPARK-39584: Fix TPCDSQueryBenchmark Measuring Performance of Wrong Query Results
          val options = Map("path" -> s"$dataLocation/$tableName")
          cometSpark.catalog.createTable(tableName, "parquet", tableColumns(tableName), options)
        }
        // Recover partitions but don't fail if a table is not partitioned.
        Try {
          cometSpark.sql(s"ALTER TABLE $tableName RECOVER PARTITIONS")
        }.getOrElse {
          logInfo(s"Recovering partitions of table $tableName failed")
        }
      }
      tableName -> cometSpark.table(tableName).count()
    }.toMap
  }

  protected def setupCBO(spark: SparkSession, cboEnabled: Boolean, tables: Seq[String]): Unit = {
    if (cboEnabled) {
      spark.sql(s"SET ${SQLConf.CBO_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.PLAN_STATS_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.JOIN_REORDER_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.HISTOGRAM_ENABLED.key}=true")

      // Analyze all the tables before running queries
      val startTime = System.nanoTime()
      tables.foreach { tableName =>
        spark.sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR ALL COLUMNS")
      }
      logInfo(
        "The elapsed time to analyze all the tables is " +
          s"${(System.nanoTime() - startTime) / NANOS_PER_SECOND.toDouble} seconds")
    } else {
      spark.sql(s"SET ${SQLConf.CBO_ENABLED.key}=false")
    }
  }

  protected def filterQueries(
      origQueries: Seq[String],
      queryFilter: Set[String],
      nameSuffix: String = ""): Seq[String] = {
    if (queryFilter.nonEmpty) {
      if (nameSuffix.nonEmpty) {
        origQueries.filter(name => queryFilter.contains(s"$name$nameSuffix"))
      } else {
        origQueries.filter(queryFilter.contains)
      }
    } else {
      origQueries
    }
  }
}
