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

/**
 * Mostly copied from
 * https://github.com/databricks/spark-sql-perf/blob/master/src/main/scala/com/databricks/spark/sql/perf/Tables.scala
 *
 * TODO: port this back to the upstream Spark similar to TPCDS benchmark
 */

package org.apache.spark.sql

import scala.util.Try

import org.slf4j.LoggerFactory

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait DataGenerator extends Serializable {
  def generate(
      sparkContext: SparkContext,
      name: String,
      partitions: Int,
      scaleFactor: String): RDD[String]
}

abstract class Tables(
    sqlContext: SQLContext,
    scaleFactor: String,
    useDoubleForDecimal: Boolean = false,
    useStringForDate: Boolean = false)
    extends Serializable {

  def dataGenerator: DataGenerator
  def tables: Seq[Table]

  private val log = LoggerFactory.getLogger(getClass)

  def sparkContext = sqlContext.sparkContext

  case class Table(name: String, partitionColumns: Seq[String], fields: StructField*) {
    val schema: StructType = StructType(fields)

    def nonPartitioned: Table = {
      Table(name, Nil, fields: _*)
    }

    /**
     * If convertToSchema is true, the data from generator will be parsed into columns and
     * converted to `schema`. Otherwise, it just outputs the raw data (as a single STRING column).
     */
    def df(convertToSchema: Boolean, numPartition: Int): DataFrame = {
      val generatedData = dataGenerator.generate(sparkContext, name, numPartition, scaleFactor)
      val rows = generatedData.mapPartitions { iter =>
        iter.map { l =>
          if (convertToSchema) {
            val values = l.split("\\|", -1).dropRight(1).map { v =>
              if (v.equals("")) {
                // If the string value is an empty string, we turn it to a null
                null
              } else {
                v
              }
            }
            Row.fromSeq(values)
          } else {
            Row.fromSeq(Seq(l))
          }
        }
      }

      if (convertToSchema) {
        val stringData =
          sqlContext.createDataFrame(
            rows,
            StructType(schema.fields.map(f => StructField(f.name, StringType))))

        val convertedData = {
          val columns = schema.fields.map { f =>
            col(f.name).cast(f.dataType).as(f.name)
          }
          stringData.select(columns: _*)
        }

        convertedData
      } else {
        sqlContext.createDataFrame(rows, StructType(Seq(StructField("value", StringType))))
      }
    }

    def convertTypes(): Table = {
      val newFields = fields.map { field =>
        val newDataType = field.dataType match {
          case _: DecimalType if useDoubleForDecimal => DoubleType
          case _: DateType if useStringForDate => StringType
          case other => other
        }
        field.copy(dataType = newDataType)
      }

      Table(name, partitionColumns, newFields: _*)
    }

    def genData(
        location: String,
        format: String,
        overwrite: Boolean,
        clusterByPartitionColumns: Boolean,
        filterOutNullPartitionValues: Boolean,
        numPartitions: Int): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore

      val data = df(format != "text", numPartitions)
      val tempTableName = s"${name}_text"
      data.createOrReplaceTempView(tempTableName)

      val writer = if (partitionColumns.nonEmpty) {
        if (clusterByPartitionColumns) {
          val columnString = data.schema.fields
            .map { field =>
              field.name
            }
            .mkString(",")
          val partitionColumnString = partitionColumns.mkString(",")
          val predicates = if (filterOutNullPartitionValues) {
            partitionColumns.map(col => s"$col IS NOT NULL").mkString("WHERE ", " AND ", "")
          } else {
            ""
          }

          val query =
            s"""
               |SELECT
               |  $columnString
               |FROM
               |  $tempTableName
               |$predicates
               |DISTRIBUTE BY
               |  $partitionColumnString
            """.stripMargin
          val grouped = sqlContext.sql(query)
          println(s"Pre-clustering with partitioning columns with query $query.")
          log.info(s"Pre-clustering with partitioning columns with query $query.")
          grouped.write
        } else {
          data.write
        }
      } else {
        // treat non-partitioned tables as "one partition" that we want to coalesce
        if (clusterByPartitionColumns) {
          // in case data has more than maxRecordsPerFile, split into multiple writers to improve
          // datagen speed files will be truncated to maxRecordsPerFile value, so the final result
          // will be the same
          val numRows = data.count
          val maxRecordPerFile =
            Try(sqlContext.getConf("spark.sql.files.maxRecordsPerFile").toInt).getOrElse(0)

          println(
            s"Data has $numRows rows clustered $clusterByPartitionColumns for $maxRecordPerFile")
          log.info(
            s"Data has $numRows rows clustered $clusterByPartitionColumns for $maxRecordPerFile")

          if (maxRecordPerFile > 0 && numRows > maxRecordPerFile) {
            val numFiles = (numRows.toDouble / maxRecordPerFile).ceil.toInt
            println(s"Coalescing into $numFiles files")
            log.info(s"Coalescing into $numFiles files")
            data.coalesce(numFiles).write
          } else {
            data.coalesce(1).write
          }
        } else {
          data.write
        }
      }
      writer.format(format).mode(mode)
      if (partitionColumns.nonEmpty) {
        writer.partitionBy(partitionColumns: _*)
      }
      println(s"Generating table $name in database to $location with save mode $mode.")
      log.info(s"Generating table $name in database to $location with save mode $mode.")
      writer.save(location)
      sqlContext.dropTempTable(tempTableName)
    }
  }

  def genData(
      location: String,
      format: String,
      overwrite: Boolean,
      partitionTables: Boolean,
      clusterByPartitionColumns: Boolean,
      filterOutNullPartitionValues: Boolean,
      tableFilter: String = "",
      numPartitions: Int = 100): Unit = {
    var tablesToBeGenerated = if (partitionTables) {
      tables
    } else {
      tables.map(_.nonPartitioned)
    }

    if (tableFilter.nonEmpty) {
      tablesToBeGenerated = tablesToBeGenerated.filter(_.name == tableFilter)
      if (tablesToBeGenerated.isEmpty) {
        throw new RuntimeException("Bad table name filter: " + tableFilter)
      }
    }

    tablesToBeGenerated.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.genData(
        tableLocation,
        format,
        overwrite,
        clusterByPartitionColumns,
        filterOutNullPartitionValues,
        numPartitions)
    }
  }
}
