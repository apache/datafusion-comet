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

import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometNativeCompaction

/** Integration tests for native Iceberg compaction using CometNativeCompaction. */
class CometIcebergCompactionSuite extends CometTestBase {

  private def icebergAvailable: Boolean = {
    try {
      Class.forName("org.apache.iceberg.catalog.Catalog")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  private def withTempIcebergDir(f: File => Unit): Unit = {
    val dir = Files.createTempDirectory("comet-compaction-test").toFile
    try {
      f(dir)
    } finally {
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) file.listFiles().foreach(deleteRecursively)
        file.delete()
      }
      deleteRecursively(dir)
    }
  }

  private def icebergCatalogConf(warehouseDir: File): Map[String, String] = Map(
    "spark.sql.catalog.compact_cat" -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.compact_cat.type" -> "hadoop",
    "spark.sql.catalog.compact_cat.warehouse" -> warehouseDir.getAbsolutePath,
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_ICEBERG_COMPACTION_ENABLED.key -> "true")

  private def loadIcebergTable(tableName: String): org.apache.iceberg.Table = {
    Spark3Util.loadIcebergTable(spark, tableName)
  }

  // ============== Basic Native Compaction Tests ==============

  test("native compaction compacts fragmented files") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.frag_table (
            id BIGINT,
            name STRING,
            value DOUBLE
          ) USING iceberg
        """)

        for (i <- 0 until 10) {
          spark.sql(s"INSERT INTO compact_cat.db.frag_table VALUES ($i, 'name_$i', ${i * 1.5})")
        }

        val filesBefore =
          spark.sql("SELECT file_path FROM compact_cat.db.frag_table.files").count()
        assert(filesBefore >= 5, s"Expected multiple files, got $filesBefore")

        val rowsBefore =
          spark.sql("SELECT count(*) FROM compact_cat.db.frag_table").collect()(0).getLong(0)

        val icebergTable = loadIcebergTable("compact_cat.db.frag_table")
        val nativeCompaction = CometNativeCompaction(spark)
        val summary = nativeCompaction.rewriteDataFiles(icebergTable)

        assert(summary.filesDeleted > 0, "Should delete files")
        assert(summary.filesAdded > 0, "Should add files")

        spark.sql("REFRESH TABLE compact_cat.db.frag_table")
        val rowsAfter =
          spark.sql("SELECT count(*) FROM compact_cat.db.frag_table").collect()(0).getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        val filesAfter =
          spark.sql("SELECT file_path FROM compact_cat.db.frag_table.files").count()
        assert(filesAfter < filesBefore, s"Expected fewer files: $filesBefore -> $filesAfter")

        spark.sql("DROP TABLE compact_cat.db.frag_table")
      }
    }
  }

  test("native compaction preserves data correctness") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.correct_table (
            id BIGINT,
            name STRING
          ) USING iceberg
        """)

        for (i <- 1 to 20) {
          spark.sql(s"INSERT INTO compact_cat.db.correct_table VALUES ($i, 'row_$i')")
        }

        val dataBefore = spark
          .sql("SELECT id, name FROM compact_cat.db.correct_table ORDER BY id")
          .collect()
          .map(r => (r.getLong(0), r.getString(1)))

        val icebergTable = loadIcebergTable("compact_cat.db.correct_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.correct_table")
        val dataAfter = spark
          .sql("SELECT id, name FROM compact_cat.db.correct_table ORDER BY id")
          .collect()
          .map(r => (r.getLong(0), r.getString(1)))

        assert(dataBefore.toSeq == dataAfter.toSeq, "Data must be identical after compaction")

        spark.sql("DROP TABLE compact_cat.db.correct_table")
      }
    }
  }

  // ============== Partitioned Table Tests ==============

  // TODO: Native compaction doesn't yet support partitioned tables correctly.
  // Partition column values stored in partition paths (not data files) are not preserved.
  ignore("native compaction on partitioned table preserves partition boundaries") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.partitioned_table (
            id BIGINT,
            category STRING,
            value DOUBLE
          ) USING iceberg
          PARTITIONED BY (category)
        """)

        for (i <- 1 to 15) {
          val cat = if (i % 3 == 0) "A" else if (i % 3 == 1) "B" else "C"
          spark.sql(
            s"INSERT INTO compact_cat.db.partitioned_table VALUES ($i, '$cat', ${i * 1.5})")
        }

        val dataBefore = spark
          .sql("SELECT id, category, value FROM compact_cat.db.partitioned_table ORDER BY id")
          .collect()

        val icebergTable = loadIcebergTable("compact_cat.db.partitioned_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.partitioned_table")
        val dataAfter = spark
          .sql("SELECT id, category, value FROM compact_cat.db.partitioned_table ORDER BY id")
          .collect()

        assert(dataBefore.toSeq == dataAfter.toSeq, "Data must be identical after compaction")

        val partitions = spark
          .sql("SELECT DISTINCT category FROM compact_cat.db.partitioned_table")
          .collect()
          .map(_.getString(0))
        assert(partitions.toSet == Set("A", "B", "C"), "Partitions should be preserved")

        spark.sql("DROP TABLE compact_cat.db.partitioned_table")
      }
    }
  }

  test("native compaction on date-partitioned table") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.date_part_table (
            id BIGINT,
            event_date DATE,
            data STRING
          ) USING iceberg
          PARTITIONED BY (days(event_date))
        """)

        for (day <- 1 to 5; i <- 1 to 3) {
          spark.sql(s"""
            INSERT INTO compact_cat.db.date_part_table
            VALUES (${(day - 1) * 3 + i}, DATE '2024-01-0$day', 'data_$i')
          """)
        }

        val rowsBefore =
          spark.sql("SELECT count(*) FROM compact_cat.db.date_part_table").collect()(0).getLong(0)

        val icebergTable = loadIcebergTable("compact_cat.db.date_part_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.date_part_table")
        val rowsAfter =
          spark.sql("SELECT count(*) FROM compact_cat.db.date_part_table").collect()(0).getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        spark.sql("DROP TABLE compact_cat.db.date_part_table")
      }
    }
  }

  // ============== Copy-on-Write vs Merge-on-Read Tests ==============

  test("native compaction on Copy-on-Write table") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.cow_table (
            id BIGINT,
            value STRING
          ) USING iceberg
          TBLPROPERTIES (
            'write.delete.mode' = 'copy-on-write',
            'write.update.mode' = 'copy-on-write'
          )
        """)

        for (i <- 1 to 10) {
          spark.sql(s"INSERT INTO compact_cat.db.cow_table VALUES ($i, 'v$i')")
        }

        spark.sql("UPDATE compact_cat.db.cow_table SET value = 'updated' WHERE id <= 3")

        val rowsBefore =
          spark.sql("SELECT count(*) FROM compact_cat.db.cow_table").collect()(0).getLong(0)

        val icebergTable = loadIcebergTable("compact_cat.db.cow_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.cow_table")
        val rowsAfter =
          spark.sql("SELECT count(*) FROM compact_cat.db.cow_table").collect()(0).getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        spark.sql("DROP TABLE compact_cat.db.cow_table")
      }
    }
  }

  test("native compaction on Merge-on-Read table with delete files") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.mor_table (
            id BIGINT,
            value STRING
          ) USING iceberg
          TBLPROPERTIES (
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read'
          )
        """)

        for (i <- 1 to 10) {
          spark.sql(s"INSERT INTO compact_cat.db.mor_table VALUES ($i, 'v$i')")
        }

        spark.sql("DELETE FROM compact_cat.db.mor_table WHERE id IN (2, 4, 6)")

        val dataBefore = spark
          .sql("SELECT id, value FROM compact_cat.db.mor_table ORDER BY id")
          .collect()
          .map(r => (r.getLong(0), r.getString(1)))

        val icebergTable = loadIcebergTable("compact_cat.db.mor_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.mor_table")
        val dataAfter = spark
          .sql("SELECT id, value FROM compact_cat.db.mor_table ORDER BY id")
          .collect()
          .map(r => (r.getLong(0), r.getString(1)))

        assert(dataBefore.toSeq == dataAfter.toSeq, "Data must be identical after compaction")
        assert(!dataAfter.map(_._1).contains(2L), "Deleted rows should remain deleted")

        spark.sql("DROP TABLE compact_cat.db.mor_table")
      }
    }
  }

  // ============== Schema Variation Tests ==============

  test("native compaction with complex schema (all common types)") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.complex_schema (
            id BIGINT,
            int_col INT,
            float_col FLOAT,
            double_col DOUBLE,
            decimal_col DECIMAL(10, 2),
            string_col STRING,
            bool_col BOOLEAN,
            date_col DATE,
            ts_col TIMESTAMP
          ) USING iceberg
        """)

        for (i <- 1 to 10) {
          spark.sql(s"""
            INSERT INTO compact_cat.db.complex_schema VALUES
            ($i, $i, ${i * 0.5}f, ${i * 1.5}, ${i * 10.25}, 'str_$i',
             ${i % 2 == 0}, DATE '2024-01-0${(i % 9) + 1}',
             TIMESTAMP '2024-01-01 0${i % 10}:00:00')
          """)
        }

        val dataBefore = spark
          .sql("SELECT * FROM compact_cat.db.complex_schema ORDER BY id")
          .collect()

        val icebergTable = loadIcebergTable("compact_cat.db.complex_schema")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.complex_schema")
        val dataAfter = spark
          .sql("SELECT * FROM compact_cat.db.complex_schema ORDER BY id")
          .collect()

        assert(
          dataBefore.length == dataAfter.length,
          s"Row count changed: ${dataBefore.length} -> ${dataAfter.length}")
        assert(
          dataBefore.map(_.toString()).toSeq == dataAfter.map(_.toString()).toSeq,
          "Data must be identical after compaction")

        spark.sql("DROP TABLE compact_cat.db.complex_schema")
      }
    }
  }

  test("native compaction with nullable columns") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.nullable_table (
            id BIGINT,
            nullable_str STRING,
            nullable_int INT
          ) USING iceberg
        """)

        for (i <- 1 to 10) {
          val strVal = if (i % 3 == 0) "NULL" else s"'value_$i'"
          val intVal = if (i % 2 == 0) "NULL" else s"$i"
          spark.sql(s"INSERT INTO compact_cat.db.nullable_table VALUES ($i, $strVal, $intVal)")
        }

        val dataBefore = spark
          .sql("SELECT * FROM compact_cat.db.nullable_table ORDER BY id")
          .collect()

        val icebergTable = loadIcebergTable("compact_cat.db.nullable_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.nullable_table")
        val dataAfter = spark
          .sql("SELECT * FROM compact_cat.db.nullable_table ORDER BY id")
          .collect()

        assert(
          dataBefore.map(_.toString()).toSeq == dataAfter.map(_.toString()).toSeq,
          "Data with nulls must be identical after compaction")

        spark.sql("DROP TABLE compact_cat.db.nullable_table")
      }
    }
  }

  // ============== Partition Transform Tests ==============

  test("native compaction on bucket-partitioned table") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.bucket_table (
            id BIGINT,
            category STRING,
            value DOUBLE
          ) USING iceberg
          PARTITIONED BY (bucket(4, id))
        """)

        for (i <- 1 to 20) {
          spark.sql(s"INSERT INTO compact_cat.db.bucket_table VALUES ($i, 'cat_$i', ${i * 1.5})")
        }

        val rowsBefore =
          spark.sql("SELECT count(*) FROM compact_cat.db.bucket_table").collect()(0).getLong(0)

        val icebergTable = loadIcebergTable("compact_cat.db.bucket_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.bucket_table")
        val rowsAfter =
          spark.sql("SELECT count(*) FROM compact_cat.db.bucket_table").collect()(0).getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        spark.sql("DROP TABLE compact_cat.db.bucket_table")
      }
    }
  }

  test("native compaction on truncate-partitioned table") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.truncate_table (
            id BIGINT,
            name STRING,
            value DOUBLE
          ) USING iceberg
          PARTITIONED BY (truncate(3, name))
        """)

        for (i <- 1 to 15) {
          spark.sql(
            s"INSERT INTO compact_cat.db.truncate_table VALUES ($i, 'name_$i', ${i * 1.5})")
        }

        val rowsBefore =
          spark.sql("SELECT count(*) FROM compact_cat.db.truncate_table").collect()(0).getLong(0)

        val icebergTable = loadIcebergTable("compact_cat.db.truncate_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.truncate_table")
        val rowsAfter =
          spark.sql("SELECT count(*) FROM compact_cat.db.truncate_table").collect()(0).getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        spark.sql("DROP TABLE compact_cat.db.truncate_table")
      }
    }
  }

  test("native compaction on month-partitioned table") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.month_part_table (
            id BIGINT,
            event_ts TIMESTAMP,
            data STRING
          ) USING iceberg
          PARTITIONED BY (month(event_ts))
        """)

        for (month <- 1 to 3; i <- 1 to 3) {
          val monthStr = f"$month%02d"
          spark.sql(s"""
            INSERT INTO compact_cat.db.month_part_table
            VALUES (${(month - 1) * 3 + i}, TIMESTAMP '2024-$monthStr-15 10:00:00', 'data_$i')
          """)
        }

        val rowsBefore =
          spark
            .sql("SELECT count(*) FROM compact_cat.db.month_part_table")
            .collect()(0)
            .getLong(0)

        val icebergTable = loadIcebergTable("compact_cat.db.month_part_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.month_part_table")
        val rowsAfter =
          spark
            .sql("SELECT count(*) FROM compact_cat.db.month_part_table")
            .collect()(0)
            .getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        spark.sql("DROP TABLE compact_cat.db.month_part_table")
      }
    }
  }

  test("native compaction on hour-partitioned table") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.hour_part_table (
            id BIGINT,
            event_ts TIMESTAMP,
            data STRING
          ) USING iceberg
          PARTITIONED BY (hour(event_ts))
        """)

        for (hour <- 1 to 4; i <- 1 to 2) {
          val hourStr = f"$hour%02d"
          spark.sql(s"""
            INSERT INTO compact_cat.db.hour_part_table
            VALUES (${(hour - 1) * 2 + i}, TIMESTAMP '2024-01-15 $hourStr:30:00', 'data_$i')
          """)
        }

        val rowsBefore =
          spark.sql("SELECT count(*) FROM compact_cat.db.hour_part_table").collect()(0).getLong(0)

        val icebergTable = loadIcebergTable("compact_cat.db.hour_part_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.hour_part_table")
        val rowsAfter =
          spark.sql("SELECT count(*) FROM compact_cat.db.hour_part_table").collect()(0).getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        spark.sql("DROP TABLE compact_cat.db.hour_part_table")
      }
    }
  }

  // ============== Multiple Partition Columns ==============

  test("native compaction on multi-column partitioned table") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.multi_part_table (
            id BIGINT,
            region STRING,
            event_date DATE,
            value DOUBLE
          ) USING iceberg
          PARTITIONED BY (days(event_date), bucket(2, region))
        """)

        val regions = Seq("US", "EU", "APAC")
        for (day <- 1 to 3; region <- regions) {
          spark.sql(s"""
            INSERT INTO compact_cat.db.multi_part_table
            VALUES (${day * 10 + regions.indexOf(region)}, '$region',
                    DATE '2024-01-0$day', ${day * 1.5})
          """)
        }

        val rowsBefore =
          spark
            .sql("SELECT count(*) FROM compact_cat.db.multi_part_table")
            .collect()(0)
            .getLong(0)

        val icebergTable = loadIcebergTable("compact_cat.db.multi_part_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.multi_part_table")
        val rowsAfter =
          spark
            .sql("SELECT count(*) FROM compact_cat.db.multi_part_table")
            .collect()(0)
            .getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        spark.sql("DROP TABLE compact_cat.db.multi_part_table")
      }
    }
  }

  // ============== Schema Evolution Tests ==============

  test("native compaction after schema evolution (add column)") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.schema_evo_table (
            id BIGINT,
            name STRING
          ) USING iceberg
        """)

        for (i <- 1 to 5) {
          spark.sql(s"INSERT INTO compact_cat.db.schema_evo_table VALUES ($i, 'name_$i')")
        }

        spark.sql("ALTER TABLE compact_cat.db.schema_evo_table ADD COLUMN value DOUBLE")

        for (i <- 6 to 10) {
          spark.sql(
            s"INSERT INTO compact_cat.db.schema_evo_table VALUES ($i, 'name_$i', ${i * 1.5})")
        }

        val rowsBefore =
          spark
            .sql("SELECT count(*) FROM compact_cat.db.schema_evo_table")
            .collect()(0)
            .getLong(0)

        val icebergTable = loadIcebergTable("compact_cat.db.schema_evo_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.schema_evo_table")
        val rowsAfter =
          spark
            .sql("SELECT count(*) FROM compact_cat.db.schema_evo_table")
            .collect()(0)
            .getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        val nullCount = spark
          .sql("SELECT count(*) FROM compact_cat.db.schema_evo_table WHERE value IS NULL")
          .collect()(0)
          .getLong(0)
        assert(nullCount == 5, s"Expected 5 nulls for old rows, got $nullCount")

        spark.sql("DROP TABLE compact_cat.db.schema_evo_table")
      }
    }
  }

  // ============== Nested Type Tests ==============

  test("native compaction with struct column") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.struct_table (
            id BIGINT,
            info STRUCT<name: STRING, age: INT>
          ) USING iceberg
        """)

        for (i <- 1 to 10) {
          spark.sql(
            s"INSERT INTO compact_cat.db.struct_table VALUES ($i, named_struct('name', 'n$i', 'age', $i))")
        }

        val dataBefore = spark
          .sql("SELECT id, info.name, info.age FROM compact_cat.db.struct_table ORDER BY id")
          .collect()

        val icebergTable = loadIcebergTable("compact_cat.db.struct_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.struct_table")
        val dataAfter = spark
          .sql("SELECT id, info.name, info.age FROM compact_cat.db.struct_table ORDER BY id")
          .collect()

        assert(
          dataBefore.map(_.toString()).toSeq == dataAfter.map(_.toString()).toSeq,
          "Struct data must be identical after compaction")

        spark.sql("DROP TABLE compact_cat.db.struct_table")
      }
    }
  }

  test("native compaction with array column") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.array_table (
            id BIGINT,
            tags ARRAY<STRING>
          ) USING iceberg
        """)

        for (i <- 1 to 10) {
          spark.sql(
            s"INSERT INTO compact_cat.db.array_table VALUES ($i, array('tag_${i}_a', 'tag_${i}_b'))")
        }

        val dataBefore = spark
          .sql("SELECT id, tags FROM compact_cat.db.array_table ORDER BY id")
          .collect()

        val icebergTable = loadIcebergTable("compact_cat.db.array_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.array_table")
        val dataAfter = spark
          .sql("SELECT id, tags FROM compact_cat.db.array_table ORDER BY id")
          .collect()

        assert(
          dataBefore.map(_.toString()).toSeq == dataAfter.map(_.toString()).toSeq,
          "Array data must be identical after compaction")

        spark.sql("DROP TABLE compact_cat.db.array_table")
      }
    }
  }

  test("native compaction with map column") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.map_table (
            id BIGINT,
            properties MAP<STRING, INT>
          ) USING iceberg
        """)

        for (i <- 1 to 10) {
          spark.sql(
            s"INSERT INTO compact_cat.db.map_table VALUES ($i, map('key_$i', $i, 'val_$i', ${i * 10}))")
        }

        val dataBefore = spark
          .sql("SELECT id, properties FROM compact_cat.db.map_table ORDER BY id")
          .collect()

        val icebergTable = loadIcebergTable("compact_cat.db.map_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.map_table")
        val dataAfter = spark
          .sql("SELECT id, properties FROM compact_cat.db.map_table ORDER BY id")
          .collect()

        assert(
          dataBefore.map(_.toString()).toSeq == dataAfter.map(_.toString()).toSeq,
          "Map data must be identical after compaction")

        spark.sql("DROP TABLE compact_cat.db.map_table")
      }
    }
  }

  // ============== Table Properties Tests ==============

  test("native compaction with custom table properties") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.props_table (
            id BIGINT,
            value STRING
          ) USING iceberg
          TBLPROPERTIES (
            'write.parquet.compression-codec' = 'zstd',
            'write.parquet.compression-level' = '3',
            'commit.retry.num-retries' = '5'
          )
        """)

        for (i <- 1 to 10) {
          spark.sql(s"INSERT INTO compact_cat.db.props_table VALUES ($i, 'v$i')")
        }

        val rowsBefore =
          spark.sql("SELECT count(*) FROM compact_cat.db.props_table").collect()(0).getLong(0)

        val icebergTable = loadIcebergTable("compact_cat.db.props_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.props_table")
        val rowsAfter =
          spark.sql("SELECT count(*) FROM compact_cat.db.props_table").collect()(0).getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        spark.sql("DROP TABLE compact_cat.db.props_table")
      }
    }
  }

  // ============== Large Decimal Tests ==============

  test("native compaction with large decimal precision") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.decimal_table (
            id BIGINT,
            small_dec DECIMAL(10, 2),
            large_dec DECIMAL(28, 10)
          ) USING iceberg
        """)

        for (i <- 1 to 10) {
          spark.sql(s"""
            INSERT INTO compact_cat.db.decimal_table VALUES
            ($i, ${i * 100.25}, ${i * 1000000.1234567890})
          """)
        }

        val dataBefore = spark
          .sql("SELECT * FROM compact_cat.db.decimal_table ORDER BY id")
          .collect()

        val icebergTable = loadIcebergTable("compact_cat.db.decimal_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.decimal_table")
        val dataAfter = spark
          .sql("SELECT * FROM compact_cat.db.decimal_table ORDER BY id")
          .collect()

        assert(
          dataBefore.map(_.toString()).toSeq == dataAfter.map(_.toString()).toSeq,
          "Decimal data must be identical after compaction")

        spark.sql("DROP TABLE compact_cat.db.decimal_table")
      }
    }
  }

  // ============== Binary/UUID Tests ==============

  test("native compaction with binary column") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        spark.sql("""
          CREATE TABLE compact_cat.db.binary_table (
            id BIGINT,
            data BINARY
          ) USING iceberg
        """)

        for (i <- 1 to 10) {
          spark.sql(
            s"INSERT INTO compact_cat.db.binary_table VALUES ($i, cast('binary_data_$i' as binary))")
        }

        val rowsBefore =
          spark.sql("SELECT count(*) FROM compact_cat.db.binary_table").collect()(0).getLong(0)

        val icebergTable = loadIcebergTable("compact_cat.db.binary_table")
        CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

        spark.sql("REFRESH TABLE compact_cat.db.binary_table")
        val rowsAfter =
          spark.sql("SELECT count(*) FROM compact_cat.db.binary_table").collect()(0).getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        spark.sql("DROP TABLE compact_cat.db.binary_table")
      }
    }
  }
}
