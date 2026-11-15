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

// Reads already generated, flat TPC-H tables and rewrites them into a nested layout.
//
// For each table, join keys (columns whose name ends in "key") and the partition columns
// (p_brand, c_mktsegment, o_orderdate, l_shipdate) stay at the top level.  Every other column
// is folded into a single-element array<struct> named `<table>_data`:
//
//   lineitem(l_orderkey, l_partkey, l_suppkey, l_shipdate,
//            lineitem_data: array<struct<l_linenumber, l_quantity, ..., l_comment>>)
//
// The array always holds exactly one element, so exploding it never changes the row count
// and the nested queries return the same answers as the flat TPC-H queries.
//
// Source columns are cast to the types below, so the output schema is the same whichever
// generator produced the flat data.
//
// Source and target can each be Parquet or Iceberg:
//
//   parquet:<path>          one table per <path>/<table> directory (or <path>/<table>.parquet)
//   iceberg:<catalog>.<db>  tables <catalog>.<db>.<table>, catalogs configured the usual way
//                           with spark.sql.catalog.<name>.* settings
//
// Usage, from benchmarks/nested:
//
//   spark-shell -i scripts/nest_tpch.scala \
//     --conf spark.nesttpch.source=parquet:/data/tpch/sf100 \
//     --conf spark.nesttpch.target=parquet:/data/tpch-nested/sf100
//
// Configuration:
//   spark.nesttpch.source       Required.  Flat tables, parquet:<path> or iceberg:<catalog>.<db>
//   spark.nesttpch.target       Required.  Where to write the nested tables, same forms
//   spark.nesttpch.tables       Comma separated subset of tables (default: all, smallest first)
//   spark.nesttpch.partitioned  Partition part, customer, orders and lineitem by p_brand,
//                               c_mktsegment, o_orderdate and l_shipdate (default true)
//   spark.nesttpch.overwrite    Replace existing target tables (default true)
//   spark.nesttpch.verify       Compare source and target row counts (default true)
//   spark.nesttpch.dryRun       Print the target DDL and exit without reading or writing data
//                               (default false)

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, col, struct}

object NestTpch {

  final case class Column(name: String, sqlType: String)

  final case class Table(
      name: String,
      topLevel: List[Column],
      nested: List[Column],
      partitionColumn: Option[String] = None) {
    def structName: String = name + "_data"
    def columns: List[Column] = topLevel ++ nested
  }

  private def c(name: String, sqlType: String): Column = Column(name, sqlType)

  private val Money = "DECIMAL(12,2)"

  // Smallest first, so a bad configuration fails in seconds rather than hours.
  val Tables: List[Table] = List(
    Table("region",
      List(c("r_regionkey", "BIGINT")),
      List(c("r_name", "STRING"), c("r_comment", "STRING"))),
    Table("nation",
      List(c("n_nationkey", "BIGINT"), c("n_regionkey", "BIGINT")),
      List(c("n_name", "STRING"), c("n_comment", "STRING"))),
    Table("supplier",
      List(c("s_suppkey", "BIGINT"), c("s_nationkey", "BIGINT")),
      List(c("s_name", "STRING"), c("s_address", "STRING"), c("s_phone", "STRING"),
        c("s_acctbal", Money), c("s_comment", "STRING"))),
    Table("part",
      List(c("p_partkey", "BIGINT"), c("p_brand", "STRING")),
      List(c("p_name", "STRING"), c("p_mfgr", "STRING"), c("p_type", "STRING"),
        c("p_size", "BIGINT"), c("p_container", "STRING"), c("p_retailprice", Money),
        c("p_comment", "STRING")),
      Some("p_brand")),
    Table("partsupp",
      List(c("ps_partkey", "BIGINT"), c("ps_suppkey", "BIGINT")),
      List(c("ps_availqty", "INT"), c("ps_supplycost", Money), c("ps_comment", "STRING"))),
    Table("customer",
      List(c("c_custkey", "BIGINT"), c("c_nationkey", "BIGINT"), c("c_mktsegment", "STRING")),
      List(c("c_name", "STRING"), c("c_address", "STRING"), c("c_phone", "STRING"),
        c("c_acctbal", Money), c("c_comment", "STRING")),
      Some("c_mktsegment")),
    Table("orders",
      List(c("o_orderkey", "BIGINT"), c("o_custkey", "BIGINT"), c("o_orderdate", "DATE")),
      List(c("o_orderstatus", "STRING"), c("o_totalprice", Money), c("o_orderpriority", "STRING"),
        c("o_clerk", "STRING"), c("o_shippriority", "INT"), c("o_comment", "STRING")),
      Some("o_orderdate")),
    Table("lineitem",
      List(c("l_orderkey", "BIGINT"), c("l_partkey", "BIGINT"), c("l_suppkey", "BIGINT"),
        c("l_shipdate", "DATE")),
      List(c("l_linenumber", "INT"), c("l_quantity", Money), c("l_extendedprice", Money),
        c("l_discount", Money), c("l_tax", Money), c("l_returnflag", "STRING"),
        c("l_linestatus", "STRING"), c("l_commitdate", "DATE"), c("l_receiptdate", "DATE"),
        c("l_shipinstruct", "STRING"), c("l_shipmode", "STRING"), c("l_comment", "STRING")),
      Some("l_shipdate")))

  // Applied to both Parquet and Iceberg targets.
  val RowGroupBytes: Long = 512L * 1024 * 1024
  val Compression = "zstd"

  // ---------------------------------------------------------------- locations

  sealed trait Location {
    def read(spark: SparkSession, table: String): DataFrame
  }

  final case class ParquetLocation(path: String) extends Location {
    def tablePath(table: String): String = path.stripSuffix("/") + "/" + table

    def read(spark: SparkSession, table: String): DataFrame = {
      // Directory per table is the common layout.  Some generators write <table>.parquet.
      val dir = new Path(tablePath(table))
      val fs = dir.getFileSystem(spark.sparkContext.hadoopConfiguration)
      val input = if (fs.exists(dir)) dir.toString else dir.toString + ".parquet"
      spark.read.parquet(input)
    }

    override def toString: String = s"parquet:$path"
  }

  final case class IcebergLocation(namespace: String) extends Location {
    def tableName(table: String): String = s"$namespace.$table"
    def read(spark: SparkSession, table: String): DataFrame = spark.table(tableName(table))
    override def toString: String = s"iceberg:$namespace"
  }

  def parseLocation(spec: String): Location = spec.split(":", 2) match {
    case Array("parquet", path) if path.nonEmpty => ParquetLocation(path)
    case Array("iceberg", ns) if ns.contains(".") => IcebergLocation(ns)
    case _ =>
      throw new IllegalArgumentException(
        s"Bad location '$spec'.  Expected parquet:<path> or iceberg:<catalog>.<database>")
  }

  // ---------------------------------------------------------------- options

  final case class Options(
      source: Location,
      target: Location,
      tables: List[Table],
      partitioned: Boolean,
      overwrite: Boolean,
      verify: Boolean,
      dryRun: Boolean)

  def parseOptions(spark: SparkSession): Options = {
    def get(key: String): Option[String] =
      spark.conf.getOption(s"spark.nesttpch.$key").map(_.trim).filter(_.nonEmpty)
    def required(key: String): String =
      get(key).getOrElse(throw new IllegalArgumentException(s"spark.nesttpch.$key is required"))
    def flag(key: String, default: Boolean): Boolean = get(key).map(_.toBoolean).getOrElse(default)

    val names = Tables.map(_.name)
    val wanted = get("tables").toList.flatMap(_.split(',')).map(_.trim.toLowerCase).filter(_.nonEmpty)
    wanted.foreach { t =>
      require(names.contains(t), s"Unknown table '$t'.  TPC-H tables: ${names.mkString(", ")}")
    }
    Options(
      source = parseLocation(required("source")),
      target = parseLocation(required("target")),
      tables = if (wanted.isEmpty) Tables else Tables.filter(t => wanted.contains(t.name)),
      partitioned = flag("partitioned", default = true),
      overwrite = flag("overwrite", default = true),
      verify = flag("verify", default = true),
      dryRun = flag("dryRun", default = false))
  }

  // ---------------------------------------------------------------- converting

  /** Iceberg DDL for the nested table.  Also printed for Parquet targets, as documentation. */
  def createTable(t: Table, name: String, partitioned: Boolean): String = {
    val width = (t.structName :: t.topLevel.map(_.name)).map(_.length).max
    def pad(s: String): String = s.padTo(width, ' ')
    val top = t.topLevel.map(col => s"    ${pad(col.name)}  ${col.sqlType},")
    val inner = t.nested.map(col => s"        ${col.name}: ${col.sqlType}").mkString(",\n")
    val partitionBy = t.partitionColumn.filter(_ => partitioned)
    val props = List(
      Some("write.parquet.row-group-size-bytes" -> RowGroupBytes.toString),
      Some("write.parquet.compression-codec" -> Compression),
      // Cluster rows by partition before writing, so each partition gets few, large files.
      partitionBy.map(_ => "write.distribution-mode" -> "hash")).flatten
    (List(s"CREATE TABLE $name (") ++ top ++
      List(s"    ${pad(t.structName)}  ARRAY<STRUCT<", inner + ">>", ")", "USING iceberg") ++
      partitionBy.map(p => s"PARTITIONED BY ($p)").toList ++
      List("TBLPROPERTIES (", props.map { case (k, v) => s"  '$k'='$v'" }.mkString(",\n"), ")"))
      .mkString("\n")
  }

  /** Flat source table to nested rows, with every column cast to the target type. */
  def nest(t: Table, df: DataFrame): DataFrame = {
    val available = df.columns.map(_.toLowerCase).toSet
    val missing = t.columns.map(_.name).filterNot(available.contains)
    require(missing.isEmpty, s"[${t.name}] source has no column ${missing.mkString(", ")}")

    def typed(c: Column) = col(c.name).cast(c.sqlType).as(c.name)
    // Single-element array, so exploding it never changes the row count.  The point is to put
    // the columns behind an array<struct> access path, not to fan rows out.
    val projection = t.topLevel.map(typed) :+ array(struct(t.nested.map(typed): _*)).as(t.structName)
    df.select(projection: _*)
  }

  def convert(spark: SparkSession, t: Table, opts: Options): Unit = {
    val partitionBy = t.partitionColumn.filter(_ => opts.partitioned).toList
    val ddlName = opts.target match {
      case i: IcebergLocation => i.tableName(t.name)
      case p: ParquetLocation => s"parquet.`${p.tablePath(t.name)}`"
    }
    println(s"[${t.name}] ${opts.source} -> ${opts.target}")
    println(createTable(t, ddlName, opts.partitioned))
    if (opts.dryRun) return

    val source = opts.source.read(spark, t.name)
    val nested = nest(t, source)

    opts.target match {
      case p: ParquetLocation =>
        // One shuffle so that each partition directory gets few files rather than one per task.
        val clustered = if (partitionBy.isEmpty) nested else nested.repartition(partitionBy.map(col): _*)
        clustered.write
          .mode(if (opts.overwrite) "overwrite" else "errorifexists")
          .option("compression", Compression)
          .option("parquet.block.size", RowGroupBytes.toString)
          .partitionBy(partitionBy: _*)
          .parquet(p.tablePath(t.name))

      case i: IcebergLocation =>
        val name = i.tableName(t.name)
        spark.sql(s"CREATE NAMESPACE IF NOT EXISTS ${i.namespace}")
        if (opts.overwrite) spark.sql(s"DROP TABLE IF EXISTS $name")
        spark.sql(createTable(t, name, opts.partitioned))
        nested.writeTo(name).append()
    }

    if (opts.verify) {
      val sourceRows = source.count()
      val targetRows = opts.target.read(spark, t.name).count()
      require(sourceRows == targetRows,
        s"[${t.name}] row count mismatch: source=$sourceRows target=$targetRows")
      println(s"[${t.name}] done, $targetRows rows")
    } else {
      println(s"[${t.name}] done")
    }
  }

  def main(spark: SparkSession): Unit = {
    val opts = parseOptions(spark)
    println(s"Source: ${opts.source}")
    println(s"Target: ${opts.target}")
    println(s"Tables: ${opts.tables.map(_.name).mkString(", ")}")
    opts.tables.foreach(convert(spark, _, opts))
    if (opts.dryRun) println("Dry run: nothing was read or written")
    else println(s"Converted ${opts.tables.size} table(s) into ${opts.target}")
  }
}

try { NestTpch.main(spark); sys.exit(0) }
catch { case e: Throwable => e.printStackTrace(); sys.exit(1) }
