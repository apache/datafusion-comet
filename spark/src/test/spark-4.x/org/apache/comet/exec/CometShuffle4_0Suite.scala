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

package org.apache.comet.exec

import java.util.Collections

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryCatalog, InMemoryTableCatalog}
import org.apache.spark.sql.connector.expressions.Expressions.identity
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{FloatType, LongType, StringType, TimestampType}

class CometShuffle4_0Suite extends CometColumnarShuffleSuite {
  override protected val asyncShuffleEnable: Boolean = false

  protected val adaptiveExecutionEnabled: Boolean = true

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryCatalog].getName)
  }

  override def afterAll(): Unit = {
    spark.sessionState.conf.unsetConf("spark.sql.catalog.testcat")
    super.afterAll()
  }

  private val emptyProps: java.util.Map[String, String] = {
    Collections.emptyMap[String, String]
  }
  private val items: String = "items"
  private val itemsColumns: Array[Column] = Array(
    Column.create("id", LongType),
    Column.create("name", StringType),
    Column.create("price", FloatType),
    Column.create("arrive_time", TimestampType))

  private val purchases: String = "purchases"
  private val purchasesColumns: Array[Column] = Array(
    Column.create("item_id", LongType),
    Column.create("price", FloatType),
    Column.create("time", TimestampType))

  protected def catalog: InMemoryCatalog = {
    val catalog = spark.sessionState.catalogManager.catalog("testcat")
    catalog.asInstanceOf[InMemoryCatalog]
  }

  private def createTable(
      table: String,
      columns: Array[Column],
      partitions: Array[Transform],
      catalog: InMemoryTableCatalog = catalog): Unit = {
    catalog.createTable(Identifier.of(Array("ns"), table), columns, partitions, emptyProps)
  }

  private def selectWithMergeJoinHint(t1: String, t2: String): String = {
    s"SELECT /*+ MERGE($t1, $t2) */ "
  }

  private def createJoinTestDF(
      keys: Seq[(String, String)],
      extraColumns: Seq[String] = Nil,
      joinType: String = ""): DataFrame = {
    val extraColList = if (extraColumns.isEmpty) "" else extraColumns.mkString(", ", ", ", "")
    sql(s"""
           |${selectWithMergeJoinHint("i", "p")}
           |id, name, i.price as purchase_price, p.price as sale_price $extraColList
           |FROM testcat.ns.$items i $joinType JOIN testcat.ns.$purchases p
           |ON ${keys.map(k => s"i.${k._1} = p.${k._2}").mkString(" AND ")}
           |ORDER BY id, purchase_price, sale_price $extraColList
           |""".stripMargin)
  }

  test("Fallback to Spark for unsupported partitioning") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(3, 19.5, cast('2020-02-01' as timestamp)), " +
        "(5, 26.0, cast('2023-01-01' as timestamp)), " +
        "(6, 50.0, cast('2023-02-01' as timestamp))")

    Seq(true, false).foreach { shuffle =>
      withSQLConf(
        SQLConf.V2_BUCKETING_ENABLED.key -> "true",
        "spark.sql.sources.v2.bucketing.shuffle.enabled" -> shuffle.toString,
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
        val df = createJoinTestDF(Seq("id" -> "item_id"))
        checkSparkAnswer(df)
      }
    }
  }
}
