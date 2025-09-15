package org.apache.comet

class CometFuzzAggregateSuite extends CometFuzzTestBase {

  test("count distinct") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.columns) {
      val sql = s"SELECT count(distinct $col) FROM t1"
      val (_, cometPlan) = checkSparkAnswer(sql)
      if (usingDataSourceExec) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }

  test("count(*) group by single column") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.columns) {
      // cannot run fully natively due to range partitioning and sort
      val sql = s"SELECT $col, count(*) FROM t1 GROUP BY $col ORDER BY $col"
      val (_, cometPlan) = checkSparkAnswer(sql)
      if (usingDataSourceExec) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }

  test("count(col) group by single column") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    val groupCol = df.columns.head
    for (col <- df.columns.drop(1)) {
      // cannot run fully natively due to range partitioning and sort
      val sql = s"SELECT $groupCol, count($col) FROM t1 GROUP BY $groupCol ORDER BY $groupCol"
      val (_, cometPlan) = checkSparkAnswer(sql)
      if (usingDataSourceExec) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }

  test("count(col1, col2, ..) group by single column") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    val groupCol = df.columns.head
    val otherCol = df.columns.drop(1)
    // cannot run fully natively due to range partitioning and sort
    val sql = s"SELECT $groupCol, count(${otherCol.mkString(", ")}) FROM t1 " +
      s"GROUP BY $groupCol ORDER BY $groupCol"
    val (_, cometPlan) = checkSparkAnswer(sql)
    if (usingDataSourceExec) {
      assert(1 == collectNativeScans(cometPlan).length)
    }
  }

  test("min/max aggregate") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.columns) {
      // cannot run fully native due to HashAggregate
      val sql = s"SELECT min($col), max($col) FROM t1"
      val (_, cometPlan) = checkSparkAnswer(sql)
      if (usingDataSourceExec) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }

}
